"""Regression: stopping a live server must be silent, even mid-request.

Adopters never see noise from ``ChannelsLiveServerTestCase`` teardown: no
``CancelledError`` traceback chain, no ``Future exception was never retrieved``
dump, no ``Task was destroyed but it is pending`` (the ``asyncio`` handler
``Task.__del__`` reaches for a task the loop was closed under).  That holds even
when the class is torn down while a real request is still running.

The sample project is shaped like a deployed one (see
``test_live_server_cached_chain.py``): the Django handler is built once, at
import time, and a sync-only middleware sits above the async view, so the
request crosses ``sync_to_async`` (the middleware) and ``async_to_sync`` (the
handler below it) on its way to the view -- the shape which produced the
traceback chains reported against this code.

Work that outlives the request must survive the *request's* cancellation just
long enough for the server to stop it on purpose: a view which shields its
write ("the client may go away, the write must not") leaves exactly such a task
running when the application instance is cancelled.  Closing the loop under it
is what makes asyncio complain; the shutdown has to cancel and consume it
first.

The class below fires a real request, waits until it is parked in the shielded
work, then returns from the test while the request is still in flight.  unittest
then runs the class cleanups: the server's own teardown first, the check last
(cleanups run in reverse order of registration), so the check sees everything
teardown wrote.
"""

import asyncio
import contextlib
import gc
import io
import logging
import threading
import unittest
import urllib.request

from django.http import HttpResponse
from django.test.utils import override_settings
from django.urls import path

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import _enter_class_context

#: How long the view parks in async code.  Longer than any shutdown budget the
#: live server may spend, so the request is certainly still in flight when the
#: class is torn down.
SLOW_VIEW_SECONDS = 60

#: Text that must never reach an adopter's console at teardown.
FORBIDDEN_OUTPUT = (
    "Traceback",
    "CancelledError",
    "Future exception was never retrieved",
    "Task was destroyed but it is pending",
    "Exception inside application",
)

#: Set once the request has entered the view, and once the shielded work below
#: it is running: the request is then in flight in the shape under test.
view_entered = threading.Event()
shielded_work_started = threading.Event()


async def _work_outliving_the_request():
    """The unit of work the view refuses to see cancelled with the request."""
    shielded_work_started.set()
    await asyncio.sleep(SLOW_VIEW_SECONDS)


async def slow_view(request):
    """Async view behind the sync-only middleware: the bridge chain."""
    view_entered.set()
    # Shielded so that dropping the client does not abandon the work: the task
    # keeps running after the request's cancellation, as a shutdown racing the
    # request would find it.
    await asyncio.shield(_work_outliving_the_request())
    return HttpResponse("slow")


urlpatterns = [path("slow/", slow_view)]


class _RecordingHandler(logging.Handler):
    """Keep the rendered text of every record passed to it."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record):
        try:
            self.messages.append(self.format(record))
        except Exception:  # pragma: no cover - formatting must not break tests
            self.messages.append(record.getMessage())


class _TeardownOutput:
    """Collect everything the server writes to the console while it stops.

    ``redirect_stdout``/``redirect_stderr`` catch ``print`` calls and any
    logging handler created during teardown.  A handler on the root logger and
    on ``asyncio`` catches records routed through logging's already-configured
    console handlers: the unretrieved-future and destroyed-task reports are
    ``logger.error`` records on the ``asyncio`` logger, whose console handler
    was bound to the real stderr before teardown began.
    """

    def __init__(self):
        self.stdout = io.StringIO()
        self.stderr = io.StringIO()
        self.logs = _RecordingHandler()
        self._stack: contextlib.ExitStack | None = None

    def __enter__(self) -> "_TeardownOutput":
        self.logs.setFormatter(logging.Formatter())
        logging.getLogger().addHandler(self.logs)
        logging.getLogger("asyncio").addHandler(self.logs)
        self._stack = contextlib.ExitStack()
        self._stack.__enter__()
        self._stack.enter_context(contextlib.redirect_stdout(self.stdout))
        self._stack.enter_context(contextlib.redirect_stderr(self.stderr))
        return self

    def __exit__(self, *exc_info) -> None:
        """Stop capturing; safe to call more than once."""
        stack, self._stack = self._stack, None
        try:
            if stack is not None:
                stack.__exit__(*exc_info)
        finally:
            logging.getLogger("asyncio").removeHandler(self.logs)
            logging.getLogger().removeHandler(self.logs)

    def text(self) -> str:
        """Return everything captured, in one string."""
        return "\n".join(
            [
                self.stdout.getvalue(),
                self.stderr.getvalue(),
                "\n".join(self.logs.messages),
            ]
        )


def enter_no_logs(*logger_names: str, level: int = logging.WARNING):
    """Enter ``assertNoLogs`` contexts which outlive the test method.

    The teardown whose silence they guard runs in a class cleanup, after the
    test method has returned, so ``self.assertNoLogs(...)`` cannot wrap it: the
    contexts are entered from ``setUpClass`` and exited by the cleanup. A
    throwaway ``TestCase`` supplies the assertion machinery (its own cleanups
    never run).

    Returns the contexts to pass to :func:`exit_no_logs`.
    """
    verifier = unittest.TestCase()
    contexts = [verifier.assertNoLogs(name, level=level) for name in logger_names]
    for context in contexts:
        context.__enter__()
    return contexts


def exit_no_logs(contexts) -> None:
    """Exit contexts from :func:`enter_no_logs`, failing if any record was logged."""
    for context in contexts:
        context.__exit__(None, None, None)


def finish_teardown_capture(capture, no_logs) -> None:
    """Stop the teardown assertions, then the output capture they supplement.

    The no-logs contexts restore the handlers they replaced, so they are exited
    while the capture is still installed; the capture then removes its own
    handlers for the last time.
    """
    try:
        exit_no_logs(no_logs)
    finally:
        if capture is not None:
            capture.__exit__(None, None, None)


class LiveServerGracefulShutdownTest(ChannelsLiveServerTestCase):
    """Tear the class down while a request is still in flight."""

    @classmethod
    def setUpClass(cls):
        # Registered before the server's own cleanups, so it runs after them:
        # unittest runs class cleanups in reverse order of registration.
        cls.addClassCleanup(cls._assert_teardown_was_silent)
        _enter_class_context(cls, override_settings(ROOT_URLCONF=__name__))
        super().setUpClass()
        # The loop the reactor runs on, checked closed once teardown is over.
        cls.server_loop = cls.server_thread.reactor._asyncioEventloop
        cls.capture = _TeardownOutput().__enter__()
        cls.no_logs = enter_no_logs("asyncio", "daphne.server")

    def test_request_in_flight_at_teardown(self):
        outcome: dict[str, object] = {}

        def fire():
            try:
                with urllib.request.urlopen(
                    f"{self.live_server_url}/slow/",
                    timeout=SLOW_VIEW_SECONDS + 10,
                ) as response:
                    outcome["status"] = response.status
            except OSError as exc:
                # Expected when the shutting-down server drops the connection.
                outcome["error"] = exc

        cls = type(self)
        cls.request_thread = threading.Thread(target=fire, daemon=True)
        cls.request_outcome = outcome
        cls.request_thread.start()
        self.assertTrue(
            view_entered.wait(10),
            "the request never reached the async half of the bridge chain",
        )
        self.assertTrue(
            shielded_work_started.wait(10),
            "the view's shielded work never started",
        )
        # Returning from the test is the point: the class teardown starts while
        # the request is still parked in the view.

    @classmethod
    def _assert_teardown_was_silent(cls):
        """Assert the stopped server wrote nothing, and left no task behind."""
        try:
            # Teardown is over: the server thread has been joined and its loop
            # closed, because class cleanups run in reverse order. Every value
            # is read defensively: a failure in setUpClass must surface as
            # itself, not as an AttributeError raised from this check.
            request_thread = getattr(cls, "request_thread", None)
            if request_thread is not None:
                request_thread.join(timeout=5)
            outcome = getattr(cls, "request_outcome", None) or {}
            loop = getattr(cls, "server_loop", None)
            leaked = [repr(task) for task in asyncio.all_tasks(loop)] if loop else []
            capture = getattr(cls, "capture", None)

            # Let the loop's abandoned work report itself now, inside the
            # capture window, instead of whenever the collector reaches it.
            gc.collect()
            output = capture.text() if capture is not None else ""

            problems = []
            if loop is None:
                problems.append("setUpClass never recorded the server's loop")
            else:
                if loop.is_running():
                    problems.append("the server's event loop is still running")
                if not loop.is_closed():
                    problems.append("the server's event loop was left open")
            if request_thread is not None and request_thread.is_alive():
                problems.append("the in-flight request never finished")
            if leaked:
                problems.append(
                    "pending tasks leaked past teardown: " + "; ".join(leaked)
                )
            markers = [marker for marker in FORBIDDEN_OUTPUT if marker in output]
            if markers:
                problems.append(
                    "teardown wrote to the console ("
                    + ", ".join(repr(marker) for marker in markers)
                    + "):\n"
                    + output
                )
            if outcome.get("status") is None and "error" not in outcome:
                problems.append(f"the request neither completed nor failed: {outcome}")
            if problems:
                raise AssertionError("; ".join(problems))
        finally:
            # Never leave the redirected streams in place, whatever happened.
            finish_teardown_capture(
                getattr(cls, "capture", None), getattr(cls, "no_logs", ())
            )
