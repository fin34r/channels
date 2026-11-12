"""Regression: a view which ignores cancellation must not hold teardown open.

Daphne's shutdown cancels every application instance and then *waits* for them
with a ``Deferred`` built from ``asyncio.gather``. A view which swallows
``CancelledError`` therefore keeps the reactor's loop thread inside that
gather: ``terminate()`` cannot join the server thread.

The shutdown must bound its grace period and, once it expires, retire every
still-running instance the same way it retires an abandoned request: cancel it,
consume its outcome, and hand daphne a finished future in its place, so
``kill_all_applications`` has nothing left to await. The expiry is reported at
WARNING, because the stranded worker is the adopter's problem to look at.

The application under test is a bare ASGI application rather than the sample
project's Django one. A live server runs in a thread, and when the work a
request is waiting on keeps refusing cancellation, asgiref's ``async_to_sync``
bridge parks the middleware's worker thread on the stopped event loop for good.
Executor workers are joined by ``concurrent.futures`` at interpreter exit, so a
test which did that through Django could never let the test process exit. The
request-through-Django shape, and the bridge in particular, are
``test_live_server_graceful_shutdown``'s and ``test_live_server_cached_chain``'s
subject; this module is about what a stopping server does with an application
instance which never finishes.
"""

import asyncio
import logging
import socket
import threading
import time

from django.test import TransactionTestCase

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import ChannelsLiveServerThread
from tests.sample_project.tests.test_live_server_graceful_shutdown import (
    FORBIDDEN_OUTPUT,
    _TeardownOutput,
    enter_no_logs,
    finish_teardown_capture,
)

#: How long the application sleeps before swallowing the next cancellation.
#: When the shutdown's cancel arrives, it swallows it and keeps "working" for
#: this long -- far past the shortened grace period -- so the shutdown must give
#: up on it.
STUBBORN_VIEW_SECONDS = 30

#: Set once the application is running: the request is then in flight.
view_entered = threading.Event()


async def stubborn_application(scope, receive, send):
    """ASGI application which swallows the shutdown's cancellation, then sleeps on."""
    view_entered.set()
    try:
        await asyncio.sleep(STUBBORN_VIEW_SECONDS)
    except asyncio.CancelledError:
        # "The write must not be abandoned": the application keeps going after
        # the cancellation daphne's shutdown delivers.
        await asyncio.sleep(STUBBORN_VIEW_SECONDS)
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"late"})


class _StubbornApplication:
    """Serve the application above, whatever project application it is handed.

    ``static_handler`` is called with the project's application; this test has
    no use for it (see the module docstring).
    """

    def __init__(self, application):
        # The project application is deliberately unused; see the module
        # docstring.
        pass

    async def __call__(self, scope, receive, send):
        return await stubborn_application(scope, receive, send)


class _QuickShutdownThread(ChannelsLiveServerThread):
    """A live server which gives up after a second of grace and of cancel.

    The default budgets (five seconds each) would make each stubborn teardown
    here cost ten seconds; shortening them changes only how long the server
    waits, not how it retires what is still running.
    """

    shutdown_grace_period = 1
    shutdown_cancel_timeout = 1


#: How long a teardown may take with the budgets above: grace plus cancel plus a
#: settle margin. The pre-fix shutdown waits for the immortal task, so this fails
#: by design against it.
TEARDOWN_BUDGET = (
    _QuickShutdownThread.shutdown_grace_period
    + _QuickShutdownThread.shutdown_cancel_timeout
    + 3
)


def _port_is_open(host, port, timeout=2):
    """Report whether something is listening on ``host:port``."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


class LiveServerStubbornRequestTest(ChannelsLiveServerTestCase):
    """Tear the class down while a request refuses to stop."""

    server_thread_class = _QuickShutdownThread
    static_handler = _StubbornApplication

    @classmethod
    def setUpClass(cls):
        # Registered before the server's own cleanups, so it runs after them.
        cls.addClassCleanup(cls._assert_teardown_was_bounded)
        view_entered.clear()
        super().setUpClass()
        cls.capture = _TeardownOutput().__enter__()
        cls.no_logs = enter_no_logs("asyncio", "daphne.server")

    def test_request_survives_cancellation_at_teardown(self):
        cls = type(self)
        host, port = cls.host, cls.server_thread.port
        cls.socket = socket.create_connection((host, port), timeout=10)
        cls.socket.sendall(
            (
                f"GET /stubborn/ HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode("ascii")
        )
        self.assertTrue(
            view_entered.wait(10),
            "the request never reached the application",
        )
        # Returning from the test is the point: the class teardown starts while
        # the request is parked in a cancellation-swallowing loop.
        cls.teardown_started = time.monotonic()

    @classmethod
    def _assert_teardown_was_bounded(cls):
        """Assert teardown finished promptly, freed the socket, and was quiet."""
        try:
            elapsed = time.monotonic() - getattr(
                cls, "teardown_started", time.monotonic()
            )
            thread = getattr(cls, "server_thread", None)
            sock = getattr(cls, "socket", None)
            problems = []
            if elapsed > TEARDOWN_BUDGET:
                problems.append(
                    f"teardown took {elapsed:.1f}s, so the shutdown waited for "
                    "the request which refuses to be cancelled"
                )
            if thread is not None:
                if thread.is_alive():
                    problems.append("the server thread outlived tearDownClass")
                if _port_is_open(cls.host, thread.port):
                    problems.append("the server's listening socket is still open")
            else:
                problems.append("setUpClass never recorded the server thread")
            capture = getattr(cls, "capture", None)
            output = capture.text() if capture is not None else ""
            if sock is not None:
                sock.close()
            markers = [marker for marker in FORBIDDEN_OUTPUT if marker in output]
            if markers:
                problems.append(
                    "teardown wrote to the console ("
                    + ", ".join(repr(marker) for marker in markers)
                    + "):\n"
                    + output
                )
            if problems:
                raise AssertionError("; ".join(problems))
        finally:
            finish_teardown_capture(
                getattr(cls, "capture", None), getattr(cls, "no_logs", ())
            )


class LiveServerGraceExpiryWarningTest(TransactionTestCase):
    """The grace-period expiry is reported at WARNING, and still completes.

    This drives a server thread's shutdown inside ``assertLogs`` directly: the
    warning is emitted from the server thread during teardown, which a class
    cleanup cannot wrap in an assertion context.
    """

    def test_grace_period_exceeded_is_reported(self):
        view_entered.clear()
        thread = _QuickShutdownThread(
            "localhost",
            _StubbornApplication,
            connections_override={},
            port=0,
        )
        thread.daemon = True
        thread.start()
        sock = None
        try:
            self.assertTrue(
                thread.is_ready.wait(thread.startup_timeout),
                "the live server never became ready",
            )
            if thread.error is not None:
                raise thread.error
            sock = socket.create_connection(("localhost", thread.port), timeout=10)
            sock.sendall(
                (
                    f"GET /stubborn/ HTTP/1.1\r\n"
                    f"Host: localhost:{thread.port}\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                ).encode("ascii")
            )
            self.assertTrue(
                view_entered.wait(10),
                "the request never reached the application",
            )
            started = time.monotonic()
            with self.assertLogs(
                "channels.testing.live", level=logging.WARNING
            ) as captured:
                with self.assertNoLogs("asyncio", level=logging.WARNING):
                    with self.assertNoLogs("daphne.server", level=logging.WARNING):
                        thread.terminate()
            elapsed = time.monotonic() - started
            self.assertFalse(thread.is_alive())
            self.assertLessEqual(
                elapsed,
                TEARDOWN_BUDGET,
                "the shutdown waited for the request it had retired",
            )
            self.assertTrue(
                any(
                    "grace period exceeded" in message.lower()
                    for message in captured.output
                ),
                f"the expiry warning was not logged: {captured.output!r}",
            )
        finally:
            if sock is not None:
                sock.close()
            if thread.is_alive():
                try:
                    thread.terminate()
                except RuntimeError:
                    # Nothing more can be done; the test has already failed.
                    pass
