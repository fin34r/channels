"""Regression: a second live server in one process must still serve requests.

Thread-based live servers share the process with the test runner. A server
stopped while thread-sensitive work is in flight leaves the process-global
worker blocked on a loop which never runs again, and every later server in the
same process queues behind that worker and never answers. Under the old
fork-per-server model each server had its own process, hence its own worker, so
the state could not leak between servers.
"""

import asyncio
import threading
import urllib.request

from asgiref.sync import async_to_sync, sync_to_async
from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler
from django.http import HttpResponse
from django.test.utils import override_settings
from django.urls import path

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import ChannelsLiveServerThread, _enter_class_context

#: Set once a request has crossed back into async code, i.e. is inside the
#: bridge and has handed control to the server thread's event loop.
bridge_entered = threading.Event()

#: Set while the process-global thread-sensitive worker is occupied, and set
#: once the class which occupied it is done with that state so the worker can
#: exit.
worker_occupied = threading.Event()
worker_release = threading.Event()


class _QuickShutdownThread(ChannelsLiveServerThread):
    """A server which gives up its drain after a second.

    The baseline below strands the process-global worker on purpose, so the
    drain has nothing to await: the shortened budget keeps that baseline from
    costing the default grace period twice per run.
    """

    shutdown_grace_period = 1
    shutdown_cancel_timeout = 1


async def _blocked_bridge():
    """Async half of the view below: announce the crossing, then stay pending."""
    bridge_entered.set()
    await asyncio.sleep(0.5)


def bridge_view(request):
    """Sync view that crosses back into async code — asgiref's nested bridge."""
    async_to_sync(_blocked_bridge)()
    return HttpResponse("ok")


def _return_none():
    """Do nothing: the unit of thread-sensitive work used by the tests below."""


def _occupy_worker():
    """
    Hold the thread-sensitive worker until released.

    A worker blocked inside a bridge looks the same to the servers which follow:
    it never returns to the queue, so no other thread-sensitive work ever runs.
    Blocking explicitly keeps the test free of a permanent hang — the worker can
    be released once a later server has taken over, so the interpreter can still
    exit.
    """
    worker_occupied.set()
    # Long enough that the occupied state is what the next server meets.
    worker_release.wait(60)


def _occupy_global_worker():
    """Start a thread which claims the process-global thread-sensitive worker."""
    threading.Thread(
        target=lambda: asyncio.run(
            sync_to_async(_occupy_worker, thread_sensitive=True)()
        ),
        daemon=True,
    ).start()


def _fire_bridge_request(base_url):
    """Send one request into the bridge from a thread which ignores its outcome."""

    def fire():
        try:
            urllib.request.urlopen(f"{base_url}/bridge/", timeout=5).read()
        except OSError:
            # Expected when the server is torn down underneath the request.
            pass

    threading.Thread(target=fire, daemon=True).start()


def thread_sensitive_work_runs(timeout):
    """
    Report whether thread-sensitive work runs within ``timeout``.

    Awaiting ``sync_to_async(thread_sensitive=True)`` from a task with no
    enclosing ``async_to_sync`` or ``ThreadSensitiveContext`` runs on the
    process-global executor — the one which a stopped server can leave blocked.
    The work is submitted from a thread of its own, so a blocked worker shows up
    as a timeout rather than as a hung test.
    """
    finished = threading.Event()

    def run():
        asyncio.run(sync_to_async(_return_none, thread_sensitive=True)())
        finished.set()

    threading.Thread(target=run, daemon=True).start()
    return finished.wait(timeout)


class _OuterThreadSensitiveHandler:
    """
    Application wrapper running one thread-sensitive call per request.

    An outermost ``sync_to_async(thread_sensitive=True)`` call, made by a
    wrapper before the request's own ``ThreadSensitiveContext`` is entered, lands
    on the process-global executor: this is the shape an adopter can write.
    (Django's own ``ASGIStaticFilesHandler`` is not that shape: it defers
    thread-insensitively, inside the request's context.)
    """

    def __init__(self, application):
        self.application = application

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            await sync_to_async(_return_none, thread_sensitive=True)()
        return await self.application(scope, receive, send)


urlpatterns = [path("bridge/", bridge_view)]


class _BridgeUrlconfMixin:
    @classmethod
    def setUpClass(cls):
        _enter_class_context(cls, override_settings(ROOT_URLCONF=__name__))
        super().setUpClass()


class LiveServerFirstStrandedRequestTest(
    _BridgeUrlconfMixin, ChannelsLiveServerTestCase
):
    """Tear the class down while a request is parked inside the bridge."""

    @classmethod
    def setUpClass(cls):
        # Registered before the server's own cleanups, so it runs after them. If
        # only this class is selected, the occupied worker must still be
        # released at teardown, or the interpreter waits out the occupancy.
        cls.addClassCleanup(worker_release.set)
        # Fresh module state: a class which ran earlier in this process (or a
        # second baseline below) must not leave the worker already released.
        bridge_entered.clear()
        worker_occupied.clear()
        worker_release.clear()
        super().setUpClass()

    def test_request_in_flight_at_teardown(self):
        _fire_bridge_request(self.live_server_url)
        self.assertTrue(
            bridge_entered.wait(10),
            "the request never reached the async half of the bridge",
        )

        # Also meet the next server with the process-global thread-sensitive
        # worker unavailable, which is the state a bridge stranded by a stopped
        # server leaves behind.
        _occupy_global_worker()
        self.assertTrue(worker_occupied.wait(10), "the worker was never occupied")
        # Guard the fixture itself: an available worker would make the next
        # class pass for the wrong reason.
        self.assertFalse(
            thread_sensitive_work_runs(timeout=2),
            "thread-sensitive work still ran, so the next class proves nothing",
        )


class LiveServerSecondServesAfterStrandedTest(
    _BridgeUrlconfMixin, ChannelsLiveServerTestCase
):
    """The next server in this process must still answer thread-sensitive work.

    This class does not rely on the first class having run before it: it
    reproduces the stranded-worker state itself (a throwaway server torn down
    with a request parked in the bridge and the process-global worker
    occupied), so it passes just as well when selected alone or scheduled onto
    another parallel worker.
    """

    # Send requests through the process-global executor, as this wrapper does.
    static_handler = _OuterThreadSensitiveHandler

    @classmethod
    def setUpClass(cls):
        # Class cleanups run last in, first out, so this releases the worker the
        # baseline occupied only after this class's server has stopped: the
        # replaced worker then exits instead of holding the interpreter open.
        cls.addClassCleanup(worker_release.set)
        cls._establish_stranded_baseline()
        super().setUpClass()

    @classmethod
    def _establish_stranded_baseline(cls):
        """Leave behind the state a torn-down server with a stranded bridge leaves."""
        bridge_entered.clear()
        worker_occupied.clear()
        worker_release.clear()
        # Shared with the server thread, as in ``_start_server_thread``: needed
        # for in-memory SQLite, and a no-op for this project's file-backed one.
        connections_override = cls._make_connections_override()
        for conn in connections_override.values():
            conn.inc_thread_sharing()
        previous = _QuickShutdownThread(
            cls.host,
            ASGIStaticFilesHandler,
            connections_override=connections_override,
            port=cls.port,
        )
        previous.daemon = True
        previous.start()
        try:
            if not previous.is_ready.wait(previous.startup_timeout):
                raise RuntimeError("the baseline live server never became ready")
            if previous.error is not None:
                raise previous.error
            # The baseline runs before this class's own setUpClass appends its
            # allowed host, so install it around the request itself.
            with override_settings(ALLOWED_HOSTS=[cls.host], ROOT_URLCONF=__name__):
                _fire_bridge_request(f"http://{cls.host}:{previous.port}")
                if not bridge_entered.wait(10):
                    raise RuntimeError("the baseline request never reached the bridge")
            _occupy_global_worker()
            if not worker_occupied.wait(10):
                raise RuntimeError("the baseline worker was never occupied")
        finally:
            previous.terminate()
            for conn in connections_override.values():
                conn.dec_thread_sharing()

    def test_thread_sensitive_executor_is_usable(self):
        """Work must not queue behind the worker of the previous server."""
        self.assertTrue(
            thread_sensitive_work_runs(timeout=10),
            "thread-sensitive work still queues behind the previous server's worker",
        )

    def test_url_serves(self):
        with urllib.request.urlopen(
            f"{self.live_server_url}/bridge/", timeout=10
        ) as response:
            self.assertEqual(response.status, 200)
