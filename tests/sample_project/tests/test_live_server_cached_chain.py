"""Regression: a later live server must serve through the cached ASGI chain.

The sample project is shaped like a deployed one: ``config/asgi.py`` builds its
Django handler once, at import time (``get_asgi_application()``), so the whole
process shares one handler and one cached middleware chain. Where a sync-only
middleware sits above an async-capable one -- the middleware mix of most
projects, and of the project this regression was reported against -- Django
adapts the handler below it with ``async_to_sync``, i.e. it puts an
``AsyncToSync`` bridge inside that cached chain.

A live server runs a reactor of its own, hence an event loop of its own, which
is stopped when the server stops. Stopped, not closed: a stopped loop still
accepts ``call_soon_threadsafe()`` and silently queues the coroutine on a loop
nobody will ever run again. asgiref 3.9 caches the loop on the bridge the first
time it bridges (``AsyncToSync.main_event_loop``), so a later live server in the
same process -- a second test class -- hands its work to the previous server's
dead loop: the request worker blocks forever, the response never arrives, and
the server can no longer shut down. asgiref only takes its self-contained
fallback (a loop of its own, in a thread of its own) when scheduling on the
cached loop raises, which it does for a *closed* loop.

So a live server must close the loop its reactor ran on once the reactor has
returned, and the second class below both proves that invariant and serves a
real request through the cached chain.
"""

import urllib.request

from asgiref.sync import AsyncToSync
from django.core.handlers.asgi import ASGIHandler
from django.test.utils import override_settings

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import ChannelsLiveServerThread
from tests.sample_project.config import asgi
from tests.sample_project.sampleapp import middleware as sample_middleware


def bridges():
    """Return the ``async_to_sync`` bridges in the cached middleware chain."""
    return [
        handler
        for handler in sample_middleware.get_response_handlers
        if isinstance(handler, AsyncToSync)
    ]


def reactor_loop(server_thread):
    """Return the event loop the server thread's reactor ran on."""
    # ``daphne.server.Server.run()`` installs the reactor's own loop as the
    # current one and runs it; Twisted keeps it on the reactor.
    return server_thread.reactor._asyncioEventloop


def serves(url):
    """Return the status of a GET against ``url``, or fail within 10 seconds."""
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.status


class CachedChainFirstServerTest(ChannelsLiveServerTestCase):
    """The first server resolves the bridge onto the loop it runs."""

    def test_cached_chain_holds_one_bridge(self):
        # The premise of this module: one process-wide handler, and a bridge
        # inside its cached chain. Without both, the test below cannot fail
        # for the reason it documents.
        self.assertIsInstance(asgi.django_asgi_app, ASGIHandler)
        self.assertIs(
            asgi.application.application_mapping["http"],
            asgi.django_asgi_app,
        )
        self.assertEqual(
            len(bridges()),
            1,
            "the sync-only middleware above an async one must leave exactly "
            "one async_to_sync bridge in the handler's cached chain",
        )

    def test_url_serves(self):
        self.assertEqual(serves(f"{self.live_server_url}/admin/login/"), 200)


class CachedChainSecondServerTest(ChannelsLiveServerTestCase):
    """A server started after another one stopped must still serve.

    The class starts and stops its own baseline server first, so it proves this
    property without depending on another class having run before it in the
    same process -- it holds when the class is selected alone or scheduled onto
    a separate parallel worker.
    """

    @classmethod
    def setUpClass(cls):
        # Serves a request through the cached chain, so asgiref caches the
        # baseline server's loop on the bridge; terminate() then stops and
        # closes that loop.
        cls.previous_loop = cls._stop_previous_server()
        super().setUpClass()

    @classmethod
    def _stop_previous_server(cls):
        """Run a throwaway live server to completion, returning its loop."""
        # Shared with the server thread, as in ``_start_server_thread``: needed
        # for in-memory SQLite, and a no-op for this project's file-backed one.
        connections_override = cls._make_connections_override()
        for conn in connections_override.values():
            conn.inc_thread_sharing()
        previous = ChannelsLiveServerThread(
            cls.host,
            cls.static_handler,
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
            loop = reactor_loop(previous)
            # The baseline runs before this class's own setUpClass appends its
            # allowed host, so install it around the request itself.
            with override_settings(ALLOWED_HOSTS=[cls.host]):
                status = serves(f"http://{cls.host}:{previous.port}/admin/login/")
            if status != 200:
                raise RuntimeError(
                    f"the baseline live server answered with status {status}"
                )
        finally:
            previous.terminate()
            for conn in connections_override.values():
                conn.dec_thread_sharing()
        return loop

    def test_previous_server_loop_is_closed(self):
        loop = self.previous_loop
        self.assertFalse(loop.is_running())
        self.assertTrue(
            loop.is_closed(),
            "the previous live server's event loop was stopped but left open: "
            "a cached reference to it still accepts work, and asgiref will "
            "queue the next server's requests onto a loop nobody runs",
        )
        # The property asgiref relies on: scheduling on that loop must raise,
        # rather than accept work which will never run.
        with self.assertRaises(RuntimeError):
            loop.call_soon_threadsafe(lambda: None)
        # A bridge which cached that loop -- asgiref 3.9 caches it on the
        # instance -- must not still point at a stopped-but-open loop. Later
        # asgiref resolves the loop per call and has no such attribute.
        for bridge in bridges():
            cached = getattr(bridge, "main_event_loop", None)
            if cached is None:
                continue
            self.assertTrue(
                cached.is_closed() or cached.is_running(),
                "the cached middleware chain still bridges onto a stopped "
                "event loop",
            )

    def test_url_serves(self):
        self.assertEqual(serves(f"{self.live_server_url}/admin/login/"), 200)
