"""Regression: a request abandoned by its client must not outlive its data.

A test's browser can go away while its last fetch is still in flight: the
request is then still running when the test ends. Django flushes the test
database at the end of every test and only stops the live server afterwards, so
an abandoned request reaches its next query after the rows it was reading are
gone -- the ``DoesNotExist`` and "did not affect any rows" failures adopters
report -- and daphne is left with an application instance to cancel at
shutdown, which it reports as having taken too long to die.

The view below is synchronous, as a deployed project's views are, so Django
runs it through ``sync_to_async(thread_sensitive=True)`` on a worker thread:
cancelling the request's coroutine does not stop it. What the view's late query
saw is the evidence that separates "the request finished against its data" from
"the request ran on after its data was destroyed".
"""

import socket
import threading
import time

from django.http import HttpResponse
from django.test.utils import override_settings
from django.urls import path

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import _enter_class_context
from tests.sample_project.sampleapp.consumers import disconnect_codes
from tests.sample_project.sampleapp.models import Message
from tests.sample_project.tests.test_live_server_graceful_shutdown import (
    _TeardownOutput,
    enter_no_logs,
    finish_teardown_capture,
)
from tests.sample_project.tests.test_live_server_websocket_subscriber import (
    first_frame,
    open_websocket,
    socket_closed,
)

#: The row the abandoned request reads once its client is gone.
ROW_TITLE = "abandoned request row"

#: How long the view keeps working after its client is gone. Well inside the
#: server's grace period, so teardown has time to let the work finish.
ABANDONED_VIEW_SECONDS = 1.5

#: How long the class teardown may take: the abandoned view's own run plus two
#: seconds of headroom. The server's grace period is five seconds, so a
#: teardown which waits it out fails this by design.
TEARDOWN_BUDGET = ABANDONED_VIEW_SECONDS + 2

#: Text that must never reach an adopter's console at teardown.
FORBIDDEN_OUTPUT = (
    "took too long to shut down",
    "Application instance",
    "Exception inside application",
    "Traceback",
    "CancelledError",
    "Future exception was never retrieved",
    "Task was destroyed but it is pending",
)

#: Set once the view is running: the request is then established and in flight.
view_entered = threading.Event()

#: What the view's late query saw: the row's title, or the exception it raised.
late_query_outcomes: list[object] = []


def abandoned_request_view(request):
    """Sync view whose work outlives the client that asked for it."""
    view_entered.set()
    time.sleep(ABANDONED_VIEW_SECONDS)
    try:
        message = Message.objects.get(pk=request.GET["row"])
    except Exception as exc:
        # Record whatever the late query hit -- DoesNotExist, or a database
        # error from a table which was truncated while the request ran.
        late_query_outcomes.append(exc)
        raise
    late_query_outcomes.append(message.title)
    return HttpResponse("slow")


urlpatterns = [path("abandoned/", abandoned_request_view)]


class LiveServerAbandonedRequestTest(ChannelsLiveServerTestCase):
    """Tear the class down with a request its client abandoned mid-flight."""

    @classmethod
    def setUpClass(cls):
        # Registered before the server's own cleanups, so it runs after them:
        # unittest runs class cleanups in reverse order of registration.
        cls.addClassCleanup(cls._assert_abandoned_request_was_retired)
        _enter_class_context(cls, override_settings(ROOT_URLCONF=__name__))
        late_query_outcomes.clear()
        super().setUpClass()
        cls.capture = _TeardownOutput().__enter__()
        cls.no_logs = enter_no_logs("asyncio", "daphne.server")

    def test_request_abandoned_by_its_client(self):
        cls = type(self)
        row = Message.objects.create(title=ROW_TITLE, message="abandoned")
        host, port = cls.host, cls.server_thread.port
        sock = socket.create_connection((host, port), timeout=10)
        try:
            sock.sendall(
                (
                    f"GET /abandoned/?row={row.pk} HTTP/1.1\r\n"
                    f"Host: {host}:{port}\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                ).encode("ascii")
            )
            self.assertTrue(
                view_entered.wait(10),
                "the request never reached the view",
            )
        finally:
            # The browser quits mid-fetch: the response is never read, and the
            # request keeps running server-side.
            sock.close()
        # Returning from the test is the point: the test database is flushed
        # and the class torn down with the request still in flight.
        cls.teardown_started = time.monotonic()

    @classmethod
    def _assert_abandoned_request_was_retired(cls):
        """Assert the request finished against its data, and teardown was quiet."""
        try:
            elapsed = time.monotonic() - getattr(
                cls, "teardown_started", time.monotonic()
            )
            problems = []
            if elapsed > TEARDOWN_BUDGET:
                problems.append(
                    f"teardown took {elapsed:.1f}s, so it waited out the grace "
                    "period instead of letting the request finish"
                )
            # The late query must find the row: a request which reaches its
            # query after the flush gets DoesNotExist, as adopters saw.
            if late_query_outcomes != [ROW_TITLE]:
                problems.append(
                    "the abandoned request did not run against its data: "
                    f"{late_query_outcomes!r}"
                )
            pending = [
                details.get("application_instance")
                for details in (
                    getattr(cls.server_thread.httpd, "connections", None) or {}
                ).values()
                if details.get("application_instance") is not None
                and not details["application_instance"].done()
            ]
            if pending:
                problems.append(
                    f"{len(pending)} application instance(s) outlived teardown"
                )
            capture = getattr(cls, "capture", None)
            output = capture.text() if capture is not None else ""
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
            # Never leave the redirected streams in place, whatever happened.
            finish_teardown_capture(
                getattr(cls, "capture", None), getattr(cls, "no_logs", ())
            )


class LiveServerWebsocketAndAbandonedRequestTest(ChannelsLiveServerTestCase):
    """One teardown must retire both an open subscriber and an abandoned request.

    A page can end a test with its subscription still open *and* a fetch which
    its browser abandoned; both must be retired rather than left for daphne's
    force-kill, and neither may make the teardown noisy.
    """

    @classmethod
    def setUpClass(cls):
        # Registered before the server's own cleanups, so it runs after them.
        cls.addClassCleanup(cls._assert_both_were_retired)
        _enter_class_context(cls, override_settings(ROOT_URLCONF=__name__))
        late_query_outcomes.clear()
        view_entered.clear()
        super().setUpClass()
        cls.capture = _TeardownOutput().__enter__()
        cls.no_logs = enter_no_logs("asyncio", "daphne.server")

    def test_both_open_at_class_teardown(self):
        cls = type(self)
        cls.disconnects_before = len(disconnect_codes)
        row = Message.objects.create(title=ROW_TITLE, message="abandoned")
        cls.socket, rest = open_websocket(self.live_server_url)
        self.assertTrue(
            first_frame(cls.socket, rest),
            "the consumer never sent its initial state",
        )
        host, port = cls.host, cls.server_thread.port
        sock = socket.create_connection((host, port), timeout=10)
        try:
            sock.sendall(
                (
                    f"GET /abandoned/?row={row.pk} HTTP/1.1\r\n"
                    f"Host: {host}:{port}\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                ).encode("ascii")
            )
            self.assertTrue(
                view_entered.wait(10),
                "the request never reached the view",
            )
        finally:
            # The browser quits mid-fetch while the subscriber stays open.
            sock.close()
        cls.teardown_started = time.monotonic()

    @classmethod
    def _assert_both_were_retired(cls):
        try:
            elapsed = time.monotonic() - getattr(
                cls, "teardown_started", time.monotonic()
            )
            problems = []
            if elapsed > TEARDOWN_BUDGET:
                problems.append(
                    f"teardown took {elapsed:.1f}s, so it waited out the grace "
                    "period instead of retiring the open connections"
                )
            sock = getattr(cls, "socket", None)
            if sock is not None:
                try:
                    if not socket_closed(sock):
                        problems.append("the server left the subscriber socket open")
                finally:
                    sock.close()
            else:
                problems.append("setUp kept no subscriber socket to check")
            new_codes = disconnect_codes[getattr(cls, "disconnects_before", 0) :]
            if not new_codes:
                problems.append("the subscriber's disconnect handler never ran")
            elif 1001 not in new_codes:
                problems.append(
                    "the subscriber was not closed gracefully: "
                    f"disconnect codes {new_codes!r}"
                )
            if late_query_outcomes != [ROW_TITLE]:
                problems.append(
                    "the abandoned request did not run against its data: "
                    f"{late_query_outcomes!r}"
                )
            pending = [
                details["application_instance"]
                for details in (
                    getattr(cls.server_thread.httpd, "connections", None) or {}
                ).values()
                if details.get("application_instance") is not None
                and not details["application_instance"].done()
            ]
            if pending:
                problems.append(
                    f"{len(pending)} application instance(s) outlived teardown"
                )
            capture = getattr(cls, "capture", None)
            output = capture.text() if capture is not None else ""
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
