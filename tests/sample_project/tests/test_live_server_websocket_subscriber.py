"""Regression: a websocket subscriber open at class teardown must close.

A page which subscribes to live updates holds its socket until it navigates
away, so a class can end with the peer still connected and the consumer parked
in its receive loop. Cancelling it is daphne's force-kill path -- it reports the
instance as "took too long to shut down" and skips the consumer's ``disconnect``
handler -- so the server retires open websockets before it drains. The
assertions below are that the handler ran, the socket was closed with the
graceful close code, and the teardown wrote nothing.
"""

import base64
import os
import socket
import time
import urllib.parse
import urllib.request

from django.test.utils import override_settings

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import ChannelsLiveServerThread
from tests.sample_project.sampleapp.consumers import disconnect_codes
from tests.sample_project.tests.test_live_server_graceful_shutdown import (
    _TeardownOutput,
    enter_no_logs,
    finish_teardown_capture,
)

# The sample project's subscriber route (``config/asgi.py``).
WS_PATH = "/ws/message/"

# How long the class teardown may take when the subscriber is retired through
# its own disconnect path. The grace period is five seconds, so a teardown
# which waits it out (the force-kill path) fails this by design.
TEARDOWN_BUDGET = 3.0

# Text that must never reach an adopter's console at teardown.
FORBIDDEN_OUTPUT = (
    "took too long to shut down",
    "Application instance",
    "Exception inside application",
    "Traceback",
    "CancelledError",
    "Future exception was never retrieved",
    "Task was destroyed but it is pending",
)


def open_websocket(server_url, path=WS_PATH):
    """Open a real websocket against the live server, returning the socket.

    A raw upgrade keeps the connection in *this* process, exactly as a browser
    page's is; ``WebsocketCommunicator`` would drive the consumer in-process and
    could not observe the server's own teardown. Returns the bytes received
    past the response headers, since the first frame may arrive with them.
    """
    parsed = urllib.parse.urlsplit(server_url)
    host, port = parsed.hostname, parsed.port
    sock = socket.create_connection((host, port), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        f"Origin: http://{host}:{port}\r\n"
        "\r\n"
    )
    try:
        sock.sendall(request.encode("ascii"))
        sock.settimeout(10)
        buffered = b""
        while b"\r\n\r\n" not in buffered:
            chunk = sock.recv(4096)
            if not chunk:
                raise AssertionError(
                    "the server closed the socket during the handshake"
                )
            buffered += chunk
        head, _, rest = buffered.partition(b"\r\n\r\n")
        status = head.split(b"\r\n", 1)[0]
        if b"101" not in status:
            raise AssertionError(f"websocket handshake was rejected: {status!r}")
    except BaseException:
        sock.close()
        raise
    return sock, rest


def first_frame(sock, buffered, timeout=10):
    """Return the consumer's first frame payload, waiting if it is not buffered."""
    if buffered:
        return buffered
    sock.settimeout(timeout)
    return sock.recv(4096)


def socket_closed(sock, timeout=5):
    """Report whether the peer has closed the connection within ``timeout``."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        sock.settimeout(remaining)
        try:
            chunk = sock.recv(4096)
        except ConnectionResetError:
            return True
        except OSError:
            return False
        if not chunk:
            return True


class LiveServerFirstWebSocketSubscriberTest(ChannelsLiveServerTestCase):
    """Tear the class down with a subscriber socket still open."""

    @classmethod
    def setUpClass(cls):
        # Registered before the server's own cleanups, so it runs after them:
        # unittest runs class cleanups in reverse order of registration.
        cls.addClassCleanup(cls._assert_subscriber_was_retired)
        super().setUpClass()
        cls.capture = _TeardownOutput().__enter__()
        cls.no_logs = enter_no_logs("asyncio", "daphne.server")

    def test_socket_open_at_class_teardown(self):
        cls = type(self)
        cls.disconnects_before = len(disconnect_codes)
        cls.socket, rest = open_websocket(self.live_server_url)
        self.assertTrue(
            first_frame(cls.socket, rest),
            "the consumer never sent its initial state",
        )
        # Teardown starts with the subscription still connected.
        cls.teardown_started = time.monotonic()

    @classmethod
    def _assert_subscriber_was_retired(cls):
        """Assert the shutdown closed the subscriber instead of cancelling it."""
        try:
            elapsed = time.monotonic() - getattr(
                cls, "teardown_started", time.monotonic()
            )
            problems = []
            if elapsed > TEARDOWN_BUDGET:
                problems.append(
                    "teardown took %.1fs, so the shutdown waited out the grace "
                    "period instead of closing the subscriber" % elapsed
                )
            sock = getattr(cls, "socket", None)
            if sock is not None:
                try:
                    if not socket_closed(sock):
                        problems.append("the server left the subscriber socket open")
                finally:
                    sock.close()
            new_codes = disconnect_codes[getattr(cls, "disconnects_before", 0) :]
            if not new_codes:
                problems.append(
                    "the consumer's disconnect handler never ran: the "
                    "shutdown cancelled the application instance instead of "
                    "closing it"
                )
            elif new_codes[-1] != 1001:
                problems.append(
                    "the subscriber was not closed gracefully: "
                    f"disconnect code {new_codes[-1]!r}"
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


class LiveServerTwoWebSocketSubscribersTest(ChannelsLiveServerTestCase):
    """Two subscribers open at once: both are closed, each runs its handler."""

    @classmethod
    def setUpClass(cls):
        cls.addClassCleanup(cls._assert_both_subscribers_were_retired)
        super().setUpClass()
        cls.capture = _TeardownOutput().__enter__()
        cls.no_logs = enter_no_logs("asyncio", "daphne.server")

    def test_two_sockets_open_at_class_teardown(self):
        cls = type(self)
        cls.disconnects_before = len(disconnect_codes)
        cls.sockets = []
        for _ in range(2):
            sock, rest = open_websocket(self.live_server_url)
            self.assertTrue(
                first_frame(sock, rest),
                "the consumer never sent its initial state",
            )
            cls.sockets.append(sock)
        cls.teardown_started = time.monotonic()

    @classmethod
    def _assert_both_subscribers_were_retired(cls):
        try:
            elapsed = time.monotonic() - getattr(
                cls, "teardown_started", time.monotonic()
            )
            problems = []
            if elapsed > TEARDOWN_BUDGET:
                problems.append(
                    f"teardown took {elapsed:.1f}s, so the shutdown waited out "
                    "the grace period instead of closing the subscribers"
                )
            for sock in getattr(cls, "sockets", []):
                try:
                    if not socket_closed(sock):
                        problems.append("the server left a subscriber socket open")
                finally:
                    sock.close()
            new_codes = disconnect_codes[getattr(cls, "disconnects_before", 0) :]
            if len(new_codes) < 2:
                problems.append(
                    "a consumer's disconnect handler never ran: the shutdown "
                    "cancelled an application instance instead of closing it "
                    f"(codes: {new_codes!r})"
                )
            elif new_codes[:2] != [1001, 1001]:
                problems.append(
                    "the subscribers were not closed gracefully: "
                    f"disconnect codes {new_codes[:2]!r}"
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


class LiveServerSecondServesAfterWebSocketTest(ChannelsLiveServerTestCase):
    """The next server in this process must still serve requests and sockets.

    This class does not rely on another class having run first: it starts and
    stops its own baseline server (with a subscriber still open at shutdown)
    before its own server starts, so it passes just as well when selected on
    its own or split into another parallel worker.
    """

    @classmethod
    def setUpClass(cls):
        cls._stop_previous_server_with_subscriber()
        super().setUpClass()

    @classmethod
    def _stop_previous_server_with_subscriber(cls):
        """Serve a subscriber from a throwaway server, then tear it down."""
        disconnects_before = len(disconnect_codes)
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
        sock = None
        try:
            if not previous.is_ready.wait(previous.startup_timeout):
                raise RuntimeError("the baseline live server never became ready")
            if previous.error is not None:
                raise previous.error
            # The baseline runs before this class's own setUpClass appends its
            # allowed host, so install it around the handshake itself.
            with override_settings(ALLOWED_HOSTS=[cls.host]):
                sock, rest = open_websocket(f"http://{cls.host}:{previous.port}")
                first_frame(sock, rest)
        finally:
            previous.terminate()
            for conn in connections_override.values():
                conn.dec_thread_sharing()

            if sock is not None:
                sock.close()
        if len(disconnect_codes) <= disconnects_before:
            raise RuntimeError("the baseline subscriber was never retired")

    def test_http_request_is_served(self):
        with urllib.request.urlopen(
            f"{self.live_server_url}/admin/login/", timeout=10
        ) as response:
            self.assertEqual(response.status, 200)

    def test_subscriber_connects_and_receives_its_first_payload(self):
        sock, rest = open_websocket(self.live_server_url)
        try:
            self.assertTrue(
                first_frame(sock, rest),
                "the consumer never sent its initial state",
            )
        finally:
            sock.close()
