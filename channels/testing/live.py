import threading

from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler
from django.db import connections
from django.test.testcases import TransactionTestCase
from django.test.utils import modify_settings
from django.utils.functional import classproperty

from channels.routing import get_default_application


def _enter_class_context(cls, cm):
    """
    Enter context manager ``cm`` and exit it during ``cls``'s class cleanups.

    ``unittest.TestCase.enterClassContext()`` only exists on Python 3.11+.
    On Python 3.10, fall back to the ``addClassCleanup()`` based equivalent
    (as in django/django commit ed4f837), looking the special methods up on
    the type to match the ``with`` statement.
    """
    enter_class_context = getattr(cls, "enterClassContext", None)
    if enter_class_context is not None:
        return enter_class_context(cm)
    cm_type = type(cm)
    try:
        enter = cm_type.__enter__
        exit_ = cm_type.__exit__
    except AttributeError:
        raise TypeError(
            f"'{cm_type.__module__}.{cm_type.__qualname__}' object does not "
            "support the context manager protocol"
        ) from None
    result = enter(cm)
    cls.addClassCleanup(exit_, cm, None, None, None)
    return result


class ChannelsLiveServerThread(threading.Thread):
    """Thread for running a live ASGI server while the tests are running."""

    # How long to wait for the server to report that it is ready.
    startup_timeout = 10

    def __init__(self, host, static_handler, connections_override=None, port=0):
        self.host = host
        self.port = port
        self.is_ready = threading.Event()
        self.error = None
        self.static_handler = static_handler
        self.connections_override = connections_override
        self.httpd = None
        self.reactor = None
        self._terminated = False
        super().__init__()

    def run(self):
        """
        Set up the live server and databases, and then loop over handling
        ASGI requests.
        """
        if self.connections_override:
            # Override this thread's database connections with the ones
            # provided by the main thread.
            for alias, conn in self.connections_override.items():
                connections[alias] = conn
        try:
            # Lazy import: keeps ``import channels.testing`` importable without daphne
            # (advisory extra); neither daphne module imports twisted at module level.
            from daphne.testing import _reinstall_reactor

            # The reactor installed at import time belongs to the thread which
            # imported Daphne, and a reactor can only be run once. Reinstall it
            # so this thread gets its own, runnable reactor. Otherwise the
            # second live server in a process fails with ReactorAlreadyRunning.
            _reinstall_reactor()

            # Re-import here, after the reinstall: both bind the reactor at
            # import time, so this is what binds the freshly installed reactor.
            from daphne.server import Server
            from twisted.internet import reactor

            self.reactor = reactor
            self.httpd = self._create_server(Server)
            # The server will call ready_callable when ready
            self.httpd.run()
        except Exception as e:
            self.error = e
        finally:
            if self.error is not None:
                # Close the listening sockets here: terminate() only stops a
                # live thread, else a fixed-port class hits "Address already in use".
                self._close_listening_ports()
            if not self.is_ready.is_set():
                # Never leave setUpClass() waiting for a server which has
                # already exited.
                if self.error is None:
                    self.error = RuntimeError(
                        "Daphne server exited before becoming ready"
                    )
                self.is_ready.set()
            connections.close_all()

    def _create_server(self, server_class):
        """
        Build the Daphne server, recording the ports it listens on so that
        ``terminate()`` can close exactly those sockets.
        """
        # Lazy import: see run(); not needed to import channels.testing.
        from daphne.endpoints import build_endpoint_description_strings

        endpoints = build_endpoint_description_strings(host=self.host, port=self.port)
        # Create the handler for serving static files
        application = self.static_handler(get_default_application())

        class _PortRecordingServer(server_class):
            def __init__(self, **kwargs):
                self.listening_ports = []
                super().__init__(**kwargs)

            def listen_success(self, port):
                self.listening_ports.append(port)
                super().listen_success(port)

        return _PortRecordingServer(
            application=application,
            endpoints=endpoints,
            signal_handlers=False,
            ready_callable=self._server_is_ready,
            verbosity=0,
        )

    def _server_is_ready(self):
        """Called by Daphne when the server is ready and listening."""
        # Report the port actually bound: binding to port zero asks the OS to
        # allocate one.
        if self.httpd is not None and self.httpd.listening_addresses:
            self.port = self.httpd.listening_addresses[0][1]
        self.is_ready.set()

    def terminate(self):
        if not self._terminated:
            self._terminated = True
            if self.is_alive() and self.is_ready.is_set() and self.reactor is not None:
                # Close the listening sockets before stopping the reactor:
                # a reactor stop alone leaves them open. Both must run on the
                # reactor's own thread. Established connections are left to
                # the reactor shutdown rather than being torn down abruptly
                # with disconnectAll().
                self.reactor.callFromThread(self._stop_reactor)
        self.join(timeout=5)
        if self.is_alive():
            raise RuntimeError("ChannelsLiveServerThread failed to stop")

    def _stop_reactor(self):
        """Close the listening sockets, then stop the reactor (server thread)."""
        try:
            for port in getattr(self.httpd, "listening_ports", ()):
                port.stopListening()
        finally:
            self.reactor.stop()

    def _close_listening_ports(self):
        """Close the listening sockets recorded by ``_PortRecordingServer``."""
        for port in getattr(self.httpd, "listening_ports", ()):
            try:
                port.stopListening()
            except Exception:
                # Nothing useful to do from a thread that is about to exit.
                pass


class ChannelsLiveServerTestCase(TransactionTestCase):
    """
    Do basically the same as TransactionTestCase but also launch a live ASGI
    server in a separate thread so that the tests may use another testing
    framework, such as Selenium for example, instead of the built-in dummy
    client.
    It inherits from TransactionTestCase instead of TestCase because the
    threads don't share the same transactions (unless if using in-memory
    sqlite) and each thread needs to commit all their transactions so that the
    other thread can see the changes.
    """

    host = "localhost"
    port = 0
    server_thread_class = ChannelsLiveServerThread
    static_handler = ASGIStaticFilesHandler

    @classproperty
    def live_server_url(cls):
        return "http://%s:%s" % (cls.host, cls.server_thread.port)

    @classproperty
    def live_server_ws_url(cls):
        return "ws://%s:%s" % (cls.host, cls.server_thread.port)

    @classproperty
    def allowed_host(cls):
        return cls.host

    @classmethod
    def _make_connections_override(cls):
        connections_override = {}
        for conn in connections.all():
            # If using in-memory sqlite databases, pass the connections to
            # the server thread.
            if conn.vendor == "sqlite" and conn.is_in_memory_db():
                connections_override[conn.alias] = conn
        return connections_override

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _enter_class_context(
            cls, modify_settings(ALLOWED_HOSTS={"append": cls.allowed_host})
        )
        cls._start_server_thread()

    @classmethod
    def _start_server_thread(cls):
        connections_override = cls._make_connections_override()
        for conn in connections_override.values():
            # Explicitly enable thread-shareability for this connection.
            conn.inc_thread_sharing()

        cls.server_thread = cls._create_server_thread(connections_override)
        cls.server_thread.daemon = True
        cls.server_thread.start()
        cls.addClassCleanup(cls._terminate_thread)

        # Wait for the live server to be ready
        if not cls.server_thread.is_ready.wait(cls.server_thread.startup_timeout):
            raise RuntimeError(
                "Live server thread failed to become ready within "
                f"{cls.server_thread.startup_timeout} seconds."
            )
        if cls.server_thread.error:
            raise cls.server_thread.error

    @classmethod
    def _create_server_thread(cls, connections_override):
        return cls.server_thread_class(
            cls.host,
            cls.static_handler,
            connections_override=connections_override,
            port=cls.port,
        )

    @classmethod
    def _terminate_thread(cls):
        try:
            # Terminate the live server's thread.
            cls.server_thread.terminate()
        finally:
            # Restore shared connections' non-shareability.
            for conn in cls.server_thread.connections_override.values():
                conn.dec_thread_sharing()
