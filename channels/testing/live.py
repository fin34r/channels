import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from asgiref.sync import SyncToAsync, sync_to_async
from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler
from django.db import connections
from django.test.testcases import TransactionTestCase
from django.test.utils import modify_settings
from django.utils.functional import classproperty

from channels.routing import get_default_application

logger = logging.getLogger(__name__)

#: How long in-flight connections, and thread-sensitive work started alongside
#: them, get to finish once the server stops accepting new ones. Only work that
#: is still running pays this delay.
SHUTDOWN_GRACE_PERIOD = 5

#: How long work cancelled at shutdown gets to unwind before the loop is closed
#: under it. Work which ignores cancellation must not hold the teardown open,
#: and must not be reported either: see ``_close_event_loop()``.
SHUTDOWN_CANCEL_TIMEOUT = 5


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


def _noop():
    """Sentinel run on the thread-sensitive executor to detect that it drained."""


class _ThreadSensitiveExecutorSwap:
    """The executors a live server exchanged, kept so it can undo the exchange.

    ``previous`` is the process-wide executor the server found and replaced
    with ``installed`` (see ``_reset_thread_sensitive_executor()``);
    ``previous_probe`` is a sentinel submitted to ``previous`` before the swap,
    and it only completes if that executor's worker was still able to take
    work.
    """

    __slots__ = ("previous", "installed", "previous_probe")

    def __init__(self, previous, installed, previous_probe):
        self.previous = previous
        self.installed = installed
        self.previous_probe = previous_probe

    @property
    def previous_usable(self):
        """Report whether the executor this server replaced can still run work."""
        return self.previous_probe is not None and self.previous_probe.done()


def _reset_thread_sensitive_executor():
    """
    Give the server that is about to start a thread-sensitive executor of its own.

    ``SyncToAsync`` runs every thread-sensitive ``sync_to_async`` call on one
    process-wide executor, and while such a call executes, code below it may
    bridge back into the loop which scheduled it: ``AsyncToSync`` reads that
    loop from the worker's thread-local state, schedules the coroutine on it
    and blocks the worker until the loop has run the coroutine. Stopping a live
    server while such work is in flight therefore leaves the worker blocked on
    a loop which never runs again -- and as there is only one worker, every
    later server in the same process queues behind it and never answers a
    request. An outermost ``sync_to_async(thread_sensitive=True)`` call made by
    adopter code -- an application wrapper in front of Django's handler, such as
    the thread-reuse regression's ``_OuterThreadSensitiveHandler`` -- is exactly
    the work that ends up there, because it runs outside the request's own
    ``ThreadSensitiveContext``. (Django's static file handler is not such a call:
    it defers thread-insensitively, inside the request's context.)

    A forked live server was a process of its own, which is why this state could
    not leak between servers. Sequential servers inside one process -- what a
    test runner produces -- need the same isolation: replacing the executor
    before the server starts means its thread-sensitive work can neither queue
    behind blocked work nor bridge into a loop that no longer runs.

    A worker which is already blocked inside such a bridge cannot be woken at
    all, and because ``concurrent.futures`` joins executor workers at
    interpreter exit, it also prevents the test process from exiting -- which is
    why stopping a server must not strand one (see ``_stop_reactor``).

    Returns the record needed to undo the swap when the server stops
    (``_restore_thread_sensitive_executor()``), or ``None`` if asgiref does not
    keep the executor on the class.
    """
    previous = getattr(SyncToAsync, "single_thread_executor", None)
    if previous is None:
        # Not every asgiref release exposes it, and a later one may create it
        # lazily: there is nothing to replace, and the server runs without the
        # isolation rather than failing to start.
        logger.debug(
            "asgiref exposes no SyncToAsync.single_thread_executor; this live "
            "server shares the process-wide thread-sensitive executor"
        )
        return None
    # Probe the outgoing executor before the swap: the sentinel runs at once if
    # its worker is idle, and never if the worker is blocked inside a bridge.
    # That answer decides, at teardown, whether it is safe to put it back.
    probe = None
    submit = getattr(previous, "submit", None)
    if submit is not None:
        try:
            probe = submit(_noop)
        except RuntimeError:
            # Already shut down by an earlier server: nothing to restore.
            probe = None
    # ``SyncToAsync`` looks the executor up on the class on every call, so
    # replacing it takes effect for all calls made from here on. The outgoing
    # executor is left alone (not shut down) until the server stops: shutting
    # it down here would make it impossible to restore.
    installed = ThreadPoolExecutor(max_workers=1)
    SyncToAsync.single_thread_executor = installed
    return _ThreadSensitiveExecutorSwap(previous, installed, probe)


def _restore_thread_sensitive_executor(swap, drained):
    """Put the process-wide thread-sensitive executor back after a server stops.

    Whatever this server installed must be over by now -- its work has been
    drained, or the server never got as far as serving -- but leaving it in
    place would leave the process pointing at an executor which no longer runs
    anything, so the executor the server found is restored when it can still
    work. A replaced executor whose worker never drained (it is parked inside a
    bridge onto a loop which has stopped) is not restored: it would mute every
    later server in this process. In that case the fresh executor is
    intentionally left in place, live, so later thread-sensitive work in the
    process still has an executor which can run it.
    """
    if swap is None:
        return
    if getattr(SyncToAsync, "single_thread_executor", None) is not swap.installed:
        # Something else -- a later server, or adopter code -- took ownership
        # while this server ran: theirs is the live one, so leave it alone.
        logger.debug(
            "The process-wide thread-sensitive executor was replaced while a "
            "live server was running; not restoring the previous one"
        )
        return
    if drained and swap.previous_usable:
        SyncToAsync.single_thread_executor = swap.previous
        swap.installed.shutdown(wait=False)
        # The swap itself still races with concurrent readers: a thread which
        # read ``installed`` just before this point can submit to it after the
        # shutdown above and get ``RuntimeError: cannot schedule new futures
        # after shutdown``. There is no server work left to schedule, so the
        # window is narrowed rather than eliminated.
        logger.debug(
            "Restored the process-wide thread-sensitive executor after a live "
            "server stopped; a concurrent reader may still see the shut-down one"
        )
        return
    if swap.previous_usable:
        # The executor it replaced had drained and is not coming back: release
        # its worker. What keeps the swap in place is that this server's own
        # executor never drained before its loop stopped.
        logger.debug(
            "Leaving this live server's thread-sensitive executor in place: "
            "the executor it installed did not drain before its loop stopped"
        )
        swap.previous.shutdown(wait=False)
        return
    logger.debug(
        "Leaving this live server's thread-sensitive executor in place: the "
        "replaced executor cannot be restored because its worker never drained"
    )


def _reset_txaio_reactor():
    """Forget the reactor ``txaio`` cached, so this thread re-resolves it.

    ``txaio.tx`` caches the reactor on first use and autobahn builds its
    websocket timers from that cache, so a second server would schedule them on
    the first server's closed loop. Called when a server starts and again when
    it stops: between two servers the cache points at a reactor which has
    already stopped.
    """
    try:
        # Lazy import: txaio only arrives with the optional daphne extra.
        import txaio.tx
    except ImportError:
        return
    config = getattr(txaio.tx, "config", None)
    if config is not None:
        config.loop = None


def _consume_task_outcome(task):
    """Retrieve a retired task's outcome, so asyncio never reports it."""
    if not task.cancelled():
        # An exception left in a finished task is reported by the loop's
        # exception handler when the task is freed.
        task.exception()


class ChannelsLiveServerThread(threading.Thread):
    """Thread for running a live ASGI server while the tests are running."""

    # How long to wait for the server to report that it is ready.
    startup_timeout = 10

    # How long in-flight requests, and thread-sensitive work started alongside
    # them, get to finish once the server stops accepting new ones. Only work
    # that is still running pays this delay, and stopping the loop out from
    # under it is not survivable: see ``_stop_reactor``.
    shutdown_grace_period = SHUTDOWN_GRACE_PERIOD

    # How long work cancelled at shutdown gets to unwind before the loop is
    # closed under it: see ``_cancel_pending_tasks``.
    shutdown_cancel_timeout = SHUTDOWN_CANCEL_TIMEOUT

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
        # Whether the thread-sensitive work this server scheduled has finished:
        # see ``_drain_thread_sensitive_executor``. Nothing to drain until the
        # server has served.
        self._executor_drained = True
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
        # The executor this server installed for thread-sensitive work, and the
        # state needed to undo the swap when it stops; see
        # _reset_thread_sensitive_executor().
        executor_swap = None
        try:
            # Thread-sensitive work must land on an executor of this server's
            # own, not on the one a previously stopped server may have left
            # blocked; see _reset_thread_sensitive_executor().
            executor_swap = _reset_thread_sensitive_executor()

            # Lazy import: keeps ``import channels.testing`` importable without daphne
            # (advisory extra); neither daphne module imports twisted at module level.
            from daphne.testing import _reinstall_reactor

            # The reactor installed at import time belongs to the thread which
            # imported Daphne, and a reactor can only be run once. Reinstall it
            # so this thread gets its own, runnable reactor. Otherwise the
            # second live server in a process fails with ReactorAlreadyRunning.
            _reinstall_reactor()
            # Autobahn's websocket timers must target this thread's reactor.
            _reset_txaio_reactor()

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
            # ``reactor.run()`` returns once the reactor has stopped, so its
            # event loop has stopped running: this is the first point at which
            # that loop can be closed. It is also the last: closing it here,
            # rather than around ``httpd.run()`` alone, means a server which
            # never got as far as running does not leak the fresh loop's
            # epoll fd.
            self._close_event_loop()
            # Nothing must keep txaio pointing at this thread's reactor once
            # the server has stopped: autobahn would build the next server's
            # websocket timers from that cache, on a loop nobody runs.
            _reset_txaio_reactor()
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
            # Last, once nothing can schedule thread-sensitive work any more:
            # give the process back the executor it had before this server
            # replaced it. See _restore_thread_sensitive_executor().
            _restore_thread_sensitive_executor(executor_swap, self._executor_drained)

    def _close_event_loop(self):
        """Close the event loop the reactor ran on, quietly.

        ``reactor.run()`` stops the loop but leaves it open, and an open loop
        still accepts ``call_soon_threadsafe()``: the callback is queued for a
        run which will never happen. Anything holding a reference to that loop
        therefore goes on handing work to a loop nobody runs. Django's handler
        chain caches an ``async_to_sync`` bridge for the life of the process,
        and asgiref 3.9 caches the loop on that bridge, so the next live server
        in this process would queue its requests onto this server's loop and
        block its workers forever. Closed, the loop raises instead, which is
        what makes asgiref fall back to a loop of its own.

        Only a stopped loop can be closed, hence closing after
        ``reactor.run()`` returns rather than from ``_stop_reactor``: the drain
        of in-flight work started there runs while the loop is still running,
        and the loop then stops with the reactor.

        Work which outlived the reactor's own shutdown was already cancelled and
        consumed while the loop was running (``_cancel_pending_tasks``, from
        ``_stop_reactor``'s ``stop_when_idle``), so that closing the loop here
        cannot make asyncio report it afterwards. Cancelling it here instead
        would mean running the loop again, and daphne's ``application_checker``
        and ``timeout_checker`` are due by then: they would reap and report
        connections after daphne has finished shutting down.

        A failure is recorded rather than raised: the server has served and
        stopped correctly, and an error here must not mask that.
        """
        loop = getattr(self.reactor, "_asyncioEventloop", None)
        if loop is None:
            return
        try:
            loop.close()
        except Exception as e:
            # Say so: this runs in the server thread, after setUpClass() has
            # already consumed ``self.error``, so recording it is not enough to
            # make it visible. It matters because a loop left open keeps
            # accepting ``call_soon_threadsafe()`` -- and asgiref caches the
            # loop on a bridge -- so the next server in this process hands its
            # work to a loop nobody runs and never answers. (``txaio``'s cached
            # reactor, reset in ``run()``, is the websocket-side twin of that.)
            if self._executor_drained:
                logger.debug(
                    "Could not close the live server's event loop", exc_info=True
                )
            else:
                logger.warning(
                    "Could not close the live server's event loop while "
                    "thread-sensitive work was still running: a later live "
                    "server in this process may block on it",
                    exc_info=True,
                )
            if self.error is None:
                self.error = e

    async def _cancel_pending_tasks(self):
        """Cancel the tasks the loop is about to be stopped under.

        ``reactor.stop()`` cancels the application instances Daphne knows about,
        but not everything a request started: work a view shielded from the
        request's cancellation, or a task spawned inside one, is still pending
        when the loop stops. Closing the loop under such a task is not silent --
        asyncio reports the destruction of a pending task, and an exception
        never retrieved from a future, through the loop's exception handler --
        so they are cancelled and their outcome consumed here, as
        ``asyncio.run()`` does with the tasks it is about to abandon.

        Called from ``stop_when_idle``, while the loop is legitimately running:
        cancelling and settling means running the loop, and running it once the
        reactor has stopped would let daphne's already-due
        ``application_checker`` and ``timeout_checker`` fire after daphne has
        shut down, reaping connections and reporting application exceptions one
        layer below the teardown.

        Bounded by ``shutdown_cancel_timeout``: a task which refuses to end
        must not hold teardown open. It is left behind, with its destruction
        report silenced, because nothing adopter-facing may be printed.
        """
        current = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current]
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks, timeout=self.shutdown_cancel_timeout)
        for task in tasks:
            if not task.done():
                # It ignored cancellation; the loop closes under it, and the
                # report must not fire later.
                task._log_destroy_pending = False
            elif not task.cancelled():
                # Retrieve the outcome: an exception left in a finished task
                # is reported by the loop's exception handler when it is freed.
                task.exception()

    def _create_server(self, server_class):
        """Build the Daphne server, recording the sockets it binds."""
        # Lazy import: see run(); not needed to import channels.testing.
        from daphne.endpoints import build_endpoint_description_strings
        from twisted.internet import defer

        endpoints = build_endpoint_description_strings(host=self.host, port=self.port)
        # Create the handler for serving static files
        application = self.static_handler(get_default_application())
        live_thread = self

        class _PortRecordingServer(server_class):
            """Daphne server which keeps the listening sockets it binds.

            ``Server.listeners`` does hold one Deferred per endpoint, but Daphne
            chains its own ``listen_success`` callback onto each of them, and
            that callback returns ``None``: the Deferred's final result is
            ``None`` too, never the ``IListeningPort``. The port is only
            visible where Daphne is handed it, so it is recorded there.
            """

            def __init__(self, **kwargs):
                self.listening_ports = []
                super().__init__(**kwargs)

            def listen_success(self, port):
                self.listening_ports.append(port)
                super().listen_success(port)

            def listen_error(self, failure):
                """Record the real reason the server could not listen.

                Daphne only logs the failure and stops, so the thread would
                otherwise report the generic "exited before becoming ready"
                and hide the cause (an occupied port, a bad host, ...).
                """
                live_thread.error = RuntimeError(
                    f"Daphne could not listen: {failure.getErrorMessage()}"
                )
                super().listen_error(failure)

            def kill_all_applications(self):
                """Cancel every application instance, skipping idle connections.

                This papers over a Daphne bug: ``Server.kill_all_applications``
                reads ``details["application_instance"]`` unconditionally, but
                a connection which never started an application (an idle
                keep-alive or preconnect socket) has only ``{"connected": ...}``
                in its details. The resulting ``KeyError`` aborts Daphne's
                ``before:shutdown`` hook before a single instance is cancelled,
                so the shutdown waits for work it never even asked to stop.
                Worth filing upstream; the cancellation semantics below match
                Daphne's own for the instances which do exist.
                """
                wait_for = []
                for details in self.connections.values():
                    application_instance = details.get("application_instance")
                    if application_instance is None:
                        continue
                    if not application_instance.done():
                        application_instance.cancel()
                        wait_for.append(application_instance)
                logger.info("Killed %i pending application instances", len(wait_for))
                # Make Twisted wait until they're all dead
                wait_deferred = defer.Deferred.fromFuture(asyncio.gather(*wait_for))
                wait_deferred.addErrback(lambda failure: None)
                return wait_deferred

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
                try:
                    self.reactor.callFromThread(self._stop_reactor)
                except RuntimeError:
                    # The reactor stopped -- and its loop closed -- between the
                    # liveness check above and this call, so the shutdown is
                    # already under way and the callback has nowhere to run.
                    # Fall through to the join and its error check rather than
                    # raising over a server which is stopping on its own.
                    logger.debug(
                        "Live server reactor stopped before its shutdown could "
                        "be scheduled; joining anyway",
                        exc_info=True,
                    )
        # The thread's own shutdown is bounded but not instantaneous: it stops
        # accepting connections, waits up to ``shutdown_grace_period`` for
        # in-flight requests and again for queued thread-sensitive work, then
        # cancels whatever is left for up to ``shutdown_cancel_timeout`` and
        # tears the reactor down. Wait for longer than all of that, or a server
        # which is shutting down correctly would be reported as failed to stop.
        self.join(
            timeout=(2 * self.shutdown_grace_period + self.shutdown_cancel_timeout + 5)
        )
        if self.is_alive():
            raise RuntimeError("ChannelsLiveServerThread failed to stop")

    def quiesce(self):
        """Let in-flight requests finish before the test's data is destroyed.

        Django flushes the test database at the end of every test and only stops
        the live server afterwards, so a request still running here would reach
        its next query after the rows it was reading no longer exist -- the
        failures adopters see when a serializer reads or saves a row that
        teardown has already removed. A sync view runs on a worker thread, which
        cancelling the request does not stop, so the work gets the same bounded
        chance to finish that the shutdown gives it. Only requests whose client
        has already gone are waited for: one whose client is still connected is
        left to finish against the flushed test database, a deliberate
        trade-off rather than an oversight.
        """
        if self.reactor is None or not self.is_alive():
            return
        if not getattr(self.reactor, "running", False):
            # Nothing is serving, and a call queued on a stopped reactor would
            # never run: do not wait for it.
            return
        finished = threading.Event()
        try:
            self.reactor.callFromThread(
                lambda: asyncio.ensure_future(self._quiesce(finished))
            )
        except RuntimeError:
            # The reactor stopped between the check above and the call: there
            # is nothing left to quiesce, and no one left to set the event.
            logger.debug(
                "Live server reactor stopped before it could be quiesced",
                exc_info=True,
            )
            return
        # Bounded: a loop which has stopped must not hold the test up. The wait
        # is sliced, because the wake-up itself can be lost -- the callback is
        # queued just as the reactor stops -- and waiting out the full budget
        # for an event nobody will set would stall the teardown for nothing.
        deadline = time.monotonic() + self.shutdown_grace_period + 1
        while not finished.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not getattr(self.reactor, "running", False):
                break
            finished.wait(min(remaining, 0.1))

    async def _quiesce(self, finished):
        """Wait for requests whose peer has gone, then retire the stragglers."""
        try:
            pending = [
                instance
                for instance in self._abandoned_request_instances()
                if not instance.done()
            ]
            if pending:
                await asyncio.wait(pending, timeout=self.shutdown_grace_period)
            for protocol in self._abandoned_http_protocols():
                self._retire_http_request(protocol)
        finally:
            # Never leave the caller waiting on a loop which has stopped.
            finished.set()

    def _stop_reactor(self):
        """Close the listening sockets, drain in-flight requests, stop the reactor.

        Stopping the reactor while a request is still running is not safe.
        ``reactor.stop()`` fires Daphne's ``before:shutdown`` hook, which
        cancels every application instance; a cancelled request is inside
        Django's ``async with ThreadSensitiveContext()``, whose exit shuts the
        request's executor down -- on older asgiref releases the exit joins on
        the loop thread, while current releases join in a helper thread. The
        request's own sync thread may itself be waiting for a callback on this
        very loop (any ``async_to_sync`` below the sync stack), so on the older
        releases the two wait on each other forever: the loop thread never
        returns from the join, Daphne never finishes cancelling, and
        ``terminate()`` cannot stop the server. Give running requests a bounded
        chance to finish first; anything still running when the budget expires
        is retired, and whatever is left after that is cancelled and settled
        here, while the loop still runs (``_cancel_pending_tasks``), rather than
        after the reactor has stopped.

        Request tasks are not the only work that has to be over before the loop
        stops: thread-sensitive work started alongside them runs on the
        process-wide executor and is only fed by this loop while it runs. It is
        drained afterwards (``_drain_thread_sensitive_executor``) so that it is
        finished rather than stranded, since a stranded worker never returns to
        the queue and would mute every later server in the process.
        """

        async def stop_when_idle():
            # Stop accepting new connections first: everything below is about
            # the ones already established.
            for port in self._listening_ports():
                port.stopListening()
            # A consumer parked on a still-connected peer cannot finish on its
            # own; retire those websockets, then the drain only has to wait.
            for websocket in self._websocket_protocols():
                self._retire_websocket(websocket)
            # A request whose peer has gone cannot be answered, and cancelling
            # it cannot stop the worker thread its view runs on: retire it now
            # rather than waiting out the grace period for it.
            for protocol in self._abandoned_http_protocols():
                self._retire_http_request(protocol)
            pending = [
                instance
                for instance in self._application_instances()
                if not instance.done()
            ]
            if pending:
                await asyncio.wait(pending, timeout=self.shutdown_grace_period)
                dropped = [instance for instance in pending if not instance.done()]
                if dropped:
                    # Daphne's own shutdown cancels and then *waits* for every
                    # application instance it knows about, so an instance left
                    # running here would hold the loop thread -- and with it the
                    # teardown -- open. Retire those instances exactly as an
                    # abandoned request is retired, leaving Daphne nothing to
                    # await. A worker stranded behind a request which refuses to
                    # die can also hang interpreter exit, hence the warning.
                    logger.warning(
                        "Live server grace period exceeded: dropping %d "
                        "still-running application instance(s) so the shutdown "
                        "has nothing left to await; a stranded worker can hang "
                        "interpreter exit",
                        len(dropped),
                    )
                    open_connections = getattr(self.httpd, "connections", None) or {}
                    for details in list(open_connections.values()):
                        self._retire_application_instance(details)
            self._executor_drained = await self._drain_thread_sensitive_executor()
            # Cancel and settle whatever outlived the request that started it,
            # while the loop is still running: doing it after the reactor has
            # stopped would run the loop for daphne's already-due checkers.
            await self._cancel_pending_tasks()
            # Last-moment re-check: a keep-alive request on an already
            # established connection can still start an application instance
            # after the retirement pass above (the listeners are closed, so that
            # is the only way one can appear). Daphne's shutdown would cancel
            # and *wait* for it on the loop thread, so retire it the same way.
            open_connections = getattr(self.httpd, "connections", None) or {}
            for details in list(open_connections.values()):
                self._retire_application_instance(details)
            self.reactor.stop()

        asyncio.ensure_future(stop_when_idle())

    def _websocket_protocols(self):
        """Yield the websocket connections the server still has open.

        Daphne marks only its websocket protocol with ``application_type``, so
        an established HTTP request is never mistaken for a subscriber.
        """
        for protocol in list(getattr(self.httpd, "connections", None) or ()):
            if getattr(protocol, "application_type", None) == "websocket":
                yield protocol

    def _retire_websocket(self, protocol):
        """Close one open websocket, consumer side first.

        A subscriber still holding its socket never leaves its receive loop, so
        give it the disconnect message a client close would deliver: the
        consumer runs its own handler instead of being cancelled at shutdown.
        ``muted`` keeps the peer's half of the handshake from arriving as a
        second disconnect; the close frame itself is best-effort.
        """
        queue = getattr(protocol, "application_queue", None)
        if queue is None:
            # The handshake never reached the application: nothing to retire.
            return
        protocol.muted = True
        try:
            if protocol.state == protocol.STATE_OPEN:
                protocol.sendClose(code=1001)
        except Exception:
            logger.debug("Could not send a close frame at shutdown", exc_info=True)
        queue.put_nowait({"type": "websocket.disconnect", "code": 1001})

    def _http_protocols(self):
        """Yield the connections which still have a request in flight."""
        for protocol, details in list(
            (getattr(self.httpd, "connections", None) or {}).items()
        ):
            if details.get("application_instance") is None:
                continue
            if getattr(protocol, "application_type", None) == "websocket":
                continue
            yield protocol

    def _abandoned_http_protocols(self):
        """Yield the in-flight requests whose peer has already gone."""
        connections = getattr(self.httpd, "connections", None) or {}
        for protocol in self._http_protocols():
            if connections[protocol].get("disconnected"):
                yield protocol

    def _abandoned_request_instances(self):
        """Yield the application instances of requests whose peer has gone."""
        connections = getattr(self.httpd, "connections", None) or {}
        for protocol in self._abandoned_http_protocols():
            instance = connections[protocol].get("application_instance")
            if instance is not None:
                yield instance

    def _retire_application_instance(self, details):
        """Cancel one running application instance and hand Daphne a finished one.

        The application task is cancelled and its outcome consumed, and a
        resolved future is swapped into the connection's details in its place:
        Daphne's periodic checker then has nothing to report as having taken too
        long to shut down, and the shutdown's ``kill_all_applications`` gather
        has nothing left to await. Cancelling is best-effort: work which ignores
        cancellation is left for ``_cancel_pending_tasks`` to abandon quietly.

        Must be called from the reactor's event loop: the replacement future
        belongs to the running loop, which is the loop Daphne inspects.
        """
        instance = details.get("application_instance")
        if instance is None or instance.done():
            return
        instance.cancel()
        instance.add_done_callback(_consume_task_outcome)
        retired = asyncio.get_running_loop().create_future()
        retired.set_result(None)
        details["application_instance"] = retired

    def _retire_http_request(self, protocol):
        """Retire one abandoned request instead of having the shutdown kill it.

        No response will be read, so the application instance is retired (see
        ``_retire_application_instance``) and the connection closed. Cancelling
        cannot stop the worker thread a sync view runs on, so the instance is
        replaced rather than waited for.
        """
        details = (getattr(self.httpd, "connections", None) or {}).get(protocol)
        if details is None:
            return
        self._retire_application_instance(details)
        transport = getattr(protocol, "transport", None)
        if transport is not None:
            try:
                transport.loseConnection()
            except Exception:
                logger.debug("Could not close an abandoned request", exc_info=True)

    async def _drain_thread_sensitive_executor(self):
        """Let thread-sensitive work already under way finish.

        An outermost ``sync_to_async(thread_sensitive=True)`` call -- one made
        outside a request's ``ThreadSensitiveContext``, as an adopter's wrapper
        may -- runs on the process-wide executor; a bridge from that work back
        into this loop is only fed while this loop runs and would block the
        worker forever once the loop stops. Submitting a sentinel and awaiting
        it means everything queued before the sentinel has run. Bounded by the
        same grace period as the requests above: work which cannot finish must
        not hold the teardown open.

        Returns whether the work drained: the caller uses that to decide if the
        executor this server installed is still usable afterwards (see
        ``_restore_thread_sensitive_executor``).
        """
        try:
            await asyncio.wait_for(
                sync_to_async(_noop, thread_sensitive=True)(),
                timeout=self.shutdown_grace_period,
            )
        except asyncio.TimeoutError:
            return False
        return True

    def _application_instances(self):
        """Yield the application tasks Daphne is currently running."""
        connections = getattr(self.httpd, "connections", None) or {}
        for details in connections.values():
            instance = details.get("application_instance")
            if instance is not None:
                yield instance

    def _listening_ports(self):
        """Return the listening sockets the server has bound.

        Daphne discards the ``IListeningPort`` a successful bind produces (see
        ``_PortRecordingServer``), so the ports are the ones recorded when the
        server was built. Nothing has been recorded yet if the server never got
        as far as binding.
        """
        return list(getattr(self.httpd, "listening_ports", ()))

    def _close_listening_ports(self):
        """Close the sockets the server was listening on."""
        for port in self._listening_ports():
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

    def _fixture_teardown(self):
        """Let abandoned requests finish before the test's data is destroyed.

        Django flushes the test database at the end of every test and stops the
        live server only afterwards, so a request its client abandoned would
        otherwise run its next query against rows which no longer exist.
        """
        server_thread = getattr(self, "server_thread", None)
        if server_thread is not None:
            # A custom ``server_thread_class`` need only provide the
            # ``Thread`` protocol plus ``terminate()``: quiescing is an
            # enhancement of this thread, not part of the extension point.
            quiesce = getattr(server_thread, "quiesce", None)
            if quiesce is not None:
                quiesce()
        super()._fixture_teardown()

    @classmethod
    def _terminate_thread(cls):
        try:
            # Terminate the live server's thread.
            cls.server_thread.terminate()
        finally:
            # Restore shared connections' non-shareability.
            for conn in cls.server_thread.connections_override.values():
                conn.dec_thread_sharing()
