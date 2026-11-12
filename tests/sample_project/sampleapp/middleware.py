"""Middleware giving the sample project a deployed project's shape.

A deployed project mixes middlewares which implement only the synchronous
protocol with ones which also offer an async implementation. Django adapts a
sync-only middleware for the project's async handler, and part of that
adaptation puts an ``async_to_sync`` bridge into the handler's cached
middleware chain -- see ``tests/test_live_server_cached_chain.py``.
"""

#: ``get_response`` callables handed to :class:`SyncOnlyMiddleware` when a
#: handler built its middleware chain, in construction order. A handler builds
#: its chain once, so this records what the process's cached chain holds.
get_response_handlers: list[object] = []


class SyncOnlyMiddleware:
    """Middleware which offers no async implementation at all."""

    # ``sync_capable``/``async_capable`` default to true/false: only the
    # synchronous protocol is implemented, which is what makes Django bridge
    # the handler below it with ``async_to_sync``.
    def __init__(self, get_response):
        self.get_response = get_response
        get_response_handlers.append(get_response)

    def __call__(self, request):
        return self.get_response(request)
