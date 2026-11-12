Testing
=======

Testing Channels consumers is a little trickier than testing normal Django
views due to their underlying asynchronous nature.

To help with testing, Channels provides test helpers called *Communicators*,
which allow you to wrap up an ASGI application (like a consumer) into its own
event loop and ask it questions.

`You can test asynchronous code
<https://docs.djangoproject.com/en/stable/topics/testing/tools/#testing-
asynchronous-code>`_ using Django's ``TestCase``. Alternately, you can use
``pytest`` with its
`pytest-asyncio <https://github.com/pytest-dev/pytest-asyncio>`_ plugin.


Setting Up Async Tests
----------------------

To use Django's ``TestCase`` you simply define an ``async def`` test method in
order to provide the appropriate async context::

    from django.test import TestCase
    from channels.testing import HttpCommunicator
    from myproject.myapp.consumers import MyConsumer

    class MyTests(TestCase):
        async def test_my_consumer(self):
            communicator = HttpCommunicator(MyConsumer.as_asgi(), "GET", "/test/")
            response = await communicator.get_response()
            self.assertEqual(response["body"], b"test response")
            self.assertEqual(response["status"], 200)

To use ``pytest`` you need to set it up with async test support, and
presumably Django test support as well. You can do this by installing the
``pytest-django`` and ``pytest-asyncio`` packages:

.. code-block:: sh

    python -m pip install -U pytest-django pytest-asyncio

Then, you need to decorate the tests you want to run async with
``pytest.mark.asyncio``. Note that you can't mix this with ``unittest.TestCase``
subclasses; you have to write async tests as top-level test functions in the
native ``pytest`` style:

.. code-block:: python

    import pytest
    from channels.testing import HttpCommunicator
    from myproject.myapp.consumers import MyConsumer

    @pytest.mark.asyncio
    async def test_my_consumer():
        communicator = HttpCommunicator(MyConsumer.as_asgi(), "GET", "/test/")
        response = await communicator.get_response()
        assert response["body"] == b"test response"
        assert response["status"] == 200

There's a few variants of the Communicator - a plain one for generic usage,
and one each for HTTP and WebSockets specifically that have shortcut methods,


ApplicationCommunicator
-----------------------

``ApplicationCommunicator`` is the generic test helper for any ASGI application.
It provides several basic methods for interaction as explained below.

You should only need this generic class for non-HTTP/WebSocket tests, though
you might need to fall back to it if you are testing things like HTTP chunked
responses or long-polling, which aren't supported in ``HttpCommunicator`` yet.

.. note::
    ``ApplicationCommunicator`` extends the class provided by the base ``asgiref``
    package. Channels adds support for running unit tests with async consumers.

To construct it, pass it an application and a scope:

.. code-block:: python

    from channels.testing import ApplicationCommunicator
    communicator = ApplicationCommunicator(MyConsumer.as_asgi(), {"type": "http", ...})

send_input
~~~~~~~~~~

Call it to send an event to the application:

.. code-block:: python

    await communicator.send_input({
        "type": "http.request",
        "body": b"chunk one \x01 chunk two",
    })

receive_output
~~~~~~~~~~~~~~

Call it to receive an event from the application:

.. code-block:: python

    event = await communicator.receive_output(timeout=1)
    assert event["type"] == "http.response.start"

.. _application_communicator-receive_nothing:

receive_nothing
~~~~~~~~~~~~~~~

Call it to check that there is no event waiting to be received from the
application:

.. code-block:: python

    assert await communicator.receive_nothing(timeout=0.1, interval=0.01) is False
    # Receive the rest of the http request from above
    event = await communicator.receive_output()
    assert event["type"] == "http.response.body"
    assert event.get("more_body") is True
    event = await communicator.receive_output()
    assert event["type"] == "http.response.body"
    assert event.get("more_body") is None
    # Check that there isn't another event
    assert await communicator.receive_nothing() is True
    # You could continue to send and receive events
    # await communicator.send_input(...)

The method has two optional parameters:

* ``timeout``: number of seconds to wait to ensure the queue is empty. Defaults
  to 0.1.
* ``interval``: number of seconds to wait for another check for new events.
  Defaults to 0.01.

wait
~~~~

Call it to wait for an application to exit (you'll need to either do this or wait for
it to send you output before you can see what it did using mocks or inspection):

.. code-block:: python

    await communicator.wait(timeout=1)

If you're expecting your application to raise an exception, use ``pytest.raises``
around ``wait``:

.. code-block:: python

    with pytest.raises(ValueError):
        await communicator.wait()


HttpCommunicator
----------------

``HttpCommunicator`` is a subclass of ``ApplicationCommunicator`` specifically
tailored for HTTP requests. You need only instantiate it with your desired
options:

.. code-block:: python

    from channels.testing import HttpCommunicator
    communicator = HttpCommunicator(MyHttpConsumer.as_asgi(), "GET", "/test/")

And then wait for its response:

.. code-block:: python

    response = await communicator.get_response()
    assert response["body"] == b"test response"

You can pass the following arguments to the constructor:

* ``method``: HTTP method name (unicode string, required)
* ``path``: HTTP path (unicode string, required)
* ``body``: HTTP body (bytestring, optional)

The response from the ``get_response`` method will be a dict with the following
keys:

* ``status``: HTTP status code (integer)
* ``headers``: List of headers as (name, value) tuples (both bytestrings)
* ``body``: HTTP response body (bytestring)


WebsocketCommunicator
---------------------

``WebsocketCommunicator`` allows you to more easily test WebSocket consumers.
It provides several convenience methods for interacting with a WebSocket
application, as shown in this example:

.. code-block:: python

    from channels.testing import WebsocketCommunicator
    communicator = WebsocketCommunicator(SimpleWebsocketApp.as_asgi(), "/testws/")
    connected, subprotocol = await communicator.connect()
    assert connected
    # Test sending text
    await communicator.send_to(text_data="hello")
    response = await communicator.receive_from()
    assert response == "hello"
    # Close
    await communicator.disconnect()

.. note::

    All of these methods are coroutines, which means you must ``await`` them.
    If you do not, your test will either time out (if you forgot to await a
    send) or try comparing things to a coroutine object (if you forgot to
    await a receive).

.. important::

    If you don't call ``WebsocketCommunicator.disconnect()`` before your test
    suite ends, you may find yourself getting ``RuntimeWarnings`` about
    things never being awaited, as you will be killing your app off in the
    middle of its lifecycle. You do not, however, have to ``disconnect()`` if
    your app already raised an error.

You can also pass an ``application`` built with ``URLRouter`` instead of the
plain consumer class. This lets you test applications that require positional
or keyword arguments in the ``scope``:

.. code-block:: python

    from channels.testing import WebsocketCommunicator
    application = URLRouter([
        path("testws/<message>/", KwargsWebSocketApp.as_asgi()),
    ])
    communicator = WebsocketCommunicator(application, "/testws/test/")
    connected, subprotocol = await communicator.connect()
    assert connected
    # Test on connection welcome message
    message = await communicator.receive_from()
    assert message == 'test'
    # Close
    await communicator.disconnect()

.. note::

    Since the ``WebsocketCommunicator`` class takes a URL in its constructor,
    a single Communicator can only test a single URL. If you want to test
    multiple different URLs, use multiple Communicators.

connect
~~~~~~~

Triggers the connection phase of the WebSocket and waits for the application
to either accept or deny the connection. Takes no parameters and returns
either:

* ``(True, <chosen_subprotocol>)`` if the socket was accepted.
  ``chosen_subprotocol`` defaults to ``None``.
* ``(False, <close_code>)`` if the socket was rejected.
  ``close_code`` defaults to ``1000``.

send_to
~~~~~~~

Sends a data frame to the application. Takes exactly one of ``bytes_data``
or ``text_data`` as parameters, and returns nothing:

.. code-block:: python

    await communicator.send_to(bytes_data=b"hi\0")

This method will type-check your parameters for you to ensure what you are
sending really is text or bytes.

send_json_to
~~~~~~~~~~~~

Sends a JSON payload to the application as a text frame. Call it with
an object and it will JSON-encode it for you, and return nothing:

.. code-block:: python

    await communicator.send_json_to({"hello": "world"})

receive_from
~~~~~~~~~~~~

Receives a frame from the application and gives you either ``bytes`` or
``text`` back depending on the frame type:

.. code-block:: python

    response = await communicator.receive_from()

Takes an optional ``timeout`` argument with a number of seconds to wait before
timing out, which defaults to 1. It will typecheck your application's responses
for you as well, to ensure that text frames contain text data, and binary
frames contain binary data.

receive_json_from
~~~~~~~~~~~~~~~~~

Receives a text frame from the application and decodes it for you:

.. code-block:: python

    response = await communicator.receive_json_from()
    assert response == {"hello": "world"}

Takes an optional ``timeout`` argument with a number of seconds to wait before
timing out, which defaults to 1.

receive_nothing
~~~~~~~~~~~~~~~

Checks that there is no frame waiting to be received from the application. For
details see
:ref:`ApplicationCommunicator <application_communicator-receive_nothing>`.

disconnect
~~~~~~~~~~

Closes the socket from the client side. Takes nothing and returns nothing.

You do not need to call this if the application instance you're testing already
exited (for example, if it errored), but if you do call it, it will just
silently return control to you.


ChannelsLiveServerTestCase
--------------------------

If you just want to run standard Selenium or other tests that require a
webserver to be running for external programs, you can use
``ChannelsLiveServerTestCase``, which is a drop-in replacement for the
standard Django ``LiveServerTestCase``:

.. code-block:: python

    from channels.testing import ChannelsLiveServerTestCase

    class SomeLiveTests(ChannelsLiveServerTestCase):

        def test_live_stuff(self):
            call_external_testing_thing(self.live_server_url)

.. note::

    If you use SQLite, include a test database file name in your settings to
    tell Django to use a file database:

    .. code-block:: python

        DATABASES = {
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": os.path.join(BASE_DIR, "db.sqlite3"),
                "TEST": {
                    "NAME": os.path.join(BASE_DIR, "db_test.sqlite3"),
                },
            },
        }

    In-memory SQLite test databases are passed to the server thread (as
    Django's own ``LiveServerTestCase`` does, sharing the connection between
    the two threads), but a file database is recommended, and is what
    Channels' own tests use, because the server thread shares the test
    database with the test process and closes its database connections when
    it stops.

The live server runs in a separate thread rather than a forked child process,
so it no longer has to spawn and synchronise a separate process, and
``manage.py test --parallel`` is supported.

static_handler
~~~~~~~~~~~~~~

Subclass ``ChannelsLiveServerTestCase`` with ``static_handler`` set in order
to customise how static files are served (Django's ``StaticLiveServerTestCase``
uses ``ASGIStaticFilesHandler``, which is also the default here, so you don't
need to run collectstatic before or as a part of your tests setup).

The ``serve_static`` and ``static_wrapper`` attributes are gone, replaced by
``static_handler``.

server_thread_class
~~~~~~~~~~~~~~~~~~~

The server runs in a ``threading.Thread`` built from ``server_thread_class``,
which defaults to ``channels.testing.live.ChannelsLiveServerThread``. Set it
on a subclass to customise how the server is started or stopped.

startup_timeout
~~~~~~~~~~~~~~~

``server_thread_class.startup_timeout`` (default: ``10``) is the number of
seconds ``setUpClass()`` waits for the server to report that it is listening.
If the server thread exits before then, its exception is raised instead.

shutdown_grace_period
~~~~~~~~~~~~~~~~~~~~~

``server_thread_class.shutdown_grace_period`` (default: ``5``) is how many
seconds a stopping server gives work which is already under way (in-flight
requests, and thread-sensitive work started alongside them) to finish before
the event loop is stopped.

Stopping the loop underneath such work is not survivable: work bridging back
into the loop stays blocked on a loop which will not run again, and the
process-wide thread-sensitive worker running it never returns to its queue.
Every later server in the same process would then queue behind that worker, so
the grace period is what keeps a teardown from creating that state. It is only
paid by a server which still has work in flight. Work that ignores cancellation
cannot be stopped by the grace period, and may leave a worker that must be
joined at interpreter exit.

shutdown_cancel_timeout
~~~~~~~~~~~~~~~~~~~~~~~

``server_thread_class.shutdown_cancel_timeout`` (default: ``5``) is how many
seconds work cancelled at teardown gets to unwind before the event loop is
closed underneath it. Closing the loop under pending work is not silent --
asyncio reports the destroyed task, and any exception never retrieved -- so that
work is cancelled and its outcome consumed first. Work which ignores
cancellation is left behind with its report silenced: a teardown never waits for
work which will not stop, and never prints anything.

quiesce
~~~~~~~

Django flushes the test database at the end of every test and stops the live
server only afterwards, so a request still running at that point would run its
next query against rows which no longer exist. The server thread's
``quiesce()`` is therefore called before the flush: it waits, for up to
``shutdown_grace_period``, for requests whose client has gone, then retires the
ones which are left. Only requests whose client has already gone are waited
for: a request whose client is still connected is left to finish against the
flushed test database, a deliberate trade-off rather than an oversight.

Quiescing is part of the default thread, not of the extension point: a custom
``server_thread_class`` need only provide the ``threading.Thread`` protocol plus
``terminate()``, and is asked to quiesce only if it defines ``quiesce()``.

One live server per process
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The live server takes ownership of the process-global state its reactor needs:
the installed Twisted reactor, the event loop the reactor runs on, asgiref's
process-wide thread-sensitive executor, and ``txaio``'s cached reactor. It
installs or resets each of them when it starts, and restores or replaces each of
them when it stops, so only one live server can run in a process at a time: a
second one started while the first is still serving interferes with it. A test
runner starts one at a time in each process, so an ordinary class -- or a
``--parallel`` worker -- is unaffected.

Private dependencies
~~~~~~~~~~~~~~~~~~~~

The thread-based live server builds on these private interfaces, which are
therefore required at these minimum versions:

* ``daphne.testing._reinstall_reactor`` (daphne 4.0+), for a reactor of its own
  per server thread.
* ``daphne.server.Server.connections``, ``WSProtocol.muted`` and
  ``WSProtocol.application_queue`` (daphne 4.0+), for draining in-flight
  requests and retiring open websockets.
* ``reactor._asyncioEventloop`` (Twisted's asyncio reactor), for closing the
  loop the reactor ran on.
* ``SyncToAsync.single_thread_executor`` (asgiref 3.9+), for giving each server
  its own thread-sensitive executor. Without it the server logs why, and runs
  without that isolation.

