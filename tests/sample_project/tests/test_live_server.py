import urllib.request

from django.db import connections
from django.http import HttpResponse
from django.test.utils import override_settings
from django.urls import path

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import _enter_class_context


def database_name_view(request):
    """
    Report the database name used by the connection handling this request.

    The request is served by the live server, so this value exposes which
    database the server-side connections are bound to.
    """
    return HttpResponse(str(connections["default"].settings_dict["NAME"]))


# URLconf for the database name test below. It is installed with
# override_settings(ROOT_URLCONF=__name__).
urlpatterns = [
    path("db-name/", database_name_view),
]


class LiveServerDatabaseNameTest(ChannelsLiveServerTestCase):
    """The live server must use the same test database as the test process."""

    @classmethod
    def setUpClass(cls):
        # Capture the test database name before the server thread is started:
        # the thread must not rewrite it (#2194).
        cls.expected_database_name = connections["default"].settings_dict["NAME"]
        _enter_class_context(cls, override_settings(ROOT_URLCONF=__name__))
        super().setUpClass()

    def test_server_uses_test_database(self):
        url = f"{self.live_server_url}/db-name/"
        with urllib.request.urlopen(url) as response:
            server_database_name = response.read().decode()
        self.assertEqual(server_database_name, str(self.expected_database_name))
        self.assertEqual(
            server_database_name,
            str(connections["default"].settings_dict["NAME"]),
        )


class LiveServerFirstTest(ChannelsLiveServerTestCase):
    def test_url_serves(self):
        url = f"{self.live_server_url}/admin/login/"
        with urllib.request.urlopen(url) as response:
            self.assertEqual(response.status, 200)


class LiveServerSecondTest(ChannelsLiveServerTestCase):
    def test_url_serves(self):
        url = f"{self.live_server_url}/admin/login/"
        with urllib.request.urlopen(url) as response:
            self.assertEqual(response.status, 200)
