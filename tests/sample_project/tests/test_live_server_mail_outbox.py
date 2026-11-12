"""Regression: mail sent by a live-served view lands in the test's outbox.

``ChannelsLiveServerTestCase`` used to serve requests from a forked server
process, so the locmem email backend filled ``mail.outbox`` over there and the
test process saw an empty outbox -- a test could never assert on mail its own
views sent (#2194). The server now shares the test process, so what the view
sends is what the test reads.

The assertion below is the regression: it fails whenever the request is served
by a process other than the test's own.
"""

import urllib.request

from django.core import mail
from django.core.mail import send_mail
from django.http import HttpResponse
from django.test.utils import override_settings
from django.urls import path

from channels.testing import ChannelsLiveServerTestCase
from channels.testing.live import _enter_class_context

#: What the view sends, and what the test expects to read back.
MAIL_SUBJECT = "mail from the live server"
MAIL_RECIPIENT = "outbox@example.com"


def send_mail_view(request):
    """Sync view which sends one message, as a deployed project's view would."""
    send_mail(MAIL_SUBJECT, "Sent while serving a request.", None, [MAIL_RECIPIENT])
    return HttpResponse("sent")


urlpatterns = [path("send-mail/", send_mail_view)]


class LiveServerMailOutboxTest(ChannelsLiveServerTestCase):
    """Mail sent while serving must reach the outbox of the test process."""

    @classmethod
    def setUpClass(cls):
        _enter_class_context(cls, override_settings(ROOT_URLCONF=__name__))
        super().setUpClass()

    def test_mail_sent_by_view_lands_in_test_outbox(self):
        url = f"{self.live_server_url}/send-mail/"
        with urllib.request.urlopen(url, timeout=10) as response:
            self.assertEqual(response.status, 200)
        # The test runner empties the outbox before every test, so this count
        # is the view's mail alone.
        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.subject, MAIL_SUBJECT)
        self.assertEqual(message.to, [MAIL_RECIPIENT])
