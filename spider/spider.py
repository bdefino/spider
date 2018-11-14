# Copyright 2018 Bailey Defino
# <https://bdefino.github.io>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import Queue
import socket
import ssl
import time
import urllib2

import callback
from lib import threaded
import url

__doc__ = "web spiders"

class RequestFactory:
    """a callable for creating factory requests"""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, url):
        return urllib2.Request(url, *self.args, **self.kwargs)

class Spider:
    """
    a web spider (by default, single-threaded)
    which gets/puts URLs to/from the url_queue (which should implement the
    native Python url_queue API)
    """
    
    def __init__(self, url_queue = None, callback = None,
            request_factory = None, url_class = None, *urlopen_args,
            **urlopen_kwargs):
        if not callback:
            callback = callback.DEFAULT_CALLBACK
        self.callback = callback

        if not request_factory:
            request_factory = RequestFactory()
        self.request_factory = request_factory

        if not url_class:
            url_class = url.DEFAULT_URL_CLASS
        self.url_class = url_class # this should be (a subclass of) uri.URL
        self.urlopen_args = urlopen_args
        self.urlopen_kwargs = urlopen_kwargs

        if not url_queue:
            url_queue = Queue.Queue()
        self.url_queue = url_queue

    def __call__(self):
        """continually crawl until told otherwise"""
        try:
            while not self.url_queue.empty() \
                    and self.handle_url(self.url_queue.get()):
                pass
        except KeyboardInterrupt:
            pass

    def __enter__(self):
        if hasattr(self.url_queue, "__enter__"):
            getattr(self.url_queue, "__enter__")()

    def __exit__(self, *exception):
        if hasattr(self.url_queue, "__exit__"):
            getattr(self.url_queue, "__exit__")()

    def handle_url(self, url):
        try:
            response = urllib2.urlopen(self.request_factory(url),
                *self.urlopen_args, **self.urlopen_kwargs)
            _continue, links = self.callback(response)
        except (socket.error, ssl.SSLError, urllib2.HTTPError,
                urllib2.URLError):
            return True

        for l in links:
            self.url_queue.put(l)
        return _continue

class ThreadedSpider(Spider, threaded.Threaded):
    def __init__(self, url_queue = None, callback = None, nthreads = 1,
            request_factory = None, url_class = None, *args, **kwargs):
        Spider.__init__(self, url_queue, callback, request_factory, url_class,
            *args, **kwargs)
        threaded.Threaded.__init__(self, nthreads, True)

    def __call__(self):
        try:
            while self.empty() or self.get().output:
                if self.url_queue.empty() and not self.nactive_threads.get():
                    break
                self.execute(self.handle_url, self.url_queue.get())
        except KeyboardInterrupt:
            pass
