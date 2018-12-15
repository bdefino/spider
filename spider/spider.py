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
from lib import disque, threaded
import requestfactory
import url

__doc__ = "web spiders"

class Spider:
    """
    a web spider (by default, single-threaded)
    which gets/puts URLs to/from the url_queue (which should implement the
    native Python url_queue API)
    """
    
    def __init__(self, url_queue = None, callback = callback.DEFAULT_CALLBACK,
            request_factory = requestfactory.RequestFactory(),
            url_class = url.DEFAULT_URL_CLASS, *urlopen_args,
            **urlopen_kwargs):
        self.callback = callback
        self.request_factory = request_factory
        self.url_class = url_class # this should be (a subclass of) uri.URL
        self.urlopen_args = urlopen_args
        self.urlopen_kwargs = urlopen_kwargs

        if not url_queue:
            url_queue = disque.Disque("queue", chunk_size = 2048) # for speed
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
        """crawl and return whether to continue"""
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

class BlockingSpider(Spider):
    """a spider that blocks until a new worker thread can be allocated"""
    
    def __init__(self, nthreads = 1, *args, **kwargs):
        Spider.__init__(self, *args, **kwargs)
        self.ntasks = threaded.Synchronized(0)
        self._threaded = threaded.Blocking(nthreads, True)

    def __call__(self):
        """continually crawl until told otherwise"""
        try:
            while not self.url_queue.empty() or self.ntasks.get() > 0:
                try:
                    self._threaded.put(self._handle_handle_url,
                        self.url_queue.get())
                except ValueError:
                    continue
                self.ntasks.transform(lambda n: n + 1)
        except KeyboardInterrupt:
            pass

    def _handle_handle_url(self, url):
        """
        crawl and modify the number of tasks
        to signal whether to continue
        """
        if not self.handle_url(url): # signal exit
            self.ntasks.set(0) # offset of -1 from 0 accounts for loop
        self.ntasks.transform(lambda n: n - 1)
