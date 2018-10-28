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
import csv
import HTMLParser
import os
import Queue
import StringIO
import sys
import threading
import time
import urllib2

sys.path.append(os.path.realpath(__file__))

import db as _db
import threaded
import uri
import webgraphdb
import withfile

__doc__ = "web spiders"

global DEFAULT_CALLBACK

global DEFAULT_URL_CLASS
DEFAULT_URL_CLASS = uri.URL

def _extract_links(header, body):
    """parse links from an HTTP response"""
    parser = _DefaultHTMLParser()
    
    try:
        if header.has_key("Content-Type"):
            for k_v in header["Content-Type"].split(';'):
                if not '=' in k_v:
                    continue
                k, v = [e.strip() for e in k_v.split('=')]

                if k.lower() == "charset":
                    try:
                        body = body.decode(v)
                        break
                    except ValueError:
                        pass
        parser.feed(body)
        parser.close()
    except KeyboardInterrupt:
        raise KeyboardInterrupt()
    return parser._urls

def _help():
    """print help text"""
    print "a basic web spider\n" \
          "Usage: python spider.py [OPTIONS] URLS\n" \
          "OPTIONS\n" \
          "\t\t--bodies PATH\tstore response bodies to a database\n" \
          "\t-h, --help\tshow this text and exit\n" \
          "\t\t--headers PATH\tstore response headers to a database\n" \
          "\t-n, --nthreads INT\tthe number of concurrent threads\n" \
          "\t-t, --timeout FLOAT\tthe timeout\n" \
          "\t\t--webgraph PATH\tstore webgraph to a database\n" \
          "URLS\n" \
          "\ta list of URLs"

def main():
    """run the spider"""
    i = 1
    callback = DEFAULT_CALLBACK
    nthreads = 0
    queue = Queue.Queue()
    request_factory = None
    spider = None
    timeout = None

    if len(sys.argv) < 2:
        _help()
        sys.exit()

    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg.startswith("--"):
            arg = arg[2:]

            if arg == "bodies":
                if i == len(sys.argv) - 1:
                    print "Missing argument."
                    _help()
                    sys.exit()
                callback = BodyStorageCallback(_db.DB(sys.argv[i + 1]))
                i += 1
            elif arg == "headers":
                if i == len(sys.argv) - 1:
                    print "Missing argument."
                    _help()
                    sys.exit()
                callback = HeaderStorageCallback(_db.DB(sys.argv[i + 1]))
                i += 1
            elif arg == "help":
                _help()
                sys.exit()
            elif arg == "nthreads":
                if i == len(sys.argv) - 1:
                    print "Missing argument."
                    _help()
                    sys.exit()

                try:
                    nthreads = int(sys.argv[i + 1])
                except ValueError:
                    pass
                i += 1
            elif arg == "responses":
                if i == len(sys.argv) - 1:
                    print "Missing argument."
                    _help()
                    sys.exit()
                callback = StorageCallback(_db.DB(sys.argv[i + 1]))
                i += 1
            elif arg == "timeout":
                if i == len(sys.argv) - 1:
                    print "Missing argument."
                    _help()
                    sys.exit()

                try:
                    timeout = float(sys.argv[i + 1])
                except ValueError:
                    pass
                i += 1
            elif arg == "webgraph":
                if i == len(sys.argv) - 1:
                    print "Missing argument."
                    _help()
                    sys.exit()
                callback = WebgraphStorageCallback(_db.DB(sys.argv[i + 1]))
                i += 1
            else:
                print "Invalid argument."
                _help()
                sys.exit()
        elif arg.startswith('-'):
            arg = arg[1:]

            for c in arg:
                if c == 'h':
                    _help()
                    sys.exit()
                elif c == 'n':
                    if i == len(sys.argv) - 1:
                        print "Missing argument."
                        _help()
                        sys.exit()

                    try:
                        nthreads = int(sys.argv[i + 1])
                    except ValueError:
                        pass
                    i += 1
                elif c == 't':
                    if i == len(sys.argv) - 1:
                        print "Missing argument."
                        _help()
                        sys.exit()

                    try:
                        timeout = float(sys.argv[i + 1])
                    except ValueError:
                        pass
                    i += 1
                else:
                    print "Invalid flag."
                    _help()
                    sys.exit()
        elif arg:
            queue.put(arg)
        i += 1

    if nthreads:
        spider = ThreadedSpider(callback = callback, nthreads = nthreads,
            queue = queue, timeout = timeout)
    else:
        spider = Spider(callback = callback, queue = queue, timeout = timeout)
    spider()

class Callback:
    """
    the base class for a callback, which both extracts links
    and tells the spider whether to continue (similarly to ftw and nftw in C)
    """
    
    def __init__(self, url_class = None, depth = -1):
        self.depth = 0
        self.depth_remaining = depth
        self._lock = threading.RLock() # automatically determines blocking
        
        if not url_class:
            url_class = DEFAULT_URL_CLASS
        self.url_class = url_class

    def __call__(self, response):
        """must return a tuple as such: (continue?, links)"""
        with self._lock:
            self.depth += 1
            self.depth_remaining -= 1
            url = self.url_class(response.url)

            if __debug__:
                print "(%u)" % self.depth, url
            return not self.depth == 0, \
                [str(url.bind(l)) for l in _extract_links(response.info(),
                    response.read())]

DEFAULT_CALLBACK = Callback()

class StorageCallback(Callback):
    """
    a Callback capable of storage via a db.DB instance

    _generate_data is intended to be overridden,
    however the same may be done for _generate_id

    default behavior is to store the full packet
    """
    
    def __init__(self, db, *args, **kwargs):
        Callback.__init__(self, *args, **kwargs)

        assert isinstance(db, _db.DB), "db must be a db.DB instance"
        self.db = db
        self.db.__enter__()

    def __call__(self, response):
        self.db[self._generate_id(response)] = self._generate_data(response)
        return Callback.__call__(self, response)

    def __del__(self):
        self.db.__exit__()

    def _generate_data(self, response):
        """return data from the response"""
        return bytearray().join((str(response.info()), "\r\n\r\n",
            response.read()))

    def _generate_id(self, response):
        """return an ID for a response"""
        return response.url

class BodyStorageCallback(StorageCallback):
    def __init__(self, *args, **kwargs):
        StorageCallback.__init__(self, *args, **kwargs)

    def _generate_data(self, response):
        return response.read()

class HeaderStorageCallback(StorageCallback):
    def __init__(self, *args, **kwargs):
        StorageCallback.__init__(self, *args, **kwargs)

    def _generate_data(self, response):
        return str(response.info())

class WebgraphStorageCallback(StorageCallback):
    """
    store the webgraph node as a CSV list

    note that the _generate_data function takes a list of links
    instead of a response
    """
    
    def __init__(self, *args, **kwargs):
        StorageCallback.__init__(self, *args, **kwargs)

    def __call__(self, response):
        _continue, links = Callback.__call__(self, response)
        self.db[self._generate_id(response)] = self._generate_data(response)
        return _continue, links

    def _generate_data(self, links):
        """return a CSV list of links"""
        fp = StringIO.StringIO()
        writer = csv.writer(fp)

        for l in links:
            writer.writerow([l])
        doc = fp.getvalue()
        fp.close()
        return doc

class _DefaultHTMLParser(HTMLParser.HTMLParser):
    """HTML link parser (href attributes only)"""
    
    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self._urls = []
    
    def handle_starttag(self, tag, attrs):
        """override HTMLParser.HTMLParser.handle_starttag"""
        for a, v in attrs:
            if a.lower().strip() == "href":
                self._urls.append(v)

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
    which gets/puts URLs to/from the queue (which should implement the
    native Python queue API)
    """
    
    def __init__(self, queue = None, callback = None, request_factory = None,
            url_class = None, *urlopen_args, **urlopen_kwargs):
        if not callback:
            callback = DEFAULT_CALLBACK
        self.callback = callback
        
        if not queue:
            queue = Queue.Queue()
        self.queue = queue

        if not request_factory:
            request_factory = RequestFactory()
        self.request_factory = request_factory

        if not url_class:
            url_class = DEFAULT_URL_CLASS
        self.url_class = url_class # this should be (a subclass of) uri.URL
        self.urlopen_args = urlopen_args
        self.urlopen_kwargs = urlopen_kwargs

    def __call__(self):
        """continually crawl until told otherwise"""
        try:
            while not self.queue.empty() and self.handle_url(self.queue.get()):
                pass
        except KeyboardInterrupt:
            pass

    def __enter__(self):
        if hasattr(self.queue, "__enter__"):
            getattr(self.queue, "__enter__")()

    def __exit__(self, *exception):
        if hasattr(self.queue, "__exit__"):
            getattr(self.queue, "__exit__")()

    def handle_url(self, url):
        try:
            response = urllib2.urlopen(self.request_factory(url),
                *self.urlopen_args, **self.urlopen_kwargs)
        except (urllib2.HTTPError, urllib2.URLError): # ignore errors
            return True
        _continue, links = self.callback(response)

        for l in links:
            self.queue.put(l)
        return _continue

class ThreadedSpider(Spider, threaded.Threaded):
    def __init__(self, queue = None, callback = None, nthreads = 1,
            request_factory = None, url_class = None, *args, **kwargs):
        Spider.__init__(self, queue, callback, request_factory, url_class,
            *args, **kwargs)
        threaded.Threaded.__init__(self, nthreads, True)

    def __call__(self):
        try:
            while self.output_queue.empty() \
                    or self.output_queue.get().output:
                with self._nactive_threads_lock:
                    if self.queue.empty() and not self.nactive_threads:
                        break
                self.execute(self.handle_url, self.queue.get())
        except KeyboardInterrupt:
            pass
        # can't efficiently wait for termination of all child threads

if __name__ == "__main__":
    main()
