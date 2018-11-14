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
import StringIO
import threading

from lib import db as _db
from lib import withfile
import url

__doc__ = "spider callbacks"

global DEFAULT_CALLBACK

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
            url_class = url.DEFAULT_URL_CLASS
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
