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
import os
import StringIO
import threading

import htmlextract
from lib import db as _db
import url

__doc__ = "spider callbacks"

global DEFAULT_CALLBACK

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
                [str(url.bind(l)) for l in htmlextract.extract_links(
                    response.info(), response.read())]

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
        body = StringIO.StringIO(response.read())
        
        def seek_set_when_read(n = None):
            """
            go back to the beginning when the contents are exhausted

            this is especially useful for getting around the issue
            of multiple reads on a socket._fileobject
            """
            content = body.read(n)
            pos = seek_to = body.tell()
            body.seek(0, os.SEEK_END)

            if pos == body.tell():
                seek_to = 0
            body.seek(seek_to, os.SEEK_SET)
            return content

        response.read = seek_set_when_read
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
    store the webgraph node as a JSON list

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
        """return a JSON list of links"""
        return json.dumps(links)
