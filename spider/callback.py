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
import json
import os
import StringIO
import threading

import htmlextract
from lib import db as _db
import rule
import url

__doc__ = "spider callbacks"

global DEFAULT_CALLBACK

class Callback:
    """
    the base class for a callback, which both extracts links
    and tells the spider whether to continue (similarly to ftw and nftw in C)
    """
    
    def __init__(self, url_class = None, rules = (), depth = -1):
        self.depth = 0
        self.depth_remaining = depth
        self._lock = threading.RLock() # automatically determines blocking
        self.rules = rules
        
        if not url_class:
            url_class = url.DEFAULT_URL_CLASS
        self.url_class = url_class

    def _apply_rules(self, link):
        """
        apply the rules to a link, with short-circuit execution;
        return whether the link satisfies the rules
        """
        for rule in self.rules:
            if not rule(link):
                return False
        return True

    def __call__(self, response):
        """must return a tuple as such: (continue?, links)"""
        with self._lock:
            self.depth += 1
            self.depth_remaining -= 1
            url = self.url_class(response.url)

            if __debug__:
                print "(%u)" % self.depth, url
            links = [str(url.bind(l)) for l in htmlextract.extract_links(
                response.info(), response.read())]
            return not self.depth == 0, \
                filter(self._apply_rules, links) # save queue space

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
        sio = StringIO.StringIO(response.read())
        response.read = sio.read # bypass socket._fileobject restrictions
        _continue, links = Callback.__call__(self, response)
        
        if _continue: # store
            sio.seek(0, os.SEEK_SET)
            self.db[self._generate_id(response)] = self._generate_data(
                response)
        return _continue, links

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
        self.db[self._generate_id(response)] = self._generate_data(links)
        return _continue, links

    def _generate_data(self, links):
        """return a JSON list of links"""
        return json.dumps(links)
