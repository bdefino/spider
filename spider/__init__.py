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
__package__ = __name__

import callback
from callback import BodyStorageCallback, Callback, DEFAULT_CALLBACK, \
    HeaderStorageCallback, StorageCallback, WebgraphStorageCallback
import htmlextract
from htmlextract import AttributeExtractor, extract_links, Extractor, \
    TagExtractor
from lib import threaded, uri
import spider
from spider import RequestFactory, Spider
import url
from url import DEFAULT_URL_CLASS

__doc__ = "simple web spidering"

if __name__ == "__main__":
    import Queue
    import sys
    sys.argv += ["http://google.com", "-n", "10", "-t", "0.5"]
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
    
    i = 1
    _callback = callback.DEFAULT_CALLBACK
    nthreads = 0
    request_factory = None
    _spider = None
    timeout = None
    url_queue = Queue.Queue()

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
                _callback = BodyStorageCallback(_db.DB(sys.argv[i + 1]))
                i += 1
            elif arg == "headers":
                if i == len(sys.argv) - 1:
                    print "Missing argument."
                    _help()
                    sys.exit()
                _callback = HeaderStorageCallback(_db.DB(sys.argv[i + 1]))
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
                _callback = callback.StorageCallback(_db.DB(sys.argv[i + 1]))
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
                _callback = callback.WebgraphStorageCallback(_db.DB(
                    sys.argv[i + 1]))
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
            url_queue.put(arg)
        i += 1

    if nthreads:
        _spider = spider.SlavingSpider(nthreads, url_queue, _callback,
            timeout = timeout)
    else:
        _spider = spider.Spider(url_queue, _callback, timeout = timeout)
    _spider()
