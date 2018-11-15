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
import HTMLParser
import Queue

__doc__ = "basic HTML extraction"

def extract_links(header, body, src = False):
    """convenience function to parse links from an HTTP response"""
    links = []
    parser = AttributeExtractor("href", "src")
    
    try:
        parser.feed(body, header)
        parser.close()
    except KeyboardInterrupt:
        raise KeyboardInterrupt()

    while 1:
        try:
            links.append(parser.get_nowait()[1])
        except Queue.Empty:
            break
    return links

class Extractor(HTMLParser.HTMLParser, Queue.Queue):
    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        Queue.Queue.__init__(self)

    def feed(self, body, header = None):
        """attempt to decode the HTML body before feeding"""
        if header and header.has_key("Content-Type"):
            for k_v in header["Content-Type"].split(';'):
                if not '=' in k_v:
                    continue
                k, v = [e.strip() for e in k_v.split('=', 1)]

                if not k.lower() == "charset":
                    continue
                
                try:
                    body = body.decode(v)
                    break
                except ValueError:
                    pass
        HTMLParser.HTMLParser.feed(self, body)

class AttributeExtractor(Extractor):
    def __init__(self, *attrs):
        Extractor.__init__(self)
        self.attrs = [a.lower().strip() for a in attrs]
    
    def handle_starttag(self, tag, attrs):
        for a, v in attrs:
            if a.lower().strip() in self.attrs:
                self.put((a, v))

class TagExtractor(Extractor):
    def __init__(self, *tags):
        Extractor.__init__(self)
        self.tags = [t.lower() for t in tags]
    
    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.tags:
            self.put((tag, attrs))
