# Copyright (C) 2018 Bailey Defino
# <https://hiten2.github.io>

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
import socket

__doc__ = """
superior URI parsing and representation

loosely based on RFC documents, with a focus on flexibility
"""

global COMMONLY_ENCODED_CHARS # for reference only
COMMONLY_ENCODED_CHARS = "\t\n \"%-.<>\\^_`{|}~"

global RESERVED_CHARS
RESERVED_CHARS = "!#$&'()*+,/:;=?@[]"

def decode(string, chars = None, avoid = None):
    """
    URI-decode a string

    if which chars are unspecified, encode reserved characters only
    """
    as_list = []
    _decode = lambda s: chr(int(s[1:], 16))
    i = 0

    while i < len(string):
        substring = string[i:i + 3]
        
        try:
            c = _decode(substring)
        except ValueError:
            as_list.append(string[i])
            i += 1
            continue
        
        if avoid and substring in avoid:
            as_list.append(string[i])
        elif (chars and c in chars) or (not chars and c in RESERVED_CHARS):
            as_list.append(c)
            i += 2
        i += 1
    return ''.join(as_list)

def encode(string, chars = None, avoid = None):
    """
    URI-encode a string

    if which chars are unspecified, encode reserved characters only
    """
    as_list = list(string)
    _encode = lambda c: '%' + hex(ord(c))[2:].zfill(2)

    for i, c in enumerate(as_list):
        if avoid and c in avoid:
            continue
        elif (chars and c in chars) or (not chars and c in RESERVED_CHARS):
            as_list[i] = _encode(c)
    return ''.join(as_list)

def remove_redundancies(path):
    """remove redundancies from a path"""
    new_path = str(path)
    
    while "//" in new_path:
        new_path = new_path.replace("//", '/')
    return new_path

def resolve(path):
    """
    resolve a path

    similar to os.path.normpath
    """
    resolved = path.split('/')

    i = len(resolved) - 1
    
    while '.' in resolved:
        resolved.remove('.')
    absolute = not resolved or resolved[0] == ''
    
    while ".." in resolved:
        i = resolved.index("..")
        del resolved[i]
        
        if i > 0:
            del resolved[i - 1]
    
    if absolute:
        resolved.insert(0, '')
    del absolute
    return remove_redundancies('/'.join(resolved))

class CSVFlavor:
    """
    basic flavoring for a CSV document

    note that the quote and sep attributes are individual characters
    """

    def __init__(self, newline = "\r\n", quotechar = '\"', sepchar = ','):
        self.newline = newline
        self.quotechar = quotechar
        self.sepchar = sepchar

class CSV(list):
    """an RFC 4180-compliant CSV implementation"""

    def __init__(self, csv = None, flavor = None):
        if not flavor:
            flavor = CSVFlavor()
        self.flavor = flavor
        
        if type(csv) in (list, tuple):
            list.__init__(self, csv)

            if not self or not type(self[0]) in (list, tuple):
                self = [self]
            return
        elif not isinstance(csv, str):
            raise TypeError("csv must be a list, tuple, or string")
        list.__init__(self)
        
        entry = ''
        esc = False
        i = 0
        row = []
        
        while i < len(csv):
            c = csv[i]
            
            if esc:
                if csv[i:i + 1] == self.flavor.quotechar:
                    if (i == len(csv) - 1
                            or not csv[i + 1:i + 2] == self.flavor.quotechar):
                        esc = False
                    else:
                        entry += c
                        i += 1
                else:
                    entry += c
            elif csv[i:i + 1] == self.flavor.quotechar:
                esc = True
            elif csv[i:i + len(self.flavor.newline)] == self.flavor.newline:
                self.append(row)
                i += len(newline)
                row = []
            elif csv[i:i + 1] == self.flavor.sepchar:
                row.append(entry)
                entry = ''
            else:
                entry += c
            i += 1

        if entry:
            row.append(entry)
        
        if row:
            self.append(row)

    def __str__(self):
        lines = []

        for row in self: # escape values
            line = []

            for entry in row:
                entry = str(entry)
                
                if (self.flavor.newline in entry
                        or self.flavor.quotechar in entry
                        or self.flavor.sepchar in entry):
                    entry = "%s%s%s" % (self.flavor.quotechar,
                        entry.replace(self.flavor.quotechar,
                            2 * self.flavor.quotechar), self.flavor.quotechar)
                line.append(entry)
            lines.append(self.flavor.sepchar.join(line))
        return self.flavor.newline.join(lines)

class Query(dict):
    """a URI-safe query string"""

    def __init__(self, query = None):
        if isinstance(query, dict):
            dict.__init__(self, query)
            return
        elif not isinstance(query, str):
            raise TypeError("query must be a dictionary or a string")
        dict.__init__(self)
        
        for var in query.split('&'):
            if var:
                k = var
                v = None
                
                if '=' in var:
                    k, v = [e.strip() for e in var.split('=')]
                self[k] = v

    def __setitem__(self, key, value):
        if self.has_key(key):
            if not isinstance(self[key], list):
                dict.__setitem__(self, key, [self[key]])
            self[key].append(value)
        else:
            dict.__setitem__(self, key, value)
    
    def __str__(self):
        """
        return a string representation
        but don't add '=' after keys with a value of None
        """
        as_list = []

        for k in sorted(self.keys()):
            vs = self[k]

            if not isinstance(vs, list):
                vs = [vs]
            k = str(k)
            
            for v in vs:
                if v == None:
                    as_list.append(encode(k))
                else:
                    as_list.append(encode(k) + '=' + encode(str(v)))
        return '&'.join(as_list)

class URI:
    """
    RFC 3986's universal resource identifier

    note that subclasses fully override the __str__ function
    """

    def __init__(self, uri = ''):
        if not isinstance(uri, str):
            raise TypeError("uri must be a string")
        self.fragment = None
        self.query = None
        self.scheme = None
        # [scheme:][hier-part][?query][#fragment]

        for a, s in (("fragment", '#'), ("query", '?')):
            if s in uri:
                setattr(self, a, uri[uri.find(s) + 1:])
                uri = uri[:uri.find(s)]

        if not self.query == None:
            self.query = Query(self.query)
        # [scheme:][hier-part]
        
        if ':' in uri and not '.' in uri[:uri.find(':')]:
            self.scheme = uri[:uri.find(':')]
            uri = uri[uri.find(':') + 1:]
        # [hier-part]
        
        self.hier_part = uri

    def __str__(self):
        as_list = []
        
        if not self.scheme == None:
            as_list.append(encode(self.scheme))
            as_list.append(':')
        # [scheme:]

        if self.hier_part:
            as_list.append(self.hier_part)
        # [scheme:][hier-part]
        
        for a, s in (("query", '?'), ("fragment", '#')):
            if not getattr(self, a) == None:
                as_list.append(s)
                as_list.append(getattr(self, a))
        # [scheme:][hier-part][?query][#fragment]
        return ''.join((str(e) for e in as_list))

class SMS(URI):
    """
    the SMS URI as per RFC 5724

    note that the specification allows or a maximum of 1 value for the body
    """
    
    def __init__(self, uri = ''):
        # sms:[recipients][?query]
        
        URI.__init__(self, uri)
        assert self.query == None or isinstance(self.query["body"], str), \
            "query must have at most 1 body"
        assert self.scheme == "sms", "sms scheme must be \"sms\""
        # [recipients]
        
        self.recipients = CSV(self.hier_part)

    def __str__(self):
        as_list = []
        
        if not self.scheme == None:
            as_list.append(encode(self.scheme))
            as_list.append(':')
        # [scheme:]

        if self.recipients:
            as_list.append(CSV(list(self.recipients)))
        # [scheme:][recipients]
        
        for a, s in (("query", '?'), ("fragment", '#')):
            if not getattr(self, a) == None:
                as_list.append(s)
                as_list.append(getattr(self, a))
        # [scheme:][recipients][?query][#fragment]
        return ''.join((str(e) for e in as_list))

class TEL(URI):
    """the telephone URI as per RFC 3966"""

    def __init__(self, uri = ''):
        # tel:[recipients]
        
        URI.__init__(self, uri)
        assert self.scheme == "tel", "tel scheme must be \"tel\""
        # [recipients]
        
        self.recipients = CSV(self.hier_part)

    def __str__(self):
        as_list = []
        
        if not self.scheme == None:
            as_list.append(encode(self.scheme))
            as_list.append(':')
        # [scheme:]

        if self.recipients:
            as_list.append(CSV(list(self.recipients)))
        # [scheme:][recipients]
        
        for a, s in (("query", '?'), ("fragment", '#')):
            if not getattr(self, a) == None:
                as_list.append(s)
                as_list.append(getattr(self, a))
        # [scheme:][recipients][?query][#fragment]
        return ''.join((str(e) for e in as_list))

class URL(URI):
    """
    a highly configurable URL that expands upon RFC 1738

    fully overrides URI parsing
    """
    attributes = [
        "domains",
        "fragment",
        "ip_version",
        "parameters",
        "password",
        "path",
        "port",
        "query",
        "scheme",
        "scheme_is_protocol",
        "username"
        ]
    
    def __init__(self, url = ''):
        URI.__init__(self) # override parsing
        
        self.domains = []
        self.fragment = None
        self.ip_version = None
        self.parameters = None
        self.password = None
        self.path = None
        self.port = None
        self.query = None
        self.scheme = None
        self.scheme_is_protocol = False
        self.username = None
        
        # [scheme:][//][user[:password]@][host[:port]]
        # [/path][;parameters][?query][#fragment]

        for a, s in (("fragment", '#'), ("query", '?'), ("parameters", ';')):
            if s in url:
                setattr(self, a, url[url.find(s) + 1:])
                url = url[:url.find(s)]
        # [scheme:][//][user[:password]@][host[:port]][/path]

        if "//" in url:
            self.scheme_is_protocol = True
            url = url[:url.find("//")] + url[url.find("//") + 2:]
        # [scheme:][user[:password]@][host[:port]][/path]
        
        if '/' in url:
            self.path = url[url.find('/'):]
            url = url[:url.find('/')]
        # [scheme:][user[:password]@][host[:port]]

        if ':' in url and not '.' in url[:url.find(':')]:
            self.scheme = url[:url.find(':')]
            url = url[url.find(':') + 1:]

            try:
                self.port = socket.getservbyname(self.scheme)
            except socket.error:
                pass
        # [user[:password]@][host[:port]]
        
        if '@' in url:
            if ':' in url[:url.find('@')]:
                self.password = url[url.find(':') + 1:url.find('@')]
                url = url[:url.find(':')] + url[url.find('@'):]
            # [user@][host[:port]]
            
            self.username = url[:url.find('@')]
            url = url[url.find('@') + 1:]
        # [host[:port]]
        self.ip_version = 4
        
        if '[' in url:
            self.ip_version = 6
            
            if ']' in url:
                self.domains = url[
                    url.find('[') + 1:url.find(']')
                    ].split(':')
                url = url[:url.find('[')] + url[url.find(']') + 1:]
            else:
                raise SyntaxError("invalid domain syntax: \"%s\"" % str(url))
        elif not url in "..":
            if ':' in url:
                self.domains = url[:url.find(':')]
                url = url[url.find(':'):]
            else:
                self.domains = url
                url = ''

            if '.' in self.domains:
                self.domains = self.domains.split('.')
            else:
                d = ''
                p = ''
                
                if self.domains:
                    d = self.domains

                if self.path:
                    p = self.path
                self.path = d + p
                self.domains = []
                del d
                del p
        
        if self._domains().startswith('.'):
            self.path = self._domains() + self.path
            self.domains = []
        url = url.strip()
        # [:port]
        
        if url.startswith(':'):
            try:
                self.port = int(url[1:].strip())
                url = ''
            except ValueError:
                pass
        
        if url:
            if self.path:
                self.path = '/'.join((e for e
                    in (url, self.path) if e))
            else:
                self.path = url
        return

    def bind(self, child):
        """return a URL instance equivalent to self + child"""
        bound = URL(child)
        child = URL(child)

        if bound.scheme == None:
            bound.scheme = self.scheme
            bound.scheme_is_protocol = (self.scheme_is_protocol
                or child.scheme_is_protocol)

            if not bound.domains:
                bound.domains = self.domains

                if not bound.port:
                    bound.port = self.port
                
                if not bound.path or not bound.path.startswith('/'):
                    paths = []
                    
                    if self.path:
                        paths.append(self.path[:self.path.rfind('/')])

                    if child.path:
                        paths.append(child.path)
                    bound.path = '/'.join(paths)
        return bound

    def _domains(self):
        """return a string representation of the domains"""
        as_string = ''

        if self.ip_version == 4:
            as_string = '.'.join(self.domains)
        elif self.ip_version == 6:
            as_string = "[%s]" % ':'.join(self.domains)
        return as_string

    def __eq__(self, other):
        """return whether the URL is equivalent to another URL"""
        if other:
            if isinstance(other, str):
                other = URL(other)
            
            if not isinstance(other, URL):
                raise TypeError(
                    "cannot compare URL to type \"%s\"" % str(type(other))
                    )
            for a in URL.attributes:
                if (not a == "path"
                        and not getattr(self, a) == getattr(other, a)):
                    return False
            return resolve(self.path) == resolve(other.path)
        return False
    
    def __str__(self):
        """return a string representation"""
        as_list = []

        if not self.scheme == None:
            as_list.append(self.scheme)
            as_list.append(':')
        # [scheme:]

        if self.scheme_is_protocol:
            as_list.append("//")
        # [scheme:][//]

        if not self.username == None:
            as_list.append(encode(self.username))

            if not self.password == None:
                for s in (':', encode(self.password)):
                    as_list.append(s)
            as_list.append('@')
        # [scheme:][//][user[:password]@]

        if self.domains:
            as_list.append(self._domains())
            detected_port = None

            if isinstance(self.scheme, str):
                try:
                    detected_port = socket.getservbyname(self.scheme)
                except socket.error:
                    pass
            
            if (isinstance(self.port, int)
                    and not self.port == detected_port):
                as_list.append(':')
                as_list.append(self.port)
        # [scheme:][//][user[:password]@][host[:port]]
        
        if self.path:
            as_list.append(resolve(self.path))
        # [scheme:][//][user[:password]@][host[:port]]
        # [/path]

        for a, s in (("parameters", ';'), ("query", '?'), ("fragment", '#')):
            if not getattr(self, a) == None:
                as_list.append(s)
                as_list.append(getattr(self, a))
        # [scheme:][//][user[:password]@][host[:port]]
        # [/path][;parameters][?query][#fragment]
        return ''.join((str(e) for e in as_list))
