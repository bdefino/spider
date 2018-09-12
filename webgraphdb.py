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
import os
import sys

sys.path.append(os.path.realpath(__file__))

import db

__doc__ = """basic webgraph database management"""

class WebgraphDB(db.DB):
    """a basic webgraph database"""

    def __init__(self, *args, **kwargs):
        db.DB.__init__(self, *args, **kwargs)

    def __getitem__(self, name):
        """retrieve a list of links"""
        try:
            data = db.DB.__getitem__(self, name)
        except (IOError, OSError):
            return []
        return [l.strip() for l in data.split('\n')]
    
    def __setitem__(self, name, links = ()):
        """store a list of links"""
        for l in links:
            if l:
                self.append(name, l)
