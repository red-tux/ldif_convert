#!/bin/env python

# show_stats.py  Show some of the performance stats generated for convert_ldif
#
# Copyright (C) 2019  Red Hat
#               2022  Andrew Nelson
# Author:  Andrew Nelson nelsonab@red-tux.net
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import sys
import pstats

p = pstats.Stats(sys.argv[1])
p.strip_dirs().sort_stats('calls').print_stats()