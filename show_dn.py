#!/bin/env python

# show_dn.py  Script to show specific DNs from the given ldif file
#
# Copyright (C) 2019  Red Hat
# Author:  Andrew Nelson anelson@redhat.com
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


import ldif_text
import ldif_logger
import argparse
import sys

ldif_text.show_error_msg=False

def read_chunks(source_file):
  f=open(source_file,'r',100000)
  chunk_lines=""
  for line in f:
    if line == "\n":
      chunk_lines=chunk_lines.replace("\n ","")
      yield chunk_lines
      chunk_lines = ""
    else:
      chunk_lines += line
  # special case where there is no blank line at the end of the file
  if chunk_lines!="":
    yield chunk_lines

parser = argparse.ArgumentParser()
parser.add_argument("source")
parser.add_argument("dn",nargs='+')
args = parser.parse_args()

dn_list=[]

if args.dn[0]=='-':
  for line in sys.stdin:
    dn_list.append(line.strip())
else:
  dn_list=args.dn

for chunk in read_chunks(args.source):
  dn=ldif_text.DN(chunk,import_mode='raw')
  if dn.dn in dn_list:
    dn_list.remove(dn.dn)
    print(dn)
    print
  if not dn_list:
    break
