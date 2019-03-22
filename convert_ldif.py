#!/bin/env python

# convert_ldif.py  Scripted conversion of LDIFF files for use when
#                  migrating from one LDAP server/environment to
#                  another.  This often includes a need for deleting
#                  objects and or attributes, along with modifying the
#                  data stored in some attributes to comply with schema
#                  rules.
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

import re
import yaml

count = 0
lines = 0

#settings=yaml.load(open('settings.yml'),Loader=yaml.FullLoader)
settings=yaml.load(open('settings.yml'))
outf = open(settings["output_file"],"w")

class Logger:
  logf = None
  current_dn = ""

  def __init__(self, logfile):
    self.logf = open(settings["log_file"],"w")

  def __del__(self):
    self.logf.close()

  def set_dn(self,dn):
    self.current_dn=dn
  
  def msg(self, str):
    # To save on log file space, we will only write the DN when there is
    # an actual message.  current_dn is used as the flag for so that it only
    # get's written once.
    if self.current_dn:
      self.logf.write ("Processing: %s\n" % (self.current_dn) )
      self.current_dn = ""
    self.logf.write(str)
    self.logf.write("\n")

def read_chunks():
  f=open(settings["input_file"])
  global lines
  chunk_lines=""
  for line in f.readlines():
    lines += 1
    if re.match(r'^$',line) is not None:
      chunk_lines=re.sub(r'\n ','',chunk_lines,flags=re.MULTILINE|re.DOTALL )
      yield chunk_lines
      chunk_lines = ""
    else:
      chunk_lines += line

def rm_attr(chunk,attribute):
  global log
  regex=r"^%s:.*$" % (attribute)
  sub=re.sub(regex,'',chunk,flags=re.MULTILINE|re.DOTALL)
  if sub != chunk:
    log.msg(" attribute '%s' removed" % (attribute))
    chunk=sub
  return chunk

log=Logger(settings["log_file"])

for chunk in read_chunks():
  count += 1
  full_dn=""
  dn=""
  dn_r = re.search(r'^(dn: (.*))$', chunk, re.MULTILINE )
  if dn_r is not None:
    full_dn=dn_r.group(1)
    dn=dn_r.group(2)

  log.set_dn(full_dn)

  if settings["clean_empty"] is not None:
    if isinstance(settings["clean_empty"], str) and  settings["clean_empty"].lower() == "all":
      atrs = re.search(r'^(\w*):$', chunk, flags=re.MULTILINE|re.DOTALL)
      while atrs is not None:
        for atr in atrs.groups():
          regex=r"^%s:$" % (atr)
          sub=re.sub(regex,'',chunk,flags=re.MULTILINE)
          if sub != chunk:
            log.msg(" Empty attribute '%s' removed" % (atr))
            chunk=sub
        atrs = re.search(r'^(\w*):$', chunk, flags=re.MULTILINE|re.DOTALL)

  for attr in settings["remove_attrs"]:
     chunk=rm_attr(chunk,attr)

  if dn in settings["dn_remove_attrs"]:
    for attr in settings["dn_remove_attrs"][dn]:
      chunk = rm_attr(chunk,attr)
  
  #clean up empty lines before writing
  chunk="\n".join(filter(bool, chunk.splitlines()))
  outf.write(chunk)
  outf.write("\n\n")
  
  if (count % 1000 ) == 0:
    print("Chunks processed: %s   Lines read: %s" % (count,lines))

outf.close()

print("Chunks processed: %s   Lines read: %s" % (count,lines))