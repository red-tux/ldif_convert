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
from sets import Set

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

class DN:
  lines=[]
  dn=""

  def __init__(self,chunk,skip_empty=False):
    self.lines=chunk.splitlines()
    dn_r = re.search(r'^dn: (.*)$', chunk, re.MULTILINE )
    if dn_r is not None:
      self.dn=dn_r.group(1)
    if skip_empty:
      self.lines=[ x for x in self.lines if x!="" ]

  def __str__(self):
    return self.str()

  def str(self):
    return "\n".join(filter(lambda x: x!="", self.lines))

  def atr_map(self,mapfunc):
    self.lines = map(mapfunc, self.lines)
    return self.lines

  def atr_filter(self,filterfunc):
    global log
    tmp_lines=self.lines
    # Filter test is true for keeping
    self.lines=[ x for x in self.lines if not filterfunc(x)]
    for x in Set(tmp_lines).difference(Set(self.lines)):
      log.msg(" attribute filtered out:  '%s'" % (x))

    return self.lines

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

def rm_filter(atr,dn):
  global log
  regex=r"^%s:" % (atr)
  dn.filter
  sub=re.sub(regex,'',chunk,flags=re.MULTILINE|re.DOTALL)
  if sub != chunk:
    log.msg(" attribute '%s' removed" % (attribute))
    chunk=sub
  return chunk

def schema_regex(dn_atr):
  global log
  for atr in settings["schema_regex"]:
      regex=r"^%s: " % (atr)
      if re.search( regex, dn_atr) is not None:
        log.msg(" Schema regex found '%s'" % (atr))
        find=r"^%s: %s" % (atr, settings["schema_regex"][atr]["find"])
        replace="%s: %s" %(atr, settings["schema_regex"][atr]["replace"])
        # print find
        # print replace
        # print
        dn_atr=re.sub(find,replace,dn_atr)
  return dn_atr

log=Logger(settings["log_file"])

clean_empty=False
if settings["clean_empty"] is not None:
  if isinstance(settings["clean_empty"], str) and  settings["clean_empty"].lower() == "all":
    clean_empty=True

for chunk in read_chunks():
  count += 1

  dn=DN(chunk,skip_empty=clean_empty)
  log.set_dn(dn.dn)

  for attr in settings["remove_attrs"]:
    regex=r"^%s:" % (attr)
    dn.atr_filter(lambda a: re.search(regex,a) is not None)

  if dn.dn in settings["dn_remove_attrs"]:
    for attr in settings["dn_remove_attrs"][dn.dn]:
      regex=r"^%s:" % (attr)
      dn.atr_filter(lambda a: re.search(regex,a) is not None)

  if settings["schema_regex"] is not None:
    for atr in settings["schema_regex"]:
        dn.atr_map(schema_regex)
  
  #clean up empty lines before writing
  outf.write(dn.str())
  outf.write("\n\n")
  
  if (count % 1000 ) == 0:
    print("Chunks processed: %s   Lines read: %s" % (count,lines))

outf.close()

print("Chunks processed: %s   Lines read: %s" % (count,lines))