#!/bin/env python3

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
# from sets import Set
from timeit import default_timer as timer
import resource
import base64
from functools import partial
import ldif_text
import ldif_logger

count = 0
lines = 0
validation_errors = 0

stats_interval = 25000
perf_interval = 10000

#settings=yaml.load(open('settings.yml'),Loader=yaml.FullLoader)
settings=yaml.safe_load(open('settings.yml'))
outf = open(settings["output_file"],"w")

def read_chunks():
  f=open(settings["input_file"],'r',100000)
  global lines
  chunk_lines=""
  for line in f:
    lines += 1
    if line == "\n":
      chunk_lines=chunk_lines.replace("\n ","")
      yield chunk_lines
      chunk_lines = ""
    else:
      chunk_lines += line
  # special case where there is no blank line at the end of the file
  if chunk_lines!="":
    yield chunk_lines

def schema_regex(line):
  global settings
  if not isinstance(line, ldif_text.Atribute):
    return line
  
  if line.name not in settings["schema_regex"]:
    return line
  atr=settings["schema_regex"][line.name]
  if re.match(atr["find"],line.value) is not None:
    new_val=re.sub(atr["find"], atr["replace"],line.value)
    ldif_logger.log.msg(" Schema regex '%s'  '%s' -> '%s'" %(line.name,line.value,new_val))
    line.value=new_val
  return line

def schema_validate(line):
  global settings
  global validation_errors

  if not isinstance(line, ldif_text.Atribute):
    return line
  
  if line.name not in settings["schema_validate"]:
    return line

  if re.match(settings["schema_validate"][line.name],line.value) is None:
    ldif_logger.log.msg (" Validation error, rejecting, for '%s: %s'" % (line.name, line.value))
    validation_errors +=1
    return None
  
  return line

def rename_atr(atr_hash, line):
  # Ignore plain lines and comments
  if not isinstance(line, ldif_text.Atribute):
    return line
  
  if line.name in atr_hash:
    ldif_logger.log.msg(" atribute rename '%s' -> '%s'" %(line.name, atr_hash[line.name]))
    line.name=atr_hash[line.name]
  
  return line

ldif_logger.log=ldif_logger.Logger(settings["log_file"])

if "IgnoreB64Errors" in settings:
  ldif_text.ignore_b64_errors = settings["IgnoreB64Errors"]
if "b64_no_convert" in settings:  
  ldif_text.b64_no_convert = settings["b64_no_convert"]

clean_empty=False
if settings["clean_empty"] is not None:
  if isinstance(settings["clean_empty"], str) and  settings["clean_empty"].lower() == "all":
    clean_empty=True

start_time=timer()
chunk_start=timer()
last_lines = 0

for chunk in read_chunks():
  count += 1

  dn=ldif_text.DN(chunk,skip_empty=clean_empty)

  if "remove_objects" in settings:
    for obj in settings["remove_objects"]:
      dn.line_filter(lambda l: l.name=="objectClass" and l.value==obj, msg="object filtered out")
      for atr in settings["remove_objects"][obj]:
        msgstr = "atribute of '%s' object filtered out" % (obj)
        dn.line_filter(lambda l: l.name==atr, msg=msgstr)
  
  if "remove_attrs" in settings:
    for attr in settings["remove_attrs"]:
      dn.line_filter(lambda l: l.name==attr, msg="global atribute filtered out")

  if "dn_remove_attrs" in settings and dn.dn in settings["dn_remove_attrs"]:
    for attr in settings["dn_remove_attrs"][dn.dn]:
      dn.line_filter(lambda l: l.name==attr,msg="specific atribute filtered out")

  if "schema_regex" in settings and settings["schema_regex"] is not None:
    dn.atr_map(schema_regex)
  
  if "rename_atrs" in settings and settings["rename_atrs"] is not None:
    for rename_dn in settings["rename_atrs"]:
      rex=r".*%s$" % (rename_dn)
      if re.match(rex, dn.dn, flags=re.IGNORECASE) is not None:
        dn.atr_map(lambda l: rename_atr(settings["rename_atrs"][rename_dn],l))


  if "schema_validate" in settings and settings["schema_validate"] is not None:
    dn.atr_map(schema_validate)
  
  #clean up empty lines before writing
  outf.write(dn.str())
  outf.write("\n\n")
  
  if (count % stats_interval ) == 0:
    chunk_time=timer()-chunk_start
    cps = stats_interval/chunk_time
    line_delta = lines-last_lines
    print("Chunks: %s   Lines: %s (delta: %s)   Elapsed: %s   CPS:  %s" % (count,lines,line_delta,chunk_time,cps))
    # print resource.getrusage(resource.RUSAGE_SELF)
    chunk_start=timer()
    last_lines=lines
    ldif_logger.log.flush()

outf.close()

total_time=timer()-start_time
cps = count/total_time
line_delta = lines-last_lines
print("Chunks: %s   Lines: %s (delta: %s)   Elapsed: %s   CPS:  %s" % (count,lines,line_delta,total_time,cps))
print("total validation errors: %s" % (validation_errors))
