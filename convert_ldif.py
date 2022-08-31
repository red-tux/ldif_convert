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
import argparse
import sys

count = 0
lines = 0
validation_errors = 0

stats_interval = 25000
perf_interval = 10000

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
    return 
    
  if settings.get("case_insensitive",False):
    comp_hash = {}
    for k,v in atr_hash.items():
      comp_hash[k.lower()]=v
    lname = line.name.lower()
  else:
    comp_hash = atr_hash
    lname = line.name
  if lname in comp_hash:
    ldif_logger.log.msg(" atribute rename '%s' -> '%s'" %(lname, comp_hash[lname]))
    line.name=comp_hash[lname]
  
  return line

#################
# Main Entry Point
#################

parser = argparse.ArgumentParser(description='Converter of ldif files')
parser.add_argument('settings',nargs='?', default='settings.yml',help='Settings file to load, default: "settings.yml"')

try:
  args=parser.parse_args()
except SystemExit as err:
  if err.code == 2: parser.print_help()
  sys.exit(err.code)

settings=yaml.safe_load(open(args.settings))
outf = open(settings["output_file"],"w")
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

if settings.get("case_insensitive",False):
  ldif_logger.log.msg("Case Insensitive option enabled")
  comparitor = lambda a,b: a.lower()==b.lower()
else:
  ldif_logger.log.msg("Case Insensitive option not found, searches will be case sensitive")
  comparitor = lambda a,b: a==b

for chunk in read_chunks():
  count += 1

  dn=ldif_text.DN(chunk,skip_empty=clean_empty)

  # Stashing for now, intended to allow for renaming a dn suffix globally
  # for now may be better to leverage schema regex
  # if "rename_suffix" in settings:
  #   from_str = "(%s)$" % re.escape(settings['rename_suffix']['from'])
  #   to_str = settings['rename_suffix']['to']
  #   new_dn=re.sub(from_str, to_str, dn.dn)
  #   ldif_logger.log.msg(" Suffix Rename '%s' -> '%s'" %(dn.dn,new_dn))
  #   dn.dn=new_dn

  if "remove_objects" in settings:
    for obj in settings["remove_objects"]:

      dn.line_filter(lambda l: comparitor(l.name,"objectClass") and comparitor(l.value,obj), msg="object filtered out")
      for atr in settings["remove_objects"][obj]:
        msgstr = "atribute of '%s' object filtered out" % (obj)
        dn.line_filter(lambda l: comparitor(l.name,atr), msg=msgstr)
  
  if "remove_attrs" in settings:
    for attr in settings.get("remove_attrs",[]):
      dn.line_filter(lambda l: comparitor(l.name,attr), msg="global atribute filtered out")

  if "dn_remove_attrs" in settings and dn.dn in settings["dn_remove_attrs"]:
    for attr in settings["dn_remove_attrs"][dn.dn]:
      dn.line_filter(lambda l: comparitor(l.name,attr), msg="specific atribute filtered out")

  if "schema_regex" in settings and settings["schema_regex"] is not None:
    dn.atr_map(schema_regex)

  if "rename_atrs" in settings:
    for attr in settings['rename_atrs']:
      dn.atr_map(lambda l: rename_atr(settings["rename_atrs"],l))

  if "rename_dn_atrs" in settings and settings["rename_dn_atrs"] is not None:
    for rename_dn in settings["rename_dn_atrs"]:
      rex=r".*%s$" % (rename_dn)
      if re.match(rex, dn.dn, flags=re.IGNORECASE) is not None:
        dn.atr_map(lambda l: rename_atr(settings["rename_dn_atrs"][rename_dn],l))


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
