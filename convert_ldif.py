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
from timeit import default_timer as timer
import resource
import base64

count = 0
lines = 0

stats_interval = 25000
perf_interval = 10000

#settings=yaml.load(open('settings.yml'),Loader=yaml.FullLoader)
settings=yaml.load(open('settings.yml'))
outf = open(settings["output_file"],"w")

class Logger:
  logf = None
  current_dn = ""
  header_written = False
  start_size = 0

  def __init__(self, logfile):
    self.logf = open(settings["log_file"],"w")

  def __del__(self):
    self.logf.close()

  def set_dn(self,dn,size=0):
    self.current_dn=dn
    self.header_written = False
    self.start_size = size
  
  def msg(self, str):
    # To save on log file space, we will only write the DN when there is
    # an actual message.  current_dn is used as the flag for so that it only
    # get's written once.
    if not self.header_written:
      self.logf.write ("Processing: %s\n" % (self.current_dn) )
      self.header_written = True
    self.logf.write(str.encode('utf-8'))
    self.logf.write("\n")

class Line:
  name=""
  value=""

  def __init__(self,data):
    self.value=data
  
  def __str__(self):
    return self.value

  def dump(self):
    return self.__str__()


class Comment(Line):
  pass

class Atribute(Line):
  def __init__(self,name,value):
    self.name=name
    self.value=value

  def __str__(self):
    if type(self.value) is unicode:
      return "%s: %s" % (self.name,self.value.encode('utf-8'))
    else:
      return "%s: %s" % (self.name,self.value)
  
  def dump(self):
    if type(self.value) is unicode:
      return "%s:: %s" % (self.name,base64.b64encode(self.value.encode('utf-8')))
    else:
      return "%s: %s" % (self.name,self.value)

class B64_Atribute(Atribute):
  def __str__(self):
    return "%s:: %s" % (self.name,self.value)
  
  def dump(self):
    return "%s:: %s" % (self.name,self.value)


class DN:
  lines=[]
  dn="NONE"

  def __init__(self,chunk,skip_empty=False):
    global log
    dn_r=re.search(r'^dn: (.*)$',chunk,re.MULTILINE)
    # dn_r = self.match(r'^dn: (.*)$')
    if dn_r is not None:
      self.dn=dn_r.group(1)
  
    log.set_dn(self.dn)

    # Outer list comprehension ignores "none" fields returned by filter
    self.lines = [x for x in 
                    [ self.import_line_filter(l,skip_empty=skip_empty) for l in chunk.splitlines()] 
                  if x ] 

  def __str__(self):
    return self.str()

  def import_line_filter(self,line,skip_empty=False):
    global settings
    global log

    linerex = re.match(r'(^\s*#.*)|(^\w+)(:{1,2})\s*(.*)$',line)
    if linerex is None:
      log.msg("ERROR importing line: '%s'" % (line))
      log.msg("  Leaving as is, continuing\n")
      return Line(line)
    if linerex.group(1) is not None:
      return Comment(line)  #comment found, ignore

    atr_name=linerex.group(2)
    atr_sep=linerex.group(3)
    atr_data=linerex.group(4)

    # Double colon separator means base64 endoded data
    if atr_sep == "::":  
      if atr_name in settings["b64_no_convert"]:
        return B64_Atribute(atr_name, atr_data)
      else:
        log.msg(" Converting base 64 '%s'" % (line))
        atr_data = base64.b64decode(atr_data).decode('utf-8')


        try:
          atr_data = atr_data.decode('ascii')
        except UnicodeEncodeError:
          pass  # the data contains unicode characters, ignore and don't convert

        line="%s: %s" %(atr_name,atr_data)
        log.msg("             result '%s'" % (line))

    atr_data_rstrip=atr_data.rstrip()
    if atr_data!=atr_data_rstrip:
      log.msg(" Trailing whitespace removed from: '%s'" % (atr_name))
      atr_data=atr_data_rstrip
    # If noting found for group 4, atribute has empty data
    if atr_data == "" and skip_empty:
      log.msg(" Skipping blank atribute: '%s'" %(atr_name))
      return None
        
    return Atribute(atr_name,atr_data)    

  def match(self,regex,flags=0):
    rex=re.compile(regex,flags)
    for line in self.lines:
      result = rex.match(line)
      # result = re.match(regex,line,flags)
      if result is not None:
        return result
    return None

  def str(self):
    return "\n".join([x.dump() for x in self.lines if x is not None])
      # join(filter(lambda x: x!="", self.lines))

  def atr_map(self,mapfunc):
    self.lines = map(mapfunc, self.lines)
    return self.lines

  def line_filter_helper(self,filterbool,line,msg):
    global log
    if filterbool:
      log.msg(" %s:  '%s'" % (msg,line))

    
    return filterbool


  def line_filter(self,filterfunc,msg="line filtered out"):
    # Filter test is true for keeping
    self.lines=[ x for x in self.lines if not self.line_filter_helper(filterfunc(x),x,msg)]

    return self.lines

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
  global log
  global settings
  if not isinstance(line, Atribute):
    return line
  
  if line.name not in settings["schema_regex"]:
    return line
  atr=settings["schema_regex"][line.name]
  if line.name=="timestamp":
    print atr
    print(re.match(atr["find"],line.value))
  if re.match(atr["find"],line.value) is not None:
    new_val=re.sub(atr["find"], atr["replace"],line.value)
    log.msg(" Schema regex '%s'  '%s' -> '%s'" %(line.name,line.value,new_val))
    line.value=new_val
  return line

log=Logger(settings["log_file"])

clean_empty=False
if settings["clean_empty"] is not None:
  if isinstance(settings["clean_empty"], str) and  settings["clean_empty"].lower() == "all":
    clean_empty=True

start_time=timer()
chunk_start=timer()
last_lines = 0

for chunk in read_chunks():
  count += 1

  dn=DN(chunk,skip_empty=clean_empty)

  for obj in settings["remove_objects"]:
    dn.line_filter(lambda l: l.name=="objectClass" and l.value==obj, msg="object filtered out")
  
  for atr in settings["remove_attrs"]:
    dn.line_filter(lambda l: l.name==atr, msg="global atribute filtered out")

  if dn.dn in settings["dn_remove_attrs"]:
    for attr in settings["dn_remove_attrs"][dn.dn]:
      dn.line_filter(lambda l: l.name==attr,msg="specific atribute filtered out")

  if settings["schema_regex"] is not None:
    dn.atr_map(schema_regex)
  
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

outf.close()

total_time=timer()-start_time
cps = count/total_time
line_delta = lines-last_lines
print("Chunks: %s   Lines: %s (delta: %s)   Elapsed: %s   CPS:  %s" % (count,lines,line_delta,total_time,cps))
