# ldif_text.py  Library for manipulating text which is in an LDIF like
#               format.
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
import base64
import ldif_logger

# Global setting to ignore base64 conversion errors (they will be logged however)
ignore_b64_errors = True

# Global setting of attributes which base64 conversion should not be attempted
b64_no_convert = []

# Show general error messages
show_error_msg=True

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
    # if type(self.value) is unicode:
    #   return "%s: %s" % (self.name,self.value.encode('utf-8'))
    # else:
      return "%s: %s" % (self.name,self.value)
  
  def dump(self):
    # if type(self.value) is unicode:
    #   return "%s:: %s" % (self.name,base64.b64encode(self.value.encode('utf-8')))
    # else:
      return "%s: %s" % (self.name,self.value)

class B64_Atribute(Atribute):
  def __str__(self):
    return "%s:: %s" % (self.name,self.value)
  
  def dump(self):
    return "%s:: %s" % (self.name,self.value)

class DN:
  lines=[]
  dn="NONE"
  ignore_b64_errors=True

  def __init__(self,chunk,skip_empty=False):
    dn_r=re.search(r'^dn: (.*)$',chunk,re.MULTILINE)
    # dn_r = self.match(r'^dn: (.*)$')
    if dn_r is not None:
      self.dn=dn_r.group(1)
      self.dn=re.sub(r",\s+",",",self.dn)
  
    if ldif_logger.log is not None: ldif_logger.log.set_dn(self.dn)

    # Outer list comprehension ignores "none" fields returned by filter
    self.lines = [x for x in 
                    [ self.import_line_filter(l,skip_empty=skip_empty) for l in chunk.splitlines()] 
                  if x ] 

  def __str__(self):
    return self.str()

  def import_line_filter(self,line,skip_empty=False):
    global b64_no_convert
    global ignore_b64_errors

    linerex = re.match(r'(^\s*#.*)|(^\w+)(:{1,2})\s*(.*)$',line)
    if linerex is None:
      if ldif_logger.log is not None: ldif_logger.log.msg("ERROR importing line: '%s'" % (line))
      if ldif_logger.log is not None: ldif_logger.log.msg("  Leaving as is, continuing\n")
      return Line(line)
    if linerex.group(1) is not None:
      return Comment(line)  #comment found, ignore

    atr_name=linerex.group(2)
    atr_sep=linerex.group(3)
    atr_data=linerex.group(4)

    # Double colon separator means base64 endoded data
    if atr_sep == "::":  
      if atr_name in b64_no_convert:
        return B64_Atribute(atr_name, atr_data)
      else:
        if ldif_logger.log is not None: ldif_logger.log.msg(" Converting base 64 '%s'" % (line))

        # Here we try to perform a decode of the base64 data.  If the resulting string
        # is one that would normally be printable then we proceed with the non-base64
        # attribute processing, otherwise we return a base64 object without further
        # processing.
        try: 
          atr_data_dec = base64.b64decode(atr_data).decode('utf-8')
        except UnicodeDecodeError as e:
          if show_error_msg: print("Error importing '%s' for 'dn: %s'" % (atr_name, self.dn))
          if ldif_logger.log is not None: ldif_logger.log.msg(" Base64 error for atribute '%s'" % (atr_name))
          if ldif_logger.log is not None: ldif_logger.log.msg("  Error message: %s" % str(e))
          if ignore_b64_errors:
            if show_error_msg: print(" Inoring error, setting data to base64 and continuing")
            if ldif_logger.log is not None: ldif_logger.log.msg("  Inoring error, setting data to base64 and continuing")
            return B64_Atribute(atr_name, atr_data)

        if atr_data_dec.isprintable():
          atr_data = atr_data_dec
        else:
          if ldif_logger.log is not None: ldif_logger.log.msg("   Not Converted")
          return(B64_Atribute(atr_name, atr_data))

        line="%s: %s" %(atr_name,atr_data)
        if ldif_logger.log is not None: ldif_logger.log.msg("             result '%s'" % (line))

    atr_data_rstrip=atr_data.rstrip()
    if atr_data!=atr_data_rstrip:
      if ldif_logger.log is not None: ldif_logger.log.msg(" Trailing whitespace removed from: '%s'" % (atr_name))
      atr_data=atr_data_rstrip
    # If noting found for group 4, atribute has empty data
    if atr_data == "" and skip_empty:
      if ldif_logger.log is not None: ldif_logger.log.msg(" Skipping blank atribute: '%s'" %(atr_name))
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
    if filterbool:
      if ldif_logger.log is not None: ldif_logger.log.msg(" %s:  '%s'" % (msg,line))
    
    return filterbool

  def line_filter(self,filterfunc,msg="line filtered out"):
    # Filter test is true for keeping
    self.lines=[ x for x in self.lines if not self.line_filter_helper(filterfunc(x),x,msg)]

    return self.lines
