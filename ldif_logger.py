# ldif_logger.py  Logging module for use by the ldif_text module
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

class Logger:
  logf = None
  current_dn = ""
  header_written = False
  start_size = 0

  def __init__(self, logfile):
    self.logf = open(logfile,"w")

  def __del__(self):
    self.logf.close()

  def flush(self):
    self.logf.flush()

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
    self.logf.write(str)
    self.logf.write("\n")

log = None