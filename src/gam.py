#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# GAMWRAPPER
#
# Copyright 2019, All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""GAMWRAPPER is aA API tool which allows Administrators to control their G Suite domain and accounts.

For more information, see https://github.com/taers232c/GAMWRAPPER
"""

__author__ = 'Ross Scroggs <ross.scroggs@gmail.com>'
__version__ = '4.89.07'
__license__ = 'Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)'

import codecs
import collections
import configparser
import datetime
from html.entities import name2codepoint
from html.parser import HTMLParser
import http.client as http_client
import io
import json
import os
import platform
import random
import re
import socket
import string
import struct
import sys
import time

from gamlib import glapi as API
from gamlib import glcfg as GC
from gamlib import glgapi as GAPI
from gamlib import glgcp as GCP
from gamlib import glgdata as GDATA
from gamlib import glglobals as GM
from gamlib import glmsgs as Msg

import atom
import gdata.apps.audit.service
import gdata.apps.service
import gdata.apps.contacts
import gdata.apps.contacts.service
import gdata.apps.emailsettings.service
import gdata.apps.sites
import gdata.apps.sites.service
import googleapiclient
import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
import httplib2
from iso8601 import iso8601
import google.oauth2.service_account
import google_auth_httplib2
import oauth2client.client
import oauth2client.file
import oauth2client.tools
from oauth2client.contrib.multiprocess_file_storage import MultiprocessFileStorage

# Python 3
string_types = (str,)
simple_types = (bool, float, int)
non_compound_types = (str, bool, float, int)
char_type = chr
text_type = str

def iteritems(d, **kw):
  return iter(d.items(**kw))

def itervalues(d, **kw):
  return iter(d.values(**kw))

def ISOformatTimeStamp(timestamp):
  return timestamp.isoformat('T', 'seconds')

GM.Globals[GM.GAM_PATH] = os.path.dirname(os.path.realpath(__file__)) if not getattr(sys, 'frozen', False) else os.path.dirname(sys.executable)

GIT_USER = 'taers232c'
GAM = 'GAMLite'
GAM_URL = 'https://github.com/{0}/{1}'.format(GIT_USER, GAM)
GAM_INFO = 'GAM {0} - {1} / {2} / Python {3}.{4}.{5} {6} / {7} {8} /'.format(__version__, GAM_URL,
                                                                             __author__,
                                                                             sys.version_info[0], sys.version_info[1], sys.version_info[2],
                                                                             sys.version_info[3],
                                                                             platform.platform(), platform.machine())
GAM_RELEASES = 'https://github.com/{0}/{1}/releases'.format(GIT_USER, GAM)
GAM_WIKI = 'https://github.com/{0}/{1}/wiki'.format(GIT_USER, GAM)
GAM_LATEST_RELEASE = 'https://api.github.com/repos/{0}/{1}/releases/latest'.format(GIT_USER, GAM)

TRUE = 'true'
FALSE = 'false'
TRUE_VALUES = [TRUE, 'on', 'yes', 'enabled', '1']
FALSE_VALUES = [FALSE, 'off', 'no', 'disabled', '0']
TRUE_FALSE = [TRUE, FALSE]
ERROR = 'ERROR'
ERROR_PREFIX = ERROR+': '
WARNING = 'WARNING'
WARNING_PREFIX = WARNING+': '
MAX_GOOGLE_SHEET_CELLS = 5000000 # See https://support.google.com/drive/answer/37603
UTF8 = 'utf-8'
UTF8_SIG = 'utf-8-sig'
FN_GAM_CFG = 'gam.cfg'
MY_DRIVE = 'My Drive'
TEAM_DRIVE = 'Drive'
LOWERNUMERIC_CHARS = string.ascii_lowercase+string.digits
ALPHANUMERIC_CHARS = LOWERNUMERIC_CHARS+string.ascii_uppercase

# Python 3.7 values
DEFAULT_CSV_READ_MODE = 'r'
DEFAULT_FILE_APPEND_MODE = 'a'
DEFAULT_FILE_READ_MODE = 'r'
DEFAULT_FILE_WRITE_MODE = 'w'

# Google API constants
APPLICATION_VND_GOOGLE_APPS = 'application/vnd.google-apps.'
MIMETYPE_GA_DOCUMENT = APPLICATION_VND_GOOGLE_APPS+'document'
MIMETYPE_GA_DRAWING = APPLICATION_VND_GOOGLE_APPS+'drawing'
MIMETYPE_GA_FILE = APPLICATION_VND_GOOGLE_APPS+'file'
MIMETYPE_GA_FOLDER = APPLICATION_VND_GOOGLE_APPS+'folder'
MIMETYPE_GA_FORM = APPLICATION_VND_GOOGLE_APPS+'form'
MIMETYPE_GA_FUSIONTABLE = APPLICATION_VND_GOOGLE_APPS+'fusiontable'
MIMETYPE_GA_MAP = APPLICATION_VND_GOOGLE_APPS+'map'
MIMETYPE_GA_PRESENTATION = APPLICATION_VND_GOOGLE_APPS+'presentation'
MIMETYPE_GA_SCRIPT = APPLICATION_VND_GOOGLE_APPS+'script'
MIMETYPE_GA_SHORTCUT = APPLICATION_VND_GOOGLE_APPS+'drive-sdk'
MIMETYPE_GA_SITE = APPLICATION_VND_GOOGLE_APPS+'site'
MIMETYPE_GA_SPREADSHEET = APPLICATION_VND_GOOGLE_APPS+'spreadsheet'

GOOGLE_NAMESERVERS = ['8.8.8.8', '8.8.4.4']
NEVER_DATE = '1970-01-01'
NEVER_DATETIME = '1970-01-01 00:00'
NEVER_TIME = '1970-01-01T00:00:00.000Z'
NEVER_END_DATE = '1969-12-31'
NEVER_START_DATE = NEVER_DATE

# Cloudprint
CLOUDPRINT_ACCESS_URL = 'https://www.google.com/cloudprint/addpublicprinter.html?printerid={0}&key={1}'
# Program return codes
UNKNOWN_ERROR_RC = 1
USAGE_ERROR_RC = 2
SOCKET_ERROR_RC = 3
GOOGLE_API_ERROR_RC = 4
NETWORK_ERROR_RC = 5
FILE_ERROR_RC = 6
MEMORY_ERROR_RC = 7
KEYBOARD_INTERRUPT_RC = 8
HTTP_ERROR_RC = 9
SCOPES_NOT_AUTHORIZED = 10
DATA_ERROR_RC = 11
API_ACCESS_DENIED_RC = 12
CONFIG_ERROR_RC = 13
NO_SCOPES_FOR_API_RC = 15
CLIENT_SECRETS_JSON_REQUIRED_RC = 16
OAUTH2SERVICE_JSON_REQUIRED_RC = 16
OAUTH2_TXT_REQUIRED_RC = 16
INVALID_JSON_RC = 17
JSON_ALREADY_EXISTS_RC = 17
AUTHENTICATION_TOKEN_REFRESH_ERROR_RC = 18
HARD_ERROR_RC = 19
# Information
ENTITY_IS_A_USER_RC = 20
ENTITY_IS_A_USER_ALIAS_RC = 21
ENTITY_IS_A_GROUP_RC = 22
ENTITY_IS_A_GROUP_ALIAS_RC = 23
ORPHANS_COLLECTED_RC = 30
# Warnings/Errors
ACTION_FAILED_RC = 50
ACTION_NOT_PERFORMED_RC = 51
INVALID_ENTITY_RC = 52
BAD_REQUEST_RC = 53
ENTITY_IS_NOT_UNIQUE_RC = 54
DATA_NOT_AVALIABLE_RC = 55
ENTITY_DOES_NOT_EXIST_RC = 56
ENTITY_DUPLICATE_RC = 57
ENTITY_IS_NOT_AN_ALIAS_RC = 58
ENTITY_IS_UKNOWN_RC = 59
NO_ENTITIES_FOUND = 60
INVALID_DOMAIN_RC = 61
INVALID_DOMAIN_VALUE_RC = 62
INVALID_TOKEN_RC = 63
JSON_LOADS_ERROR_RC = 64
MULTIPLE_DELETED_USERS_FOUND_RC = 65
MULTIPLE_PROJECT_FOLDERS_FOUND_RC = 65
INSUFFICIENT_PERMISSIONS_RC = 67
REQUEST_COMPLETED_NO_RESULTS_RC = 71
REQUEST_NOT_COMPLETED_RC = 72
SERVICE_NOT_APPLICABLE_RC = 73
TARGET_DRIVE_SPACE_ERROR_RC = 74
USER_REQUIRED_TO_CHANGE_PASSWORD_ERROR_RC = 75
USER_SUSPENDED_ERROR_RC = 76
#
def convertSysToUTF8(data):
  return data

def convertUTF8toSys(data):
  return data

def escapeCRsNLs(value):
  return value.replace('\r', '\\r').replace('\n', '\\n')

def unescapeCRsNLs(value):
  return value.replace('\\r', '\r').replace('\\n', '\n')

def executeBatch(dbatch):
  dbatch.execute()
  if GC.Values[GC.INTER_BATCH_WAIT] > 0:
    time.sleep(GC.Values[GC.INTER_BATCH_WAIT])

def StringIOobject(initbuff=None):
  if initbuff is None:
    return io.StringIO()
  return io.StringIO(initbuff)

def systemErrorExit(sysRC, message):
  if message:
    stderrErrorMsg(message)
  sys.exit(sysRC)

def readStdin(prompt):
  return input(prompt)

def writeStdout(data):
  try:
    GM.Globals[GM.STDOUT].get(GM.REDIRECT_MULTI_FD, sys.stdout).write(data)
  except IOError as e:
    systemErrorExit(FILE_ERROR_RC, e)

def flushStdout():
  try:
    GM.Globals[GM.STDOUT].get(GM.REDIRECT_MULTI_FD, sys.stdout).flush()
  except IOError as e:
    systemErrorExit(FILE_ERROR_RC, e)

def writeStderr(data):
  flushStdout()
  try:
    GM.Globals[GM.STDERR].get(GM.REDIRECT_MULTI_FD, sys.stderr).write(data)
  except IOError as e:
    systemErrorExit(FILE_ERROR_RC, e)

def flushStderr():
  try:
    GM.Globals[GM.STDERR].get(GM.REDIRECT_MULTI_FD, sys.stderr).flush()
  except IOError as e:
    systemErrorExit(FILE_ERROR_RC, e)

class _DeHTMLParser(HTMLParser):
  def __init__(self):
    HTMLParser.__init__(self)
    self.__text = []

  def handle_data(self, data):
    self.__text.append(data)

  def handle_charref(self, name):
    self.__text.append(char_type(int(name[1:], 16)) if name.startswith('x') else char_type(int(name)))

  def handle_entityref(self, name):
    cp = name2codepoint.get(name)
    if cp:
      self.__text.append(char_type(cp))
    else:
      self.__text.append('&'+name)

  def handle_starttag(self, tag, attrs):
    if tag == 'p':
      self.__text.append('\n\n')
    elif tag == 'br':
      self.__text.append('\n')
    elif tag == 'a':
      for attr in attrs:
        if attr[0] == 'href':
          self.__text.append('({0}) '.format(attr[1]))
          break
    elif tag == 'div':
      if not attrs:
        self.__text.append('\n')
    elif tag in ['http:', 'https']:
      self.__text.append(' ({0}//{1}) '.format(tag, attrs[0][0]))

  def handle_startendtag(self, tag, attrs):
    if tag == 'br':
      self.__text.append('\n\n')

  def text(self):
    return re.sub(r'\n{2}\n+', '\n\n', re.sub(r'\n +', '\n', ''.join(self.__text))).strip()

def dehtml(text):
  parser = _DeHTMLParser()
  parser.feed(str(text))
  parser.close()
  return parser.text()

# Format a key value list
#   key, value	-> "key: value" + ", " if not last item
#   key, ''	-> "key:" + ", " if not last item
#   key, None	-> "key" + " " if not last item
def formatKeyValueList(prefixStr, kvList, suffixStr):
  msg = prefixStr
  i = 0
  l = len(kvList)
  while i < l:
    if isinstance(kvList[i], simple_types):
      msg += str(kvList[i])
    else:
      msg += kvList[i]
    i += 1
    if i < l:
      val = kvList[i]
      if (val is not None) or (i == l-1):
        msg += ':'
        if (val is not None) and (not isinstance(val, string_types) or val):
          msg += ' '
          if isinstance(val, simple_types):
            msg += str(val)
          else:
            msg += val
        i += 1
        if i < l:
          msg += ', '
      else:
        i += 1
        if i < l:
          msg += ' '
  msg += suffixStr
  return msg

# Error exits
def setSysExitRC(sysRC):
  GM.Globals[GM.SYSEXITRC] = sysRC

def printErrorMessage(sysRC, message):
  setSysExitRC(sysRC)
  writeStderr(formatKeyValueList('', [ERROR, message], '\n'))

def stderrErrorMsg(message):
  writeStderr(convertUTF8toSys('\n{0}{1}\n'.format(ERROR_PREFIX, message)))

def stderrWarningMsg(message):
  writeStderr(convertUTF8toSys('\n{0}{1}\n'.format(WARNING_PREFIX, message)))

# Something's wrong with CustomerID
def accessErrorMessage(cd):
  try:
    callGAPI(cd.customers(), 'get',
             throw_reasons=[GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerKey=GC.Values[GC.CUSTOMER_ID], fields='id')
  except GAPI.badRequest:
    return formatKeyValueList('',
                              ['Customer ID', GC.Values[GC.CUSTOMER_ID],
                               Msg.INVALID],
                              '')
  except GAPI.resourceNotFound:
    return formatKeyValueList('',
                              ['Customer ID', GC.Values[GC.CUSTOMER_ID],
                               Msg.DOES_NOT_EXIST],
                              '')
  except GAPI.forbidden:
    return formatKeyValueList('',
                              ['Customer ID', GC.Values[GC.CUSTOMER_ID],
                               'Domain', GC.Values[GC.DOMAIN],
                               'User', GM.Globals[GM.ADMIN],
                               Msg.ACCESS_FORBIDDEN],
                              '')
  return None

def accessErrorExit(cd):
  systemErrorExit(INVALID_DOMAIN_RC, accessErrorMessage(cd or buildGAPIObject(API.DIRECTORY)))

def ClientAPIAccessDeniedExit():
  stderrErrorMsg(Msg.API_ACCESS_DENIED)
  missingScopes = API.getClientScopesSet(GM.Globals[GM.CURRENT_CLIENT_API])-GM.Globals[GM.CURRENT_CLIENT_API_SCOPES]
  if missingScopes:
    writeStderr(Msg.API_CHECK_CLIENT_AUTHORIZATION.format(GM.Globals[GM.OAUTH2_CLIENT_ID],
                                                          ','.join(sorted(missingScopes))))
  systemErrorExit(API_ACCESS_DENIED_RC, None)

def SvcAcctAPIAccessDeniedExit():
  stderrErrorMsg(Msg.API_ACCESS_DENIED)
  writeStderr(Msg.API_CHECK_SVCACCT_AUTHORIZATION.format(GM.Globals[GM.OAUTH2SERVICE_CLIENT_ID],
                                                         ','.join(sorted(API.getSvcAcctScopesSet(GM.Globals[GM.CURRENT_SVCACCT_API]))),
                                                         GM.Globals[GM.CURRENT_SVCACCT_USER]))
  systemErrorExit(API_ACCESS_DENIED_RC, None)

def APIAccessDeniedExit():
  if GM.Globals[GM.CURRENT_CLIENT_API]:
    ClientAPIAccessDeniedExit()
  if GM.Globals[GM.CURRENT_SVCACCT_API]:
    SvcAcctAPIAccessDeniedExit()
  systemErrorExit(API_ACCESS_DENIED_RC, Msg.API_ACCESS_DENIED)

def invalidClientSecretsJsonExit():
  stderrErrorMsg(Msg.DOES_NOT_EXIST_OR_HAS_INVALID_FORMAT.format('Client Secrets File', GC.Values[GC.CLIENT_SECRETS_JSON]))
  writeStderr(Msg.INSTRUCTIONS_CLIENT_SECRETS_JSON)
  systemErrorExit(CLIENT_SECRETS_JSON_REQUIRED_RC, None)

def invalidOauth2serviceJsonExit():
  stderrErrorMsg(Msg.DOES_NOT_EXIST_OR_HAS_INVALID_FORMAT.format('Service Account OAuth2 File', GC.Values[GC.OAUTH2SERVICE_JSON]))
  writeStderr(Msg.INSTRUCTIONS_OAUTH2SERVICE_JSON)
  systemErrorExit(OAUTH2SERVICE_JSON_REQUIRED_RC, None)

def invalidOauth2TxtExit():
  stderrErrorMsg(Msg.DOES_NOT_EXIST_OR_HAS_INVALID_FORMAT.format('Client OAuth2 File', GC.Values[GC.OAUTH2_TXT]))
  writeStderr(Msg.EXECUTE_GAM_OAUTH_CREATE)
  systemErrorExit(OAUTH2_TXT_REQUIRED_RC, None)

def invalidDiscoveryJsonExit(fileName):
  stderrErrorMsg(Msg.DOES_NOT_EXIST_OR_HAS_INVALID_FORMAT.format('Discovery File', fileName))
  systemErrorExit(INVALID_JSON_RC, None)

# Choices is the valid set of choices that was expected
def formatChoiceList(choices):
  choiceList = list(choices)
  if len(choiceList) <= 5:
    return '|'.join(choiceList)
  return '|'.join(sorted(choiceList))

def removeCourseIdScope(courseId):
  if courseId.startswith('d:'):
    return courseId[2:]
  return courseId

def addCourseIdScope(courseId):
  if not courseId.isdigit() and courseId[:2] != 'd:':
    return 'd:{0}'.format(courseId)
  return courseId

UID_PATTERN = re.compile(r'u?id: ?(.+)', re.IGNORECASE)

def integerLimits(minVal, maxVal, item='integer'):
  if (minVal is not None) and (maxVal is not None):
    return '{0} {1}<=x<={2}'.format(item, minVal, maxVal)
  if minVal is not None:
    return '{0} x>={1}'.format(item, minVal)
  if maxVal is not None:
    return '{0} x<={1}'.format(item, maxVal)
  return '{0} x'.format(item)

def makeOrgUnitPathAbsolute(path):
  if path == '/':
    return path
  if path.startswith('/'):
    return path.rstrip('/')
  if path.startswith('id:'):
    return path
  if path.startswith('uid:'):
    return path[1:]
  return '/'+path.rstrip('/')

def makeOrgUnitPathRelative(path):
  if path == '/':
    return path
  if path.startswith('/'):
    return path[1:].rstrip('/')
  if path.startswith('id:'):
    return path
  if path.startswith('uid:'):
    return path[1:]
  return path.rstrip('/')

def encodeOrgUnitPath(path):
  if path.find('+') == -1 and path.find('%') == -1:
    return path
  encpath = ''
  for c in path:
    if c == '+':
      encpath += '%2B'
    elif c == '%':
      encpath += '%25'
    else:
      encpath += c
  return encpath

YYYYMMDD_FORMAT = '%Y-%m-%d'
TIMEZONE_FORMAT_REQUIRED = 'Z|(+|-(hh:mm))'

def formatLocalTime(dateTimeStr):
  if dateTimeStr == NEVER_TIME:
    return GC.Values[GC.NEVER_TIME]
  if not GM.Globals[GM.CONVERT_TO_LOCAL_TIME] or not dateTimeStr.endswith('Z'):
    return dateTimeStr
  try:
    timestamp, _ = iso8601.parse_date(dateTimeStr)
    return ISOformatTimeStamp(timestamp.astimezone(GC.Values[GC.TIMEZONE]))
  except (iso8601.ParseError, OverflowError):
    return dateTimeStr

def formatLocalTimestamp(timestamp):
  return ISOformatTimeStamp(datetime.datetime.fromtimestamp(int(timestamp)//1000, GC.Values[GC.TIMEZONE]))

def formatLocalDatestamp(timestamp):
  return datetime.datetime.fromtimestamp(int(timestamp)//1000, GC.Values[GC.TIMEZONE]).strftime(YYYYMMDD_FORMAT)

def formatHTTPError(http_status, reason, message):
  return '{0}: {1} - {2}'.format(http_status, reason, message)

def getHTTPError(responses, http_status, reason, message):
  if reason in responses:
    return responses[reason]
  return formatHTTPError(http_status, reason, message)

# Warnings
def entityServiceNotApplicableWarning(entityType, entityName):
  setSysExitRC(SERVICE_NOT_APPLICABLE_RC)
  writeStderr(formatKeyValueList('',
                                 [entityType, entityName, Msg.SERVICE_NOT_APPLICABLE],
                                 '\n'))

def cleanFilename(filename):
  for ch in '\\/:':
    filename = filename.replace(ch, '_')
  return filename

# Set file encoding to handle UTF8 BOM
def setEncoding(mode, encoding):
  if 'b' in mode:
    return {}
  if not encoding:
    encoding = GM.Globals[GM.SYS_ENCODING]
  if 'r' in mode and encoding.lower().replace('-', '') == 'utf8':
    encoding = UTF8_SIG
  return {'encoding': encoding}

# Open a file
def openFile(filename, mode=DEFAULT_FILE_READ_MODE, encoding=None, newline=None,
             continueOnError=False, displayError=True, stripUTFBOM=False):
  try:
    if filename != '-':
      kwargs = setEncoding(mode, encoding)
      f = open(os.path.expanduser(filename), mode, newline=newline, **kwargs)
      if stripUTFBOM:
        if 'b' in mode:
          if f.read(3) != b'\xef\xbb\xbf':
            f.seek(0)
        elif not kwargs['encoding'].lower().startswith('utf'):
          if f.read(3).encode('iso-8859-1', 'replace') != codecs.BOM_UTF8:
            f.seek(0)
        else:
          if f.read(1) != '\ufeff':
            f.seek(0)
      return f
    if 'r' in mode:
      return StringIOobject(text_type(sys.stdin.read()))
    if 'b' not in mode:
      return sys.stdout
    return os.fdopen(os.dup(sys.stdout.fileno()), 'wb')
  except (IOError, LookupError, UnicodeError) as e:
    if continueOnError:
      if displayError:
        stderrWarningMsg(e)
        setSysExitRC(FILE_ERROR_RC)
      return None
    systemErrorExit(FILE_ERROR_RC, e)

# Close a file
def closeFile(f):
  try:
    f.close()
    return True
  except IOError as e:
    stderrErrorMsg(e)
    setSysExitRC(FILE_ERROR_RC)
    return False

# Read a file
def readFile(filename, mode=DEFAULT_FILE_READ_MODE, encoding=None, newline=None,
             continueOnError=False, displayError=True):
  try:
    if filename != '-':
      kwargs = setEncoding(mode, encoding)
      with open(os.path.expanduser(filename), mode, newline=newline, **kwargs) as f:
        return f.read()
    return text_type(sys.stdin.read())
  except (IOError, LookupError, UnicodeDecodeError, UnicodeError) as e:
    if continueOnError:
      if displayError:
        stderrWarningMsg(e)
        setSysExitRC(FILE_ERROR_RC)
      return None
    systemErrorExit(FILE_ERROR_RC, e)

def incrAPICallsRetryData(errMsg, delta):
  GM.Globals[GM.API_CALLS_RETRY_DATA].setdefault(errMsg, [0, 0.0])
  GM.Globals[GM.API_CALLS_RETRY_DATA][errMsg][0] += 1
  GM.Globals[GM.API_CALLS_RETRY_DATA][errMsg][1] += delta

def initAPICallsRateCheck():
  GM.Globals[GM.RATE_CHECK_COUNT] = 0
  GM.Globals[GM.RATE_CHECK_START] = time.time()

def checkAPICallsRate():
  GM.Globals[GM.RATE_CHECK_COUNT] += 1
  if GM.Globals[GM.RATE_CHECK_COUNT] >= GC.Values[GC.API_CALLS_RATE_LIMIT]:
    current = time.time()
    delta = int(current-GM.Globals[GM.RATE_CHECK_START])
    if 0 <= delta < 100:
      delta = (100-delta)+3
      error_message = 'API calls per 100 seconds limit {0} exceeded'.format(GC.Values[GC.API_CALLS_RATE_LIMIT])
      writeStderr('{0}{1}: Backing off: {2} seconds\n'.format(WARNING_PREFIX, error_message, delta))
      flushStderr()
      time.sleep(delta)
      if GC.Values[GC.SHOW_API_CALLS_RETRY_DATA]:
        incrAPICallsRetryData(error_message, delta)
      GM.Globals[GM.RATE_CHECK_START] = time.time()
    else:
      GM.Globals[GM.RATE_CHECK_START] = current
    GM.Globals[GM.RATE_CHECK_COUNT] = 0

# Set global variables from config file
# Check for GAM updates based on status of no_update_check in config file
# Return True if there are additional commands on the command line
def SetGlobalVariables(configFile, sectionName=None):

  def _stringInQuotes(value):
    return (len(value) > 1) and (((value.startswith('"') and value.endswith('"'))) or ((value.startswith("'") and value.endswith("'"))))

  def _stripStringQuotes(value):
    if _stringInQuotes(value):
      return value[1:-1]
    return value

  def _selectSection(value):
    if (not value) or (value.upper() == configparser.DEFAULTSECT):
      return configparser.DEFAULTSECT
    if GM.Globals[GM.PARSER].has_section(value):
      return value
    systemErrorExit(CONFIG_ERROR_RC, formatKeyValueList('', ['Section', value, Msg.NOT_FOUND], ''))

  def _printValueError(sectionName, itemName, value, errMessage):
    status['errors'] = True
    printErrorMessage(CONFIG_ERROR_RC,
                      formatKeyValueList('',
                                         ['Config File', GM.Globals[GM.GAM_CFG_FILE],
                                          'Section', sectionName,
                                          'Item', itemName,
                                          'Value', value,
                                          errMessage],
                                         ''))

  def _getCfgBoolean(sectionName, itemName):
    value = GM.Globals[GM.PARSER].get(sectionName, itemName).lower()
    if value in TRUE_VALUES:
      return True
    if value in FALSE_VALUES:
      return False
    _printValueError(sectionName, itemName, value, '{0}: {1}'.format(Msg.EXPECTED, formatChoiceList(TRUE_FALSE)))
    return False

  def _getCfgCharacter(sectionName, itemName):
    value = codecs.escape_decode(bytes(_stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName)), UTF8))[0].decode(UTF8)
    if not value and (itemName == 'csv_output_field_delimiter'):
      return ' '
    if len(value) == 1:
      return value
    _printValueError(sectionName, itemName, '"{0}"'.format(value), '{0}: {1}'.format(Msg.EXPECTED, integerLimits(1, 1, Msg.STRING_LENGTH)))
    return ''

  def _getCfgChoice(sectionName, itemName):
    value = _stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName))
    choices = GC.VAR_INFO[itemName][GC.VAR_CHOICES]
    if value in choices:
      return choices[value]
    _printValueError(sectionName, itemName, '"{0}"'.format(value), '{0}: {1}'.format(Msg.EXPECTED, ','.join(choices)))
    return ''

  def _getCfgNumber(sectionName, itemName):
    value = GM.Globals[GM.PARSER].get(sectionName, itemName)
    minVal, maxVal = GC.VAR_INFO[itemName][GC.VAR_LIMITS]
    try:
      number = int(value) if GC.VAR_INFO[itemName][GC.VAR_TYPE] == GC.TYPE_INTEGER else float(value)
      if ((minVal is None) or (number >= minVal)) and ((maxVal is None) or (number <= maxVal)):
        return number
    except ValueError:
      pass
    _printValueError(sectionName, itemName, value, '{0}: {1}'.format(Msg.EXPECTED, integerLimits(minVal, maxVal)))
    return 0

  def _getCfgString(sectionName, itemName):
    value = _stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName))
    minLen, maxLen = GC.VAR_INFO[itemName].get(GC.VAR_LIMITS, (None, None))
    if ((minLen is None) or (len(value) >= minLen)) and ((maxLen is None) or (len(value) <= maxLen)):
      return value
    _printValueError(sectionName, itemName, '"{0}"'.format(value), '{0}: {1}'.format(Msg.EXPECTED, integerLimits(minLen, maxLen, Msg.STRING_LENGTH)))
    return ''

  def _getCfgTimezone(sectionName, itemName):
    value = _stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName).lower())
    if value == 'utc':
      GM.Globals[GM.CONVERT_TO_LOCAL_TIME] = False
      return iso8601.UTC
    GM.Globals[GM.CONVERT_TO_LOCAL_TIME] = True
    if value == 'local':
      return iso8601.Local
    try:
      return iso8601.parse_timezone_str(value)
    except (iso8601.ParseError, OverflowError):
      pass
    _printValueError(sectionName, itemName, value, '{0}: {1}'.format(Msg.EXPECTED, TIMEZONE_FORMAT_REQUIRED))
    return ''

  def _getCfgDirectory(sectionName, itemName):
    dirPath = os.path.expanduser(_stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName)))
    if (not dirPath) or (not os.path.isabs(dirPath)):
      if (sectionName != configparser.DEFAULTSECT) and (GM.Globals[GM.PARSER].has_option(sectionName, itemName)):
        dirPath = os.path.join(os.path.expanduser(_stripStringQuotes(GM.Globals[GM.PARSER].get(configparser.DEFAULTSECT, itemName))), dirPath)
      if not os.path.isabs(dirPath):
        dirPath = os.path.join(GM.Globals[GM.GAM_CFG_PATH], dirPath)
    return dirPath

  def _getCfgFile(sectionName, itemName):
    value = os.path.expanduser(_stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName)))
    if value and not os.path.isabs(value):
      value = os.path.expanduser(os.path.join(_getCfgDirectory(sectionName, GC.CONFIG_DIR), value))
    elif not value and itemName == GC.CACERTS_PEM:
      value = os.path.join(GM.Globals[GM.GAM_PATH], GC.FN_CACERTS_PEM)
    return value

  def _getCfgSection(sectionName, itemName):
    value = _stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName))
    if (not value) or (value.upper() == configparser.DEFAULTSECT):
      return configparser.DEFAULTSECT
    if GM.Globals[GM.PARSER].has_section(value):
      return value
    _printValueError(sectionName, itemName, value, Msg.NOT_FOUND)
    return configparser.DEFAULTSECT

  def _readGamCfgFile(config, fileName):
    try:
      with open(fileName, 'r') as f:
        config.read_file(f)
    except (configparser.MissingSectionHeaderError, configparser.ParsingError) as e:
      systemErrorExit(CONFIG_ERROR_RC, formatKeyValueList('',
                                                          ['Config File', fileName,
                                                           Msg.INVALID, str(e)],
                                                          ''))
    except IOError as e:
      systemErrorExit(FILE_ERROR_RC, e)

  GM.Globals[GM.GAM_CFG_FILE] = configFile
  GM.Globals[GM.PARSER] = configparser.RawConfigParser(defaults=collections.OrderedDict(sorted(list(GC.Defaults.items()), key=lambda t: t[0])))
  _readGamCfgFile(GM.Globals[GM.PARSER], GM.Globals[GM.GAM_CFG_FILE])
  status = {'errors': False}
  if sectionName is not None:
    sectionName = _selectSection(sectionName)
  else:
    sectionName = _getCfgSection(configparser.DEFAULTSECT, GC.SECTION)
# Handle todrive_nobrowser and todrive_noemail if not present
  value = GM.Globals[GM.PARSER].get(configparser.DEFAULTSECT, GC.TODRIVE_NOBROWSER)
  if value == '':
    GM.Globals[GM.PARSER].set(configparser.DEFAULTSECT, GC.TODRIVE_NOBROWSER, str(_getCfgBoolean(configparser.DEFAULTSECT, GC.NO_BROWSER)).lower())
  value = GM.Globals[GM.PARSER].get(configparser.DEFAULTSECT, GC.TODRIVE_NOEMAIL)
  if value == '':
    GM.Globals[GM.PARSER].set(configparser.DEFAULTSECT, GC.TODRIVE_NOEMAIL, str(not _getCfgBoolean(configparser.DEFAULTSECT, GC.NO_BROWSER)).lower())
# Assign global variables, directories, timezone first as other variables depend on them
  for itemName in sorted(GC.VAR_INFO):
    varType = GC.VAR_INFO[itemName][GC.VAR_TYPE]
    if varType == GC.TYPE_DIRECTORY:
      GC.Values[itemName] = _getCfgDirectory(sectionName, itemName)
    elif varType == GC.TYPE_TIMEZONE:
      GC.Values[itemName] = _getCfgTimezone(sectionName, itemName)
  GM.Globals[GM.DATETIME_NOW] = datetime.datetime.now(GC.Values[GC.TIMEZONE])
# Everything else
  for itemName in sorted(GC.VAR_INFO):
    varType = GC.VAR_INFO[itemName][GC.VAR_TYPE]
    if varType == GC.TYPE_BOOLEAN:
      GC.Values[itemName] = _getCfgBoolean(sectionName, itemName)
    elif varType == GC.TYPE_CHARACTER:
      GC.Values[itemName] = _getCfgCharacter(sectionName, itemName)
    elif varType == GC.TYPE_CHOICE:
      GC.Values[itemName] = _getCfgChoice(sectionName, itemName)
    elif varType in [GC.TYPE_INTEGER, GC.TYPE_FLOAT]:
      GC.Values[itemName] = _getCfgNumber(sectionName, itemName)
    elif varType == GC.TYPE_HEADERFILTER:
      pass
    elif varType == GC.TYPE_ROWFILTER:
      pass
    elif varType == GC.TYPE_STRING:
      GC.Values[itemName] = _getCfgString(sectionName, itemName)
    elif varType == GC.TYPE_FILE:
      GC.Values[itemName] = _getCfgFile(sectionName, itemName)
  if status['errors']:
    sys.exit(CONFIG_ERROR_RC)
  GC.Values[GC.DOMAIN] = GC.Values[GC.DOMAIN].lower()
# Create/set mode for oauth2.txt.lock
  if not GM.Globals[GM.OAUTH2_TXT_LOCK]:
    fileName = '{0}.lock'.format(GC.Values[GC.OAUTH2_TXT])
    if not os.path.isfile(fileName):
      closeFile(openFile(fileName, mode=DEFAULT_FILE_APPEND_MODE))
      os.chmod(fileName, 0o666)
    GM.Globals[GM.OAUTH2_TXT_LOCK] = fileName
# Override httplib2 settings
  httplib2.debuglevel = GC.Values[GC.DEBUG_LEVEL]
# Set environment variables so GData API can find cacerts.pem
  os.environ['REQUESTS_CA_BUNDLE'] = GC.Values[GC.CACERTS_PEM]
  os.environ['DEFAULT_CA_BUNDLE_PATH'] = GC.Values[GC.CACERTS_PEM]
  os.environ['SSL_CERT_FILE'] = GC.Values[GC.CACERTS_PEM]
  httplib2.CA_CERTS = GC.Values[GC.CACERTS_PEM]
  return True

def handleServerError(e):
  errMsg = str(e)
  if 'setting tls' not in errMsg:
    systemErrorExit(NETWORK_ERROR_RC, errMsg)
  stderrErrorMsg(errMsg)
  writeStderr(Msg.DISABLE_TLS_MIN_MAX)
  systemErrorExit(NETWORK_ERROR_RC, None)

def getHttpObj(cache=None, timeout=None, override_min_tls=None, override_max_tls=None):
  tls_minimum_version = override_min_tls if override_min_tls else GC.Values[GC.TLS_MIN_VERSION] if GC.Values[GC.TLS_MIN_VERSION] else None
  tls_maximum_version = override_max_tls if override_max_tls else GC.Values[GC.TLS_MAX_VERSION] if GC.Values[GC.TLS_MAX_VERSION] else None
  return httplib2.Http(cache=cache,
                       timeout=timeout,
                       ca_certs=GC.Values[GC.CACERTS_PEM],
                       disable_ssl_certificate_validation=GC.Values[GC.NO_VERIFY_SSL],
                       tls_maximum_version=tls_maximum_version,
                       tls_minimum_version=tls_minimum_version)

def handleOAuthTokenError(e, soft_errors):
  errMsg = str(e)
  if errMsg.replace('.', '') in API.OAUTH2_TOKEN_ERRORS or errMsg.startswith('Invalid response'):
    if soft_errors:
      return None
    if not GM.Globals[GM.CURRENT_SVCACCT_USER]:
      ClientAPIAccessDeniedExit()
    systemErrorExit(SERVICE_NOT_APPLICABLE_RC, Msg.SERVICE_NOT_APPLICABLE_THIS_ADDRESS.format(GM.Globals[GM.CURRENT_SVCACCT_USER]))
  stderrErrorMsg('Authentication Token Error - {0}'.format(errMsg))
  APIAccessDeniedExit()

def getOauth2TxtCredentials(storageOnly=False):
  while True:
    try:
      storage = MultiprocessFileStorage(GC.Values[GC.OAUTH2_TXT], API.GAM_SCOPES)
      if storageOnly:
        return storage
      credentials = storage.get()
      if credentials:
        return credentials
    except (KeyError, ValueError):
      pass
    except IOError as e:
      systemErrorExit(FILE_ERROR_RC, e)
    return None

def waitOnFailure(n, retries, error_code, error_message):
  delta = min(2 ** n, 60)+float(random.randint(1, 1000))/1000
  if n > 3:
    writeStderr('Temporary error: {0} - {1}, Backing off: {2} seconds, Retry: {3}/{4}\n'.format(error_code, error_message, int(delta), n, retries))
    flushStderr()
  time.sleep(delta)
  if GC.Values[GC.SHOW_API_CALLS_RETRY_DATA]:
    incrAPICallsRetryData(error_message, delta)

def getClientCredentials(forceRefresh=False):
  credentials = getOauth2TxtCredentials()
  if not credentials or credentials.invalid:
    invalidOauth2TxtExit()
  if credentials.access_token_expired or forceRefresh:
    retries = 3
    for n in range(1, retries+1):
      try:
        credentials.refresh(getHttpObj())
        break
      except (httplib2.HttpLib2Error, google.auth.exceptions.TransportError, RuntimeError) as e:
        if n != retries:
          waitOnFailure(n, retries, NETWORK_ERROR_RC, str(e))
          continue
        handleServerError(e)
      except (oauth2client.client.AccessTokenRefreshError, google.auth.exceptions.RefreshError) as e:
        if isinstance(e.args, tuple):
          e = e.args[0]
        handleOAuthTokenError(e, False)
  credentials.user_agent = GAM_INFO
  return credentials

def _getValueFromOAuth(field):
  if not GM.Globals[GM.DECODED_ID_TOKEN]:
    GM.Globals[GM.DECODED_ID_TOKEN] = getClientCredentials().id_token
  return GM.Globals[GM.DECODED_ID_TOKEN].get(field, 'Unknown')

def getSvcAcctCredentials(scopes, act_as):
  try:
    if not GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]:
      json_string = readFile(GC.Values[GC.OAUTH2SERVICE_JSON], continueOnError=True, displayError=True)
      if not json_string:
        invalidOauth2serviceJsonExit()
      GM.Globals[GM.OAUTH2SERVICE_JSON_DATA] = json.loads(json_string)
    credentials = google.oauth2.service_account.Credentials.from_service_account_info(GM.Globals[GM.OAUTH2SERVICE_JSON_DATA])
    credentials = credentials.with_scopes(scopes)
    credentials = credentials.with_subject(act_as)
    GM.Globals[GM.ADMIN] = GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]['client_email']
    GM.Globals[GM.OAUTH2SERVICE_CLIENT_ID] = GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]['client_id']
    return credentials
  except (ValueError, IndexError, KeyError):
    invalidOauth2serviceJsonExit()

def checkGAPIError(e, soft_errors=False, retryOnHttpError=False, service=None):
  try:
    error = json.loads(e.content.decode(UTF8))
  except ValueError:
    eContent = e.content.decode(UTF8) if isinstance(e.content, bytes) else e.content
    if (e.resp['status'] == '503') and (eContent.startswith('Quota exceeded for the current request')):
      return (e.resp['status'], GAPI.QUOTA_EXCEEDED, eContent)
    if (e.resp['status'] == '403') and (eContent.startswith('Request rate higher than configured')):
      return (e.resp['status'], GAPI.QUOTA_EXCEEDED, eContent)
    if (e.resp['status'] == '502') and ('Bad Gateway' in eContent):
      return (e.resp['status'], GAPI.BAD_GATEWAY, eContent)
    if (e.resp['status'] == '504') and ('Gateway Timeout' in e.content):
      return (e.resp['status'], GAPI.GATEWAY_TIMEOUT, e.content)
    if (e.resp['status'] == '403') and ('Invalid domain.' in eContent):
      error = {'error': {'code': 403, 'errors': [{'reason': GAPI.NOT_FOUND, 'message': 'Domain not found'}]}}
    elif (e.resp['status'] == '403') and ('Domain cannot use apis.' in eContent):
      error = {'error': {'code': 403, 'errors': [{'reason': GAPI.DOMAIN_CANNOT_USE_APIS, 'message': 'Domain cannot use apis'}]}}
    elif (e.resp['status'] == '400') and ('InvalidSsoSigningKey' in eContent):
      error = {'error': {'code': 400, 'errors': [{'reason': GAPI.INVALID, 'message': 'InvalidSsoSigningKey'}]}}
    elif (e.resp['status'] == '400') and ('UnknownError' in eContent):
      error = {'error': {'code': 400, 'errors': [{'reason': GAPI.INVALID, 'message': 'UnknownError'}]}}
    elif (e.resp['status'] == '400') and ('FeatureUnavailableForUser' in eContent):
      error = {'error': {'code': 400, 'errors': [{'reason': GAPI.SERVICE_NOT_AVAILABLE, 'message': 'Feature Unavailable For User'}]}}
    elif (e.resp['status'] == '400') and ('EntityDoesNotExist' in eContent):
      error = {'error': {'code': 400, 'errors': [{'reason': GAPI.NOT_FOUND, 'message': 'Entity Does Not Exist'}]}}
    elif (e.resp['status'] == '400') and ('EntityNameNotValid' in eContent):
      error = {'error': {'code': 400, 'errors': [{'reason': GAPI.INVALID_INPUT, 'message': 'Entity Name Not Valid'}]}}
    elif (e.resp['status'] == '400') and ('Failed to parse Content-Range header' in e.content):
      error = {'error': {'code': 400, 'errors': [{'reason': GAPI.BAD_REQUEST, 'message': 'Failed to parse Content-Range header'}]}}
    elif retryOnHttpError:
      if hasattr(service._http.request, 'credentials'):
        service._http.request.credentials.refresh(getHttpObj())
      return (-1, None, None)
    elif soft_errors:
      stderrErrorMsg(eContent)
      return (0, None, None)
    else:
      systemErrorExit(HTTP_ERROR_RC, eContent)
  if 'error' in error:
    http_status = error['error']['code']
    try:
      message = error['error']['errors'][0]['message']
    except KeyError:
      message = error['error']['message']
    if http_status == 500:
      if not message:
        message = Msg.UNKNOWN
        error = {'error': {'errors': [{'reason': GAPI.UNKNOWN_ERROR, 'message': message}]}}
      elif 'Backend Error' in message:
        error = {'error': {'errors': [{'reason': GAPI.BACKEND_ERROR, 'message': message}]}}
      elif 'Role assignment exists: RoleAssignment' in message:
        error = {'error': {'errors': [{'reason': GAPI.DUPLICATE, 'message': message}]}}
      elif 'Operation not supported' in message:
        error = {'error': {'errors': [{'reason': GAPI.OPERATION_NOT_SUPPORTED, 'message': message}]}}
      elif 'Failed status in update settings response' in message:
        error = {'error': {'errors': [{'reason': GAPI.INVALID_INPUT, 'message': message}]}}
    elif http_status == 403:
      if 'The caller does not have permission' in message:
        error = {'error': {'errors': [{'reason': GAPI.PERMISSION_DENIED, 'message': message}]}}
    elif http_status == 404:
      if 'Requested entity was not found' in message:
        error = {'error': {'errors': [{'reason': GAPI.NOT_FOUND, 'message': message}]}}
    elif http_status == 409:
      if 'Requested entity already exists' in message:
        error = {'error': {'errors': [{'reason': GAPI.ALREADY_EXISTS, 'message': message}]}}
  else:
    if 'error_description' in error:
      if error['error_description'] == 'Invalid Value':
        message = error['error_description']
        http_status = 400
        error = {'error': {'errors': [{'reason': GAPI.INVALID, 'message': message}]}}
      else:
        systemErrorExit(GOOGLE_API_ERROR_RC, str(error))
    else:
      systemErrorExit(GOOGLE_API_ERROR_RC, str(error))
  try:
    reason = error['error']['errors'][0]['reason']
    for messageItem in GAPI.REASON_MESSAGE_MAP.get(reason, []):
      if messageItem[0] in message:
        if reason in [GAPI.NOT_FOUND, GAPI.RESOURCE_NOT_FOUND]:
          message = Msg.DOES_NOT_EXIST
        reason = messageItem[1]
        break
    if reason == GAPI.INVALID_SHARING_REQUEST:
      loc = message.find('User message: ')
      if loc != 1:
        message = message[loc+15:]
  except KeyError:
    reason = '{0}'.format(http_status)
  return (http_status, reason, message)

def callGAPI(service, function,
             bailOnInternalError=False, bailOnTransientError=False, soft_errors=False,
             throw_reasons=None, retry_reasons=None, retries=10,
             **kwargs):
  if throw_reasons is None:
    throw_reasons = []
  if retry_reasons is None:
    retry_reasons = []
  all_retry_reasons = GAPI.DEFAULT_RETRY_REASONS+retry_reasons
  method = getattr(service, function)
  svcparms = dict(list(kwargs.items())+GM.Globals[GM.EXTRA_ARGS_LIST])
  if GC.Values[GC.API_CALLS_RATE_CHECK]:
    checkAPICallsRate()
  for n in range(1, retries+1):
    try:
      return method(**svcparms).execute()
    except googleapiclient.errors.HttpError as e:
      http_status, reason, message = checkGAPIError(e, soft_errors=soft_errors, retryOnHttpError=n < 3, service=service)
      if http_status == -1:
        continue
      if http_status == 0:
        return None
      if (n != retries) and (reason in all_retry_reasons):
        if reason in [GAPI.INTERNAL_ERROR, GAPI.BACKEND_ERROR] and bailOnInternalError and n == 2:
          raise GAPI.REASON_EXCEPTION_MAP[reason](message)
        waitOnFailure(n, retries, reason, message)
        if reason == GAPI.TRANSIENT_ERROR and bailOnTransientError:
          raise GAPI.REASON_EXCEPTION_MAP[reason](message)
        continue
      if reason in throw_reasons:
        if reason in GAPI.REASON_EXCEPTION_MAP:
          raise GAPI.REASON_EXCEPTION_MAP[reason](message)
        raise e
      if soft_errors:
        stderrErrorMsg('{0}: {1} - {2}{3}'.format(http_status, reason, message, ['', ': Giving up.'][n > 1]))
        return None
      if reason == GAPI.INSUFFICIENT_PERMISSIONS:
        APIAccessDeniedExit()
      systemErrorExit(HTTP_ERROR_RC, formatHTTPError(http_status, reason, message))
    except (oauth2client.client.AccessTokenRefreshError, google.auth.exceptions.RefreshError) as e:
      if isinstance(e.args, tuple):
        e = e.args[0]
      handleOAuthTokenError(e, GAPI.SERVICE_NOT_AVAILABLE in throw_reasons)
      raise GAPI.REASON_EXCEPTION_MAP[GAPI.SERVICE_NOT_AVAILABLE](str(e))
    except (http_client.ResponseNotReady, socket.error) as e:
      errMsg = 'Connection error: {0}'.format(convertSysToUTF8(str(e) or repr(e)))
      if n != retries:
        waitOnFailure(n, retries, SOCKET_ERROR_RC, errMsg)
        continue
      if soft_errors:
        writeStderr(convertUTF8toSys('\n{0}{1} - Giving up.\n'.format(ERROR_PREFIX, errMsg)))
        return None
      systemErrorExit(SOCKET_ERROR_RC, errMsg)
    except ValueError as e:
      if service._http.cache is not None:
        service._http.cache = None
        continue
      systemErrorExit(GOOGLE_API_ERROR_RC, str(e))
    except TypeError as e:
      systemErrorExit(GOOGLE_API_ERROR_RC, str(e))
    except (httplib2.HttpLib2Error, google.auth.exceptions.TransportError, RuntimeError) as e:
      if n != retries:
        service._http.connections = {}
        waitOnFailure(n, retries, NETWORK_ERROR_RC, str(e))
        continue
      handleServerError(e)
    except IOError as e:
      systemErrorExit(FILE_ERROR_RC, str(e))

def _processGAPIpagesResult(results, items, allResults, totalItems):
  if results:
    pageToken = results.get('nextPageToken')
    if items in results:
      pageItems = len(results[items])
      totalItems += pageItems
      if allResults is not None:
        allResults.extend(results[items])
    else:
      results = {items: []}
      pageItems = 0
  else:
    pageToken = None
    results = {items: []}
    pageItems = 0
  return (pageToken, totalItems)

def callGAPIpages(service, function, items,
                  maxItems=0,
                  throw_reasons=None, retry_reasons=None,
                  **kwargs):
  if throw_reasons is None:
    throw_reasons = []
  if retry_reasons is None:
    retry_reasons = []
  allResults = []
  totalItems = 0
  maxResults = kwargs.get('maxResults', 0)
  tweakMaxResults = maxItems and maxResults
  while True:
    if tweakMaxResults and maxItems-totalItems < maxResults:
      kwargs['maxResults'] = maxItems-totalItems
    results = callGAPI(service, function,
                       throw_reasons=throw_reasons, retry_reasons=retry_reasons,
                       **kwargs)
    pageToken, totalItems = _processGAPIpagesResult(results, items, allResults, totalItems)
    if not pageToken or (maxItems and totalItems >= maxItems):
      return allResults
    kwargs['pageToken'] = pageToken

def callGAPIitems(service, function, items,
                  throw_reasons=None, retry_reasons=None,
                  **kwargs):
  if throw_reasons is None:
    throw_reasons = []
  if retry_reasons is None:
    retry_reasons = []
  results = callGAPI(service, function,
                     throw_reasons=throw_reasons, retry_reasons=retry_reasons,
                     **kwargs)
  if results:
    return results.get(items, [])
  return []

def checkCloudPrintResult(result, throw_messages=None):
  if isinstance(result, bytes):
    result = result.decode(UTF8)
  if throw_messages is None:
    throw_messages = []
  if isinstance(result, string_types):
    try:
      result = json.loads(result)
    except ValueError:
      systemErrorExit(JSON_LOADS_ERROR_RC, result)
  if not result['success']:
    message = result['message']
    if message in throw_messages:
      if message in GCP.MESSAGE_EXCEPTION_MAP:
        raise GCP.MESSAGE_EXCEPTION_MAP[message](message)
    systemErrorExit(ACTION_FAILED_RC, '{0}: {1}'.format(result['errorCode'], result['message']))
  return result

def callGCP(service, function,
            throw_messages=None,
            **kwargs):
  result = callGAPI(service, function,
                    **kwargs)
  return checkCloudPrintResult(result, throw_messages=throw_messages)

def readDiscoveryFile(api_version):
  disc_filename = '{0}.json'.format(api_version)
  disc_file = os.path.join(GM.Globals[GM.GAM_PATH], disc_filename)
  if os.path.isfile(disc_file):
    json_string = readFile(disc_file, continueOnError=True, displayError=True)
  else:
    json_string = None
  if not json_string:
    invalidDiscoveryJsonExit(disc_file)
  try:
    discovery = json.loads(json_string)
    return (disc_file, discovery)
  except ValueError:
    invalidDiscoveryJsonExit(disc_file)

DISCOVERY_URIS = [googleapiclient.discovery.V1_DISCOVERY_URI, googleapiclient.discovery.V2_DISCOVERY_URI]

def getAPIversionHttpService(api):
  hasLocalJSON = API.hasLocalJSON(api)
  api, version, api_version, v2discovery = API.getVersion(api)
  httpObj = getHttpObj(cache=GM.Globals[GM.CACHE_DIR])
  if api in GM.Globals[GM.CURRENT_API_SERVICES] and version in GM.Globals[GM.CURRENT_API_SERVICES][api]:
    service = googleapiclient.discovery.build_from_document(GM.Globals[GM.CURRENT_API_SERVICES][api][version], http=httpObj)
    if GM.Globals[GM.CACHE_DISCOVERY_ONLY]:
      httpObj.cache = None
    return (api_version, httpObj, service)
  if not hasLocalJSON:
    retries = 3
    for n in range(1, retries+1):
      try:
        service = googleapiclient.discovery.build(api, version, http=httpObj, cache_discovery=False,
                                                  discoveryServiceUrl=DISCOVERY_URIS[v2discovery])
        GM.Globals[GM.CURRENT_API_SERVICES].setdefault(api, {})
        GM.Globals[GM.CURRENT_API_SERVICES][api][version] = service._rootDesc.copy()
        if GM.Globals[GM.CACHE_DISCOVERY_ONLY]:
          httpObj.cache = None
        return (api_version, httpObj, service)
      except googleapiclient.errors.UnknownApiNameOrVersion as e:
        systemErrorExit(GOOGLE_API_ERROR_RC, Msg.UNKNOWN_API_OR_VERSION.format(str(e), __author__))
      except (googleapiclient.errors.InvalidJsonError, KeyError, ValueError):
        httpObj.cache = None
        if n != retries:
          waitOnFailure(n, retries, INVALID_JSON_RC, Msg.INVALID_JSON_INFORMATION)
          continue
        systemErrorExit(INVALID_JSON_RC, Msg.INVALID_JSON_INFORMATION)
      except (http_client.ResponseNotReady, socket.error) as e:
        errMsg = 'Connection error: {0}'.format(convertSysToUTF8(str(e) or repr(e)))
        if n != retries:
          waitOnFailure(n, retries, SOCKET_ERROR_RC, errMsg)
          continue
        systemErrorExit(SOCKET_ERROR_RC, errMsg)
      except (httplib2.HttpLib2Error, google.auth.exceptions.TransportError, RuntimeError) as e:
        if n != retries:
          httpObj.connections = {}
          waitOnFailure(n, retries, NETWORK_ERROR_RC, str(e))
          continue
        handleServerError(e)
      except IOError as e:
        systemErrorExit(FILE_ERROR_RC, str(e))
  disc_file, discovery = readDiscoveryFile(api_version)
  try:
    service = googleapiclient.discovery.build_from_document(discovery, http=httpObj)
    GM.Globals[GM.CURRENT_API_SERVICES].setdefault(api, {})
    GM.Globals[GM.CURRENT_API_SERVICES][api][version] = service._rootDesc.copy()
    if GM.Globals[GM.CACHE_DISCOVERY_ONLY]:
      httpObj.cache = None
    return (api_version, httpObj, service)
  except (KeyError, ValueError):
    invalidDiscoveryJsonExit(disc_file)
  except IOError as e:
    systemErrorExit(FILE_ERROR_RC, str(e))

def buildGAPIObject(api):
  _, httpObj, service = getAPIversionHttpService(api)
  credentials = getClientCredentials()
  try:
    API_Scopes = set(list(service._rootDesc['auth']['oauth2']['scopes']))
  except KeyError:
    API_Scopes = set(API.VAULT_SCOPES) if api == API.VAULT else set()
  GM.Globals[GM.CURRENT_CLIENT_API] = api
  GM.Globals[GM.CURRENT_CLIENT_API_SCOPES] = API_Scopes.intersection(credentials.scopes)
  if api != API.OAUTH2 and not GM.Globals[GM.CURRENT_CLIENT_API_SCOPES]:
    systemErrorExit(NO_SCOPES_FOR_API_RC, Msg.NO_SCOPES_FOR_API.format(service._rootDesc['title']))
  retries = 3
  for n in range(1, retries+1):
    try:
      service._http = credentials.authorize(httpObj)
      if not GC.Values[GC.DOMAIN]:
        GC.Values[GC.DOMAIN] = credentials.id_token.get('hd', 'UNKNOWN').lower()
      if not GC.Values[GC.CUSTOMER_ID]:
        GC.Values[GC.CUSTOMER_ID] = GC.MY_CUSTOMER
      GM.Globals[GM.ADMIN] = credentials.id_token.get('email', 'UNKNOWN').lower()
      GM.Globals[GM.OAUTH2_CLIENT_ID] = credentials.client_id
      return service
    except (oauth2client.client.AccessTokenRefreshError, google.auth.exceptions.RefreshError) as e:
      if isinstance(e.args, tuple):
        e = e.args[0]
      handleOAuthTokenError(e, False)
    except (httplib2.HttpLib2Error, google.auth.exceptions.TransportError, RuntimeError) as e:
      if n != retries:
        httpObj.connections = {}
        waitOnFailure(n, retries, NETWORK_ERROR_RC, str(e))
        continue
      handleServerError(e)

# Override and wrap google_auth_httplib2 request methods so that the GAM
# user-agent string is inserted into HTTP request headers.
def _request_with_user_agent(request_method):
  """Inserts the GAM user-agent header kwargs sent to a method."""
  GAM_USER_AGENT = GAM_INFO

  def wrapped_request_method(self, *args, **kwargs):
    if kwargs.get('headers') is not None:
      if kwargs['headers'].get('user-agent'):
        if GAM_USER_AGENT not in kwargs['headers']['user-agent']:
          # Save the existing user-agent header and tack on the GAM user-agent.
          kwargs['headers']['user-agent'] = '{0} {1}'.format(GAM_USER_AGENT, kwargs['headers']['user-agent'])
      else:
        kwargs['headers']['user-agent'] = GAM_USER_AGENT
    else:
      kwargs['headers'] = {'user-agent': GAM_USER_AGENT}
    return request_method(self, *args, **kwargs)

  return wrapped_request_method

google_auth_httplib2.Request.__call__ = _request_with_user_agent(google_auth_httplib2.Request.__call__)
google_auth_httplib2.AuthorizedHttp.request = _request_with_user_agent(google_auth_httplib2.AuthorizedHttp.request)

def buildGAPIServiceObject(api, user, displayError=True):
  currentClientAPI = GM.Globals[GM.CURRENT_CLIENT_API]
  currentClientAPIScopes = GM.Globals[GM.CURRENT_CLIENT_API_SCOPES]
  GM.Globals[GM.CURRENT_CLIENT_API] = currentClientAPI
  GM.Globals[GM.CURRENT_CLIENT_API_SCOPES] = currentClientAPIScopes
  _, httpObj, service = getAPIversionHttpService(api)
  GM.Globals[GM.CURRENT_SVCACCT_API] = api
  GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES] = API.getSvcAcctScopesSet(api)
  GM.Globals[GM.CURRENT_SVCACCT_USER] = user
  credentials = getSvcAcctCredentials(GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES], user)
  request = google_auth_httplib2.Request(httpObj)
  retries = 3
  for n in range(1, retries+1):
    try:
      credentials.refresh(request)
      service._http = google_auth_httplib2.AuthorizedHttp(credentials, http=httpObj)
      return (user, service)
    except (httplib2.HttpLib2Error, google.auth.exceptions.TransportError, RuntimeError) as e:
      if n != retries:
        httpObj.connections = {}
        waitOnFailure(n, retries, NETWORK_ERROR_RC, str(e))
        continue
      handleServerError(e)
    except google.auth.exceptions.RefreshError as e:
      if isinstance(e.args, tuple):
        e = e.args[0]
      handleOAuthTokenError(e, True)
      if displayError:
        entityServiceNotApplicableWarning('User', user)
      return (user, None)

DEFAULT_SKIP_OBJECTS = set(['kind', 'etag', 'etags'])

# Clean a JSON object
def _cleanJSON(topStructure, listLimit=None, skipObjects=None, timeObjects=None):
  def _clean(structure, key):
    if not isinstance(structure, (dict, list)):
      if key not in timeObjects:
        if isinstance(structure, string_types) and GC.Values[GC.CSV_OUTPUT_CONVERT_CR_NL]:
          return escapeCRsNLs(structure)
        return structure
      if isinstance(structure, string_types) and not structure.isdigit():
        return formatLocalTime(structure)
      return formatLocalTimestamp(structure)
    if isinstance(structure, list):
      listLen = len(structure)
      listLen = min(listLen, listLimit or listLen)
      return [_clean(v, '') for v in structure[0:listLen]]
    return {k: _clean(v, k) for k, v in sorted(iteritems(structure)) if k not in allSkipObjects}

  allSkipObjects = DEFAULT_SKIP_OBJECTS.union(skipObjects or set())
  timeObjects = timeObjects or set()
  return _clean(topStructure, '')

def Version():
  version_data = 'GAM {0} - {1}\n{2}\nPython {3}.{4}.{5} {6}-bit {7}\ngoogle-api-python-client {8}\nhttplib2 {9}\noauth2client {10}\n{11} {12}\nPath: {13}\n'
  return version_data.format(__version__, GAM_URL, __author__, sys.version_info[0],
                             sys.version_info[1], sys.version_info[2], struct.calcsize('P')*8,
                             sys.version_info[3], googleapiclient.__version__, httplib2.__version__, oauth2client.__version__,
                             platform.platform(), platform.machine(), GM.Globals[GM.GAM_PATH]), True

def ChromeosdevicesAction(customerId, resourceId, **kwargs):
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    callGAPI(cd.chromeosdevices(), 'action',
             throw_reasons=[GAPI.INVALID, GAPI.CONDITION_NOT_MET,
                            GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, resourceId=resourceId, **kwargs)
    return ({}, True)
  except (GAPI.invalid, GAPI.conditionNotMet,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

CROS_TIME_OBJECTS = set(['lastSync', 'lastEnrollmentTime', 'supportEndDate', 'reportTime'])

def ChromeosdevicesGet(customerId, deviceId, **kwargs):
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.chromeosdevices(), 'get',
                      throw_reasons=[GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerId=customerId, deviceId=deviceId, **kwargs)
    return (_cleanJSON(result, timeObjects=CROS_TIME_OBJECTS), True)
  except (GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

def ChromeosdevicesList(customerId, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  kwargs['fields'] = 'nextPageToken,chromeosdevices({0})'.format(kwargs.get('fields', 'deviceId'))
  try:
    result = callGAPIpages(cd.chromeosdevices(), 'list', 'chromeosdevices',
                           throw_reasons=[GAPI.INVALID_INPUT, GAPI.INVALID_ORGUNIT, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                           customerId=customerId, **kwargs)
    return (_cleanJSON(result, timeObjects=CROS_TIME_OBJECTS), True)
  except (GAPI.invalidInput, GAPI.invalidOrgunit,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

def ChromeosdevicesMoveDevicesToOu(customerId, orgUnitPath, deviceIds):
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    callGAPI(cd.chromeosdevices(), 'moveDevicesToOu',
             throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, orgUnitPath=makeOrgUnitPathAbsolute(orgUnitPath), body={'deviceIds': deviceIds})
    return ({}, True)
  except (GAPI.invalidOrgunit, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

def ChromeosdevicesUpdate(customerId, deviceId, **kwargs):
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.chromeosdevices(), 'update',
                      throw_reasons=[GAPI.INVALID, GAPI.CONDITION_NOT_MET,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerId=customerId, deviceId=deviceId, **kwargs)
    return (_cleanJSON(result, timeObjects=CROS_TIME_OBJECTS), True)
  except (GAPI.invalid, GAPI.conditionNotMet,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

CUSTOMER_TIME_OBJECTS = ['customerCreationTime']

def CustomersGet(customerKey, **kwargs):
  if not customerKey:
    customerKey = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.customers(), 'get',
                      throw_reasons=[GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerKey=customerKey, **kwargs)
    result['verified'] = callGAPI(cd.domains(), 'get',
                                  throw_reasons=[GAPI.DOMAIN_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                                  customer=result['id'], domainName=result['customerDomain'], fields='verified')['verified']
    # From Jay Lee
    # If customer has changed primary domain, customerCreationTime is date of current primary being added, not customer create date.
    # We should get all domains and use oldest date
    customerCreationTime = formatLocalTime(result['customerCreationTime'])
    domains = callGAPIitems(cd.domains(), 'list', 'domains',
                            throw_reasons=[GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                            customer=customerKey, fields='domains(creationTime)')
    for domain in domains:
      domainCreationTime = formatLocalTimestamp(domain['creationTime'])
      if domainCreationTime < customerCreationTime:
        customerCreationTime = domainCreationTime
    result['customerCreationTime'] = customerCreationTime
    return (_cleanJSON(result, timeObjects=CUSTOMER_TIME_OBJECTS), True)
  except (GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden, GAPI.domainNotFound, GAPI.notFound) as e:
    return (str(e), False)

def CustomersPatch(customerKey, **kwargs):
  if not customerKey:
    customerKey = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.customers(), 'patch',
                      throw_reasons=[GAPI.DOMAIN_NOT_VERIFIED_SECONDARY, GAPI.INVALID, GAPI.INVALID_INPUT,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerKey=customerKey, **kwargs)
    return (_cleanJSON(result, timeObjects=CUSTOMER_TIME_OBJECTS), True)
  except (GAPI.domainNotVerifiedSecondary, GAPI.invalid, GAPI.invalidInput,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

DOMAIN_TIME_OBJECTS = set(['creationTime'])

def DomainsGet(customer, domainName, **kwargs):
  if not customer:
    customer = GC.Values[GC.CUSTOMER_ID]
  if not domainName:
    domainName = GC.Values[GC.DOMAIN]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.domains(), 'get',
                      throw_reasons=[GAPI.DOMAIN_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, domainName=domainName, **kwargs)
    return (_cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS), True)
  except GAPI.domainNotFound:
    return (Msg.DOES_NOT_EXIST, False)
  except (GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return (str(e), False)

def DomainsList(customer, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customer:
    customer = GC.Values[GC.CUSTOMER_ID]
  kwargs['fields'] = 'domains({0})'.format(kwargs.get('fields', 'domainName'))
  try:
    result = callGAPIpages(cd.domains(), 'list', 'domains',
                           throw_reasons=[GAPI.INVALID_MEMBER, GAPI.RESOURCE_NOT_FOUND,
                                          GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS,
                                          GAPI.FORBIDDEN, GAPI.BAD_REQUEST,
                                          GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER],
                           customer=customer, **kwargs)
    return (_cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS), True)
  except (GAPI.invalidMember, GAPI.resourceNotFound,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.badRequest, GAPI.invalidInput, GAPI.invalidParameter) as e:
    return (str(e), False)

def GroupsDelete(groupKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.groups(), 'delete',
                      throw_reasons=[GAPI.GROUP_NOT_FOUND, GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN, GAPI.INVALID],
                      groupKey=groupKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.invalid) as e:
    return (str(e), False)

def GroupsGet(groupKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.groups(), 'get',
                      throw_reasons=GAPI.GROUP_GET_THROW_REASONS,
                      retry_reasons=GAPI.GROUP_GET_RETRY_REASONS,
                      groupKey=groupKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.badRequest, GAPI.invalid, GAPI.systemError) as e:
    return (str(e), False)

def GroupsInsert(**kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.groups(), 'insert',
                      throw_reasons=GAPI.GROUP_CREATE_THROW_REASONS,
                      **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.duplicate, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.invalid, GAPI.invalidInput) as e:
    return (str(e), False)

def GroupsList(**kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if kwargs.get('customer') is None and kwargs.get('domain') is None:
    kwargs['customer'] = GC.Values[GC.CUSTOMER_ID]
  kwargs['fields'] = 'nextPageToken,groups({0})'.format(kwargs.get('fields', 'email'))
  try:
    result = callGAPIpages(cd.groups(), 'list', 'groups',
                           throw_reasons=[GAPI.INVALID_MEMBER, GAPI.RESOURCE_NOT_FOUND,
                                          GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS,
                                          GAPI.FORBIDDEN, GAPI.BAD_REQUEST,
                                          GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER],
                           **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.invalidMember, GAPI.resourceNotFound,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.badRequest, GAPI.invalidInput, GAPI.invalidParameter) as e:
    return (str(e), False)

def GroupsUpdate(groupKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.groups(), 'update',
                      throw_reasons=GAPI.GROUP_UPDATE_THROW_REASONS,
                      retry_reasons=GAPI.GROUP_GET_RETRY_REASONS,
                      groupKey=groupKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.invalid, GAPI.invalidInput) as e:
    return (str(e), False)

def GroupsAliasesDelete(groupKey, alias):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    callGAPI(cd.groups().aliases(), 'delete',
             throw_reasons=[GAPI.GROUP_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.INVALID, GAPI.FORBIDDEN,
                            GAPI.INVALID_RESOURCE, GAPI.CONDITION_NOT_MET],
             groupKey=groupKey, alias=alias)
    return ({}, True)
  except (GAPI.groupNotFound, GAPI.badRequest, GAPI.invalid, GAPI.forbidden,
          GAPI.invalidResource, GAPI.conditionNotMet) as e:
    return (str(e), False)

def GroupsAliasesInsert(groupKey, alias, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.groups().aliases(), 'insert',
                      throw_reasons=[GAPI.GROUP_NOT_FOUND, GAPI.BAD_REQUEST,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.FORBIDDEN, GAPI.DUPLICATE,
                                     GAPI.CONDITION_NOT_MET, GAPI.LIMIT_EXCEEDED],
                      groupKey=groupKey, body={'alias': alias}, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.badRequest,
          GAPI.invalid, GAPI.invalidInput, GAPI.forbidden, GAPI.duplicate,
          GAPI.conditionNotMet, GAPI.limitExceeded) as e:
    return (str(e), False)

def GroupsAliasesList(groupKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  kwargs['fields'] = 'aliases({0})'.format(kwargs.get('fields', 'alias'))
  try:
    result = callGAPIpages(cd.groups().aliases(), 'list', 'aliases',
                           throw_reasons=[GAPI.GROUP_NOT_FOUND, GAPI.BAD_REQUEST,
                                          GAPI.INVALID, GAPI.INVALID_RESOURCE, GAPI.FORBIDDEN,
                                          GAPI.CONDITION_NOT_MET],
                           groupKey=groupKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.badRequest,
          GAPI.invalid, GAPI.invalidResource, GAPI.forbidden,
          GAPI.conditionNotMet) as e:
    return (str(e), False)

def MembersDelete(groupKey, memberKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.members(), 'delete',
                      throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.MEMBER_NOT_FOUND, GAPI.INVALID_MEMBER,
                                                                GAPI.CONDITION_NOT_MET, GAPI.CONFLICT],
                      retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                      groupKey=groupKey, memberKey=memberKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden,
          GAPI.memberNotFound, GAPI.invalidMember, GAPI.conditionNotMet, GAPI.conflict) as e:
    return (str(e), False)

def MembersGet(groupKey, memberKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.members(), 'get',
                      throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.MEMBER_NOT_FOUND],
                      retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                      groupKey=groupKey, memberKey=memberKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden,
          GAPI.memberNotFound) as e:
    return (str(e), False)

def MembersInsert(groupKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.members(), 'insert',
                      throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.DUPLICATE, GAPI.MEMBER_NOT_FOUND, GAPI.RESOURCE_NOT_FOUND,
                                                                GAPI.INVALID_MEMBER, GAPI.CYCLIC_MEMBERSHIPS_NOT_ALLOWED,
                                                                GAPI.CONDITION_NOT_MET, GAPI.CONFLICT],
                      retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                      groupKey=groupKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden,
          GAPI.duplicate, GAPI.memberNotFound, GAPI.resourceNotFound,
          GAPI.invalidMember, GAPI.cyclicMembershipsNotAllowed, GAPI.conditionNotMet, GAPI.conflict) as e:
    return (str(e), False)

def MembersList(groupKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  kwargs['fields'] = 'nextPageToken,members({0})'.format(kwargs.get('fields', 'email'))
  try:
    result = callGAPIpages(cd.members(), 'list', 'members',
                           throw_reasons=GAPI.MEMBERS_THROW_REASONS,
                           retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                           groupKey=groupKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis,
          GAPI.invalid, GAPI.forbidden) as e:
    return (str(e), False)

def MembersPatch(groupKey, memberKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.members(), 'patch',
                      throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.MEMBER_NOT_FOUND, GAPI.INVALID_MEMBER],
                      retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                      groupKey=groupKey, memberKey=memberKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden,
          GAPI.memberNotFound, GAPI.invalidMember) as e:
    return (str(e), False)

def MobiledevicesAction(customerId, resourceId, **kwargs):
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    callGAPI(cd.mobiledevices(), 'action',
             bailOnInternalError=True, throw_reasons=[GAPI.INTERNAL_ERROR, GAPI.RESOURCE_ID_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, resourceId=resourceId, **kwargs)
    return ({}, True)
  except (GAPI.internalError, GAPI.resourceIdNotFound, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

def MobiledevicesDelete(customerId, resourceid):
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    callGAPI(cd.mobiledevices(), 'delete',
             bailOnInternalError=True, throw_reasons=[GAPI.INTERNAL_ERROR, GAPI.RESOURCE_ID_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, resourceid=resourceid)
    return ({}, True)
  except (GAPI.internalError, GAPI.resourceIdNotFound, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

MOBILE_TIME_OBJECTS = set(['firstSync', 'lastSync'])

def MobiledevicesGet(customerId, resourceid, **kwargs):
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.mobiledevices(), 'get',
                      throw_reasons=[GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerId=customerId, resourceid=resourceid, **kwargs)
    return (_cleanJSON(result, timeObjects=MOBILE_TIME_OBJECTS), True)
  except (GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

def MobiledevicesList(customerId, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  kwargs['fields'] = 'nextPageToken,mobiledevices({0})'.format(kwargs.get('fields', 'resourceid'))
  try:
    result = callGAPIpages(cd.mobiledevices(), 'list', 'mobiledevices',
                           throw_reasons=[GAPI.INVALID_INPUT, GAPI.INVALID_ORGUNIT, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                           customerId=customerId, **kwargs)
    return (_cleanJSON(result, timeObjects=MOBILE_TIME_OBJECTS), True)
  except (GAPI.invalidInput, GAPI.invalidOrgunit,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return (str(e), False)

def _getTopLevelOrgId(cd, customerId, parentOrgUnitPath):
  try:
    temp_org = callGAPI(cd.orgunits(), 'insert',
                        throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.BACKEND_ERROR, GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                        customerId=customerId, body={'name': 'temp-delete-me', 'parentOrgUnitPath': parentOrgUnitPath}, fields='parentOrgUnitId,orgUnitId')
  except (GAPI.invalidOrgunit, GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)
  try:
    callGAPI(cd.orgunits(), 'delete',
             throw_reasons=[GAPI.CONDITION_NOT_MET, GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND, GAPI.BACKEND_ERROR, GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
             customerId=customerId, orgUnitPath=temp_org['orgUnitId'])
  except (GAPI.conditionNotMet, GAPI.invalidOrgunit, GAPI.orgunitNotFound, GAPI.backendError):
    pass
  except (GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)
  return (temp_org['parentOrgUnitId'], True)

def OrgunitsDelete(customerId, orgUnitPath, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  try:
    result = callGAPI(cd.orgunits(), 'delete',
                      throw_reasons=[GAPI.CONDITION_NOT_MET, GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND, GAPI.BACKEND_ERROR,
                                     GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, orgUnitPath=encodeOrgUnitPath(makeOrgUnitPathRelative(orgUnitPath)), **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.conditionNotMet, GAPI.invalidOrgunit, GAPI.orgunitNotFound, GAPI.backendError,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)

def OrgunitsGet(customerId, orgUnitPath, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  if not orgUnitPath:
    orgUnitPath = '/'
  try:
    if orgUnitPath == '/':
      orgs = callGAPI(cd.orgunits(), 'list',
                      throw_reasons=[GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, type='children',
                      fields='organizationUnits(parentOrgUnitId)')
      if orgs.get('organizationUnits', []):
        orgUnitPath = orgs['organizationUnits'][0]['parentOrgUnitId']
      else:
        topLevelOrgId, status = _getTopLevelOrgId(cd, customerId, '/')
        if not status:
          return (topLevelOrgId, status)
        orgUnitPath = topLevelOrgId
    else:
      orgUnitPath = makeOrgUnitPathRelative(orgUnitPath)
    result = callGAPI(cd.orgunits(), 'get',
                      throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND, GAPI.BACKEND_ERROR,
                                     GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, orgUnitPath=encodeOrgUnitPath(orgUnitPath), **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.invalidOrgunit, GAPI.orgunitNotFound, GAPI.backendError,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)

def OrgunitsInsert(customerId, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  try:
    result = callGAPI(cd.orgunits(), 'insert',
                      throw_reasons=[GAPI.INVALID_PARENT_ORGUNIT, GAPI.INVALID_ORGUNIT, GAPI.BACKEND_ERROR,
                                     GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.invalidParentOrgunit, GAPI.invalidOrgunit, GAPI.backendError,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)

def OrgunitsList(customerId, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  kwargs['fields'] = 'organizationUnits({0})'.format(kwargs.get('fields', 'orgUnitPath,orgUnitId'))
  try:
    result = callGAPIpages(cd.orgunits(), 'list', 'organizationUnits',
                           throw_reasons=[GAPI.ORGUNIT_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                           customerId=customerId, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.orgunitNotFound, GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)

def OrgunitsUpdate(customerId, orgUnitPath, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if not customerId:
    customerId = GC.Values[GC.CUSTOMER_ID]
  if not orgUnitPath:
    orgUnitPath = '/'
  try:
    result = callGAPI(cd.orgunits(), 'update',
                      throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND, GAPI.BACKEND_ERROR, GAPI.INVALID_ORGUNIT_NAME,
                                     GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, orgUnitPath=encodeOrgUnitPath(makeOrgUnitPathRelative(orgUnitPath)), **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.invalidOrgunit, GAPI.orgunitNotFound, GAPI.backendError, GAPI.invalidOrgunitName,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)

def UsersDelete(userKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.users(), 'delete',
                      throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN],
                      userKey=userKey, **kwargs)
    return (_cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS), True)
  except (GAPI.userNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return (str(e), False)

USER_SKIP_OBJECTS = set(['thumbnailPhotoEtag'])
USER_TIME_OBJECTS = set(['creationTime', 'deletionTime', 'lastLoginTime'])

def UsersGet(userKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.users(), 'get',
                      throw_reasons=GAPI.USER_GET_THROW_REASONS+[GAPI.INVALID_INPUT],
                      userKey=userKey, **kwargs)
    return (_cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS), True)
  except (GAPI.userNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.badRequest, GAPI.invalidInput, GAPI.systemError) as e:
    return (str(e), False)

def UsersInsert(**kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.users(), 'insert',
                      throw_reasons=[GAPI.DUPLICATE, GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.INVALID_ORGUNIT, GAPI.INVALID_SCHEMA_VALUE],
                      **kwargs)
    return (_cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS), True)
  except (GAPI.duplicate, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.invalidOrgunit, GAPI.invalidSchemaValue) as e:
    return (str(e), False)

def UsersList(**kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  if kwargs.get('customer') is None and kwargs.get('domain') is None:
    kwargs['customer'] = GC.Values[GC.CUSTOMER_ID]
  kwargs['fields'] = 'nextPageToken,users({0})'.format(kwargs.get('fields', 'primaryEmail'))
  try:
    result = callGAPIpages(cd.users(), 'list', 'users',
                           throw_reasons=[GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN,
                                          GAPI.INVALID_ORGUNIT, GAPI.INVALID_INPUT,
                                          GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND],
                           **kwargs)
    return (_cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS), True)
  except (GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.invalidOrgunit, GAPI.invalidInput,
          GAPI.badRequest, GAPI.resourceNotFound) as e:
    return (str(e), False)

def UsersUpdate(userKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.users(), 'update',
                      throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.DOMAIN_NOT_FOUND, GAPI.FORBIDDEN,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.INVALID_ORGUNIT, GAPI.INVALID_SCHEMA_VALUE],
                      userKey=userKey, **kwargs)
    return (_cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS), True)
  except (GAPI.userNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.invalidOrgunit, GAPI.invalidSchemaValue) as e:
    return (str(e), False)

def UsersAliasesDelete(userKey, alias):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    callGAPI(cd.users().aliases(), 'delete',
             throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.INVALID, GAPI.FORBIDDEN,
                            GAPI.INVALID_RESOURCE, GAPI.CONDITION_NOT_MET],
             userKey=userKey, alias=alias)
    return ({}, True)
  except (GAPI.userNotFound, GAPI.badRequest, GAPI.invalid, GAPI.forbidden,
          GAPI.invalidResource, GAPI.conditionNotMet) as e:
    return (str(e), False)

def UsersAliasesInsert(userKey, alias, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  try:
    result = callGAPI(cd.users().aliases(), 'insert',
                      throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.BAD_REQUEST,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.FORBIDDEN, GAPI.DUPLICATE,
                                     GAPI.CONDITION_NOT_MET, GAPI.LIMIT_EXCEEDED],
                      userKey=userKey, body={'alias': alias}, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.userNotFound, GAPI.badRequest,
          GAPI.invalid, GAPI.invalidInput, GAPI.forbidden, GAPI.duplicate,
          GAPI.conditionNotMet, GAPI.limitExceeded) as e:
    return (str(e), False)

def UsersAliasesList(userKey, **kwargs):
  cd = buildGAPIObject(API.DIRECTORY)
  kwargs['fields'] = 'aliases({0})'.format(kwargs.get('fields', 'email'))
  try:
    result = callGAPIpages(cd.users().aliases(), 'list', 'aliases',
                           throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.BAD_REQUEST,
                                          GAPI.INVALID, GAPI.INVALID_RESOURCE, GAPI.FORBIDDEN,
                                          GAPI.CONDITION_NOT_MET],
                           userKey=userKey, **kwargs)
    return (_cleanJSON(result), True)
  except (GAPI.userNotFound, GAPI.badRequest,
          GAPI.invalid, GAPI.invalidResource, GAPI.forbidden,
          GAPI.conditionNotMet) as e:
    return (str(e), False)
