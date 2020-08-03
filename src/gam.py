#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# GAMLite
#
# Copyright 2020, All Rights Reserved.
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
"""GAMLite is an API wrapper which allows Administrators to control their G Suite domain and accounts.

For more information, see https://github.com/taers232c/GAMLite
"""

__author__ = 'Ross Scroggs <ross.scroggs@gmail.com>'
__version__ = '2.00.02'
__license__ = 'Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)'

import base64
import codecs
import collections
import configparser
import datetime
from html.entities import name2codepoint
from html.parser import HTMLParser
import http.client as http_client
import io
import json
import logging
import os
import platform
import random
import re
import string
import struct
import sys
import time

from filelock import FileLock

from gamlib import glapi as API
from gamlib import glcfg as GC
from gamlib import glgapi as GAPI
from gamlib import glgcp as GCP
from gamlib import glglobals as GM
from gamlib import glmsgs as Msg

import googleapiclient
import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
import google.oauth2.credentials
import google.oauth2.id_token
import google.oauth2.service_account
import google_auth_httplib2
import httplib2
from iso8601 import iso8601

if platform.system() == 'Linux':
  import distro

def ISOformatTimeStamp(timestamp):
  return timestamp.isoformat('T', 'seconds')

def currentISOformatTimeStamp(timespec='milliseconds'):
  return datetime.datetime.now(GC.Values[GC.TIMEZONE]).isoformat('T', timespec)

GM.Globals[GM.GAM_PATH] = os.path.dirname(os.path.realpath(__file__)) if not getattr(sys, 'frozen', False) else os.path.dirname(sys.executable)

GIT_USER = 'taers232c'
GAM = 'GAMLite'
GAM_URL = f'https://github.com/{GIT_USER}/{GAM}'
GAM_USER_AGENT = (f'{GAM} {__version__} - {GAM_URL} / '
                  f'{__author__} / '
                  f'Python {sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]} {sys.version_info[3]} / '
                  f'{platform.platform()} {platform.machine()} /'
                  )
GAM_RELEASES = f'https://github.com/{GIT_USER}/{GAM}/releases'
GAM_WIKI = f'https://github.com/{GIT_USER}/{GAM}/wiki'
GAM_LATEST_RELEASE = f'https://api.github.com/repos/{GIT_USER}/{GAM}/releases/latest'

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

# Python 3 values
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
MIMETYPE_GA_SHORTCUT = APPLICATION_VND_GOOGLE_APPS+'shortcut'
MIMETYPE_GA_3P_SHORTCUT = APPLICATION_VND_GOOGLE_APPS+'drive-sdk'
MIMETYPE_GA_SITE = APPLICATION_VND_GOOGLE_APPS+'site'
MIMETYPE_GA_SPREADSHEET = APPLICATION_VND_GOOGLE_APPS+'spreadsheet'
MIMETYPE_TEXT_HTML = 'text/html'
MIMETYPE_TEXT_PLAIN = 'text/plain'

GOOGLE_NAMESERVERS = ['8.8.8.8', '8.8.4.4']
NEVER_DATE = '1970-01-01'
NEVER_DATETIME = '1970-01-01 00:00'
NEVER_TIME = '1970-01-01T00:00:00.000Z'
NEVER_END_DATE = '1969-12-31'
NEVER_START_DATE = NEVER_DATE
REFRESH_EXPIRY = '1970-01-01T00:00:01Z'

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
SCOPES_NOT_AUTHORIZED_RC = 10
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

class _DeHTMLParser(HTMLParser): #pylint: disable=abstract-method
  def __init__(self):
    HTMLParser.__init__(self)
    self.__text = []

  def handle_data(self, data):
    self.__text.append(data)

  def handle_charref(self, name):
    self.__text.append(chr(int(name[1:], 16)) if name.startswith('x') else chr(int(name)))

  def handle_entityref(self, name):
    cp = name2codepoint.get(name)
    if cp:
      self.__text.append(chr(cp))
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
          self.__text.append(f'({attr[1]}) ')
          break
    elif tag == 'div':
      if not attrs:
        self.__text.append('\n')
    elif tag in {'http:', 'https'}:
      self.__text.append(f' ({tag}//{attrs[0][0]}) ')

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
    if isinstance(kvList[i], (bool, float, int)):
      msg += str(kvList[i])
    else:
      msg += kvList[i]
    i += 1
    if i < l:
      val = kvList[i]
      if (val is not None) or (i == l-1):
        msg += ':'
        if (val is not None) and (not isinstance(val, str) or val):
          msg += ' '
          if isinstance(val, (bool, float, int)):
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

def printLine(message):
  writeStdout(message+'\n')

def printBlankLine():
  writeStdout('\n')

def printKeyValueList(kvList):
  writeStdout(formatKeyValueList('', kvList, '\n'))

# Error exits
def setSysExitRC(sysRC):
  GM.Globals[GM.SYSEXITRC] = sysRC

def printErrorMessage(sysRC, message):
  setSysExitRC(sysRC)
  writeStderr(formatKeyValueList('', [ERROR, message], '\n'))

def stderrErrorMsg(message):
  writeStderr('\n{0}{1}\n'.format(ERROR_PREFIX, message))

def stderrWarningMsg(message):
  writeStderr('\n{0}{1}\n'.format(WARNING_PREFIX, message))

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

def ClientAPIAccessDeniedExit():
  stderrErrorMsg(Msg.API_ACCESS_DENIED)
  missingScopes = API.getClientScopesSet(GM.Globals[GM.CURRENT_CLIENT_API])-GM.Globals[GM.CURRENT_CLIENT_API_SCOPES]
  if missingScopes:
    writeStderr(Msg.API_CHECK_CLIENT_AUTHORIZATION.format(GM.Globals[GM.OAUTH2_CLIENT_ID],
                                                          ','.join(sorted(missingScopes))))
  systemErrorExit(API_ACCESS_DENIED_RC, None)

def SvcAcctAPIAccessDeniedExit():
  if GM.Globals[GM.CURRENT_SVCACCT_API] == API.GMAIL and GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES][0] == API.GMAIL_SEND_SCOPE:
    systemErrorExit(OAUTH2SERVICE_JSON_REQUIRED_RC, Msg.NO_SVCACCT_ACCESS_ALLOWED)
  stderrErrorMsg(Msg.API_ACCESS_DENIED)
  apiOrScopes = API.getAPIName(GM.Globals[GM.CURRENT_SVCACCT_API]) if GM.Globals[GM.CURRENT_SVCACCT_API] else ','.join(sorted(GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES]))
  writeStderr(Msg.API_CHECK_SVCACCT_AUTHORIZATION.format(GM.Globals[GM.OAUTH2SERVICE_CLIENT_ID],
                                                         apiOrScopes,
                                                         GM.Globals[GM.CURRENT_SVCACCT_USER]))
  systemErrorExit(API_ACCESS_DENIED_RC, None)

def APIAccessDeniedExit():
  if not GM.Globals[GM.CURRENT_SVCACCT_USER] and GM.Globals[GM.CURRENT_CLIENT_API]:
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

def expiredRevokedOauth2TxtExit():
  stderrErrorMsg(Msg.IS_EXPIRED_OR_REVOKED.format('Client OAuth2 File', GC.Values[GC.OAUTH2_TXT]))
  writeStderr(Msg.EXECUTE_GAM_OAUTH_CREATE)
  systemErrorExit(OAUTH2_TXT_REQUIRED_RC, None)

def invalidDiscoveryJsonExit(fileName):
  stderrErrorMsg(Msg.DOES_NOT_EXIST_OR_HAS_INVALID_FORMAT.format('Discovery File', fileName))
  systemErrorExit(INVALID_JSON_RC, None)

# Choices is the valid set of choices that was expected
def formatChoiceList(choices):
  choiceList = [c if c else "''" for c in choices]
  if len(choiceList) <= 5:
    return '|'.join(choiceList)
  return '|'.join(sorted(choiceList))

def addCourseIdScope(courseId):
  if not courseId.isdigit() and courseId[:2] not in {'d:', 'p:'}:
    return f'd:{courseId}'
  return courseId

def removeCourseIdScope(courseId):
  if courseId.startswith('d:'):
    return courseId[2:]
  return courseId

UID_PATTERN = re.compile(r'u?id: ?(.+)', re.IGNORECASE)
PEOPLE_PATTERN = re.compile(r'people/([0-9]+)$', re.IGNORECASE)

def integerLimits(minVal, maxVal, item='integer'):
  if (minVal is not None) and (maxVal is not None):
    return f'{item} {minVal}<=x<={maxVal}'
  if minVal is not None:
    return f'{item} x>={minVal}'
  if maxVal is not None:
    return f'{item} x<={maxVal}'
  return f'{item} x'

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
  return f'{http_status}: {reason} - {message}'

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

def fileErrorMessage(filename, e):
  return f'File: {filename}, {str(e)}'

def fdErrorMessage(f, defaultFilename, e):
  return fileErrorMessage(getattr(f, 'name') if hasattr(f, 'name') else defaultFilename, e)

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
def openFile(filename, mode=DEFAULT_FILE_READ_MODE, encoding=None, errors=None, newline=None,
             continueOnError=False, displayError=True, stripUTFBOM=False):
  try:
    if filename != '-':
      kwargs = setEncoding(mode, encoding)
      f = open(os.path.expanduser(filename), mode, errors=errors, newline=newline, **kwargs)
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
      return StringIOobject(str(sys.stdin.read()))
    if 'b' not in mode:
      return sys.stdout
    return os.fdopen(os.dup(sys.stdout.fileno()), 'wb')
  except (IOError, LookupError, UnicodeError) as e:
    if continueOnError:
      if displayError:
        stderrWarningMsg(fileErrorMessage(filename, e))
        setSysExitRC(FILE_ERROR_RC)
      return None
    systemErrorExit(FILE_ERROR_RC, fileErrorMessage(filename, e))

# Close a file
def closeFile(f, forceFlush=False):
  try:
    if forceFlush:
      # Necessary to make sure file is flushed by both Python and OS
      # https://stackoverflow.com/a/13762137/1503886
      f.flush()
      os.fsync(f.fileno())
    f.close()
    return True
  except IOError as e:
    stderrErrorMsg(fdErrorMessage(f, 'Unknown', e))
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
    return str(sys.stdin.read())
  except (IOError, LookupError, UnicodeDecodeError, UnicodeError) as e:
    if continueOnError:
      if displayError:
        stderrWarningMsg(fileErrorMessage(filename, e))
        setSysExitRC(FILE_ERROR_RC)
      return None
    systemErrorExit(FILE_ERROR_RC, fileErrorMessage(filename, e))

# Write a file
def writeFile(filename, data, mode=DEFAULT_FILE_WRITE_MODE,
              continueOnError=False, displayError=True):
  try:
    if filename != '-':
      kwargs = setEncoding(mode, None)
      with open(os.path.expanduser(filename), mode, **kwargs) as f:
        f.write(data)
      return True
    GM.Globals[GM.STDOUT].get(GM.REDIRECT_MULTI_FD, sys.stdout).write(data)
    return True
  except (IOError, LookupError, UnicodeError) as e:
    if continueOnError:
      if displayError:
        stderrErrorMsg(fileErrorMessage(filename, e))
      setSysExitRC(FILE_ERROR_RC)
      return False
    systemErrorExit(FILE_ERROR_RC, fileErrorMessage(filename, e))

# Write a file, return error
def writeFileReturnError(filename, data, mode=DEFAULT_FILE_WRITE_MODE):
  try:
    kwargs = {'encoding': GM.Globals[GM.SYS_ENCODING]} if 'b' not in mode else {}
    with open(os.path.expanduser(filename), mode, **kwargs) as f:
      f.write(data)
    return (True, None)
  except (IOError, LookupError, UnicodeError) as e:
    return (False, e)

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
      error_message = f'API calls per 100 seconds limit {GC.Values[GC.API_CALLS_RATE_LIMIT]} exceeded'
      writeStderr(f'{WARNING_PREFIX}{error_message}: Backing off: {delta} seconds\n')
      flushStderr()
      time.sleep(delta)
      if GC.Values[GC.SHOW_API_CALLS_RETRY_DATA]:
        incrAPICallsRetryData(error_message, delta)
      GM.Globals[GM.RATE_CHECK_START] = time.time()
    else:
      GM.Globals[GM.RATE_CHECK_START] = current
    GM.Globals[GM.RATE_CHECK_COUNT] = 0

class NullHandler(logging.Handler):
  def emit(self, record):
    pass

def initializeLogging():
  nh = NullHandler()
  logging.getLogger().addHandler(nh)

# Set global variables from config file
def SetGlobalVariables(configFile, sectionName=None, config=None, save=False, verify=False):

  def _stringInQuotes(value):
    return (len(value) > 1) and (((value.startswith('"') and value.endswith('"'))) or ((value.startswith("'") and value.endswith("'"))))

  def _stripStringQuotes(value):
    if _stringInQuotes(value):
      return value[1:-1]
    return value

  def _quoteStringIfLeadingTrailingBlanks(value):
    if not value:
      return "''"
    if _stringInQuotes(value):
      return value
    if (value[0] != ' ') and (value[-1] != ' '):
      return value
    return f"'{value}'"

  def _selectSection(value):
    if (not value) or (value.upper() == configparser.DEFAULTSECT):
      return configparser.DEFAULTSECT
    if GM.Globals[GM.PARSER].has_section(value):
      return value
    systemErrorExit(CONFIG_ERROR_RC, formatKeyValueList('', ['Section', value, Msg.NOT_FOUND], ''))

  def _printValueError(sectionName, itemName, value, errMessage, sysRC=CONFIG_ERROR_RC):
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
    _printValueError(sectionName, itemName, value, f'{Msg.EXPECTED}: {formatChoiceList(TRUE_FALSE)}')
    return False

  def _getCfgCharacter(sectionName, itemName):
    value = codecs.escape_decode(bytes(_stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName)), UTF8))[0].decode(UTF8)
    if not value and (itemName == 'csv_output_field_delimiter'):
      return ' '
    if len(value) == 1:
      return value
    _printValueError(sectionName, itemName, f'"{value}"', f'{Msg.EXPECTED}: {integerLimits(1, 1, Msg.STRING_LENGTH)}')
    return ''

  def _getCfgChoice(sectionName, itemName):
    value = _stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName)).lower()
    choices = GC.VAR_INFO[itemName][GC.VAR_CHOICES]
    if value in choices:
      return choices[value]
    _printValueError(sectionName, itemName, f'"{value}"', f'{Msg.EXPECTED}: {",".join(choices)}')
    return ''

  def _getCfgNumber(sectionName, itemName):
    value = GM.Globals[GM.PARSER].get(sectionName, itemName)
    minVal, maxVal = GC.VAR_INFO[itemName][GC.VAR_LIMITS]
    try:
      number = int(value) if GC.VAR_INFO[itemName][GC.VAR_TYPE] == GC.TYPE_INTEGER else float(value)
      if ((minVal is None) or (number >= minVal)) and ((maxVal is None) or (number <= maxVal)):
        return number
      if (minVal is not None) and (number < minVal):
        number = minVal
      else:
        number = maxVal
      _printValueError(sectionName, itemName, value, f'{Msg.EXPECTED}: {integerLimits(minVal, maxVal)}, {Msg.USED}: {number}', sysRC=0)
      return number
    except ValueError:
      pass
    _printValueError(sectionName, itemName, value, f'{Msg.EXPECTED}: {integerLimits(minVal, maxVal)}')
    return 0

  def _getCfgPassword(sectionName, itemName):
    value = GM.Globals[GM.PARSER].get(sectionName, itemName)
    if isinstance(value, bytes):
      return value
    value = _stripStringQuotes(value)
    if value.startswith("b'") and value.endswith("'"):
      return bytes(value[2:-1], UTF8)
    if value:
      return value
    return ''

  def _getCfgString(sectionName, itemName):
    value = _stripStringQuotes(GM.Globals[GM.PARSER].get(sectionName, itemName))
    minLen, maxLen = GC.VAR_INFO[itemName].get(GC.VAR_LIMITS, (None, None))
    if ((minLen is None) or (len(value) >= minLen)) and ((maxLen is None) or (len(value) <= maxLen)):
      return value
    _printValueError(sectionName, itemName, f'"{value}"', f'{Msg.EXPECTED}: {integerLimits(minLen, maxLen, Msg.STRING_LENGTH)}')
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
      _printValueError(sectionName, itemName, value, f'{Msg.EXPECTED}: {TIMEZONE_FORMAT_REQUIRED}')
      GM.Globals[GM.CONVERT_TO_LOCAL_TIME] = False
      return iso8601.UTC

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
      systemErrorExit(FILE_ERROR_RC, fileErrorMessage(fileName, e))

  def _writeGamCfgFile(config, fileName, action):
    try:
      with open(fileName, DEFAULT_FILE_WRITE_MODE) as f:
        config.write(f)
      printKeyValueList(['Config File', fileName, action])
    except IOError as e:
      stderrErrorMsg(fileErrorMessage(fileName, e))

  def _verifyValues(sectionName):
    printKeyValueList(['Section', sectionName])
    for itemName in sorted(GC.VAR_INFO):
      cfgValue = GM.Globals[GM.PARSER].get(sectionName, itemName)
      varType = GC.VAR_INFO[itemName][GC.VAR_TYPE]
      if varType == GC.TYPE_CHOICE:
        for choice, value in iter(GC.VAR_INFO[itemName][GC.VAR_CHOICES].items()):
          if cfgValue == value:
            cfgValue = choice
            break
      elif varType not in [GC.TYPE_BOOLEAN, GC.TYPE_INTEGER, GC.TYPE_FLOAT, GC.TYPE_PASSWORD]:
        cfgValue = _quoteStringIfLeadingTrailingBlanks(cfgValue)
      if varType == GC.TYPE_FILE:
        expdValue = _getCfgFile(sectionName, itemName)
        if cfgValue not in ("''", expdValue):
          cfgValue = f'{cfgValue} ; {expdValue}'
      elif varType == GC.TYPE_DIRECTORY:
        expdValue = _getCfgDirectory(sectionName, itemName)
        if cfgValue not in ("''", expdValue):
          cfgValue = f'{cfgValue} ; {expdValue}'
      elif (itemName == GC.SECTION) and (sectionName != configparser.DEFAULTSECT):
        continue
      printLine(f'{itemName} = {cfgValue}')

  initializeLogging()
  GM.Globals[GM.GAM_CFG_FILE] = configFile
  GM.Globals[GM.PARSER] = configparser.RawConfigParser(defaults=collections.OrderedDict(sorted(list(GC.Defaults.items()), key=lambda t: t[0])))
  _readGamCfgFile(GM.Globals[GM.PARSER], GM.Globals[GM.GAM_CFG_FILE])
  status = {'errors': False}
  if sectionName is not None:
    sectionName = _selectSection(sectionName)
  else:
    sectionName = _getCfgSection(configparser.DEFAULTSECT, GC.SECTION)
# config {"<VariableName>": "<Value>"(, "<VariableName>": "<Value>")*}
  if config:
    for itemName, value in sorted(iter(config.items)):
      varType = GC.VAR_INFO[itemName][GC.VAR_TYPE]
      if varType == GC.TYPE_BOOLEAN:
        value = value.strip().lower()
      elif varType == GC.TYPE_CHARACTER:
        value = codecs.escape_decode(bytes(value, UTF8))[0].decode(UTF8)
      elif varType == GC.TYPE_CHOICE:
        value = value.strip().lower()
      elif varType == GC.TYPE_INTEGER:
        value = str(value)
      elif varType == GC.TYPE_FLOAT:
        value = str(value)
      elif varType == GC.TYPE_PASSWORD:
        if value and value.startswith("b'") and value.endswith("'"):
          value = bytes(value[2:-1], UTF8)
      elif varType == GC.TYPE_TIMEZONE:
        value = value.strip()
      else:
        value = _quoteStringIfLeadingTrailingBlanks(value)
      GM.Globals[GM.PARSER].set(sectionName, itemName, value)
# save
  if save:
    _writeGamCfgFile(GM.Globals[GM.PARSER], GM.Globals[GM.GAM_CFG_FILE], 'Saved')
# verify
  if verify:
    _verifyValues(sectionName)
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
    elif varType == GC.TYPE_PASSWORD:
      GC.Values[itemName] = _getCfgPassword(sectionName, itemName)
    elif varType == GC.TYPE_STRING:
      GC.Values[itemName] = _getCfgString(sectionName, itemName)
    elif varType == GC.TYPE_FILE:
      GC.Values[itemName] = _getCfgFile(sectionName, itemName)
  if status['errors']:
    sys.exit(CONFIG_ERROR_RC)
# Global values cleanup
  GC.Values[GC.DOMAIN] = GC.Values[GC.DOMAIN].lower()
# Create/set mode for oauth2.txt.lock
  if not GM.Globals[GM.OAUTH2_TXT_LOCK]:
    fileName = f'{GC.Values[GC.OAUTH2_TXT]}.lock'
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
  httpObj = httplib2.Http(cache=cache,
                          timeout=timeout,
                          ca_certs=GC.Values[GC.CACERTS_PEM],
                          disable_ssl_certificate_validation=GC.Values[GC.NO_VERIFY_SSL],
                          tls_maximum_version=tls_maximum_version,
                          tls_minimum_version=tls_minimum_version)
  httpObj.redirect_codes = set(httpObj.redirect_codes) - {308}
  return httpObj

def _force_user_agent(user_agent):
  """Creates a decorator which can force a user agent in HTTP headers."""

  def decorator(request_method):
    """Wraps a request method to insert a user-agent in HTTP headers."""

    def wrapped_request_method(*args, **kwargs):
      """Modifies HTTP headers to include a specified user-agent."""
      if kwargs.get('headers') is not None:
        if kwargs['headers'].get('user-agent'):
          if user_agent not in kwargs['headers']['user-agent']:
            # Save the existing user-agent header and tack on our own.
            kwargs['headers']['user-agent'] = f'{user_agent} {kwargs["headers"]["user-agent"]}'
        else:
          kwargs['headers']['user-agent'] = user_agent
      else:
        kwargs['headers'] = {'user-agent': user_agent}
      return request_method(*args, **kwargs)

    return wrapped_request_method

  return decorator

class transportAgentRequest(google_auth_httplib2.Request):
  """A Request which forces a user agent."""

  @_force_user_agent(GAM_USER_AGENT)
  def __call__(self, *args, **kwargs): #pylint: disable=arguments-differ
    """Inserts the GAM user-agent header in requests."""
    return super(transportAgentRequest, self).__call__(*args, **kwargs)


class transportAuthorizedHttp(google_auth_httplib2.AuthorizedHttp):
  """An AuthorizedHttp which forces a user agent during requests."""

  @_force_user_agent(GAM_USER_AGENT)
  def request(self, *args, **kwargs): #pylint: disable=arguments-differ
    """Inserts the GAM user-agent header in requests."""
    return super(transportAuthorizedHttp, self).request(*args, **kwargs)

def transportCreateRequest(httpObj=None):
  """Creates a uniform Request object with a default http, if not provided.

  Args:
    httpObj: Optional httplib2.Http compatible object to be used with the request.
      If not provided, a default HTTP will be used.

  Returns:
    Request: A google_auth_httplib2.Request compatible Request.
  """
  if not httpObj:
    httpObj = getHttpObj()
  return transportAgentRequest(httpObj)

def handleOAuthTokenError(e, soft_errors):
  errMsg = str(e)
  if errMsg in API.REFRESH_PERM_ERRORS:
    if soft_errors:
      return None
    if not GM.Globals[GM.CURRENT_SVCACCT_USER]:
      expiredRevokedOauth2TxtExit()
  if errMsg.replace('.', '') in API.OAUTH2_TOKEN_ERRORS or errMsg.startswith('Invalid response'):
    if soft_errors:
      return None
    if not GM.Globals[GM.CURRENT_SVCACCT_USER]:
      ClientAPIAccessDeniedExit()
    systemErrorExit(SERVICE_NOT_APPLICABLE_RC, Msg.SERVICE_NOT_APPLICABLE_THIS_ADDRESS.format(GM.Globals[GM.CURRENT_SVCACCT_USER]))
  stderrErrorMsg(f'Authentication Token Error - {errMsg}')
  APIAccessDeniedExit()

def getOauth2TxtCredentials(exitOnError=True):
  jsonData = readFile(GC.Values[GC.OAUTH2_TXT], continueOnError=True, displayError=False)
  if jsonData:
    try:
      jsonDict = json.loads(jsonData)
      if 'client_id' in jsonDict:
        scopesList = jsonDict.get('scopes', API.REQUIRED_SCOPES)
        if set(scopesList) == API.REQUIRED_SCOPES_SET:
          if exitOnError:
            systemErrorExit(OAUTH2_TXT_REQUIRED_RC, Msg.NO_CLIENT_ACCESS_ALLOWED)
          return (None, None)
        token_expiry = jsonDict.get('token_expiry', REFRESH_EXPIRY)
        creds = google.oauth2.credentials.Credentials.from_authorized_user_file(GC.Values[GC.OAUTH2_TXT], scopesList)
        if 'id_token_jwt' not in jsonDict:
          creds.token = jsonDict['token']
          creds._id_token = jsonDict['id_token']
          GM.Globals[GM.DECODED_ID_TOKEN] = jsonDict['decoded_id_token']
        else:
          creds.token = jsonDict['access_token']
          creds._id_token = jsonDict['id_token_jwt']
          GM.Globals[GM.DECODED_ID_TOKEN] = jsonDict['id_token']
        creds.expiry = datetime.datetime.strptime(token_expiry, '%Y-%m-%dT%H:%M:%SZ')
        return (True, creds)
      if (jsonDict.get('file_version') == 2) and ('credentials' in jsonDict) and (API.GAM_SCOPES in jsonDict['credentials']):
        if not jsonDict['credentials'][API.GAM_SCOPES]:
          if exitOnError:
            systemErrorExit(OAUTH2_TXT_REQUIRED_RC, Msg.NO_CLIENT_ACCESS_ALLOWED)
          return (None, None)
        if not isinstance(jsonDict['credentials'][API.GAM_SCOPES], dict):
          importCredentials = json.loads(base64.b64decode(jsonDict['credentials'][API.GAM_SCOPES]).decode('utf-8'))
        else:
          importCredentials = jsonDict['credentials'][API.GAM_SCOPES]
        if importCredentials:
          scopesList = importCredentials.get('scopes', API.REQUIRED_SCOPES)
          if set(scopesList) == API.REQUIRED_SCOPES_SET:
            if exitOnError:
              systemErrorExit(OAUTH2_TXT_REQUIRED_RC, Msg.NO_CLIENT_ACCESS_ALLOWED)
            return (None, None)
          info = {
            'client_id': importCredentials['client_id'],
            'client_secret': importCredentials['client_secret'],
            'refresh_token': importCredentials['refresh_token']
            }
          creds = google.oauth2.credentials.Credentials.from_authorized_user_info(info, scopesList)
          creds.token = importCredentials['access_token']
          creds._id_token = importCredentials['id_token_jwt']
          GM.Globals[GM.DECODED_ID_TOKEN] = importCredentials['id_token']
          creds.expiry = datetime.datetime.strptime(REFRESH_EXPIRY, '%Y-%m-%dT%H:%M:%SZ')
          return (False, creds)
      if jsonDict and exitOnError:
        invalidOauth2TxtExit()
    except (IndexError, KeyError, SyntaxError, TypeError, ValueError):
      if exitOnError:
        invalidOauth2TxtExit()
  if exitOnError:
    systemErrorExit(OAUTH2_TXT_REQUIRED_RC, Msg.NO_CLIENT_ACCESS_ALLOWED)
  return (None, None)

def _getValueFromOAuth(field, credentials=None):
  if not GM.Globals[GM.DECODED_ID_TOKEN]:
    request = transportCreateRequest()
    if credentials is None:
      credentials = getClientCredentials()
    elif credentials.expired:
      credentials.refresh(request)
    GM.Globals[GM.DECODED_ID_TOKEN] = google.oauth2.id_token.verify_oauth2_token(credentials.id_token, request)
  return GM.Globals[GM.DECODED_ID_TOKEN].get(field, 'Unknown')

def writeClientCredentials(creds, filename):
  creds_data = {
    'client_id': creds.client_id,
    'client_secret': creds.client_secret,
    'id_token': creds.id_token,
    'refresh_token': creds.refresh_token,
    'scopes': sorted(creds.scopes),
    'token': creds.token,
    'token_expiry': creds.expiry.strftime('%Y-%m-%dT%H:%M:%SZ'),
    'token_uri': creds.token_uri,
    }
  expected_iss = ['https://accounts.google.com', 'accounts.google.com']
  if _getValueFromOAuth('iss', creds) not in expected_iss:
    systemErrorExit(OAUTH2_TXT_REQUIRED_RC, f'Wrong OAuth 2.0 credentials issuer. Got {_getValueFromOAuth("iss", creds)} expected one of {", ".join(expected_iss)}')
  request = transportCreateRequest()
  creds_data['decoded_id_token'] = google.oauth2.id_token.verify_oauth2_token(creds.id_token, request)
  GM.Globals[GM.DECODED_ID_TOKEN] = creds_data['decoded_id_token']
  if filename != '-':
    writeFile(filename, json.dumps(creds_data, indent=2, sort_keys=True)+'\n')
  else:
    writeStdout(json.dumps(creds_data, ensure_ascii=False, sort_keys=True, indent=2)+'\n')

def getClientCredentials(forceRefresh=False, forceWrite=False, filename=None):
  """Gets OAuth2 credentials which are guaranteed to be fresh and valid.
     Locks during read and possible write so that only one process will
     attempt refresh/write when running in parallel. """
  lock = FileLock(GM.Globals[GM.OAUTH2_TXT_LOCK])
  with lock:
    writeCreds, credentials = getOauth2TxtCredentials()
    if not credentials:
      invalidOauth2TxtExit()
    if credentials.expired or forceRefresh:
      retries = 3
      for n in range(1, retries+1):
        try:
          credentials.refresh(transportCreateRequest())
          if writeCreds or forceWrite:
            writeClientCredentials(credentials, filename or GC.Values[GC.OAUTH2_TXT])
          break
        except (httplib2.HttpLib2Error, google.auth.exceptions.TransportError, RuntimeError) as e:
          if n != retries:
            waitOnFailure(n, retries, NETWORK_ERROR_RC, str(e))
            continue
          handleServerError(e)
        except google.auth.exceptions.RefreshError as e:
          if isinstance(e.args, tuple):
            e = e.args[0]
          handleOAuthTokenError(e, False)
  return credentials

def waitOnFailure(n, retries, error_code, error_message):
  delta = min(2 ** n, 60)+float(random.randint(1, 1000))/1000
  if n > 3:
    writeStderr(f'Temporary error: {error_code} - {error_message}, Backing off: {int(delta)} seconds, Retry: {n}/{retries}\n')
    flushStderr()
  time.sleep(delta)
  if GC.Values[GC.SHOW_API_CALLS_RETRY_DATA]:
    incrAPICallsRetryData(error_message, delta)

def clearServiceCache(service):
  if hasattr(service._http, 'http') and hasattr(service._http.http, 'cache'):
    if service._http.http.cache is None:
      return False
    service._http.http.cache = None
    return True
  if hasattr(service._http, 'cache'):
    if service._http.cache is None:
      return False
    service._http.cache = None
    return True
  return False

DISCOVERY_URIS = [googleapiclient.discovery.V1_DISCOVERY_URI, googleapiclient.discovery.V2_DISCOVERY_URI]

def getAPIService(api, httpObj):
  api, version, v2discovery = API.getVersion(api)
  return googleapiclient.discovery.build(api, version, http=httpObj, cache_discovery=False,
                                         discoveryServiceUrl=DISCOVERY_URIS[v2discovery])

def getService(api, httpObj):
  hasLocalJSON = API.hasLocalJSON(api)
  api, version, v2discovery = API.getVersion(api)
  if api in GM.Globals[GM.CURRENT_API_SERVICES] and version in GM.Globals[GM.CURRENT_API_SERVICES][api]:
    service = googleapiclient.discovery.build_from_document(GM.Globals[GM.CURRENT_API_SERVICES][api][version], http=httpObj)
    if GM.Globals[GM.CACHE_DISCOVERY_ONLY]:
      clearServiceCache(service)
    return service
  if not hasLocalJSON:
    retries = 3
    for n in range(1, retries+1):
      try:
        service = googleapiclient.discovery.build(api, version, http=httpObj, cache_discovery=False,
                                                  discoveryServiceUrl=DISCOVERY_URIS[v2discovery])
        GM.Globals[GM.CURRENT_API_SERVICES].setdefault(api, {})
        GM.Globals[GM.CURRENT_API_SERVICES][api][version] = service._rootDesc.copy()
        if GM.Globals[GM.CACHE_DISCOVERY_ONLY]:
          clearServiceCache(service)
        return service
      except googleapiclient.errors.UnknownApiNameOrVersion as e:
        systemErrorExit(GOOGLE_API_ERROR_RC, Msg.UNKNOWN_API_OR_VERSION.format(str(e), __author__))
      except (googleapiclient.errors.InvalidJsonError, KeyError, ValueError):
        if n != retries:
          waitOnFailure(n, retries, INVALID_JSON_RC, Msg.INVALID_JSON_INFORMATION)
          continue
        systemErrorExit(INVALID_JSON_RC, Msg.INVALID_JSON_INFORMATION)
      except (http_client.ResponseNotReady, OSError, googleapiclient.errors.HttpError) as e:
        errMsg = f'Connection error: {str(e) or repr(e)}'
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
  disc_file, discovery = readDiscoveryFile(f'{api}-{version}')
  try:
    service = googleapiclient.discovery.build_from_document(discovery, http=httpObj)
    GM.Globals[GM.CURRENT_API_SERVICES].setdefault(api, {})
    GM.Globals[GM.CURRENT_API_SERVICES][api][version] = service._rootDesc.copy()
    if GM.Globals[GM.CACHE_DISCOVERY_ONLY]:
      clearServiceCache(service)
    return service
  except (KeyError, ValueError):
    invalidDiscoveryJsonExit(disc_file)
  except IOError as e:
    systemErrorExit(FILE_ERROR_RC, str(e))

def defaultSvcAcctScopes():
  scopesList = API.getSvcAcctScopesList(GC.Values[GC.USER_SERVICE_ACCOUNT_ACCESS_ONLY], False)
  saScopes = {}
  for scope in scopesList:
    saScopes.setdefault(scope['api'], [])
    saScopes[scope['api']].append(scope['scope'])
  saScopes[API.DRIVEACTIVITY_V1].append(API.DRIVE_SCOPE)
  saScopes[API.DRIVEACTIVITY_V2].append(API.DRIVE_SCOPE)
  saScopes[API.DRIVE2] = saScopes[API.DRIVE3]
  saScopes[API.DRIVETD] = saScopes[API.DRIVE3]
  saScopes[API.SHEETSTD] = saScopes[API.SHEETS]
  return saScopes

def _getSvcAcctData():
  if not GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]:
    json_string = readFile(GC.Values[GC.OAUTH2SERVICE_JSON], continueOnError=True, displayError=True)
    if not json_string:
      invalidOauth2serviceJsonExit()
    try:
      GM.Globals[GM.OAUTH2SERVICE_JSON_DATA] = json.loads(json_string)
    except (IndexError, KeyError, SyntaxError, TypeError, ValueError):
      invalidOauth2serviceJsonExit()
    if not GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]:
      systemErrorExit(OAUTH2SERVICE_JSON_REQUIRED_RC, Msg.NO_SVCACCT_ACCESS_ALLOWED)
    if API.OAUTH2SA_SCOPES not in GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]:
      GM.Globals[GM.SVCACCT_SCOPES_DEFINED] = False
      GM.Globals[GM.SVCACCT_SCOPES] = defaultSvcAcctScopes()
    else:
      GM.Globals[GM.SVCACCT_SCOPES_DEFINED] = True
      GM.Globals[GM.SVCACCT_SCOPES] = GM.Globals[GM.OAUTH2SERVICE_JSON_DATA].pop(API.OAUTH2SA_SCOPES)

def getSvcAcctCredentials(scopesOrAPI, userEmail):
  _getSvcAcctData()
  if isinstance(scopesOrAPI, str):
    GM.Globals[GM.CURRENT_SVCACCT_API] = scopesOrAPI
    GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES] = GM.Globals[GM.SVCACCT_SCOPES].get(scopesOrAPI, [])
    if not GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES]:
      systemErrorExit(OAUTH2SERVICE_JSON_REQUIRED_RC, Msg.NO_SVCACCT_ACCESS_ALLOWED)
  else:
    GM.Globals[GM.CURRENT_SVCACCT_API] = ''
    GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES] = scopesOrAPI
  try:
    credentials = google.oauth2.service_account.Credentials.from_service_account_info(GM.Globals[GM.OAUTH2SERVICE_JSON_DATA])
  except (ValueError, IndexError, KeyError):
    invalidOauth2serviceJsonExit()
  credentials = credentials.with_scopes(GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES])
  GM.Globals[GM.CURRENT_SVCACCT_USER] = userEmail
  if userEmail:
    credentials = credentials.with_subject(userEmail)
  GM.Globals[GM.ADMIN] = GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]['client_email']
  GM.Globals[GM.OAUTH2SERVICE_CLIENT_ID] = GM.Globals[GM.OAUTH2SERVICE_JSON_DATA]['client_id']
  return credentials

def checkGAPIError(e, soft_errors=False, retryOnHttpError=False):
  def makeErrorDict(code, reason, message):
    return {'error': {'code': code, 'errors': [{'reason': reason, 'message': message}]}}

  try:
    error = json.loads(e.content.decode(UTF8))
    if GC.Values[GC.DEBUG_LEVEL] > 0:
      writeStdout(f'{ERROR_PREFIX} JSON: {str(error)}+\n')
  except (IndexError, KeyError, SyntaxError, TypeError, ValueError):
    eContent = e.content.decode(UTF8) if isinstance(e.content, bytes) else e.content
    if GC.Values[GC.DEBUG_LEVEL] > 0:
      writeStdout(f'{ERROR_PREFIX} HTTP: {str(eContent)}+\n')
    if (e.resp['status'] == '503') and (eContent.startswith('Quota exceeded for the current request')):
      return (e.resp['status'], GAPI.QUOTA_EXCEEDED, eContent)
    if (e.resp['status'] == '403') and (eContent.startswith('Request rate higher than configured')):
      return (e.resp['status'], GAPI.QUOTA_EXCEEDED, eContent)
    if (e.resp['status'] == '502') and ('Bad Gateway' in eContent):
      return (e.resp['status'], GAPI.BAD_GATEWAY, eContent)
    if (e.resp['status'] == '504') and ('Gateway Timeout' in eContent):
      return (e.resp['status'], GAPI.GATEWAY_TIMEOUT, eContent)
    if (e.resp['status'] == '403') and ('Invalid domain.' in eContent):
      error = makeErrorDict(403, GAPI.NOT_FOUND, 'Domain not found')
    elif (e.resp['status'] == '403') and ('Domain cannot use apis.' in eContent):
      error = makeErrorDict(403, GAPI.DOMAIN_CANNOT_USE_APIS, 'Domain cannot use apis')
    elif (e.resp['status'] == '400') and ('InvalidSsoSigningKey' in eContent):
      error = makeErrorDict(400, GAPI.INVALID, 'InvalidSsoSigningKey')
    elif (e.resp['status'] == '400') and ('UnknownError' in eContent):
      error = makeErrorDict(400, GAPI.INVALID, 'UnknownError')
    elif (e.resp['status'] == '400') and ('FeatureUnavailableForUser' in eContent):
      error = makeErrorDict(400, GAPI.SERVICE_NOT_AVAILABLE, 'Feature Unavailable For User')
    elif (e.resp['status'] == '400') and ('EntityDoesNotExist' in eContent):
      error = makeErrorDict(400, GAPI.NOT_FOUND, 'Entity Does Not Exist')
    elif (e.resp['status'] == '400') and ('EntityNameNotValid' in eContent):
      error = makeErrorDict(400, GAPI.INVALID_INPUT, 'Entity Name Not Valid')
    elif (e.resp['status'] == '400') and ('Failed to parse Content-Range header' in eContent):
      error = makeErrorDict(400, GAPI.BAD_REQUEST, 'Failed to parse Content-Range header')
    elif (e.resp['status'] == '400') and ('Request contains an invalid argument' in eContent):
      error = makeErrorDict(400, GAPI.INVALID_ARGUMENT, 'Request contains an invalid argument')
    elif retryOnHttpError:
      return (-1, None, None)
    elif soft_errors:
      stderrErrorMsg(eContent)
      return (0, None, None)
    else:
      systemErrorExit(HTTP_ERROR_RC, eContent)
  if 'error' in error:
    http_status = error['error']['code']
    if 'errors' in error['error']:
      message = error['error']['errors'][0]['message']
      status = ''
    else:
      message = error['error']['message']
      status = error['error'].get('status', '')
    if http_status == 500:
      if not message:
        message = Msg.UNKNOWN
        error = makeErrorDict(http_status, GAPI.UNKNOWN_ERROR, message)
      elif 'Backend Error' in message:
        error = makeErrorDict(http_status, GAPI.BACKEND_ERROR, message)
      elif 'Internal error encountered' in message:
        error = makeErrorDict(http_status, GAPI.INTERNAL_ERROR, message)
      elif 'Role assignment exists: RoleAssignment' in message:
        error = makeErrorDict(http_status, GAPI.DUPLICATE, message)
      elif 'Role assignment exists: roleId' in message:
        error = makeErrorDict(http_status, GAPI.DUPLICATE, message)
      elif 'Operation not supported' in message:
        error = makeErrorDict(http_status, GAPI.OPERATION_NOT_SUPPORTED, message)
      elif 'Failed status in update settings response' in message:
        error = makeErrorDict(http_status, GAPI.INVALID_INPUT, message)
    elif http_status == 503:
      if status == 'UNAVAILABLE' or 'The service is currently unavailable' in message:
        error = makeErrorDict(http_status, GAPI.SERVICE_NOT_AVAILABLE, message)
    elif http_status == 400:
      if 'does not match' in message or 'Invalid' in message:
        error = makeErrorDict(http_status, GAPI.INVALID, message)
      elif '@AttachmentNotVisible' in message:
        error = makeErrorDict(http_status, GAPI.BAD_REQUEST, message)
      elif status == 'FAILED_PRECONDITION' or 'Precondition check failed' in message:
        error = makeErrorDict(http_status, GAPI.FAILED_PRECONDITION, message)
      elif status == 'INVALID_ARGUMENT':
        error = makeErrorDict(http_status, GAPI.INVALID_ARGUMENT, message)
    elif http_status == 403:
      if status == 'PERMISSION_DENIED' or 'The caller does not have permission' in message or 'Permission iam.serviceAccountKeys' in message:
        error = makeErrorDict(http_status, GAPI.PERMISSION_DENIED, message)
    elif http_status == 404:
      if status == 'NOT_FOUND' or 'Requested entity was not found' in message or 'does not exist' in message:
        error = makeErrorDict(http_status, GAPI.NOT_FOUND, message)
    elif http_status == 409:
      if status == 'ALREADY_EXISTS' or 'Requested entity already exists' in message:
        error = makeErrorDict(http_status, GAPI.ALREADY_EXISTS, message)
    elif http_status == 429:
      if status == 'RESOURCE_EXHAUSTED' or 'Quota exceeded' in message:
        error = makeErrorDict(http_status, GAPI.QUOTA_EXCEEDED, message)
  else:
    if 'error_description' in error:
      if error['error_description'] == 'Invalid Value':
        message = error['error_description']
        http_status = 400
        error = makeErrorDict(http_status, GAPI.INVALID, message)
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
    reason = f'{http_status}'
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
      http_status, reason, message = checkGAPIError(e, soft_errors=soft_errors, retryOnHttpError=n < 3)
      if http_status == -1:
        # The error detail indicated that we should retry this request
        # We'll refresh credentials and make another pass
        service._http.credentials.refresh(getHttpObj())
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
        stderrErrorMsg(f'{http_status}: {reason} - {message}{["", ": Giving up."][n > 1]}')
        return None
      if reason == GAPI.INSUFFICIENT_PERMISSIONS:
        APIAccessDeniedExit()
      systemErrorExit(HTTP_ERROR_RC, formatHTTPError(http_status, reason, message))
    except (httplib2.HttpLib2Error, google.auth.exceptions.TransportError, RuntimeError) as e:
      if n != retries:
        service._http.connections = {}
        waitOnFailure(n, retries, NETWORK_ERROR_RC, str(e))
        continue
      handleServerError(e)
    except google.auth.exceptions.RefreshError as e:
      if isinstance(e.args, tuple):
        e = e.args[0]
      handleOAuthTokenError(e, GAPI.SERVICE_NOT_AVAILABLE in throw_reasons)
      raise GAPI.REASON_EXCEPTION_MAP[GAPI.SERVICE_NOT_AVAILABLE](str(e))
    except (http_client.ResponseNotReady, OSError) as e:
      errMsg = f'Connection error: {str(e) or repr(e)}'
      if n != retries:
        waitOnFailure(n, retries, SOCKET_ERROR_RC, errMsg)
        continue
      if soft_errors:
        writeStderr(f'\n{ERROR_PREFIX}{errMsg} - Giving up.\n')
        return None
      systemErrorExit(SOCKET_ERROR_RC, errMsg)
    except ValueError as e:
      if clearServiceCache(service):
        continue
      systemErrorExit(GOOGLE_API_ERROR_RC, str(e))
    except TypeError as e:
      systemErrorExit(GOOGLE_API_ERROR_RC, str(e))

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
  if isinstance(result, str):
    try:
      result = json.loads(result)
    except (IndexError, KeyError, SyntaxError, TypeError, ValueError) as e:
      systemErrorExit(JSON_LOADS_ERROR_RC, f'{str(e)}: {result}')
  if not result['success']:
    message = result['message']
    if message in throw_messages:
      if message in GCP.MESSAGE_EXCEPTION_MAP:
        raise GCP.MESSAGE_EXCEPTION_MAP[message](message)
    systemErrorExit(ACTION_FAILED_RC, f'{result["errorCode"]}: {result["message"]}')
  return result

def getCloudPrintError(resp, result):
  if isinstance(result, bytes):
    result = result.decode(UTF8)
  mg = re.compile(r'<title>(.*)</title>').search(result)
  if mg:
    return mg.group(1)
  return f'Error: {resp["status"]}'

def callGCP(service, function,
            throw_messages=None,
            **kwargs):
  result = callGAPI(service, function,
                    **kwargs)
  return checkCloudPrintResult(result, throw_messages=throw_messages)

def readDiscoveryFile(api_version):
  disc_filename = f'{api_version}.json'
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
  except (IndexError, KeyError, SyntaxError, TypeError, ValueError):
    invalidDiscoveryJsonExit(disc_file)

def buildGAPIObject(api):
  credentials = getClientCredentials()
  httpObj = transportAuthorizedHttp(credentials, http=getHttpObj(cache=GM.Globals[GM.CACHE_DIR]))
  service = getService(api, httpObj)
  try:
    API_Scopes = set(list(service._rootDesc['auth']['oauth2']['scopes']))
  except KeyError:
    API_Scopes = set(API.VAULT_SCOPES) if api == API.VAULT else set()
  GM.Globals[GM.CURRENT_CLIENT_API] = api
  GM.Globals[GM.CURRENT_CLIENT_API_SCOPES] = API_Scopes.intersection(credentials.scopes)
  if api != API.OAUTH2 and not GM.Globals[GM.CURRENT_CLIENT_API_SCOPES]:
    systemErrorExit(NO_SCOPES_FOR_API_RC, Msg.NO_SCOPES_FOR_API.format(API.getAPIName(api)))
  if not GC.Values[GC.DOMAIN]:
    GC.Values[GC.DOMAIN] = GM.Globals[GM.DECODED_ID_TOKEN].get('hd', 'UNKNOWN').lower()
  if not GC.Values[GC.CUSTOMER_ID]:
    GC.Values[GC.CUSTOMER_ID] = GC.MY_CUSTOMER
  GM.Globals[GM.ADMIN] = GM.Globals[GM.DECODED_ID_TOKEN].get('email', 'UNKNOWN').lower()
  GM.Globals[GM.OAUTH2_CLIENT_ID] = credentials.client_id
  return {'service': service, 'api': api, 'scopes': GM.Globals[GM.CURRENT_CLIENT_API_SCOPES]}

def useGAPIObject(gapiObj):
  GM.Globals[GM.CURRENT_CLIENT_API] = gapiObj['api']
  GM.Globals[GM.CURRENT_CLIENT_API_SCOPES] = gapiObj['scopes']
  return gapiObj['service']

def buildGAPIServiceObject(api, userEmail, displayError=True):
  httpObj = getHttpObj(cache=GM.Globals[GM.CACHE_DIR])
  service = getService(api, httpObj)
  credentials = getSvcAcctCredentials(api, userEmail)
  request = transportCreateRequest(httpObj)
  retries = 3
  for n in range(1, retries+1):
    try:
      credentials.refresh(request)
      service._http = transportAuthorizedHttp(credentials, http=httpObj)
      return {'service': service, 'api': api, 'scopes': GM.Globals[GM.SVCACCT_SCOPES], 'user': userEmail}
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
        entityServiceNotApplicableWarning('User', userEmail)
      return None

def useGAPIServiceObject(gapiServiceObj):
  GM.Globals[GM.CURRENT_SVCACCT_API] = gapiServiceObj['api']
  GM.Globals[GM.CURRENT_SVCACCT_API_SCOPES] = gapiServiceObj['scopes']
  GM.Globals[GM.CURRENT_SVCACCT_USER] = gapiServiceObj['user']
  return gapiServiceObj['service']

DEFAULT_SKIP_OBJECTS = {'kind', 'etag', 'etags'}

# Clean a JSON object
def cleanJSON(topStructure, listLimit=None, skipObjects=None, timeObjects=None):
  def _clean(structure, key):
    if not isinstance(structure, (dict, list)):
      if key not in timeObjects:
        if isinstance(structure, str) and GC.Values[GC.CSV_OUTPUT_CONVERT_CR_NL]:
          return escapeCRsNLs(structure)
        return structure
      if isinstance(structure, str) and not structure.isdigit():
        return formatLocalTime(structure)
      return formatLocalTimestamp(structure)
    if isinstance(structure, list):
      listLen = len(structure)
      listLen = min(listLen, listLimit or listLen)
      return [_clean(v, '') for v in structure[0:listLen]]
    return {k: _clean(v, k) for k, v in sorted(iter(structure.items())) if k not in allSkipObjects}

  allSkipObjects = DEFAULT_SKIP_OBJECTS.union(skipObjects or set())
  timeObjects = timeObjects or set()
  return _clean(topStructure, '')

MACOS_CODENAMES = {
  6:  'Snow Leopard',
  7:  'Lion',
  8:  'Mountain Lion',
  9:  'Mavericks',
  10: 'Yosemite',
  11: 'El Capitan',
  12: 'Sierra',
  13: 'High Sierra',
  14: 'Mojave',
  15: 'Catalina'
  }

def getOSPlatform():
  myos = platform.system()
  if myos == 'Linux':
    pltfrm = ' '.join(distro.linux_distribution(full_distribution_name=False)).title()
  elif myos == 'Windows':
    pltfrm = ' '.join(platform.win32_ver())
  elif myos == 'Darwin':
    myos = 'MacOS'
    mac_ver = platform.mac_ver()[0]
    minor_ver = int(mac_ver.split('.')[1]) # macver 10.14.6 == minor_ver 14
    codename = MACOS_CODENAMES.get(minor_ver, '')
    pltfrm = ' '.join([codename, mac_ver])
  else:
    pltfrm = platform.platform()
  return f'{myos} {pltfrm}'

def Version():
  return (f'{GAM} {__version__} - {GAM_URL} - {GM.Globals[GM.GAM_TYPE]}\n'
          f'{__author__}\n'
          f'Python {sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]} {struct.calcsize("P")*8}-bit {sys.version_info[3]}\n'
          f'google-api-python-client {googleapiclient.__version__}\n'
          f'httplib2 {httplib2.__version__}\n'
          f'{getOSPlatform()} {platform.machine()}\n'
          f'Path: {GM.Globals[GM.GAM_PATH]}\n'
          )

# Directory API

def ASPsDelete(gapiDirObj, userKey, codeId):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.asps(), 'delete',
             throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.INVALID, GAPI.INVALID_PARAMETER, GAPI.FORBIDDEN],
             userKey=userKey, codeId=codeId)
    return {}
  except (GAPI.userNotFound, GAPI.invalid, GAPI.invalidParameter, GAPI.forbidden) as e:
    return str(e)

ASP_TIME_OBJECTS = set(['creationTine', 'lastTimeUsed'])

def ASPsList(gapiDirObj, userKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'items({0})'.format(kwargs.pop('fields', 'codeId,creationTime,lastTimeUsed,name,userKey'))
  try:
    result = callGAPIpages(cd.asps(), 'list', 'items',
                           throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.INVALID_PARAMETER],
                           userKey=userKey, fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=ASP_TIME_OBJECTS)
  except (GAPI.userNotFound, GAPI.invalidParameter) as e:
    return str(e)

def ChromeosdevicesAction(gapiDirObj, customerId, resourceId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.chromeosdevices(), 'action',
             throw_reasons=[GAPI.INVALID, GAPI.CONDITION_NOT_MET,
                            GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, resourceId=resourceId, **kwargs)
    return {}
  except (GAPI.invalid, GAPI.conditionNotMet,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

CROS_TIME_OBJECTS = set(['lastSync', 'lastEnrollmentTime', 'supportEndDate', 'reportTime'])

def ChromeosdevicesGet(gapiDirObj, customerId, deviceId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.chromeosdevices(), 'get',
                      throw_reasons=[GAPI.INVALID_PARAMETER, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerId=customerId, deviceId=deviceId, **kwargs)
    return cleanJSON(result, timeObjects=CROS_TIME_OBJECTS)
  except (GAPI.invalidParameter, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ChromeosdevicesList(gapiDirObj, customerId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,chromeosdevices({0})'.format(kwargs.pop('fields', 'deviceId'))
  try:
    result = callGAPIpages(cd.chromeosdevices(), 'list', 'chromeosdevices',
                           throw_reasons=[GAPI.INVALID_INPUT, GAPI.INVALID_ORGUNIT, GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                           customerId=customerId, fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=CROS_TIME_OBJECTS)
  except (GAPI.invalidInput, GAPI.invalidOrgunit, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ChromeosdevicesMoveDevicesToOu(gapiDirObj, customerId, orgUnitPath, deviceIds):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.chromeosdevices(), 'moveDevicesToOu',
             throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, orgUnitPath=makeOrgUnitPathAbsolute(orgUnitPath), body={'deviceIds': deviceIds})
    return {}
  except (GAPI.invalidOrgunit, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ChromeosdevicesUpdate(gapiDirObj, customerId, deviceId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.chromeosdevices(), 'update',
                      throw_reasons=[GAPI.INVALID, GAPI.INVALID_PARAMETER, GAPI.CONDITION_NOT_MET,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerId=customerId, deviceId=deviceId, **kwargs)
    return cleanJSON(result, timeObjects=CROS_TIME_OBJECTS)
  except (GAPI.invalid, GAPI.invalidParameter, GAPI.conditionNotMet,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

CUSTOMER_TIME_OBJECTS = set(['customerCreationTime'])

def CustomersGet(gapiDirObj, customerKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.customers(), 'get',
                      throw_reasons=[GAPI.INVALID_PARAMETER, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
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
    return cleanJSON(result, timeObjects=CUSTOMER_TIME_OBJECTS)
  except (GAPI.invalidParameter, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden, GAPI.domainNotFound, GAPI.notFound) as e:
    return str(e)

def CustomersPatch(gapiDirObj, customerKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.customers(), 'patch',
                      throw_reasons=[GAPI.DOMAIN_NOT_VERIFIED_SECONDARY, GAPI.INVALID, GAPI.INVALID_INPUT,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerKey=customerKey, **kwargs)
    return cleanJSON(result, timeObjects=CUSTOMER_TIME_OBJECTS)
  except (GAPI.domainNotVerifiedSecondary, GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def DomainsDelete(gapiDirObj, customer, domainName):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.domains(), 'delete',
             throw_reasons=[GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
             customer=customer, domainName=domainName)
    return {}
  except (GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

DOMAIN_TIME_OBJECTS = set(['creationTime'])

def DomainsGet(gapiDirObj, customer, domainName, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.domains(), 'get',
                      throw_reasons=[GAPI.DOMAIN_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, domainName=domainName, **kwargs)
    return cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS)
  except (GAPI.domainNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def DomainsInsert(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.domains(), 'insert',
                      throw_reasons=[GAPI.DUPLICATE, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, **kwargs)
    return cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS)
  except (GAPI.duplicate, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def DomainsList(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'domains({0})'.format(kwargs.pop('fields', 'domainName'))
  try:
    result = callGAPIpages(cd.domains(), 'list', 'domains',
                           throw_reasons=[GAPI.INVALID_MEMBER, GAPI.RESOURCE_NOT_FOUND,
                                          GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS,
                                          GAPI.FORBIDDEN, GAPI.BAD_REQUEST,
                                          GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER],
                           customer=customer, fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS)
  except (GAPI.invalidMember, GAPI.resourceNotFound,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.badRequest, GAPI.invalidInput, GAPI.invalidParameter) as e:
    return str(e)

def DomainAliasesDelete(gapiDirObj, customer, domainAliasName):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.domainAliases(), 'delete',
             throw_reasons=[GAPI.DOMAIN_ALIAS_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
             customer=customer, domainAliasName=domainAliasName)
    return {}
  except (GAPI.domainAliasNotFound, GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def DomainAliasesGet(gapiDirObj, customer, domainAliasName, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.domainAliases(), 'get',
                      throw_reasons=[GAPI.DOMAIN_ALIAS_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, domainAliasName=domainAliasName, **kwargs)
    return cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS)
  except (GAPI.domainAliasNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def DomainAliasesInsert(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.domainAliases(), 'insert',
                      throw_reasons=[GAPI.DOMAIN_NOT_FOUND, GAPI.DUPLICATE, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, **kwargs)
    return cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS)
  except (GAPI.domainNotFound, GAPI.duplicate, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def DomainAliasesList(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'domainAliases({0})'.format(kwargs.pop('fields', 'domainAliasName'))
  try:
    result = callGAPIpages(cd.domainAliases(), 'list', 'domainAliaese',
                           throw_reasons=[GAPI.INVALID_PARAMETER, GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                           customer=customer, fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=DOMAIN_TIME_OBJECTS)
  except (GAPI.invalidParameter, GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def GroupsDelete(gapiDirObj, groupKey):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.groups(), 'delete',
             throw_reasons=[GAPI.GROUP_NOT_FOUND, GAPI.DOMAIN_NOT_FOUND,
                            GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN, GAPI.INVALID],
             groupKey=groupKey)
    return {}
  except (GAPI.groupNotFound, GAPI.domainNotFound,
          GAPI.domainCannotUseApis, GAPI.forbidden, GAPI.invalid) as e:
    return str(e)

def GroupsGet(gapiDirObj, groupKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.groups(), 'get',
                      throw_reasons=GAPI.GROUP_GET_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                      retry_reasons=GAPI.GROUP_GET_RETRY_REASONS,
                      groupKey=groupKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.badRequest, GAPI.invalid, GAPI.systemError) as e:
    return str(e)

def GroupsInsert(gapiDirObj, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.groups(), 'insert',
                      throw_reasons=GAPI.GROUP_CREATE_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                      **kwargs)
    return cleanJSON(result)
  except (GAPI.duplicate, GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return str(e)

def GroupsList(gapiDirObj, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,groups({0})'.format(kwargs.pop('fields', 'email'))
  try:
    result = callGAPIpages(cd.groups(), 'list', 'groups',
                           throw_reasons=[GAPI.INVALID_MEMBER, GAPI.RESOURCE_NOT_FOUND,
                                          GAPI.BAD_REQUEST, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                          GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN],
                           fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidMember, GAPI.resourceNotFound,
          GAPI.badRequest, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return str(e)

def GroupsUpdate(gapiDirObj, groupKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.groups(), 'update',
                      throw_reasons=GAPI.GROUP_UPDATE_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                      retry_reasons=GAPI.GROUP_GET_RETRY_REASONS,
                      groupKey=groupKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.groupNotFound, GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return str(e)

def GroupsAliasesDelete(gapiDirObj, groupKey, alias):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.groups().aliases(), 'delete',
             throw_reasons=[GAPI.GROUP_NOT_FOUND, GAPI.INVALID_RESOURCE, GAPI.INVALID,
                            GAPI.BAD_REQUEST, GAPI.CONDITION_NOT_MET, GAPI.FORBIDDEN],
             groupKey=groupKey, alias=alias)
    return {}
  except (GAPI.groupNotFound, GAPI.invalid, GAPI.invalidResource,
          GAPI.badRequest, GAPI.conditionNotMet, GAPI.forbidden) as e:
    return str(e)

def GroupsAliasesInsert(gapiDirObj, groupKey, alias, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.groups().aliases(), 'insert',
                      throw_reasons=[GAPI.GROUP_NOT_FOUND, GAPI.DUPLICATE,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.CONDITION_NOT_MET, GAPI.FORBIDDEN],
                      groupKey=groupKey, body={'alias': alias}, **kwargs)
    return cleanJSON(result)
  except (GAPI.groupNotFound, GAPI.duplicate,
          GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.conditionNotMet, GAPI.forbidden) as e:
    return str(e)

def GroupsAliasesList(gapiDirObj, groupKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'aliases({0})'.format(kwargs.pop('fields', 'alias'))
  try:
    result = callGAPIpages(cd.groups().aliases(), 'list', 'aliases',
                           throw_reasons=[GAPI.GROUP_NOT_FOUND,
                                          GAPI.INVALID, GAPI.INVALID_RESOURCE,
                                          GAPI.BAD_REQUEST, GAPI.CONDITION_NOT_MET, GAPI.FORBIDDEN],
                           groupKey=groupKey, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.groupNotFound, GAPI.badRequest,
          GAPI.invalid, GAPI.invalidResource, GAPI.forbidden,
          GAPI.conditionNotMet) as e:
    return str(e)

def MembersDelete(gapiDirObj, groupKey, memberKey):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.members(), 'delete',
             throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.MEMBER_NOT_FOUND, GAPI.INVALID_MEMBER,
                                                       GAPI.CONDITION_NOT_MET, GAPI.CONFLICT],
             retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
             groupKey=groupKey, memberKey=memberKey)
    return {}
  except (GAPI.memberNotFound, GAPI.invalidMember, GAPI.conditionNotMet, GAPI.conflict,
          GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden) as e:
    return str(e)

def MembersGet(gapiDirObj, groupKey, memberKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.members(), 'get',
                      throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.MEMBER_NOT_FOUND, GAPI.INVALID_PARAMETER],
                      retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                      groupKey=groupKey, memberKey=memberKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.memberNotFound, GAPI.invalidParameter,
          GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden) as e:
    return str(e)

def MembersInsert(gapiDirObj, groupKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.members(), 'insert',
                      throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.DUPLICATE, GAPI.MEMBER_NOT_FOUND,
                                                                GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                                                GAPI.INVALID_MEMBER, GAPI.CYCLIC_MEMBERSHIPS_NOT_ALLOWED,
                                                                GAPI.CONDITION_NOT_MET, GAPI.CONFLICT],
                      retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                      groupKey=groupKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.duplicate, GAPI.memberNotFound, GAPI.resourceNotFound, GAPI.invalidParameter,
          GAPI.invalidMember, GAPI.cyclicMembershipsNotAllowed, GAPI.conditionNotMet, GAPI.conflict,
          GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden) as e:
    return str(e)

def MembersList(gapiDirObj, groupKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,members({0})'.format(kwargs.pop('fields', 'email'))
  try:
    result = callGAPIpages(cd.members(), 'list', 'members',
                           throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                           retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                           groupKey=groupKey, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidParameter,
          GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden) as e:
    return str(e)

def MembersPatch(gapiDirObj, groupKey, memberKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.members(), 'patch',
                      throw_reasons=GAPI.MEMBERS_THROW_REASONS+[GAPI.MEMBER_NOT_FOUND, GAPI.INVALID_MEMBER, GAPI.INVALID_PARAMETER],
                      retry_reasons=GAPI.MEMBERS_RETRY_REASONS,
                      groupKey=groupKey, memberKey=memberKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.memberNotFound, GAPI.invalidMember, GAPI.invalidParameter,
          GAPI.groupNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.invalid, GAPI.forbidden) as e:
    return str(e)

def MobiledevicesAction(gapiDirObj, customerId, resourceId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.mobiledevices(), 'action',
             bailOnInternalError=True,
             throw_reasons=[GAPI.INTERNAL_ERROR, GAPI.RESOURCE_ID_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, resourceId=resourceId, **kwargs)
    return {}
  except (GAPI.internalError, GAPI.resourceIdNotFound, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def MobiledevicesDelete(gapiDirObj, customerId, resourceid):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.mobiledevices(), 'delete',
             bailOnInternalError=True,
             throw_reasons=[GAPI.INTERNAL_ERROR, GAPI.RESOURCE_ID_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, resourceid=resourceid)
    return {}
  except (GAPI.internalError, GAPI.resourceIdNotFound, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

MOBILE_TIME_OBJECTS = set(['firstSync', 'lastSync'])

def MobiledevicesGet(gapiDirObj, customerId, resourceid, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.mobiledevices(), 'get',
                      throw_reasons=[GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerId=customerId, resourceid=resourceid, **kwargs)
    return cleanJSON(result, timeObjects=MOBILE_TIME_OBJECTS)
  except (GAPI.invalidParameter,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def MobiledevicesList(gapiDirObj, customerId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,mobiledevices({0})'.format(kwargs.pop('fields', 'resourceid'))
  try:
    result = callGAPIpages(cd.mobiledevices(), 'list', 'mobiledevices',
                           throw_reasons=[GAPI.INVALID_INPUT, GAPI.INVALID_ORGUNIT, GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                           customerId=customerId, fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=MOBILE_TIME_OBJECTS)
  except (GAPI.invalidInput, GAPI.invalidOrgunit, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def _getTopLevelOrgId(cd, customerId, parentOrgUnitPath):
  try:
    temp_org = callGAPI(cd.orgunits(), 'insert',
                        throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.BACKEND_ERROR,
                                       GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                        customerId=customerId, body={'name': 'temp-delete-me', 'parentOrgUnitPath': parentOrgUnitPath}, fields='parentOrgUnitId,orgUnitId')
  except (GAPI.invalidOrgunit, GAPI.backendError,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)
  try:
    callGAPI(cd.orgunits(), 'delete',
             throw_reasons=[GAPI.CONDITION_NOT_MET, GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND, GAPI.BACKEND_ERROR,
                            GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
             customerId=customerId, orgUnitPath=temp_org['orgUnitId'])
  except (GAPI.conditionNotMet, GAPI.invalidOrgunit, GAPI.orgunitNotFound, GAPI.backendError):
    pass
  except (GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return (str(e), False)
  return (temp_org['parentOrgUnitId'], True)

def OrgunitsDelete(gapiDirObj, customerId, orgUnitPath):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.orgunits(), 'delete',
             throw_reasons=[GAPI.CONDITION_NOT_MET, GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND, GAPI.BACKEND_ERROR,
                            GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
             customerId=customerId, orgUnitPath=encodeOrgUnitPath(makeOrgUnitPathRelative(orgUnitPath)))
    return {}
  except (GAPI.conditionNotMet, GAPI.invalidOrgunit, GAPI.orgunitNotFound, GAPI.backendError,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def OrgunitsGet(gapiDirObj, customerId, orgUnitPath, **kwargs):
  cd = useGAPIObject(gapiDirObj)
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
                      throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND,
                                     GAPI.BACKEND_ERROR, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, orgUnitPath=encodeOrgUnitPath(orgUnitPath), **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidOrgunit, GAPI.orgunitNotFound,
          GAPI.backendError, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def OrgunitsInsert(gapiDirObj, customerId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.orgunits(), 'insert',
                      throw_reasons=[GAPI.INVALID_PARENT_ORGUNIT, GAPI.INVALID_ORGUNIT, GAPI.INVALID_ORGUNIT_NAME,
                                     GAPI.BACKEND_ERROR, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidParentOrgunit, GAPI.invalidOrgunit, GAPI.invalidOrgunitName,
          GAPI.backendError, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def OrgunitsList(gapiDirObj, customerId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'organizationUnits({0})'.format(kwargs.pop('fields', 'orgUnitPath,orgUnitId'))
  try:
    result = callGAPIpages(cd.orgunits(), 'list', 'organizationUnits',
                           throw_reasons=[GAPI.ORGUNIT_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                           customerId=customerId, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.orgunitNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def OrgunitsUpdate(gapiDirObj, customerId, orgUnitPath, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.orgunits(), 'update',
                      throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.ORGUNIT_NOT_FOUND, GAPI.INVALID_ORGUNIT_NAME,
                                     GAPI.BACKEND_ERROR, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, orgUnitPath=encodeOrgUnitPath(makeOrgUnitPathRelative(orgUnitPath)), **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidOrgunit, GAPI.orgunitNotFound, GAPI.invalidOrgunitName,
          GAPI.backendError, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def PrivilegesList(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'items({0})'.format(kwargs.pop('fields', 'serviceId,serviceName,privilegeName,isOuScopable,childPrivileges'))
  try:
    result = callGAPIpages(cd.privileges(), 'list', 'items',
                           throw_reasons=[GAPI.INVALID_PARAMETER, GAPI.BAD_REQUEST, GAPI.CUSTOMER_NOT_FOUND, GAPI.FORBIDDEN],
                           customer=customer, fields=fields)
    return cleanJSON(result)
  except (GAPI.invalidParameter, GAPI.badRequest, GAPI.customerNotFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesBuildingsDelete(gapiDirObj, customer, buildingId):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.resources().buildings(), 'delete',
             throw_reasons=[GAPI.RESOURCE_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
             customer=customer, buildingId=buildingId)
    return {}
  except (GAPI.resourceNotFound, GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesBuildingsGet(gapiDirObj, customer, buildingId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().buildings(), 'get',
                      throw_reasons=[GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, buildingId=buildingId, **kwargs)
    return cleanJSON(result)
  except (GAPI.resourceNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesBuildingsInsert(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().buildings(), 'insert',
                      throw_reasons=[GAPI.DUPLICATE, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, **kwargs)
    return cleanJSON(result)
  except (GAPI.duplicate, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesBuildingsList(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,items({0})'.format(kwargs.pop('fields', 'resourceId,resourceName,resourceEmail,resourceDescription,resourceType'))
  try:
    result = callGAPIpages(cd.resources().buildings(), 'list', 'items',
                           throw_reasons=[GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                           customer=customer, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesBuildingsPatch(gapiDirObj, customer, buildingId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().buildings(), 'patch',
                      throw_reasons=[GAPI.DUPLICATE, GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, buildingId=buildingId, **kwargs)
    return cleanJSON(result)
  except (GAPI.duplicate, GAPI.resourceNotFound, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesCalendarsDelete(gapiDirObj, customer, calendarResourceId):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.resources().calendars(), 'delete',
             throw_reasons=[GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customer=customer, calendarResourceId=calendarResourceId)
    return {}
  except (GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesCalendarsGet(gapiDirObj, customer, calendarResourceId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().calendars(), 'get',
                      throw_reasons=[GAPI.INVALID_PARAMETER, GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, calendarResourceId=calendarResourceId, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidParameter, GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesCalendarsInsert(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().calendars(), 'insert',
                      throw_reasons=[GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.REQUIRED, GAPI.INVALID_PARAMETER, GAPI.DUPLICATE,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalid, GAPI.invalidInput, GAPI.required, GAPI.invalidParameter, GAPI.duplicate,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesCalendarsList(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,items({0})'.format(kwargs.pop('fields', 'resourceId,resourceName,resourceEmail,resourceDescription,resourceType'))
  try:
    result = callGAPIpages(cd.resources().calendars(), 'list', 'items',
                           throw_reasons=[GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                           customer=customer, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesCalendarsPatch(gapiDirObj, customer, calendarResourceId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().calendars(), 'patch',
                      throw_reasons=[GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.REQUIRED, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, calendarResourceId=calendarResourceId, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalid, GAPI.invalidInput, GAPI.required, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesFeaturesDelete(gapiDirObj, customer, featureKey):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.resources().features(), 'delete',
             throw_reasons=[GAPI.RESOURCE_NOT_FOUND, GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
             customer=customer, featureKey=featureKey)
    return {}
  except (GAPI.resourceNotFound, GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesFeaturesGet(gapiDirObj, customer, featureKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().features(), 'get',
                      throw_reasons=[GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, featureKey=featureKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.resourceNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesFeaturesInsert(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.resources().features(), 'insert',
                      throw_reasons=[GAPI.DUPLICATE, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, **kwargs)
    return cleanJSON(result)
  except (GAPI.duplicate, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesFeaturesList(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,items({0})'.format(kwargs.pop('fields', 'name'))
  try:
    result = callGAPIpages(cd.resources().features(), 'list', 'items',
                           throw_reasons=[GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                           customer=customer, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidParameter,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def ResourcesFeaturesRename(gapiDirObj, customer, oldName, newName):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.resources().features(), 'rename',
             throw_reasons=[GAPI.DUPLICATE, GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_INPUT,
                            GAPI.BAD_REQUEST, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
             customer=customer, oldName=oldName, body={'newName': newName})
    return {}
  except (GAPI.duplicate, GAPI.resourceNotFound, GAPI.invalidInput,
          GAPI.badRequest, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def RoleAssignmentsDelete(gapiDirObj, customer, roleAssignmentId):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.roleAssignments(), 'delete',
             throw_reasons=[GAPI.NOT_FOUND, GAPI.OPERATION_NOT_SUPPORTED,
                            GAPI.BAD_REQUEST, GAPI.CUSTOMER_NOT_FOUND, GAPI.FORBIDDEN],
             customer=customer, roleAssignmentId=roleAssignmentId)
    return {}
  except (GAPI.notFound, GAPI.operationNotSupported,
          GAPI.badRequest, GAPI.customerNotFound, GAPI.forbidden) as e:
    return str(e)

def RoleAssignmentsGet(gapiDirObj, customer, roleAssignmentId):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.roleAssignments(), 'get',
                      throw_reasons=[GAPI.NOT_FOUND, GAPI.OPERATION_NOT_SUPPORTED,
                                     GAPI.BAD_REQUEST, GAPI.CUSTOMER_NOT_FOUND, GAPI.FORBIDDEN],
                      customer=customer, roleAssignmentId=roleAssignmentId)
    return cleanJSON(result)
  except (GAPI.notFound, GAPI.operationNotSupported,
          GAPI.badRequest, GAPI.customerNotFound, GAPI.forbidden) as e:
    return str(e)

def RoleAssignmentsInsert(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.roleAssignments(), 'insert',
                      throw_reasons=[GAPI.INVALID_ORGUNIT, GAPI.DUPLICATE, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.CUSTOMER_NOT_FOUND, GAPI.FORBIDDEN, GAPI.INTERNAL_ERROR],
                      customer=customer, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidOrgunit, GAPI.duplicate, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.customerNotFound, GAPI.forbidden, GAPI.internalError) as e:
    return str(e)

def RoleAssignmentsList(gapiDirObj, customer, userKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,items({0})'.format(kwargs.pop('fields', 'roleAssignmentId,roleId,assignedTo,scopeType,orgUnitId'))
  try:
    result = callGAPIpages(cd.roleAssignments(), 'list', 'items',
                           throw_reasons=[GAPI.INVALID, GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.CUSTOMER_NOT_FOUND, GAPI.FORBIDDEN],
                           customer=customer, userKey=userKey, fields=fields)
    return cleanJSON(result)
  except (GAPI.invalid, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.customerNotFound, GAPI.forbidden) as e:
    return str(e)

def RolesList(gapiDirObj, customer, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,items({0})'.format(kwargs.pop('fields', 'roleId,roleName,roleDescription,isSuperAdminRole,isSystemRole'))
  try:
    result = callGAPIpages(cd.roles(), 'list', 'items',
                           throw_reasons=[GAPI.INVALID_PARAMETER, GAPI.BAD_REQUEST, GAPI.CUSTOMER_NOT_FOUND, GAPI.FORBIDDEN],
                           customer=customer, fields=fields)
    return cleanJSON(result)
  except (GAPI.invalidParameter, GAPI.badRequest, GAPI.customerNotFound, GAPI.forbidden) as e:
    return str(e)

def SchemasDelete(gapiDirObj, customerId, schemaKey):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.schemas(), 'delete',
             throw_reasons=[GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
             customerId=customerId, schemaKey=schemaKey)
    return {}
  except (GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def SchemasGet(gapiDirObj, customerId, schemaKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.schemas(), 'get',
                      throw_reasons=[GAPI.INVALID, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND, GAPI.FORBIDDEN],
                      customerId=customerId, schemaKey=schemaKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.invalid, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.resourceNotFound, GAPI.forbidden) as e:
    return str(e)

def SchemasInsert(gapiDirObj, customerId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.schemas(), 'insert',
                      throw_reasons=[GAPI.DUPLICATE, GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.CONDITION_NOT_MET, GAPI.FORBIDDEN, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, **kwargs)
    return cleanJSON(result)
  except (GAPI.duplicate, GAPI.resourceNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.conditionNotMet, GAPI.forbidden, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def SchemasList(gapiDirObj, customerId, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'schemas({0})'.format(kwargs.pop('fields', 'schemaId,schemaName,displayName,fields'))
  try:
    result = callGAPIpages(cd.schemas(), 'list', 'schemas',
                           throw_reasons=[GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                          GAPI.BAD_REQUEST, GAPI.FORBIDDEN, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                           customerId=customerId, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.resourceNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.forbidden, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def SchemasUpdate(gapiDirObj, customerId, schemaKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.schemas(), 'update',
                      throw_reasons=[GAPI.RESOURCE_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.FORBIDDEN, GAPI.INVALID_CUSTOMER_ID, GAPI.LOGIN_REQUIRED],
                      customerId=customerId, schemaKey=schemaKey, **kwargs)
    return cleanJSON(result)
  except (GAPI.resourceNotFound, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.forbidden, GAPI.invalidCustomerId, GAPI.loginRequired) as e:
    return str(e)

def TokensDelete(gapiDirObj, userKey, clientId):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.tokens(), 'get',
             throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.RESOURCE_NOT_FOUND,
                            GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
             userKey=userKey, clientId=clientId)
    callGAPI(cd.tokens(), 'delete',
             throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.RESOURCE_NOT_FOUND,
                            GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
             userKey=userKey, clientId=clientId)
    return {}
  except (GAPI.userNotFound, GAPI.resourceNotFound,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def TokensList(gapiDirObj, userKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'items({0})'.format(kwargs.pop('fields', 'clientId,displayText,anonymous,nativeApp,userKey,scopes'))
  try:
    result = callGAPIpages(cd.tokens(), 'list', 'items',
                           throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.INVALID_PARAMETER,
                                          GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.NOT_FOUND, GAPI.FORBIDDEN],
                           userKey=userKey, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.userNotFound, GAPI.invalidParameter,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.notFound, GAPI.forbidden) as e:
    return str(e)

def UsersDelete(gapiDirObj, userKey):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.users(), 'delete',
             throw_reasons=[GAPI.USER_NOT_FOUND,
                            GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN],
             userKey=userKey)
    return {}
  except (GAPI.userNotFound,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return str(e)

USER_SKIP_OBJECTS = set(['thumbnailPhotoEtag'])
USER_TIME_OBJECTS = set(['creationTime', 'deletionTime', 'lastLoginTime'])

def UsersGet(gapiDirObj, userKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.users(), 'get',
                      throw_reasons=GAPI.USER_GET_THROW_REASONS+[GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER],
                      userKey=userKey, **kwargs)
    return cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS)
  except (GAPI.userNotFound, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden, GAPI.systemError) as e:
    return str(e)

def UsersInsert(gapiDirObj, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.users(), 'insert',
                      throw_reasons=[GAPI.DUPLICATE,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.INVALID_ORGUNIT, GAPI.INVALID_SCHEMA_VALUE,
                                     GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN],
                      **kwargs)
    return cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS)
  except (GAPI.duplicate,
          GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.invalidOrgunit, GAPI.invalidSchemaValue,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return str(e)

def UsersList(gapiDirObj, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'nextPageToken,users({0})'.format(kwargs.pop('fields', 'primaryEmail'))
  try:
    result = callGAPIpages(cd.users(), 'list', 'users',
                           throw_reasons=[GAPI.BAD_REQUEST, GAPI.RESOURCE_NOT_FOUND,
                                          GAPI.INVALID_ORGUNIT, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                          GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN],
                           fields=fields, **kwargs)
    return cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS)
  except (GAPI.badRequest, GAPI.resourceNotFound,
          GAPI.invalidOrgunit, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return str(e)

def UsersUndelete(gapiDirObj, userUID, orgUnitPath):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.users(), 'undelete',
             throw_reasons=[GAPI.DELETED_USER_NOT_FOUND, GAPI.INVALID_ORGUNIT, GAPI.BAD_REQUEST, GAPI.INVALID,
                            GAPI.DOMAIN_NOT_FOUND, GAPI.DOMAIN_CANNOT_USE_APIS, GAPI.FORBIDDEN],
             userKey=userUID, body={'orgUnitPath': makeOrgUnitPathAbsolute(orgUnitPath)})
    return {}
  except (GAPI.deletedUserNotFound, GAPI.invalidOrgunit, GAPI.badRequest, GAPI.invalid,
          GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden) as e:
    return str(e)

def UsersUpdate(gapiDirObj, userKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.users(), 'update',
                      throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.DOMAIN_NOT_FOUND, GAPI.FORBIDDEN,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.INVALID_ORGUNIT, GAPI.INVALID_SCHEMA_VALUE],
                      userKey=userKey, **kwargs)
    return cleanJSON(result, skipObjects=USER_SKIP_OBJECTS, timeObjects=USER_TIME_OBJECTS)
  except (GAPI.userNotFound, GAPI.domainNotFound, GAPI.domainCannotUseApis, GAPI.forbidden,
          GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.invalidOrgunit, GAPI.invalidSchemaValue) as e:
    return str(e)

def UsersAliasesDelete(gapiDirObj, userKey, alias):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.users().aliases(), 'delete',
             throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.INVALID_RESOURCE, GAPI.INVALID,
                            GAPI.BAD_REQUEST, GAPI.CONDITION_NOT_MET, GAPI.FORBIDDEN],
             userKey=userKey, alias=alias)
    return {}
  except (GAPI.userNotFound, GAPI.invalidResource, GAPI.invalid,
          GAPI.badRequest, GAPI.conditionNotMet, GAPI.forbidden) as e:
    return str(e)

def UsersAliasesInsert(gapiDirObj, userKey, alias, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  try:
    result = callGAPI(cd.users().aliases(), 'insert',
                      throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.DUPLICATE,
                                     GAPI.INVALID, GAPI.INVALID_INPUT, GAPI.INVALID_PARAMETER,
                                     GAPI.BAD_REQUEST, GAPI.CONDITION_NOT_MET, GAPI.FORBIDDEN, GAPI.LIMIT_EXCEEDED],
                      userKey=userKey, body={'alias': alias}, **kwargs)
    return cleanJSON(result)
  except (GAPI.userNotFound, GAPI.duplicate,
          GAPI.invalid, GAPI.invalidInput, GAPI.invalidParameter,
          GAPI.badRequest, GAPI.conditionNotMet, GAPI.forbidden, GAPI.limitExceeded) as e:
    return str(e)

def UsersAliasesList(gapiDirObj, userKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'aliases({0})'.format(kwargs.pop('fields', 'email'))
  try:
    result = callGAPIpages(cd.users().aliases(), 'list', 'aliases',
                           throw_reasons=[GAPI.USER_NOT_FOUND,
                                          GAPI.INVALID, GAPI.INVALID_RESOURCE,
                                          GAPI.BAD_REQUEST, GAPI.CONDITION_NOT_MET, GAPI.FORBIDDEN],
                           userKey=userKey, fields=fields, **kwargs)
    return cleanJSON(result)
  except (GAPI.userNotFound,
          GAPI.invalid, GAPI.invalidResource,
          GAPI.badRequest, GAPI.conditionNotMet, GAPI.forbidden) as e:
    return str(e)

def VerificationCodesGenerate(gapiDirObj, userKey):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.verificationCodes(), 'generate',
             throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.INVALID, GAPI.INVALID_INPUT],
             userKey=userKey)
    return {}
  except (GAPI.userNotFound, GAPI.invalid, GAPI.invalidInput) as e:
    return str(e)

def VerificationCodesInvalidate(gapiDirObj, userKey):
  cd = useGAPIObject(gapiDirObj)
  try:
    callGAPI(cd.verificationCodes(), 'invalidate',
             throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.INVALID, GAPI.INVALID_INPUT],
             userKey=userKey)
    return {}
  except (GAPI.userNotFound, GAPI.invalid, GAPI.invalidInput) as e:
    return str(e)

def VerificationCodesList(gapiDirObj, userKey, **kwargs):
  cd = useGAPIObject(gapiDirObj)
  fields = 'items({0})'.format(kwargs.pop('fields', 'userId,verificationCode'))
  try:
    result = callGAPIpages(cd.verificationCodes(), 'list', 'items',
                           throw_reasons=[GAPI.USER_NOT_FOUND, GAPI.INVALID_PARAMETER],
                           userKey=userKey, fields=fields)
    return cleanJSON(result)
  except (GAPI.userNotFound, GAPI.invalidParameter) as e:
    return str(e)

# Drive v3 API

def DriveAbout(gapiDriveObj, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.about(), 'get',
                      throw_reasons=GAPI.DRIVE_USER_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                      **kwargs)
    return cleanJSON(result)
  except (GAPI.invalidParameter, GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

DRIVE_FILES_TIME_OBJECTS = set(['createdTime,viewedByMeTime,modifiedByMeTime,modifiedTime,sharedWithMeTime'])

def DriveFilesCreate(gapiDriveObj, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.files(), 'create',
                      throw_reasons=GAPI.DRIVE_USER_THROW_REASONS+[GAPI.FORBIDDEN, GAPI.INSUFFICIENT_PERMISSIONS,
                                                                   GAPI.INVALID, GAPI.BAD_REQUEST, GAPI.CANNOT_ADD_PARENT,
                                                                   GAPI.FILE_NOT_FOUND, GAPI.UNKNOWN_ERROR, GAPI.INVALID_PARAMETER,
                                                                   GAPI.TEAMDRIVES_SHARING_RESTRICTION_NOT_ALLOWED],
                      **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_FILES_TIME_OBJECTS)
  except (GAPI.forbidden, GAPI.insufficientFilePermissions,
          GAPI.invalid, GAPI.badRequest, GAPI.cannotAddParent,
          GAPI.fileNotFound, GAPI.unknownError, GAPI.invalidParameter,
          GAPI.teamDrivesSharingRestrictionNotAllowed,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)


def DriveFilesCopy(gapiDriveObj, fileId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.files(), 'copy',
                      throw_reasons=GAPI.DRIVE_COPY_THROW_REASONS+[GAPI.BAD_REQUEST, GAPI.INVALID_PARAMETER],
                      fileId=fileId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_FILES_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions,
          GAPI.unknownError, GAPI.cannotCopyFile, GAPI.badRequest, GAPI.fileNeverWritable,
          GAPI.badRequest, GAPI.invalidParameter,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

def DriveFilesDelete(gapiDriveObj, fileId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    callGAPI(drive.files(), 'delete',
             throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+GAPI.DRIVE3_DELETE_ACL_THROW_REASONS,
             fileId=fileId, **kwargs)
    return {}
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.cannotRemoveOwner, GAPI.cannotModifyInheritedTeamDrivePermission,
          GAPI.insufficientAdministratorPrivileges, GAPI.sharingRateLimitExceeded,
          GAPI.notFound, GAPI.permissionNotFound,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

def DriveFilesEmptyTrash(gapiDriveObj):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    callGAPI(drive.files(), 'emptyTrash',
             throw_reasons=GAPI.DRIVE_USER_THROW_REASONS)
    return {}
  except (GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

def DriveFilesGet(gapiDriveObj, fileId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.files(), 'get',
                      throw_reasons=GAPI.DRIVE_GET_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                      fileId=fileId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_PERMISSIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.invalidParameter,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

def DriveFilesList(gapiDriveObj, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  fields = 'nextPageToken,files({0})'.format(kwargs.pop('fields', 'id'))
  try:
    result = callGAPIpages(drive.files(), 'list', 'files',
                           throw_reasons=GAPI.DRIVE_USER_THROW_REASONS+[GAPI.INVALID_QUERY, GAPI.INVALID, GAPI.FILE_NOT_FOUND,
                                                                        GAPI.INVALID_PARAMETER,
                                                                        GAPI.NOT_FOUND, GAPI.TEAMDRIVE_MEMBERSHIP_REQUIRED],
                           fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_FILES_TIME_OBJECTS)
  except (GAPI.invalidQuery, GAPI.invalid, GAPI.fileNotFound,
          GAPI.invalidParameter,
          GAPI.notFound, GAPI.teamDriveMembershipRequired,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

def DriveFilesUpdate(gapiDriveObj, fileId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.files(), 'update',
                      throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+[GAPI.BAD_REQUEST, GAPI.CANNOT_ADD_PARENT,
                                                                     GAPI.CANNOT_MODIFY_VIEWERS_CAN_COPY_CONTENT,
                                                                     GAPI.INVALID_PARAMETER,
                                                                     GAPI.TEAMDRIVES_PARENT_LIMIT, GAPI.TEAMDRIVES_FOLDER_MOVE_IN_NOT_SUPPORTED,
                                                                     GAPI.TEAMDRIVES_SHARING_RESTRICTION_NOT_ALLOWED],
                      fileId=fileId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_FILES_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions,
          GAPI.unknownError, GAPI.invalid,
          GAPI.badRequest, GAPI.cannotAddParent, GAPI.cannotModifyViewersCanCopyContent, GAPI.invalidParameter,
          GAPI.teamDrivesParentLimit, GAPI.teamDrivesFolderMoveInNotSupported, GAPI.teamDrivesSharingRestrictionNotAllowed,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

DRIVE_PERMISSIONS_TIME_OBJECTS = set(['expirationTime'])

def DrivePermissionsCreate(gapiDriveObj, fileId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.permissions(), 'create',
                      bailOnInternalError=True,
                      throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+GAPI.DRIVE3_CREATE_ACL_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                      fileId=fileId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_PERMISSIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.ownershipChangeAcrossDomainNotPermitted, GAPI.teamDriveDomainUsersOnlyRestriction,
          GAPI.insufficientAdministratorPrivileges, GAPI.sharingRateLimitExceeded,
          GAPI.cannotShareTeamDriveTopFolderWithAnyoneOrDomains, GAPI.ownerOnTeamDriveItemNotSupported,
          GAPI.organizerOnNonTeamDriveItemNotSupported, GAPI.fileOrganizerOnNonTeamDriveNotSupported, GAPI.fileOrganizerNotYetEnabledForThisTeamDrive,
          GAPI.teamDrivesFolderSharingNotSupported,
          GAPI.notFound, GAPI.invalid, GAPI.invalidSharingRequest, GAPI.invalidParameter,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)


def DrivePermissionsDelete(gapiDriveObj, fileId, permissionId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    callGAPI(drive.permissions(), 'delete',
             throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+GAPI.DRIVE3_DELETE_ACL_THROW_REASONS,
             fileId=fileId, permissionId=permissionId, **kwargs)
    return {}
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.cannotRemoveOwner, GAPI.cannotModifyInheritedTeamDrivePermission,
          GAPI.insufficientAdministratorPrivileges, GAPI.sharingRateLimitExceeded,
          GAPI.notFound, GAPI.permissionNotFound,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

def DrivePermissionsGet(gapiDriveObj, fileId, permissionId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.permissions(), 'get',
                      throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+[GAPI.BAD_REQUEST, GAPI.PERMISSION_NOT_FOUND,
                                                                     GAPI.INSUFFICIENT_ADMINISTRATOR_PRIVILEGES, GAPI.INVALID_PARAMETER],
                      fileId=fileId, permissionId=permissionId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_PERMISSIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.permissionNotFound,
          GAPI.insufficientAdministratorPrivileges, GAPI.invalidParameter,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)


def DrivePermissionsList(gapiDriveObj, fileId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  fields = 'nextPageToken,permissions({0})'.format(kwargs.pop('fields', 'id'))
  try:
    result = callGAPIpages(drive.permissions(), 'list', 'permissions',
                           throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+[GAPI.NOT_FOUND,
                                                                          GAPI.INSUFFICIENT_ADMINISTRATOR_PRIVILEGES, GAPI.INVALID_PARAMETER],
                           fileId=fileId, fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_PERMISSIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.notFound, GAPI.insufficientAdministratorPrivileges, GAPI.invalidParameter,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)


def DrivePermissionsUpdate(gapiDriveObj, fileId, permissionId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.permissions(), 'update',
                      bailOnInternalError=True,
                      throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+GAPI.DRIVE3_UPDATE_ACL_THROW_REASONS+[GAPI.INVALID_PARAMETER],
                      fileId=fileId, permissionId=permissionId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_PERMISSIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.invalidOwnershipTransfer, GAPI.cannotRemoveOwner,
          GAPI.ownershipChangeAcrossDomainNotPermitted, GAPI.teamDriveDomainUsersOnlyRestriction,
          GAPI.insufficientAdministratorPrivileges, GAPI.sharingRateLimitExceeded,
          GAPI.cannotShareTeamDriveTopFolderWithAnyoneOrDomains, GAPI.ownerOnTeamDriveItemNotSupported,
          GAPI.organizerOnNonTeamDriveItemNotSupported, GAPI.fileOrganizerOnNonTeamDriveNotSupported,
          GAPI.fileOrganizerNotYetEnabledForThisTeamDrive,
          GAPI.cannotModifyInheritedTeamDrivePermission, GAPI.fieldNotWritable,
          GAPI.notFound, GAPI.permissionNotFound, GAPI.invalidParameter,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

DRIVE_REVISIONS_TIME_OBJECTS = set(['modifiedTime'])

def DriveRevisionsDelete(gapiDriveObj, fileId, revisionId):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    callGAPI(drive.revisions(), 'delete',
             throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+[GAPI.BAD_REQUEST, GAPI.REVISION_NOT_FOUND, GAPI.REVISION_DELETION_NOT_SUPPORTED,
                                                            GAPI.CANNOT_DELETE_ONLY_REVISION, GAPI.REVISIONS_NOT_SUPPORTED],
             fileId=fileId, revisionId=revisionId)
    return {}
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.revisionNotFound, GAPI.revisionDeletionNotSupported,
          GAPI.cannotDeleteOnlyRevision, GAPI.revisionsNotSupported,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

def DriveRevisionsGet(gapiDriveObj, fileId, revisionId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.revisions(), 'get',
                      throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+[GAPI.BAD_REQUEST, GAPI.REVISION_NOT_FOUND,
                                                                     GAPI.INSUFFICIENT_ADMINISTRATOR_PRIVILEGES, GAPI.INVALID_PARAMETER],
                      fileId=fileId, revisionId=revisionId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_REVISIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.revisionNotFound,
          GAPI.insufficientAdministratorPrivileges, GAPI.invalidParameter,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)


def DriveRevisionsList(gapiDriveObj, fileId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  fields = 'nextPageToken,revisions({0})'.format(kwargs.pop('fields', 'id'))
  try:
    result = callGAPIpages(drive.revisions(), 'list', 'revisions',
                           throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+[GAPI.BAD_REQUEST, GAPI.INVALID_PARAMETER, GAPI.REVISIONS_NOT_SUPPORTED],
                           fileId=fileId, fields=fields, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_REVISIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.invalidParameter, GAPI.revisionsNotSupported,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)


def DriveRevisionsUpdate(gapiDriveObj, fileId, revisionId, **kwargs):
  drive = useGAPIServiceObject(gapiDriveObj)
  try:
    result = callGAPI(drive.revisions(), 'update',
                      throw_reasons=GAPI.DRIVE_ACCESS_THROW_REASONS+[GAPI.BAD_REQUEST, GAPI.INVALID_PARAMETER,
                                                                     GAPI.REVISION_NOT_FOUND, GAPI.REVISIONS_NOT_SUPPORTED],
                      fileId=fileId, revisionId=revisionId, **kwargs)
    return cleanJSON(result, timeObjects=DRIVE_REVISIONS_TIME_OBJECTS)
  except (GAPI.fileNotFound, GAPI.forbidden, GAPI.internalError, GAPI.insufficientFilePermissions, GAPI.unknownError,
          GAPI.badRequest, GAPI.invalidParameter,
          GAPI.revisionNotFound, GAPI.revisionsNotSupported,
          GAPI.serviceNotAvailable, GAPI.authError, GAPI.domainPolicy) as e:
    return str(e)

# Gmail API

def GmailUsersGetProfile(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users(), 'getProfile',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailDraftsCreate(gapiGmailObj, uploadType, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().drafts(), 'create',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', uploadType=uploadType, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailDraftsDelete(gapiGmailObj, draftId):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().drafts(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', id=draftId)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailDraftsGet(gapiGmailObj, draftId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().drafts(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', id=draftId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailDraftsList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().drafts(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailDraftsSend(gapiGmailObj, uploadType, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().drafts(), 'send',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', uploadType=uploadType, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailDraftsUpdate(gapiGmailObj, draftId, uploadType, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().drafts(), 'update',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', id=draftId, uploadType=uploadType, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)

def GmailHistoryList(gapiGmailObj, startHistoryId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().history(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', startHistoryId=startHistoryId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailLabelsCreate(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().labels(), 'create',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.DUPLICATE, GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.duplicate, GAPI.invalidArgument) as e:
    return str(e)

def GmailLabelsDelete(gapiGmailObj, labelId):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().labels(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', id=labelId)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)

def GmailLabelsGet(gapiGmailObj, labelId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().labels(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', id=labelId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)

def GmailLabelsList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().labels(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailLabelsPatch(gapiGmailObj, labelId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().labels(), 'patch',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', id=labelId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)


def GmailLabelsUpdate(gapiGmailObj, labelId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().labels(), 'update',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', id=labelId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesBatchDelete(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'batchDelete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesBatchModify(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'batchModify',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesDelete(gapiGmailObj, messageId):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=messageId)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesGet(gapiGmailObj, messageId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=messageId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesImport(gapiGmailObj, uploadType, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'import',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', uploadType=uploadType, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesInsert(gapiGmailObj, uploadType, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'insert',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', uploadType=uploadType, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailMessagesModify(gapiGmailObj, messageId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'modify',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=messageId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesSend(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'send',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesTrash(gapiGmailObj, messageId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'trash',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=messageId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesUntrash(gapiGmailObj, messageId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages(), 'untrash',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=messageId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailMessagesAttachmentsGet(gapiGmailObj, messageId, attachmentId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().messages().attachments(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', messageId=messageId, id=attachmentId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsGetAutoForwarding(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'getAutoForwarding',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsGetImap(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'getImap',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsGetLanguage(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'getLanguage',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsGetPop(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'getPop',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsGetVacation(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'getVacation',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsUpdateAutoForwarding(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'updateAutoForwarding',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsUpdateImap(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'updateImap',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsUpdateLanguage(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'updateLanguage',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsUpdatePop(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'updatePop',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsUpdateVacation(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings(), 'updateVacation',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsDelegatesCreate(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().delegates(), 'create',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.ALREADY_EXISTS, GAPI.FAILED_PRECONDITION, GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.alreadyExists, GAPI.failedPrecondition, GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsDelegatesDelete(gapiGmailObj, delegateEmail):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().delegates(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_INPUT],
                      userId='me', deletgateEmail=delegateEmail)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidInput) as e:
    return str(e)

def GmailSettingsDelegatesGet(gapiGmailObj, delegateEmail, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().delegates(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_INPUT],
                      userId='me', deletgateEmail=delegateEmail, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidInput) as e:
    return str(e)

def GmailSettingsDelegatesList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().delegates(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsFiltersCreate(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().filters(), 'create',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.FAILED_PRECONDITION, GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.failedPrecondition, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsFiltersDelete(gapiGmailObj, filterId):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().filters(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', id=filterId)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailSettingsFiltersGet(gapiGmailObj, filterId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().filters(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', id=filterId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailSettingsFiltersList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().filters(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsForwardingAddressesCreate(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().forwardingAddresses(), 'create',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.ALREADY_EXISTS, GAPI.DUPLICATE, GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.alreadyExists, GAPI.duplicate, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsForwardingAddressesDelete(gapiGmailObj, forwardingEmail):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().forwardingAddresses(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', forwardingEmail=forwardingEmail)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailSettingsForwardingAddressesGet(gapiGmailObj, forwardingEmail, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().forwardingAddresses(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', forwardingEmail=forwardingEmail, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailSettingsForwardingAddressesList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().forwardingAddresses(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsSendAsCreate(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs(), 'create',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.ALREADY_EXISTS, GAPI.DUPLICATE, GAPI.FAILED_PRECONDITION, GAPI.INVALID_ARGUMENT],
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.alreadyExists, GAPI.duplicate, GAPI.failedPrecondition, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsSendAsDelete(gapiGmailObj, sendAsEmail):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.CANNOT_DELETE_PRIMARY_SENDAS],
                      userId='me', sendAsEmail=sendAsEmail)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.cannotDeletePrimarySendAs) as e:
    return str(e)

def GmailSettingsSendAsGet(gapiGmailObj, sendAsEmail, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', sendAsEmail=sendAsEmail, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailSettingsSendAsList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailSettingsSendAsPatch(gapiGmailObj, sendAsEmail, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs(), 'patch',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', sendAsEmail=sendAsEmail, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)


def GmailSettingsSendAsUpdate(gapiGmailObj, sendAsEmail, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs(), 'update',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND, GAPI.INVALID_ARGUMENT],
                      userId='me', sendAsEmail=sendAsEmail, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsSendAsVerify(gapiGmailObj, sendAsEmail):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs(), 'verify',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.NOT_FOUND],
                      userId='me', sendAsEmail=sendAsEmail)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.notFound) as e:
    return str(e)

def GmailSettingsSmimeInfoDelete(gapiGmailObj, sendAsEmail, smimeInfoId):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs().smimeInfo(), 'delete',
                      throw_reasons=GAPI.GMAIL_SMIME_THROW_REASONS,
                      userId='me', sendAsEmail=sendAsEmail, id=smimeInfoId)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.forbidden, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsSmimeInfoGet(gapiGmailObj, sendAsEmail, smimeInfoId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs().smimeInfo(), 'get',
                      throw_reasons=GAPI.GMAIL_SMIME_THROW_REASONS,
                      userId='me', sendAsEmail=sendAsEmail, id=smimeInfoId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.forbidden, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsSmimeInfoInsert(gapiGmailObj, sendAsEmail, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs().smimeInfo(), 'insert',
                      throw_reasons=GAPI.GMAIL_SMIME_THROW_REASONS,
                      userId='me', sendAsEmail=sendAsEmail, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.forbidden, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsSmimeInfoList(gapiGmailObj, sendAsEmail, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs().smimeInfo(), 'list',
                      throw_reasons=GAPI.GMAIL_SMIME_THROW_REASONS,
                      userId='me', sendAsEmail=sendAsEmail, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.forbidden, GAPI.invalidArgument) as e:
    return str(e)

def GmailSettingsSmimeInfoSetDefault(gapiGmailObj, sendAsEmail, smimeInfoId):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().settings().sendAs().smimeInfo(), 'setDefault',
                      throw_reasons=GAPI.GMAIL_SMIME_THROW_REASONS,
                      userId='me', sendAsEmail=sendAsEmail, id=smimeInfoId)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.forbidden, GAPI.invalidArgument) as e:
    return str(e)

def GmailThreadsDelete(gapiGmailObj, threadId):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().threads(), 'delete',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=threadId)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailThreadsGet(gapiGmailObj, threadId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().threads(), 'get',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=threadId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailThreadsList(gapiGmailObj, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().threads(), 'list',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS,
                      userId='me', **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest) as e:
    return str(e)

def GmailThreadsModify(gapiGmailObj, threadId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().threads(), 'modify',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=threadId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailThreadsTrash(gapiGmailObj, threadId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().threads(), 'trash',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=threadId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)

def GmailThreadsUntrash(gapiGmailObj, threadId, **kwargs):
  gmail = useGAPIServiceObject(gapiGmailObj)
  try:
    result = callGAPI(gmail.users().threads(), 'untrash',
                      throw_reasons=GAPI.GMAIL_THROW_REASONS+[GAPI.INVALID_ARGUMENT],
                      userId='me', id=threadId, **kwargs)
    return cleanJSON(result)
  except (GAPI.serviceNotAvailable, GAPI.badRequest,
          GAPI.invalidArgument) as e:
    return str(e)
