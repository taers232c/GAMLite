#!/usr/bin/env python3
"""Test GAMLite
"""

import os
import sys

# Move the GAMLib directory wherever you like, set that path in the following line
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__))+'/GAMLib')
from gam import gam
from gamlib import glapi as API

# Set the following values as appropriate for your domain
GAMCFG = '/Users/admin/.gam/gam.cfg'
CHROMEOS_DEVICEID = '32c61153-3808-463b-ba0a-100376212222'
DOMAIN_NAME = 'domain.com'
TESTGROUP = 'testgroup1'+'@'+DOMAIN_NAME
TESTGROUP_ALIAS = 'testgroup1'+'alias@'+DOMAIN_NAME
TESTOU = 'Fribble'
TESTOUPARENT = '/Test'
TESTSCHEMA = 'TestSchema'
TESTUSER = 'testuser1'+'@'+DOMAIN_NAME
TESTUSER_ALIAS = 'testuser1'+'alias@'+DOMAIN_NAME

customer = 'my_customer'

# Configuration
gam.SetGlobalVariables(GAMCFG)

# Version
print(gam.Version())

# Drive v3 API

gapiDriveObj = gam.buildGAPIServiceObject(API.DRIVE3, TESTUSER)

# DriveAbout
print('\nDriveAbout')
result = gam.DriveAbout(gapiDriveObj, fields='user/displayName,storageQuota')
print(result)

print('\nDriveFilesList')
result = gam.DriveFilesList(gapiDriveObj, corpora='user', q="'me' in owners", orderBy='folder,name', fields='id,name')
print(result)

# Gmail API

gapiGmailObj = gam.buildGAPIServiceObject(API.GMAIL, TESTUSER)

# GmailUsersGetProfile
print('\nGmailUsersGetProfile')
result = gam.GmailUsersGetProfile(gapiGmailObj)
print(result)

print('\nGmailLabelsList')
result = gam.GmailLabelsList(gapiGmailObj)
print(result)

print('\nGmailSettingsAutoForwardingGet')
result = gam.GmailSettingsGetAutoForwarding(gapiGmailObj)
print(result)

print('\nGmailSettingsImapGet')
result = gam.GmailSettingsGetImap(gapiGmailObj)
print(result)

print('\nGmailSettingsLanguageGet')
result = gam.GmailSettingsGetLanguage(gapiGmailObj)
print(result)

print('\nGmailSettingsPopGet')
result = gam.GmailSettingsGetPop(gapiGmailObj)
print(result)

print('\nGmailSettingsVacationGet')
result = gam.GmailSettingsGetVacation(gapiGmailObj)
print(result)

print('\nGmailSettingsDelegatesList')
result = gam.GmailSettingsDelegatesList(gapiGmailObj)
print(result)

print('\nGmailSettingsFiltersList')
result = gam.GmailSettingsFiltersList(gapiGmailObj)
print(result)

print('\nGmailSettingsForwardingAddressesList')
result = gam.GmailSettingsForwardingAddressesList(gapiGmailObj)
print(result)

print('\nGmailSettingsSendAsList')
result = gam.GmailSettingsSendAsList(gapiGmailObj)
print(result)

# Directory API
gapiDirObj = gam.buildGAPIObject(API.DIRECTORY)

# Customers
print('\nCustomersGet')
result = gam.CustomersGet(gapiDirObj, customer)
print(result)
if isinstance(result, dict):
  customer = result.get('id', customer)

# Domains
print('\nDomainsGet')
result = gam.DomainsGet(gapiDirObj, customer, DOMAIN_NAME)
print(result)
print('\nDomainsList')
result = gam.DomainsList(gapiDirObj, customer)
print(result)

# Domain Aliases
print('\nDomainAliasesList')
result = gam.DomainAliasesList(gapiDirObj, customer)
print(result)

# ASPS
print('\nASPSList')
result = gam.ASPsList(gapiDirObj, TESTUSER)
print(result)

# Chromeos Devices
print('\nChromeosdevicesGet')
result = gam.ChromeosdevicesGet(gapiDirObj, customer, deviceId=CHROMEOS_DEVICEID, fields='deviceId,orgUnitPath')
print(result)
print('\nChromeosdevicesList')
result = gam.ChromeosdevicesList(gapiDirObj, customer, fields='deviceId,orgUnitPath')

# Groups
print('\nGroupsGet')
result = gam.GroupsGet(gapiDirObj, groupKey=TESTGROUP, fields='*')
print(result)
print('\nGroupsList')
result = gam.GroupsList(gapiDirObj, customer=customer, fields='email,directMembersCount')
print(result)

# Groups Aliases
print('\nGroupsAliasesInsert')
result = gam.GroupsAliasesInsert(gapiDirObj, TESTGROUP, TESTGROUP_ALIAS)
print(result)
print('\nGroupsAliasesList')
result = gam.GroupsAliasesList(gapiDirObj, TESTGROUP, fields='*')
print(result)
print('\nGroupsAliasesDelete')
result = gam.GroupsAliasesDelete(gapiDirObj, TESTGROUP, TESTGROUP_ALIAS)

# Members
print(result)
print('\nMembersGet')
result = gam.MembersGet(gapiDirObj, TESTGROUP, TESTUSER, fields='*')
print(result)
print('\nMembersList')
result = gam.MembersList(gapiDirObj, TESTGROUP, fields='email,id,role,status,type,delivery_settings')
print(result)

# Mobile Devices
print('\nMobiledevicesList')
result = gam.MobiledevicesList(gapiDirObj, customer, fields='resourceId,name')
print(result)

# Org Units
print('\nOrgunitsInsert')
result = gam.OrgunitsInsert(gapiDirObj, customer, body={'name': TESTOU, 'parentOrgUnitPath': TESTOUPARENT})
print(result)
print('\nOrgunitsGet')
result = gam.OrgunitsGet(gapiDirObj, customer, TESTOUPARENT+'/'+TESTOU)
print(result)
print('\nOrgunitsUpdate')
result = gam.OrgunitsUpdate(gapiDirObj, customer, TESTOUPARENT+'/'+TESTOU, body={'description': 'Fribble'})
print(result)
print('\nOrgunitsList')
result = gam.OrgunitsList(gapiDirObj, customer)
print(result)
print('\nOrgunitsDelete')
result = gam.OrgunitsDelete(gapiDirObj, customer, TESTOUPARENT+'/'+TESTOU)
print(result)

# Privileges
print('\nPrivilegesList')
result = gam.PrivilegesList(gapiDirObj, customer)
print(result)

# Roles
print('\nRolesList')
result = gam.RolesList(gapiDirObj, customer)
print(result)

# Role Assignments
print('\nRoleAssignmentsList')
result = gam.RoleAssignmentsList(gapiDirObj, customer, TESTUSER)
print(result)

# Schemas
print('\nSchemasInsert')
result = gam.SchemasInsert(gapiDirObj, customer, body={'schemaName': TESTSCHEMA, 'displayName': TESTSCHEMA+' Display',
                                                       'fields': [{'fieldName': 'BoolField', 'displayName': 'BoolField Display', 'fieldType': 'BOOL'}]})
print(result)
print('\nSchemasGet')
result = gam.SchemasGet(gapiDirObj, customer, TESTSCHEMA)
print(result)
print('\nSchemasUpdate')
result = gam.SchemasUpdate(gapiDirObj, customer, TESTSCHEMA, body={'schemaName': TESTSCHEMA,
                                                                   'fields': [{'fieldName': 'IntField', 'displayName': 'IntField Display', 'fieldType': 'INT64'}]})
print(result)
print('\nSchemasList')
result = gam.SchemasList(gapiDirObj, customer)
print(result)
print('\nSchemasDelete')
result = gam.SchemasDelete(gapiDirObj, customer, TESTSCHEMA)
print(result)
print('\nUsersGet')
result = gam.UsersGet(gapiDirObj, TESTUSER, fields='*')
print(result)

# Tokens
print('\nTokensList')
result = gam.TokensList(gapiDirObj, TESTUSER)
print(result)

# Users
print('\nUsersList')
result = gam.UsersList(gapiDirObj, customer=customer, query='orgUnitPath:/Test', fields='primaryEmail,includeInGlobalAddressList,suspended')
print(result)
print('\nUsersAliasesInsert')
result = gam.UsersAliasesInsert(gapiDirObj, TESTUSER, TESTUSER_ALIAS)
print(result)

# Users Aliases
print('\nUsersAliasesList')
result = gam.UsersAliasesList(gapiDirObj, TESTUSER, fields='*')
print(result)
print('\nUsersAliasesDelete')
result = gam.UsersAliasesDelete(gapiDirObj, TESTUSER, TESTUSER_ALIAS)
print(result)

# Verification Codes
print('\nVerificationCodesGenerate')
result = gam.VerificationCodesGenerate(gapiDirObj, TESTUSER)
print(result)
print('\nVerificationCodesList')
result = gam.VerificationCodesList(gapiDirObj, TESTUSER)
print(result)
print('\nVerificationCodesInvalidate')
result = gam.VerificationCodesInvalidate(gapiDirObj, TESTUSER)
print(result)
