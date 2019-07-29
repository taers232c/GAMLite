#!/usr/bin/env python3
"""Test GAMLite
"""

import gam
from gamlib import glapi as API

#GAMCFG = '/Library/Application Support/GAM/gam.cfg'
GAMCFG = '/Users/Ross/.gam/gam.cfg'
CHROMEOS_DEVICEID = '32c61153-3808-463b-ba0a-100376212222'
DOMAIN_NAME = 'rdschool.org'
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

gapiDirObj = gam.buildGAPIObject(API.DIRECTORY)

# Customers
print('\nCustomersGet')
result, status = gam.CustomersGet(gapiDirObj, customer)
print(status, result)
if status:
  customer = result.get('id', customer)

# Domains
print('\nDomainsGet')
result, status = gam.DomainsGet(gapiDirObj, customer, DOMAIN_NAME)
print(status, result)
print('\nDomainsList')
result, status = gam.DomainsList(gapiDirObj, customer)
print(status, result)

# Domain Aliases
print('\nDomainAliasesList')
result, status = gam.DomainAliasesList(gapiDirObj, customer)
print(status, result)

# ASPS
print('\nASPSList')
result, status = gam.ASPsList(gapiDirObj, TESTUSER)
print(status, result)

# Chromeos Devices
print('\nChromeosdevicesGet')
result, status = gam.ChromeosdevicesGet(gapiDirObj, customer, deviceId=CHROMEOS_DEVICEID, fields='deviceId,orgUnitPath')
print(status, result)
print('\nChromeosdevicesList')
result, status = gam.ChromeosdevicesList(gapiDirObj, customer, fields='deviceId,orgUnitPath')

# Groups
print('\nGroupsGet')
result, status = gam.GroupsGet(gapiDirObj, groupKey=TESTGROUP, fields='*')
print(status, result)
print('\nGroupsList')
result, status = gam.GroupsList(gapiDirObj, customer=customer, fields='email,directMembersCount')
print(status, result)

# Groups Aliases
print('\nGroupsAliasesInsert')
result, status = gam.GroupsAliasesInsert(gapiDirObj, TESTGROUP, TESTGROUP_ALIAS)
print(status, result)
print('\nGroupsAliasesList')
result, status = gam.GroupsAliasesList(gapiDirObj, TESTGROUP, fields='*')
print(status, result)
print('\nGroupsAliasesDelete')
result, status = gam.GroupsAliasesDelete(gapiDirObj, TESTGROUP, TESTGROUP_ALIAS)

# Members
print(status, result)
print('\nMembersGet')
result, status = gam.MembersGet(gapiDirObj, TESTGROUP, TESTUSER, fields='*')
print(status, result)
print('\nMembersList')
result, status = gam.MembersList(gapiDirObj, TESTGROUP, fields='email,id,role,status,type,delivery_settings')
print(status, result)

# Mobile Devices
print('\nMobiledevicesList')
result, status = gam.MobiledevicesList(gapiDirObj, customer, fields='resourceId,name')
print(status, result)

# Org Units
print('\nOrgunitsInsert')
result, status = gam.OrgunitsInsert(gapiDirObj, customer, body={'name': TESTOU, 'parentOrgUnitPath': TESTOUPARENT})
print(status, result)
print('\nOrgunitsGet')
result, status = gam.OrgunitsGet(gapiDirObj, customer, TESTOUPARENT+'/'+TESTOU)
print(status, result)
print('\nOrgunitsUpdate')
result, status = gam.OrgunitsUpdate(gapiDirObj, customer, TESTOUPARENT+'/'+TESTOU, body={'description': 'Fribble'})
print(status, result)
print('\nOrgunitsList')
result, status = gam.OrgunitsList(gapiDirObj, customer)
print(status, result)
print('\nOrgunitsDelete')
result, status = gam.OrgunitsDelete(gapiDirObj, customer, TESTOUPARENT+'/'+TESTOU)
print(status, result)

# Privileges
print('\nPrivilegesList')
result, status = gam.PrivilegesList(gapiDirObj, customer)
print(status, result)

# Roles
print('\nRolesList')
result, status = gam.RolesList(gapiDirObj, customer)
print(status, result)

# Role Assignments
print('\nRoleAssignmentsList')
result, status = gam.RoleAssignmentsList(gapiDirObj, customer, TESTUSER)
print(status, result)

# Schemas
print('\nSchemasInsert')
result, status = gam.SchemasInsert(gapiDirObj, customer, body={'schemaName': TESTSCHEMA, 'displayName': TESTSCHEMA+' Display',
                                                               'fields': [{'fieldName': 'BoolField', 'displayName': 'BoolField Display', 'fieldType': 'BOOL'}]})
print(status, result)
print('\nSchemasGet')
result, status = gam.SchemasGet(gapiDirObj, customer, TESTSCHEMA)
print(status, result)
print('\nSchemasUpdate')
result, status = gam.SchemasUpdate(gapiDirObj, customer, TESTSCHEMA, body={'schemaName': TESTSCHEMA,
                                                                           'fields': [{'fieldName': 'IntField', 'displayName': 'IntField Display', 'fieldType': 'INT64'}]})
print(status, result)
print('\nSchemasList')
result, status = gam.SchemasList(gapiDirObj, customer)
print(status, result)
print('\nSchemasDelete')
result, status = gam.SchemasDelete(gapiDirObj, customer, TESTSCHEMA)
print(status, result)
print('\nUsersGet')
result, status = gam.UsersGet(gapiDirObj, TESTUSER, fields='*')
print(status, result)

# Tokens
print('\nTokensList')
result, status = gam.TokensList(gapiDirObj, TESTUSER)
print(status, result)

# Users
print('\nUsersList')
result, status = gam.UsersList(gapiDirObj, customer=customer, query='orgUnitPath:/Test', fields='primaryEmail,includeInGlobalAddressList,suspended')
print(status, result)
print('\nUsersAliasesInsert')
result, status = gam.UsersAliasesInsert(gapiDirObj, TESTUSER, TESTUSER_ALIAS)
print(status, result)

# Users Aliases
print('\nUsersAliasesList')
result, status = gam.UsersAliasesList(gapiDirObj, TESTUSER, fields='*')
print(status, result)
print('\nUsersAliasesDelete')
result, status = gam.UsersAliasesDelete(gapiDirObj, TESTUSER, TESTUSER_ALIAS)
print(status, result)

# Verification Codes
print('\nVerificationCodesGenerate')
result, status = gam.VerificationCodesGenerate(gapiDirObj, TESTUSER)
print(status, result)
print('\nVerificationCodesList')
result, status = gam.VerificationCodesList(gapiDirObj, TESTUSER)
print(status, result)
print('\nVerificationCodesInvalidate')
result, status = gam.VerificationCodesInvalidate(gapiDirObj, TESTUSER)
print(status, result)
