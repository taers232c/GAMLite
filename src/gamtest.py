#!/usr/bin/env python3
"""Test GAMLite
"""

import gam
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

# Customers
print('\nCustomersGet')
result, status = gam.CustomersGet(customer)
print(status, result)
if status:
  customer = result.get('id', customer)

# Domains
print('\nDomainsGet')
result, status = gam.DomainsGet(customer, DOMAIN_NAME)
print(status, result)
print('\nDomainsList')
result, status = gam.DomainsList(customer)
print(status, result)

# Domain Aliases
print('\nDomainAliasesList')
result, status = gam.DomainAliasesList(customer)
print(status, result)

# Chromeos Devices
print('\nChromeosdevicesGet')
result, status = gam.ChromeosdevicesGet(customer, deviceId=CHROMEOS_DEVICEID, fields='deviceId,orgUnitPath')
print(status, result)
print('\nChromeosdevicesList')
result, status = gam.ChromeosdevicesList(customer, fields='deviceId,orgUnitPath')

# Groups
print('\nGroupsGet')
result, status = gam.GroupsGet(groupKey=TESTGROUP, fields='*')
print(status, result)
print('\nGroupsList')
result, status = gam.GroupsList(customer=customer, fields='email,directMembersCount')
print(status, result)

# Groups Aliases
print('\nGroupsAliasesInsert')
result, status = gam.GroupsAliasesInsert(TESTGROUP, TESTGROUP_ALIAS)
print(status, result)
print('\nGroupsAliasesList')
result, status = gam.GroupsAliasesList(TESTGROUP, fields='*')
print(status, result)
print('\nGroupsAliasesDelete')
result, status = gam.GroupsAliasesDelete(TESTGROUP, TESTGROUP_ALIAS)

# Members
print(status, result)
print('\nMembersGet')
result, status = gam.MembersGet(TESTGROUP, TESTUSER, fields='*')
print(status, result)
print('\nMembersList')
result, status = gam.MembersList(TESTGROUP, fields='email,id,role,status,type,delivery_settings')
print(status, result)

# Mobile Devices
print('\nMobiledevicesList')
result, status = gam.MobiledevicesList(customer, fields='resourceId,name')
print(status, result)

# Org Units
print('\nOrgunitsInsert')
result, status = gam.OrgunitsInsert(customer, body={'name': TESTOU, 'parentOrgUnitPath': TESTOUPARENT})
print(status, result)
print('\nOrgunitsGet')
result, status = gam.OrgunitsGet(customer, TESTOUPARENT+'/'+TESTOU)
print(status, result)
print('\nOrgunitsUpdate')
result, status = gam.OrgunitsUpdate(customer, TESTOUPARENT+'/'+TESTOU, body={'description': 'Fribble'})
print(status, result)
print('\nOrgunitsList')
result, status = gam.OrgunitsList(customer)
print(status, result)
print('\nOrgunitsDelete')
result, status = gam.OrgunitsDelete(customer, TESTOUPARENT+'/'+TESTOU)
print(status, result)

# Schemas
print('\nSchemasInsert')
result, status = gam.SchemasInsert(customer, body={'schemaName': TESTSCHEMA, 'displayName': TESTSCHEMA+' Display',
                                               'fields': [{'fieldName': 'BoolField', 'displayName': 'BoolField Display', 'fieldType': 'BOOL'}]})
print(status, result)
print('\nSchemasGet')
result, status = gam.SchemasGet(customer, TESTSCHEMA)
print(status, result)
print('\nSchemasUpdate')
result, status = gam.SchemasUpdate(customer, TESTSCHEMA, body={'schemaName': TESTSCHEMA,
                                                           'fields': [{'fieldName': 'IntField', 'displayName': 'IntField Display', 'fieldType': 'INT64'}]})
print(status, result)
print('\nSchemasList')
result, status = gam.SchemasList(customer)
print(status, result)
print('\nSchemasDelete')
result, status = gam.SchemasDelete(customer, TESTSCHEMA)
print(status, result)
print('\nUsersGet')
result, status = gam.UsersGet(TESTUSER, fields='*')
print(status, result)

# Users
print('\nUsersList')
result, status = gam.UsersList(customer=customer, query='orgUnitPath:/Test', fields='primaryEmail,includeInGlobalAddressList,suspended')
print(status, result)
print('\nUsersAliasesInsert')
result, status = gam.UsersAliasesInsert(TESTUSER, TESTUSER_ALIAS)
print(status, result)

# Users Aliases
print('\nUsersAliasesList')
result, status = gam.UsersAliasesList(TESTUSER, fields='*')
print(status, result)
print('\nUsersAliasesDelete')
result, status = gam.UsersAliasesDelete(TESTUSER, TESTUSER_ALIAS)
print(status, result)
