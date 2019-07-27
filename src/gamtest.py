#!/usr/bin/env python3

import gam
#GAMCFG = '/Library/Application Support/GAM/gam.cfg'
GAMCFG = '/Users/Ross/.gam/gam.cfg'

gam.SetGlobalVariables(GAMCFG)
#result, status = gam.CustomersGet()
#print(status, result)
#result, status = gam.DomainsGet()
#print(status, result)
#result, status = gam.GroupsGet(groupKey='testgroup@rdschool.org', fields='*')
#print(status, result)
#result, status = gam.GroupsList(fields='email,directMembersCount')
#print(status, result)
#result, status = gam.MembersGet(groupKey='testgroup@rdschool.org', memberKey='testuser1@rdschool.org', fields='*')
#print(status, result)
#result, status = gam.MembersList(groupKey='testgroup@rdschool.org', fields='email,id,role,status,type,delivery_settings')
#print(status, result)
#result, status = gam.OrgunitsGet(orgUnitPath='/Test')
#print(status, result)
#result, status = gam.OrgunitsList()
#print(status, result)
#result, status = gam.UsersGet(userKey='testuser1@rdschool.org', fields='*')
#print(status, result)
result, status = gam.UsersList(query='orgUnitPath:/Test', fields='primaryEmail,includeInGlobalAddressList,suspended')
print(status, result)
