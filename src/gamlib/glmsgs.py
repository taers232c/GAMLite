# -*- coding: utf-8 -*-

# Copyright (C) 2019 Ross Scroggs All Rights Reserved.
#
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""GAM messages

"""

# These values can be translated into other languages
ACCESS_FORBIDDEN = 'Access Forbidden'
API_ACCESS_DENIED = 'API access Denied'
API_CHECK_CLIENT_AUTHORIZATION = 'Please make sure the Client ID: {0} is authorized for the appropriate scopes:\n{1}\n\nRun: gam oauth create\n'
API_CHECK_SVCACCT_AUTHORIZATION = 'Please make sure the Service Account Client name: {0} is authorized for the appropriate scopes:\n{1}\n\nRun: gam user {2} check serviceaccount\n'
DISABLE_TLS_MIN_MAX = 'Execute: gam select default config tls_max_version "" tls_min_version "" save\n'
DOES_NOT_EXIST = 'Does not exist'
DOES_NOT_EXIST_OR_HAS_INVALID_FORMAT = '{0}: {1}, Does not exist or has invalid format'
EXECUTE_GAM_OAUTH_CREATE = '\nPlease run\n\ngam oauth delete\ngam oauth create\n\n'
EXPECTED = 'Expected'
INSTRUCTIONS_CLIENT_SECRETS_JSON = 'Please run\n\ngam create project\ngam oauth create\n\nto create and authorize a Client account.\n'
INSTRUCTIONS_OAUTH2SERVICE_JSON = 'Please run\n\ngam create project\ngam user <user> check serviceaccount\n\nto create and authorize a Service account.\n'
INSUFFICIENT_PERMISSIONS_TO_PERFORM_TASK = 'Insufficient permissions to perform this task'
INVALID = 'Invalid'
INVALID_JSON_INFORMATION = 'Google API reported Invalid JSON Information'
NOT_FOUND = 'Not Found'
NO_CLIENT_ACCESS_ALLOWED = 'No Client Access allowed'
NO_SCOPES_FOR_API = 'There are no scopes authorized for the {0}'
NO_SVCACCT_ACCESS_ALLOWED = 'No Service Account Access allowed'
SERVICE_NOT_APPLICABLE = 'Service not applicable/Does not exist'
SERVICE_NOT_APPLICABLE_THIS_ADDRESS = 'Service not applicable for this address: {0}'
STRING_LENGTH = 'string length'
UNKNOWN = 'Unknown'
UNKNOWN_API_OR_VERSION = 'Unknown Google API or version: ({0}), contact {1}'
