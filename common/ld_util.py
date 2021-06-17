"""A collection of functions that simplify working Launch Darkly. 

This module defines functions used to
    * get a reference to the launchdarkly client object
    * create the launchdarkly identity object used to resolve identity-specific
      feature flag values
    * resolve the required params from the glue job arguments
"""
import sys
import ldclient
import common.constants as constants
from awsglue.utils import getResolvedOptions

def get_ld_client():
  """Returns the launchdarkly client object.

  Returns:
      ldclient.client.LDClient: The LaunchDarkly client interface.
  """
  params = get_ld_params()
  ldclient.set_sdk_key(params[constants.LD_SDK_KEY])
  return ldclient.get()

# Constructs the launchdarkly identity object used to resolve identity-specific feature flag values
def get_ld_identity():
  """Constructs the launchdarkly identity object used to resolve
  identity-specific feature flag values.

  Returns:
      dict: A dictionary the associates a user id with a tenant id.
  """
  params = get_ld_params()
  return {
    "key": params[constants.USER_ID],
    "custom": {
      "tenantId": params[constants.TENANT_ID]
    }
  }

def get_ld_params():
  """Returns a dictionary containing Glue job parameters.

  Returns:
      dict: A dictionary containing Glue job parameters.
  """
  try:
    return getResolvedOptions(sys.argv, [constants.LD_SDK_KEY, constants.USER_ID, constants.TENANT_ID])
  except Exception as e:
    print(str(e))
    return {}