# pylint: disable=wildcard-import unused-wildcard-import
from arcticwolf.disable_user.config import *

instance = "trial"

environment = "pre-production"

company_key = "arcticwolfnetworksinctrial01"

replicon_conn_id = "arcticwolfnetworksinctrial01_replicon_admin"

disable_user_child_dagid = f"arcticwolf_user_import_disable_user_child_{instance}"
disable_user_main_dagid = f"arcticwolf_user_import_disable_user_master_{instance}"

disabled=True
