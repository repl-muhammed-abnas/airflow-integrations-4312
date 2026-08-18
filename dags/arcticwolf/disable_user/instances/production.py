# pylint: disable=wildcard-import unused-wildcard-import
from arcticwolf.disable_user.config import *

instance = "production"

environment = "production"

company_key = "Arcticwolfnetworksinc"

replicon_conn_id = "Arcticwolfnetworksinc_replicon_int"

disable_user_child_dagid = f"arcticwolf_user_import_disable_user_child_{instance}"
disable_user_main_dagid = f"arcticwolf_user_import_disable_user_master_{instance}"
