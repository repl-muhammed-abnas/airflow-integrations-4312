# pylint: disable=wildcard-import unused-wildcard-import
from chainiqgroupag.disable_user.config import *

instance = "trial"

environment = "pre-production"

company_key = "ChainIQGroupAGTrial01"

replicon_conn_id = "ChainIQGroupAGTrial_replicon_admin"

disable_user_child_dagid = f"chainiq_user_disable_child_{instance}"
disable_user_main_dagid = f"chainiq_user_disable_master_{instance}"
