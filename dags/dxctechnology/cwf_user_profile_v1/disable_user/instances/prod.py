# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.cwf_user_profile_v1.disable_user.config import *

instance = 'production'
environment = 'production'

company_key = 'dxctechnology'
replicon_conn_id = 'DXCTechnology_http_RepliconIntFG'

main_dagid = f'dxctechnology_cwf_userprofiles_disable_master_{instance}_v1'
child_dagid = f'dxctechnology_cwf_userprofiles_disable_child_{instance}_v1'
