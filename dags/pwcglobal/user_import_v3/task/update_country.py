import rail
from pwcglobal.user_import_v3 import request_payload
null=None

def get_update_country():
    with rail.TaskGroup(group_id='update_country_task', prefix_group_id=False) as update_country_task:

        update_country = rail.RepliconServiceOperator(
            task_id='update_country',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.get_udpate_country_param
        )

        put_timeoff_policy_dataaccessscopes_for_user = rail.RepliconServiceOperator(
            task_id='put_timeoff_policy_dataaccessscopes_for_user',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.get_put_timeoff_policy_datascope_param
        )

        put_user_policy_dataaccessscopes_for_user = rail.RepliconServiceOperator(
            task_id='put_user_policy_dataaccessscopes_for_user',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.get_put_userpolicy_datascope_param
        )

        update_country >> put_timeoff_policy_dataaccessscopes_for_user >> \
            put_user_policy_dataaccessscopes_for_user

    return update_country_task
