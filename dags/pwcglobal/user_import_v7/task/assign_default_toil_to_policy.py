import json
from pwcglobal.user_import_v7.utils import request_payload
import rail


def add_toil_default_policy(user_uri):
    with rail.TaskGroup(group_id="toil_timeoff_default", prefix_group_id=False) as toil_to:
        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template(user_uri),
                    "timeOffTypeUri": request_payload.get_conf()['toiltimeofftypeuri']
                }
            }
        )

        if_timeoff_has_default_policy = rail.IfOperator(
            task_id="if_timeoff_has_default_policy",
            test=lambda: bool(rail.result(
                'get_default_time_off_type_policy_schedule_for_user')),
            yes_task="assign_default_timeoff_policy"
        )

        assign_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_default_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template(user_uri),
                    "timeOffTypeUri": request_payload.get_conf()['toiltimeofftypeuri']
                },
                "policySetScheduleEntries": json.loads(
                    json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user')).replace(
                        '"script"', '"scriptTarget"').replace('"description": null', '"description": "effective"'))
            }
        )
        get_default_time_off_type_policy_schedule_for_user >>\
            if_timeoff_has_default_policy >> rail.Label("Yes") >>\
            assign_default_timeoff_policy

        return toil_to
