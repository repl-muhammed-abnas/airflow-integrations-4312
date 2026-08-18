import rail
from pwcglobal.user_import_australia import request_payload
from pwcglobal.user_import_australia import custom_methods


def get_users_data_task(caller, next_task_id):
    with rail.TaskGroup(group_id="get_user_details", prefix_group_id=False):

        get_users_data = rail.RepliconServiceOperator(
            task_id="get_users_data",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_get_data_payload,
            response_filter=custom_methods.get_user_data
        )

        is_user_exists = rail.IfOperator(
            task_id="is_user_exists",
            test="{{result('get_users_data') | is_truthy}}",
            yes_task="is_user_enabled",
            no_task="log_user_does_not_exists"
        )

        log_user_does_not_exists = rail.WriteLogOperator(
            task_id="log_user_does_not_exists",
            log="{{dag_run.conf.log}}",
            message="User not found",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "User not found",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        is_user_enabled = rail.IfOperator(
            task_id="is_user_enabled",
            test="{{result('get_users_data')[0].enabled}}"
            if caller else "{{result('get_users_data')[0].enabled}}",
            yes_task=next_task_id,
            no_task="log_user_already_disabled"
        )

        log_user_already_disabled = rail.WriteLogOperator(
            task_id="log_user_already_disabled",
            log="{{dag_run.conf.log}}",
            message="User is already disabled",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "User is already disabled",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )
        get_users_data >> is_user_exists >> rail.Label(
            "No") >> log_user_does_not_exists >> finish
        is_user_exists >> rail.Label("Yes") >> is_user_enabled
        is_user_enabled >> rail.Label(
            "No") >> log_user_already_disabled >> finish

    return get_users_data, is_user_enabled, finish
