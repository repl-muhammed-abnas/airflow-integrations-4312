import rail
from wipro.user_import_germany_v1.utils import custom_methods, request_payload

def set_balance(status="update"):
    with rail.TaskGroup(
            group_id="set_to_balance_values_for_user",
            prefix_group_id=False
    ) as set_to:

        if_user_has_gender = rail.IfOperator(
            task_id="if_user_has_gender",
            test=lambda dag_run:bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_extension_field_values"),
                "displayText",
                "Gender",
                "textValue") or (dag_run.conf["gender"] and status == "add")),
            yes_task="get_user_annual_accrued_leaves",
            no_task="end_set_balance"
        )

        get_user_annual_accrued_leaves  = rail.RepliconServiceOperator(
            task_id="get_user_annual_accrued_leaves",
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=request_payload.get_user_annual_leaves_payload,
            data_handler=lambda response, dag_run:custom_methods.get_user_annual_leaves_taken(dag_run, response)
        )

        set_termination_time_off_balance_policy = rail.RepliconServiceOperator(
            task_id="set_termination_time_off_balance_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_germany_annual_acquistion_terminated_user_payload
        )

        end_set_balance = rail.EmptyOperator(task_id="end_set_balance")

        if_user_has_gender >> rail.Label("Yes") >>\
        get_user_annual_accrued_leaves >>\
        set_termination_time_off_balance_policy >> end_set_balance
        if_user_has_gender >> rail.Label("No") >> end_set_balance

        return set_to
