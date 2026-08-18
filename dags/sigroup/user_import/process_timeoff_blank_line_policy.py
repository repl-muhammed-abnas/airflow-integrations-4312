import json
from sigroup.user_import.utils import custom_methods
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_user_import_disable_user_blank_timeoff_policy,
       description="sigroup user import disable user unassign timeoff types child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_timeoff_allowed_against_timeoff_type = rail.IfOperator(
            task_id="if_timeoff_allowed_against_timeoff_type",
            test=lambda dag_run: bool(dag_run.conf["policyset"] and
                dag_run.conf["policyset"][0]["effectiveDate"] and
                dag_run.conf["isTimeOffAllowedAgainstThisTimeOffType"]),
            yes_task="create_timeoff_policies_list",
            no_task="write_log_disable_user_failed"
        )

        create_timeoff_policies_list = rail.SetVariableOperator(
            task_id="create_timeoff_policies_list",
            name="timeoff_list_for_user",
            value=[]
        )

        process_each_policy = rail.ForEachOperator(
            task_id="process_each_policy",
            items='{{dag_run.conf["policyset"]|to_json}}',
            start_task="if_effectivedate_less_than_enddate",
            end_task="end_timeoff_policy"
        )

        if_effectivedate_less_than_enddate = rail.IfOperator(
            task_id="if_effectivedate_less_than_enddate",
            test=lambda dag_run: custom_methods.is_policy_line_before(
                rail.result("process_each_policy"), dag_run.conf["enddate"]),
            yes_task="add_timeoff_policies_list",
            no_task="end_timeoff_policy"
        )

        add_timeoff_policies_list = rail.SetVariableOperator(
            task_id="add_timeoff_policies_list",
            name='{{result("create_timeoff_policies_list").name}}',
            value=lambda: {
                "effectiveDate": rail.result("process_each_policy")["effectiveDate"],
                "description": rail.result("process_each_policy")["description"],
                "policySet": rail.result("process_each_policy")["policySet"]
            }
        )

        end_timeoff_policy = rail.EmptyOperator(task_id="end_timeoff_policy")

        add_blank_timeoff_policy = rail.PythonOperator(
            task_id="add_blank_timeoff_policy",
            python_callable=lambda dag_run: (
                [rail.result("add_timeoff_policies_list")["value"]]
                if rail.result("add_timeoff_policies_list")["value"] else []
            ) + [custom_methods.get_stop_accrual_policy_line(dag_run)]
        )

        get_timeoff_policies = rail.PythonOperator(
            task_id="get_timeoff_policies",
            python_callable=lambda: list(map(lambda x: json.loads(json.dumps(x)
                                                                  .replace('"script"', '"scriptTarget"')
                                                                  .replace('"description": null', '"description": "effective"')),
                                             rail.result("add_blank_timeoff_policy")))
        )

        if_timeoff_policies = rail.IfOperator(
            task_id="if_timeoff_policies",
            test=lambda:rail.result("get_timeoff_policies"),
            yes_task="put_timeoff_with_initial_as_remaining_balance",
            no_task="write_log_disable_user_failed"
        )

        put_timeoff_with_initial_as_remaining_balance = rail.RepliconServiceOperator(
            task_id="put_timeoff_with_initial_as_remaining_balance",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": dag_run.conf["timeoffuri"]
                },
                "policySetScheduleEntries": rail.result("get_timeoff_policies")
            }
        )

        write_log_disable_user_failed = rail.WriteLogOperator(
            task_id="write_log_disable_user_failed",
            log='{{dag_run.conf.lookuptable}}',
            message="User time off blank line policy",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf["employeeid"],
                "Username": dag_run.conf["firstname"] + dag_run.conf["lastname"],
                "Action": "Disable user",
                "Status": "Error",
                "Details": "Termination user time off blank line policy failed",
                
            }
        )

        if_timeoff_allowed_against_timeoff_type >> rail.Label("Yes") >>\
        create_timeoff_policies_list >>\
            process_each_policy >> end_timeoff_policy
        process_each_policy >>\
            if_effectivedate_less_than_enddate >> rail.Label("Yes") >>\
            add_timeoff_policies_list >> end_timeoff_policy
        if_effectivedate_less_than_enddate >> rail.Label("No") >>\
            end_timeoff_policy>>\
        add_blank_timeoff_policy >> get_timeoff_policies >>\
        if_timeoff_policies >> rail.Label("No") >> write_log_disable_user_failed
        if_timeoff_policies >> rail.Label("Yes") >>\
            put_timeoff_with_initial_as_remaining_balance >>\
            write_log_disable_user_failed
        if_timeoff_allowed_against_timeoff_type >> rail.Label("No") >>\
        write_log_disable_user_failed
        return dag


rail.for_each_instance(create_airflow_dag)
