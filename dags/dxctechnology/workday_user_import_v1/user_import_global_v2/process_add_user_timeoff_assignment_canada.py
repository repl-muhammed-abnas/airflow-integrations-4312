from datetime import timedelta
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils import custom_methods, request_payload



def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_global_v2_users_add_user_timeoff_process_child_for_canada_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.global_add_user_timeoff_assignment_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_global, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_all_timeoffs"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_all_timeoffs",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        map_mapper_replicon_timeoffs = rail.PythonOperator(
            task_id = "map_mapper_replicon_timeoff",
            python_callable=custom_methods.map_mapper_replicon_timeoffs
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: len(rail.result("map_mapper_replicon_timeoffs")) > 0,
            yes_task="assign_timeoff_to_user"
        )


        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_timeoff_assignment_payload
        )

        # process_normal_timeoffs in for loop
        for_each_non_special_timeoff = rail.ForEachOperator(
            task_id = "for_each_non_special_timeoff",
            items=lambda: rail.result("map_mapper_replicon_timeoffs", "non_special_timeoffs"),
            start_task="get_default_timeoff_policy",
            end_task="empty_process_special_timeoff"
        )


        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri" : dag_run.conf['user_uri'],
                "timeOffTypeUri": rail.result("for_each_non_special_timeoff")['uri']
            }
        )

        update_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_update_timeoff_policies_payload
        )

        empty_process_special_timeoff = rail.EmptyOperator(
            task_id = "empty_process_special_timeoff"
        )

        for_each_special_timeoff = rail.ForEachOperator(
            task_id = "for_each_special_timeoff",
            items=lambda: rail.result("map_mapper_replicon_timeoffs", "special_timeoffs"),
            start_task="get_default_timeoff_policy",
            end_task="empty_process_special_timeoff"
        )


        get_default_special_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_special_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda : {
                "timeOffTypeUri": rail.result("for_each_non_special_timeoff")['uri']
            }
        )

        update_special_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_special_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_special_timeoff_policies_payload
        )

        empty_process_special_timeoff_end = rail.EmptyOperator(
            task_id = "empty_process_special_timeoff_end"
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Update",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Update',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_all_timeoffs

        get_all_timeoffs >> map_mapper_replicon_timeoffs >> has_any_timeoff_to_assign >> rail.Label(
            "Yes") >> assign_timeoff_to_user >> for_each_non_special_timeoff

        for_each_non_special_timeoff >> get_default_timeoff_policy >> update_timeoff_policies >> empty_process_special_timeoff
        for_each_non_special_timeoff >> empty_process_special_timeoff >> for_each_special_timeoff

        for_each_special_timeoff >> get_default_special_timeoff_policy >> update_special_timeoff_policies >> empty_process_special_timeoff_end
        for_each_special_timeoff >> empty_process_special_timeoff_end

        return dag

rail.for_each_instance(create_dag)

