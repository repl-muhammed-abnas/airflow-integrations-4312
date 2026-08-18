from datetime import timedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable


def create_add_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.portugal_add_user_timeoff_assignment_dag_id,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_portugal, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="has_timeoff_types_to_assign_to_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="has_timeoff_types_to_assign_to_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        def get_timeoff_assignment_payload(dag_run):
            return {
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": dag_run.conf["timeoffs"]
            }

        has_timeoff_types_to_assign_to_user = rail.IfOperator(
            task_id="has_timeoff_types_to_assign_to_user",
            test=lambda dag_run: len(dag_run.conf.get("timeoffs", [])) > 0,
            yes_task="assign_timeoff_to_user",
            no_task="catch_and_log_error"
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=get_timeoff_assignment_payload
        )

        # process_normal_timeoffs in for loop
        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda dag_run: [timeoff for timeoff in dag_run.conf["map_mapper_replicon_timeoff"] if timeoff['uri']],
            start_task="get_default_timeoff_policy",
            end_task="empty_process_special_timeoff"
        )

        def is_timeoff_type_prt_vacation_current_year(dag_run):
            return dag_run.conf['mapped_timeoff_data'][rail.result("for_each_timeoff")['uri']] == "[PRT] Vacation Current Year"

        def get_default_timeoff_policy_payload(dag_run):
            timeoff_type_uri = rail.result("for_each_timeoff")['uri']
            # in workato the 2nd condition is not present it's reversed here
            # is_timeoff_type_prt_vacation_current_year and else of workshift not BPS OR BPSOT
            if is_timeoff_type_prt_vacation_current_year(dag_run) and dag_run.conf['workshift'] in ['BPS', 'BPSOT']:
                timeoff_type_uri = dag_run.conf['prt_vacation_bps_bpsot'].get('uri')
            return {
                "timeOffAccount":{
                    "userUri" : dag_run.conf['user_uri'],
                    "timeOffTypeUri": timeoff_type_uri
                }
            }

        def render_template(response, dag_run):
            timeoff_type_uri = rail.result("for_each_timeoff")['uri']
            # in workato the 2nd condition is not present it's reversed here
            # is_timeoff_type_prt_vacation_current_year and else of workshift not BPS OR BPSOT
            if is_timeoff_type_prt_vacation_current_year(dag_run) and dag_run.conf['workshift'] in ['BPS', 'BPSOT']:
                timeoff_type_uri = dag_run.conf['prt_vacation_bps_bpsot'].get('uri')
            rail.set_result(key=timeoff_type_uri, val= response)
            return response

        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=get_default_timeoff_policy_payload,
            data_handler=render_template
        )

        def get_update_timeoff_policies_payload(dag_run):

            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result("get_default_timeoff_policy")
                            ).replace("null", "\"effective\""
                        ).replace("\"script\"", "\"scriptTarget\""
                        )) if rail.result("get_default_timeoff_policy") else []
            }

        update_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_update_timeoff_policies_payload
        )

        empty_process_special_timeoff = rail.EmptyOperator(
            task_id = "empty_process_special_timeoff"
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Add",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Add',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )


        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> has_timeoff_types_to_assign_to_user >> rail.Label("Yes") >> assign_timeoff_to_user >> for_each_timeoff
        has_timeoff_types_to_assign_to_user >> rail.Label("No") >> catch_and_log_error

        for_each_timeoff >> get_default_timeoff_policy >> update_timeoff_policies >> empty_process_special_timeoff
        for_each_timeoff >> empty_process_special_timeoff >> catch_and_log_error

        return dag

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
