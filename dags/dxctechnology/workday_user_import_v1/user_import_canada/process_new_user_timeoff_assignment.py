from datetime import timedelta
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_canada.utils import custom_methods, request_payload

null = None

def create_add_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_canada_users_add_user_timeoff_process_child_for_canada_dag,
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
                config.can_run_batch_task_var_name_canada, default_var='true').lower() == 'true',
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

        
        query_timeoff_data = rail.PythonOperator(
            task_id = "query_timeoff_data",
            python_callable=lambda dag_run: list(filter(lambda row: row['Type'] == 'Timeoff' and\
                                                                    row['Country'] == dag_run.conf['country'] and\
                                                                    row['Function'] == 'Workday User Sync' and\
                                                                    row['Source'] == dag_run.conf['parent_company_code'] and\
                                                                    row['personnelsubarea'] == dag_run.conf['personnelsubarea']  and\
                                                                    row['employeegroup'] == dag_run.conf['employeegroup']  and\
                                                                    row['employeesubgroup'] == dag_run.conf['employeesubgroup']  and\
                                                                    row['status'] == dag_run.conf['status'] , config.MAPPER))
        )

        map_mapper_replicon_timeoff = rail.PythonOperator(
            task_id = "map_mapper_replicon_timeoff",
            python_callable=custom_methods.map_mapper_replicon_timeoffs
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: len(rail.result("map_mapper_replicon_timeoff")) > 0,
            yes_task="assign_timeoff_to_user"
        )


        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_timeoff_assignment_payload
        )

        # process_normal_timeoffs in for loop
        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda: [timeoff for timeoff in rail.result("map_mapper_replicon_timeoff","mapped_timeoff_data") if timeoff['uri']],
            start_task="is_timeoff_special",
            end_task="empty_process_special_timeoff"
        )

        is_timeoff_special = rail.IfOperator(
            task_id = "is_timeoff_special",
            test = lambda: rail.result("for_each_timeoff")["policy_type"] == "Specific Policy",
            yes_task = "trigger_vacation_timeoff_assignment_for_users",
            no_task = "is_payrule_required_for_can_banked_time"
        )

        trigger_vacation_timeoff_assignment_for_users = rail.TriggerDagRunOperator(
            task_id = "trigger_vacation_timeoff_assignment_for_users",
            trigger_dag_id=config.workday_user_import_canada_users_process_canada_vacation_timeoff_type_child_dag,
            conf=lambda dag_run: {
                "file_name": dag_run.conf['file_name'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "payrule":  dag_run.conf['payrule'],
                "company_code": dag_run.conf['company_code'],
                "parent_company_code": dag_run.conf['parent_company_code'],
                "country": dag_run.conf['country'],
                'user_log': dag_run.conf['user_log'],
                "personnelsubarea": dag_run.conf['personnelsubarea'],
                "employeegroup": dag_run.conf['employeegroup'],
                "employeesubgroup": dag_run.conf['employeesubgroup'],
                "status": dag_run.conf['status'],
                "json_formatted_dates": {
                    "continuous_service_date": dag_run.conf['json_formatted_dates']['service_date'],
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date']
                },
                "file_data": {
                    "emp_id": dag_run.conf['file_data']['emp_id'],
                    "email_id": dag_run.conf['file_data']['email_id']
                },
                "timeoff_type_uri": rail.result("for_each_timeoff")["uri"],
                "timeoff_type_name": rail.result("for_each_timeoff")["name"]
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )


        def is_payrule_required_for_can_banked_time_callable(dag_run):
            if rail.result("for_each_timeoff")["name"] == "[CAN] Banked time":
                if dag_run.conf['payrule'] == "Canada Ontario- In/Out":
                    return "[CAN] Banked time - Canada Ontario- In/Out"
                if dag_run.conf['payrule'] == "Canada Quebec- In/Out":
                    return "[CAN] Banked time - Canada Quebec- In/Out"
            return null

        is_payrule_required_for_can_banked_time = rail.PythonOperator(
            task_id = "is_payrule_required_for_can_banked_time",
            python_callable = is_payrule_required_for_can_banked_time_callable
        )

        def get_default_timeoff_policy_payload(dag_run):
            payrule_required_for_can_banked_time = rail.result("is_payrule_required_for_can_banked_time")
            timeoff_type_uri = rail.result("for_each_timeoff")['uri']
            if payrule_required_for_can_banked_time:
                timeoff_type_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoffs'), 'name', payrule_required_for_can_banked_time, 'uri')


            return {"timeOffAccount":{
                "userUri" : dag_run.conf['user_uri'],
                "timeOffTypeUri": timeoff_type_uri
            }}


        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=get_default_timeoff_policy_payload
        )

        update_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_update_timeoff_policies_payload
        )

        empty_process_special_timeoff = rail.EmptyOperator(
            task_id = "empty_process_special_timeoff"
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Update Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['emp_id'],
                "Email": dag_run.conf['email_id'],
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_all_timeoffs

        get_all_timeoffs >> query_timeoff_data >> map_mapper_replicon_timeoff >> has_any_timeoff_to_assign >> rail.Label(
            "Yes") >> assign_timeoff_to_user >> for_each_timeoff

        for_each_timeoff >> is_timeoff_special >> rail.Label("Yes") >> trigger_vacation_timeoff_assignment_for_users >> empty_process_special_timeoff
        is_timeoff_special >> rail.Label("No") >> is_payrule_required_for_can_banked_time >> get_default_timeoff_policy >> update_timeoff_policies >> empty_process_special_timeoff
        for_each_timeoff >> empty_process_special_timeoff >> catch_and_log_error

        return dag

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
