from datetime import timedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import.user_import_global.utils import request_payload
from dxctechnology.workday_user_import.user_import_global.task.supervisor_assignment_task import assign_supervisor


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_global_users_add_user_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.global_add_user_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        create_user = rail.RepliconServiceOperator(
            task_id = "create_user",
            endpoint="/services/ImportService1.svc/PutUser3",
            data= lambda dag_run : request_payload.get_user_creation_payload(dag_run, config)
        )

        remove_timeoffs = rail.RepliconServiceOperator(
            task_id = "remove_timeoffs",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data = {
                "userUri": "{{result('create_user').uri}}",
                "timeOffTypeUris": []
            }
        )

        can_update_notification_preference = rail.IfOperator(
            task_id = "can_update_notification_preference",
            test="{{ dag_run.conf.file_data.management_lvl in ['L1', 'L2']}}",
            yes_task="update_notification_preference",
            no_task="update_product_assignment"
        )

        update_notification_preference = rail.RepliconServiceOperator(
            task_id = "update_notification_preference",
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=request_payload.get_notification_preference_to_assign
        )

        update_product_assignment = rail.RepliconServiceOperator(
            task_id = "update_product_assignment",
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=request_payload.get_product_to_assign_to_user_payload
        )

        can_update_timeentry_path = rail.IfOperator(
            task_id = "can_update_timeentry_path",
            test="{{dag_run.conf.mapper_data.time_entry_approval_path_name | is_truthy}}",
            yes_task="update_time_entry_path",
            no_task="perform_timeoff_assignment"
        )

        update_time_entry_path = rail.RepliconServiceOperator(
            task_id = "update_time_entry_path",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.get_update_time_entry_path_payload
        )

        perform_timeoff_assignment = rail.IfOperator(
            task_id = "perform_timeoff_assignment",
            test="{{ dag_run.conf.file_data.country == 'Canada' and dag_run.conf.file_data.parent_company == 'C1'}}",
            yes_task="process_new_user_timeoff_assignment_canada",
            no_task="process_new_user_timeoff_assignment"
        )

        process_new_user_timeoff_assignment_canada = rail.TriggerDagRunOperator(
            task_id = "process_new_user_timeoff_assignment_canada",
            trigger_dag_id=config.workday_user_import_global_users_add_user_timeoff_process_child_for_canada_dag,
            conf=lambda dag_run: {
                "file_name": dag_run.conf['master_file_name'],
                "user_uri": rail.result('create_user')['uri'],
                "loginName": rail.result('create_user')['loginName'],
                "company_code": dag_run.conf['file_data']['company_code'],
                "parent_company_code": dag_run.conf['file_data']['parent_company'],
                "country": dag_run.conf['file_data']['country'],
                "timeoffs": dag_run.conf['mapper_data']['timeoffs']
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        process_new_user_timeoff_assignment = rail.TriggerDagRunOperator(
            task_id = "process_new_user_timeoff_assignment",
            trigger_dag_id=config.workday_user_import_global_users_add_user_timeoff_process_child_dag,
            conf=lambda dag_run:{
                "file_name": dag_run.conf['master_file_name'],
                "user_uri": rail.result('create_user')['uri'],
                "loginName": rail.result('create_user')['loginName'],
                "company_code": dag_run.conf['file_data']['company_code'],
                "parent_company_code": dag_run.conf['file_data']['parent_company'],
                "country": dag_run.conf['file_data']['country'],
                "timeoffs": dag_run.conf['mapper_data']['timeoffs'],
                "user_log": dag_run.conf['user_log']
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        wait_for_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id = "wait_for_timeoff_assignment",
            dag_runs="""{{ result('process_new_user_timeoff_assignment_canada') or result('process_new_user_timeoff_assignment') }}""",
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        supervisor_start, supervisor_end = assign_supervisor("add_user_supervisor_assignment", "add")

        def get_log_message():
            exception_msg = rail.result('create_user', 'exception_log')
            if exception_msg:
                return f"User created partially - {rail.smartjoin_by_delim(exception_msg, ',')}"
            return "User created successfully"

        log_user_completion = rail.WriteLogOperator(
            task_id = "log_user_completion",
            message = "User Add",
            log="{{dag_run.conf.user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Exception" if bool(rail.result('create_user', 'exception_log')) else "Success",
                "Details": get_log_message()
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Add Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_user

        create_user >> remove_timeoffs >> can_update_notification_preference >> rail.Label("Yes") >> update_notification_preference\
            >> update_product_assignment
        can_update_notification_preference >> rail.Label("No") >> update_product_assignment >> can_update_timeentry_path

        can_update_timeentry_path >> rail.Label("Yes") >> update_time_entry_path >> perform_timeoff_assignment
        can_update_timeentry_path >> rail.Label("No") >> perform_timeoff_assignment

        perform_timeoff_assignment >> rail.Label("Yes, Canada & C1") >> process_new_user_timeoff_assignment_canada\
            >> wait_for_timeoff_assignment
        perform_timeoff_assignment >> rail.Label("No") >> process_new_user_timeoff_assignment\
            >> wait_for_timeoff_assignment >> supervisor_start
        supervisor_end >> log_user_completion >> catch_and_log_error

        return dag

rail.for_each_instance(create_dag)
