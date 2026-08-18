from datetime import timedelta
from airflow.models import Variable
import rail

from incyte_biosciences_international_sarl.user_import_v1.utils import request_payload
from incyte_biosciences_international_sarl.user_import_v1.tasks.process_supervisor import process_supervisor_assignment_task_group

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_users_dagid,
        description='IBIS - User Import - Process New Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_new_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_enddate_available'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_enddate_available',
            end_task='catch_and_log_errors',
        )

        is_enddate_available = rail.IfOperator(
            task_id ='is_enddate_available',
            test = lambda dag_run: bool(dag_run.conf['end_date']),
            yes_task="log_endate_exception",
            no_task="add_new_user"
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id = 'log_endate_exception',
            log = '{{ dag_run.conf.user_log }}',
            message = "User not Created, as End Date present while User Creation",
            severity='Exception',
            properties = {
                'login_name': '{{dag_run.conf.login_name}}',
                'first_name': '{{dag_run.conf.first_name}}',
                'last_name': '{{dag_run.conf.last_name}}',
                "action": "Validation",
                "status": "Exception",
                'details': "User not Created, as End Date present while User Creation"
            }
        )

        add_new_user = rail.RepliconServiceOperator(
            task_id="add_new_user",
            endpoint="/services/importService1.svc/PutUser3",
            data=request_payload.get_put_user_payload
        )

        put_user_notification_preferences = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=request_payload.put_user_notification_preferences_payload
        )

        remove_timeoff_assignments = rail.RepliconServiceOperator(
            task_id="remove_timeoff_assignments",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_remove_timeoff_payload
        )

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: bool(dag_run.conf['supervisor_login_name']),
            yes_task='search_supervisor_in_replicon',
            no_task='process_time_off_type_assignment_new_user'
        )

        process_supervisor_entry,  process_supervisor_exit = process_supervisor_assignment_task_group(
            'add_new_user', 'new_user')

        process_time_off_type_assignment_new_user = rail.TriggerDagRunOperator(
            task_id='process_time_off_type_assignment_new_user',
            trigger_dag_id=config.process_timeoff_type_assignment_new_user_dagid,
            conf=lambda dag_run:{
                "login_name": dag_run.conf['login_name'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                'useruri': rail.result('add_new_user')['uri'],
                "user_log": dag_run.conf['user_log'],
                "action": 'add',
                "country_name": dag_run.conf['country_name']

            },
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_time_off_type_assignment_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_type_assignment_new_user',
            dag_runs='{{ result("process_time_off_type_assignment_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_time_off_type_error_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_off_type_error_logs',
            dag_runs='{{ result("process_time_off_type_assignment_new_user") }}',
            dagrun_task_id='catch_and_log_errors',
            flatten=True,
        )

        has_any_error_present = rail.IfOperator(
            task_id="has_any_error_present",
            test="{{ result('gather_time_off_type_error_logs') | is_truthy }}",
            yes_task= 'log_error_present',
            no_task='log_user_completion'
        )

        log_error_present = rail.EmptyOperator(
            task_id='log_error_present'
        )

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log='{{ dag_run.conf.user_log }}',
            message=request_payload.get_add_user_message,
            severity=request_payload.get_add_user_severity,
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Add",
                "status": request_payload.get_add_user_severity(),
                'details': request_payload.get_add_user_message()
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message="\
                {%- if get_task_state('add_new_user') == 'success' -%} \
                    User Added Partially; {{ get_error_message() }}\
                {%- else -%}\
                    User not created; {{ get_error_message() }}\
                {%- endif -%}",
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "last_name": "{{dag_run.conf.last_name}}",
                "first_name": "{{dag_run.conf.first_name}}",
                "action": "Add",
                'status': 'Error',
                'details': "\
                {%- if get_task_state('add_new_user') == 'success' -%} \
                    User Added Partially; {{ get_error_message() }}\
                {%- else -%}\
                    User not created; {{ get_error_message() }}\
                {%- endif -%}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_enddate_available

        is_enddate_available >> rail.Label('Yes') >> log_endate_exception >> catch_and_log_errors
        is_enddate_available >> rail.Label('No') >> add_new_user

        add_new_user >> put_user_notification_preferences >> remove_timeoff_assignments >> is_supervisor_in_feed_file
        is_supervisor_in_feed_file >> rail.Label('No') >> process_time_off_type_assignment_new_user
        is_supervisor_in_feed_file >> rail.Label('Yes') >> process_supervisor_entry
        process_supervisor_exit >> process_time_off_type_assignment_new_user
        process_time_off_type_assignment_new_user >> wait_for_process_time_off_type_assignment_new_user
        wait_for_process_time_off_type_assignment_new_user >> gather_time_off_type_error_logs
        gather_time_off_type_error_logs >> has_any_error_present
        has_any_error_present >> rail.Label('No') >> log_user_completion >> catch_and_log_errors >> log_to_sumo
        has_any_error_present >> rail.Label('No') >> log_error_present >> catch_and_log_errors


        return dag

rail.for_each_instance(create_child_dag)
