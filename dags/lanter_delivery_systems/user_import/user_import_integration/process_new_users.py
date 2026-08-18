from datetime import timedelta
from airflow.models import Variable
import rail

from lanter_delivery_systems.user_import.user_import_integration.utils import request_payload
from lanter_delivery_systems.user_import.user_import_integration.tasks.process_supervisor import process_supervisor_assignment_task_group

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_users_dagid,
        description='Lanter Delivery Systems User Import - Process New Users',
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
            no_task='add_new_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='add_new_user',
            end_task='catch_and_log_errors',
        )

        add_new_user = rail.RepliconServiceOperator(
            task_id="add_new_user",
            endpoint="/services/importService1.svc/PutUser3",
            data=request_payload.get_put_user_payload
        )

        remove_timeoff_assignments = rail.RepliconServiceOperator(
            task_id="remove_timeoff_assignments",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_remove_timeoff_payload
        )

        add_product_licences = rail.RepliconServiceOperator(
            task_id='add_product_licences',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=request_payload.get_product_license_payload
        )

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: bool(dag_run.conf['supervisorusername']),
            yes_task='search_supervisor_in_replicon',
            no_task='log_user_completion'
        )

        process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
            'add_new_user', 'new_user')


        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_add_user_message,
            severity=request_payload.get_add_user_severity,
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
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
                "loginname": "{{dag_run.conf.loginname}}",
                "lastname": "{{dag_run.conf.lastname}}",
                "firstname": "{{dag_run.conf.firstname}}",
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
        can_run_batch_task >> rail.Label('No') >> add_new_user

        add_new_user >> remove_timeoff_assignments >> add_product_licences >> is_supervisor_in_feed_file >> rail.Label('No') >> log_user_completion
        is_supervisor_in_feed_file >> rail.Label('Yes') >> process_supervisor_entry
        process_supervisor_exit >> log_user_completion
        log_user_completion >> catch_and_log_errors >> log_to_sumo


        return dag

rail.for_each_instance(create_child_dag)
