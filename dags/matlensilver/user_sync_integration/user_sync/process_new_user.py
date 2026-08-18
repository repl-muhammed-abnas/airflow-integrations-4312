from datetime import timedelta
import rail

from matlensilver.user_sync_integration.user_sync.utils import request_payload
from matlensilver.user_sync_integration.user_sync.utils import python_callable_method
from matlensilver.user_sync_integration.user_sync.tasks.process_supervisor import process_supervisor_assignment_task_group


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'matlen_silver_user_sync_child_process_new_user_{config.instance}',
        description='Matlen_Silver User Sync Process New User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        has_valid_add_fields = rail.IfOperator(
            task_id ='has_valid_add_fields',
            test = request_payload.test_valid_fields,
            yes_task="add_user_exception_log",
            no_task="log_invalid_add_fields"
        )

        log_invalid_add_fields =rail.WriteLogOperator(
            task_id = 'log_invalid_add_fields',
            message = request_payload.get_invalid_fields_message,
            severity='Exception',
            properties = lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        add_user_exception_log = rail.CreateLogOperator(
            task_id='add_user_exception_log'
        )

        add_user_error_logs = rail.CreateLogOperator(
            task_id='add_user_error_logs'
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

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: dag_run.conf['supervisorname'] and dag_run.conf['supervisorcode'],
            yes_task='is_supervisor_same_as_user',
            no_task='log_supervisor_not_in_feedfile'
        )

        log_supervisor_not_in_feedfile = rail.WriteLogOperator(
            task_id='log_supervisor_not_in_feedfile',
            log="{{ result('add_user_exception_log') }}",
            message="Supervisor details not present in feed file",
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Exception',
            },
        )

        process_supervisor_task_entry, process_supervisor_task_exit = process_supervisor_assignment_task_group(
            'add_new_user', 'new_user')

        process_time_off_assignment = rail.TriggerDagRunOperator(
            task_id='process_time_off_assignment',
            trigger_dag_id=f'matlen_silver_user_sync_child_process_time_off_assignment_new_user_{config.instance}',
            conf=lambda dag_run: request_payload.get_process_time_off_assignment_conf(
                dag_run, 'new_user'),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_time_off_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_assignment',
            dag_runs='{{ result("process_time_off_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_exception_logs = rail.PythonOperator(
            task_id='get_all_exception_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['add_user_exception_log']
        )

        get_all_error_logs = rail.PythonOperator(
            task_id='get_all_error_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['add_user_error_logs']
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message=request_payload.get_add_completion_message,
            severity=request_payload.get_add_severity,
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'firstname': dag_run.conf['firstname'],
                'lastname': dag_run.conf['lastname'],
                'status': 'Error' if rail.result('get_all_error_logs') else ('Exception' if rail.result('get_all_exception_logs') else 'Success'),
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        has_valid_add_fields >> rail.Label('No') >> log_invalid_add_fields >> catch_and_log_errors
        has_valid_add_fields >> rail.Label('Yes') >> add_user_exception_log
        add_user_exception_log >> add_user_error_logs >> add_new_user >> remove_timeoff_assignments >> is_supervisor_in_feed_file >> rail.Label(
            'Yes') >> process_supervisor_task_entry
        is_supervisor_in_feed_file >> rail.Label(
            'No') >> log_supervisor_not_in_feedfile >> process_time_off_assignment
        process_supervisor_task_exit >> process_time_off_assignment
        process_time_off_assignment >> wait_for_process_time_off_assignment >> [
            get_all_exception_logs, get_all_error_logs] >> log_completion >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag_wbs)
