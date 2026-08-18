from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
import rail
from wipro.auto_shift_assignment.monthly_assignment_v1.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_auto,
        description=f"Wipro Auto Shift Assignment For New Users Batch Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child_2
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_shift_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_shift_log',
            end_task='catch_and_log_errors',
        )

        create_shift_log = rail.CreateLogOperator(
            task_id='create_shift_log'
        )

        get_all_records = rail.PythonOperator(
            task_id='get_all_records',
            python_callable=request_payload.get_details
        )

        create_auto_shift_assigment_batch = rail.RepliconServiceOperator(
            task_id="create_auto_shift_assigment_batch",
            endpoint="/services/ShiftAssignmentService1.svc/CreateShiftAssignmentBatch",
            data=lambda dag_run: request_payload.get_shift_details(
                config, dag_run)
        )

        execute_shift_download_batch, wait_for_shift_download_batch = rail.batch_execution(
            'execute_shift_download_batch', create_auto_shift_assigment_batch.task_id)

        auto_shift_assignment_success = rail.WriteLogOperator(
            task_id='auto_shift_assignment_success',
            message="Auto Shift Assignment completed",
            log='{{ result("create_shift_log") }}',
            items=lambda dag_run: request_payload.check_and_get_items(dag_run),
            severity='Success',
            properties=lambda item: {
                'username': item['user_name'],
                'employeeid': item['employee_id'],
                'status': 'Success',
                'country': item['country'],
                'schedule': item['schedule']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_shift_log") }}',
            severity='Error',
            items=lambda dag_run: request_payload.check_and_get_items(dag_run),
            message='{{ get_error_message() }}',
            properties=lambda item: {
                'username': item['user_name'],
                'employeeid': item['employee_id'],
                'status': 'Error',
                'country': item['country'],
                'schedule': item['schedule']
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> create_shift_log

        create_shift_log >>\
        get_all_records >> create_auto_shift_assigment_batch >> \
        execute_shift_download_batch >> wait_for_shift_download_batch >> auto_shift_assignment_success >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
