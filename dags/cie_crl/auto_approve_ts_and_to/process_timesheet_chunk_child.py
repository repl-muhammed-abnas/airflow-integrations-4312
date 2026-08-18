# pylint: disable=unnecessary-lambda line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from cie_crl.auto_approve_ts_and_to.utils import request_payload, python_callable_method


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_process_timesheet_chunk_{location}child{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}_process_timesheet_chunk_child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_run,
    ) as dag:

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )
        
        get_bulk_validation = rail.RepliconServiceOperator(
            task_id="get_bulk_validation",
            endpoint="/services/TimesheetService1.svc/BulkGetMostRecentValidationResults",
            data=request_payload.get_bulk_validation_payload
        )
        
        filter_validation_uris = rail.PythonOperator(
            task_id='filter_validation_uris',
            python_callable=python_callable_method.filter_error_uris,
            op_args=[config]
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_timesheet_batch'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timesheet_batch',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timesheet_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_batch',
            endpoint='/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch',
            data=request_payload.get_timehseet_approve_batch,
            response_filter=lambda response: response.json()['d']
        )

        approve_timesheet = rail.RepliconServiceOperator(
            task_id="approve_timesheet",
            endpoint="/services/TimesheetApprovalService1.svc/ExecuteTimesheetApprovalBatch2",
            data=request_payload.execute_batch_timesheet_data(
                "{{ result('create_timesheet_batch') }}"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        create_timesheet_batch_retries = rail.RepliconServiceOperator(
            task_id='create_timesheet_batch_retries',
            trigger_rule='one_failed',
            endpoint='/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch',
            data=request_payload.get_timehseet_approve_batch,
            response_filter=lambda response: response.json()['d']
        )

        approve_timesheet_retries = rail.RepliconServiceOperator(
            task_id="approve_timesheet_retries",
            endpoint="/services/TimesheetApprovalService1.svc/ExecuteTimesheetApprovalBatch2",
            data=request_payload.execute_batch_timesheet_data(
                "{{ result('create_timesheet_batch_retries') }}"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        entry_approval_logs_entry = rail.WriteLogOperator(
            task_id='entry_approval_logs_entry',
            log="{{ result('create_log') }}",
            severity="success",
            message="time sheet approval completed",
            properties=python_callable_method.filter_entries_log_properties
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )


        create_log >> get_bulk_validation >> filter_validation_uris >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_timesheet_batch
        create_timesheet_batch >> approve_timesheet >> create_timesheet_batch_retries >> approve_timesheet_retries >> finish >> log_to_sumo >> entry_approval_logs_entry

    return dag


rail.for_each_instance(create_child_dag_wbs)
