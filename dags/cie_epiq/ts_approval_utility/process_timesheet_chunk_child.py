# pylint: disable=unnecessary-lambda line-too-long
from datetime import timedelta
import rail
from cie_epiq.ts_approval_utility.utils import request_payload, data_formatting


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    run_type = f'{config.run_type}_' if config.run_type else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_process_timesheet_chunk_{run_type}child{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}infosys_process_timesheet_chunk_child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_run,
    ) as dag:

        get_chunk_timesheets = rail.PythonOperator(
            task_id="get_chunk_timesheets",
            python_callable=data_formatting.get_chunk_timesheet_uris,
        )

        filter_errors_timesheets = rail.RepliconServiceOperator(
            task_id="filter_errors_timesheets",
            endpoint='/services/TimesheetService1.svc/BulkGetMostRecentValidationResults',
            data=request_payload.get_all_ts_uris,
            response_filter=lambda response: data_formatting.get_validated_ts_uris(
                response)
        )

        has_valid_timesheets = rail.IfOperator(
            task_id='has_valid_timesheets',
            test="{{ result('filter_errors_timesheets').get('has_valid_TS') }}",
            yes_task='create_timesheet_batch',
            no_task='finish'
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

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_chunk_timesheets >> filter_errors_timesheets >> has_valid_timesheets
        has_valid_timesheets >> rail.Label('Yes') >> create_timesheet_batch >> approve_timesheet >> create_timesheet_batch_retries >> approve_timesheet_retries >> finish >> log_to_sumo
        has_valid_timesheets >> rail.Label('No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
