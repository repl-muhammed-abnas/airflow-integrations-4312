# pylint: disable=unnecessary-lambda line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from cie_darkmattertechnologies.ts_submit_utility.utils import request_payload, data_formatting


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_process_timesheet_chunk_{location}child_v2{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}process_timesheet_chunk_child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=request_payload.get_environment_variables(config).get("max_child_run", 3),
    ) as dag:

        # rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        config_params = request_payload.get_environment_variables(config)
        execution_timeout_days = config_params.get("execution_timeout_days", 14)

        create_log = rail.CreateLogOperator(
            task_id='create_log'
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
                days=execution_timeout_days),
        )

        create_timesheet_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_batch',
            endpoint='/services/TimesheetApprovalService1.svc/CreateSubmitBatch2',
            data=request_payload.get_timesheet_submit_batch,
            response_filter=lambda response: response.json()['d']
        )

        submit_timesheet = rail.RepliconServiceOperator(
            task_id="submit_timesheet",
            endpoint="/services/TimesheetApprovalService1.svc/ExecuteTimesheetApprovalBatch2",
            data=request_payload.execute_batch_timesheet_data(
                "{{ result('create_timesheet_batch') }}"),
            execution_timeout=timedelta(days=execution_timeout_days),
            retries=0,
        )

        create_timesheet_batch_retries = rail.RepliconServiceOperator(
            task_id='create_timesheet_batch_retries',
            trigger_rule='one_failed',
            endpoint='/services/TimesheetApprovalService1.svc/CreateSubmitBatch2',
            data=request_payload.get_timesheet_submit_batch,
            response_filter=lambda response: response.json()['d']
        )

        submit_timesheet_retries = rail.RepliconServiceOperator(
            task_id="submit_timesheet_retries",
            endpoint="/services/TimesheetApprovalService1.svc/ExecuteTimesheetApprovalBatch2",
            data=request_payload.execute_batch_timesheet_data(
                "{{ result('create_timesheet_batch_retries') }}"),
            execution_timeout=timedelta(days=execution_timeout_days),
            retries=0,
        )

        get_ts_success_or_fail_count = rail.RepliconServiceOperator(
            task_id='get_ts_success_or_fail_count',
            trigger_rule='all_done',
            endpoint='/services/TimesheetService1.svc/BulkGetTimesheetDetails',
            data=request_payload.get_processed_ts_uri,
            response_filter=lambda response: data_formatting.filter_success_fail_ts_count(
                response)
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        ts_approval_logs_entry = rail.WriteLogOperator(
            task_id='ts_approval_logs_entry',
            log="{{ result('create_log') }}",
            severity="success",
            message="time entry approval completed",
            properties=data_formatting.filter_ts_log_properties
        )

        create_log >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_timesheet_batch
        create_timesheet_batch >> submit_timesheet >> create_timesheet_batch_retries >> submit_timesheet_retries >> get_ts_success_or_fail_count >> finish >> log_to_sumo >> ts_approval_logs_entry

    return dag


rail.for_each_instance(create_child_dag_wbs)
