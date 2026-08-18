# pylint: disable=unnecessary-lambda line-too-long
from datetime import timedelta
from airflow.models import Variable
import rail
from cie_infosys.ts_approval_utility_v2.utils import request_payload, data_formatting


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_process_entry_chunk_{location}child_v2{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}infosys_process_entry_chunk_child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_run,
    ) as dag:

        # rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='validate_timeentry_batch'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='validate_timeentry_batch',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        validate_timeentry_batch = rail.RepliconServiceOperator(
            task_id='validate_timeentry_batch',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/BulkGetTimeEntryRevisionGroupApprovalDetails',
            data=request_payload.validate_batch_entry_payload,
            response_filter=lambda response: data_formatting.filter_deleted_uris(
                response)
        )

        create_timeentry_batch = rail.RepliconServiceOperator(
            task_id='create_timeentry_batch',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/CreateForcedApproveBatch',
            data=request_payload.create_batch_entry_payload,
            response_filter=lambda response: response.json()['d']
        )

        approve_timeentry = rail.RepliconServiceOperator(
            task_id="approve_timeentry",
            endpoint="/services/BatchManagementService1.svc/ExecuteInBackground",
            data=request_payload.execute_batch_entry_data(
                "{{ result('create_timeentry_batch') }}"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        create_timeentry_batch_retries = rail.RepliconServiceOperator(
            task_id='create_timeentry_batch_retries',
            trigger_rule='one_failed',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/CreateForcedApproveBatch',
            data=request_payload.create_batch_entry_payload,
            response_filter=lambda response: response.json()['d']
        )

        approve_timeentry_retries = rail.RepliconServiceOperator(
            task_id="approve_timeentry_retries",
            endpoint="/services/BatchManagementService1.svc/ExecuteInBackground",
            data=request_payload.execute_batch_entry_data(
                "{{ result('create_timeentry_batch_retries') }}"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        get_success_or_fail_count = rail.RepliconServiceOperator(
            task_id='get_success_or_fail_count',
            trigger_rule='all_done',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/BulkGetTimeEntryRevisionGroupApprovalDetails',
            data=request_payload.get_processed_entries_uri,
            response_filter=lambda response: data_formatting.filter_success_fail_entries_count(
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

        entry_approval_logs_entry = rail.WriteLogOperator(
            task_id='entry_approval_logs_entry',
            log="{{ result('create_log') }}",
            severity="success",
            message="time entry approval completed",
            properties=data_formatting.filter_etnries_log_properties
        )

        create_log >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> validate_timeentry_batch
        validate_timeentry_batch >> create_timeentry_batch >> approve_timeentry >> create_timeentry_batch_retries >> approve_timeentry_retries >> get_success_or_fail_count >>finish >> log_to_sumo >> entry_approval_logs_entry

    return dag


rail.for_each_instance(create_child_dag_wbs)
