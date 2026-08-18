"""
CRL Office Schedule Sync - Master DAG
Orchestrates the synchronization of office schedules from SuccessFactors to Replicon
"""
from datetime import timedelta
from pendulum import now
import rail
import json
from crl.office_schedule_import_v1.utils import custom_methods


null = None


def create_dag(config):
    """Create the master DAG for office schedule synchronization"""
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'CRL office schedule import Master {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        # View the DAG run configuration
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Record job start time
        job_start_time = rail.PythonOperator(
            task_id='job_start_time',
            python_callable=lambda: now(
                config.time_zone).strftime("%Y-%m-%dT%H:%M:%S%z"),
        )

        # Check if payload contains WorkSchedule data
        is_data_available = rail.IfOperator(
            task_id='is_data_available',
            test=lambda dag_run: bool(dag_run.conf.get(
                'payload', {}).get('WorkSchedule')),
            yes_task="create_log",
            no_task="send_mail_blank_data"
        )

        # Send email notification when payload is blank
        send_mail_blank_data = rail.EmailOperator(
            task_id='send_mail_blank_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Office Schedule import from SAP - No records to process | {{current_time_in_specified_tz("' + config.time_zone + '")}}',
            html_content='''templates/emails/no_records_mail.html''',
        )

        # Create log for tracking all operations
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        # Create collection from WorkSchedule payload data
        create_schedule_collection = rail.CreateCollectionOperator(
            task_id='create_schedule_collection',
            source=lambda dag_run: json.dumps(
                dag_run.conf['payload']['WorkSchedule']),
            name='schedule_collection',
            columns={
                "ScheduleName": "schedule_name",
                "Scheduledescription": "schedule_description",
                "Pattern": "pattern",
                "StartDate": "start_date"
            }
        )

        # Check if collection has any records
        if_collection_has_records = rail.IfOperator(
            task_id='if_collection_has_records',
            test="{{ result('create_schedule_collection', 'length') > 0 }}",
            yes_task='query_invalid_records',
            no_task='send_mail_blank_data'
        )

        # Query records with validation failures (missing fields)
        # StartDate is required only for non-7-day patterns
        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query='''SELECT *
                FROM schedule_collection
                WHERE
                    NULLIF(TRIM(schedule_name), '') IS NULL
                    OR NULLIF(TRIM(pattern), '') IS NULL
                    OR (LENGTH(pattern) - LENGTH(REPLACE(pattern, '|', '')) + 1 != 7
                        AND NULLIF(TRIM(start_date), '') IS NULL)'''
        )

        # Check if there are any invalid records
        if_invalid_records = rail.IfOperator(
            task_id='if_invalid_records',
            test="{{ result('query_invalid_records', 'length') > 0 }}",
            yes_task='log_invalid_records',
            no_task='query_valid_records'
        )

        # Log all invalid records with detailed error messages
        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_log') }}",
            items="{{ result('query_invalid_records') }}",
            message='Invalid record',
            severity='Exception',
            properties=lambda item: custom_methods.get_validation_error_properties(
                item)
        )

        # Query records that passed all validation checks
        # Valid records: have schedule_name, pattern, and StartDate if non-7-day pattern
        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query='''SELECT *
                FROM schedule_collection
                WHERE
                    NULLIF(TRIM(schedule_name), '') IS NOT NULL
                    AND NULLIF(TRIM(pattern), '') IS NOT NULL
                    AND (LENGTH(pattern) - LENGTH(REPLACE(pattern, '|', '')) + 1 = 7
                        OR NULLIF(TRIM(start_date), '') IS NOT NULL)'''
        )

        # Check if there are any valid records to process
        if_valid_records = rail.IfOperator(
            task_id='if_valid_records',
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task='get_all_office_schedules',
            no_task='dummy_process_log_generation'
        )

        # Get all existing office schedules from Replicon for deduplication
        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
            data_handler=lambda res: list(map(lambda item: {
                "existing_office_schedule_name": item['displayText'],
                "existing_office_schedule_uri": item['uri']}, res)) if res else []
        )

        create_replicon_office_schedule_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_office_schedule_collection',
            source=lambda: rail.result('get_all_office_schedules'),
            name='replicon_office_schedules'
        )

        query_payload_schedules_left_join_existing_schedules = rail.QueryCollectionOperator(
            task_id='query_payload_schedules_left_join_existing_schedules',
            query="""SELECT *
                FROM
                    query_valid_records 
                LEFT JOIN
                    replicon_office_schedules ON LOWER(query_valid_records.schedule_name) = LOWER(replicon_office_schedules.existing_office_schedule_name)""",
            name='schedules_to_process'
        )

        query_existing_schedules = rail.QueryCollectionOperator(
            task_id='query_existing_schedules',
            query="""SELECT *
                FROM
                    schedules_to_process 
                WHERE
                    NULLIF(existing_office_schedule_name, '') IS NOT NULL""",
            name='existing_schedules_to_skip'
        )

        if_existing_schedules_to_skip_present = rail.IfOperator(
            task_id='if_existing_schedules_to_skip_present',
            test="{{ result('query_existing_schedules') | length > 0 }}",
            yes_task='log_existing_schedules_from_payload',
            no_task='query_final_office_schedule_to_process'
        )

        log_existing_schedules_from_payload = rail.WriteLogOperator(
            task_id='log_existing_schedules_from_payload',
            log="{{ result('create_log') }}",
            items="{{ result('query_existing_schedules') }}",
            message='Schedule already present',
            severity='Skipped',
            properties=lambda item: {
                'schedule_name': item['schedule_name'],
                'pattern': item['pattern'],
                'start_date': item.get('start_date', ''),
                'action': 'Validation',
                'status': 'Skipped',
                'details': "Office Schedule already present in replicon"
            }
        )

        query_final_office_schedule_to_process = rail.QueryCollectionOperator(
            task_id='query_final_office_schedule_to_process',
            query="""SELECT *
                FROM
                    schedules_to_process 
                WHERE
                    NULLIF(existing_office_schedule_name, '') IS NULL""",
            name='final_office_schedules_to_process'
        )

        if_final_office_schedules_to_process_present = rail.IfOperator(
            task_id='if_final_office_schedules_to_process_present',
            test="{{ result('query_final_office_schedule_to_process') | length > 0 }}",
            yes_task='dummy_trigger_create_schedule',
            no_task='dummy_process_log_generation'
        )

        dummy_trigger_create_schedule = rail.EmptyOperator(
            task_id='dummy_trigger_create_schedule',
        )

        trigger_create_schedule = rail.trigger_parallel_dagrun(
            task_id="trigger_create_schedule",
            trigger_dag_id=config.create_schedule_dag_id,
            parallel_count=config.parallel_count_process_schedules,
            items=lambda: rail.result(
                "query_final_office_schedule_to_process"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item
            }
        )

        # Collect all project processing DAG run IDs for monitoring and log gathering
        get_process_schedule_dag_ids = rail.PythonOperator(
            task_id='get_process_schedule_dag_ids',
            python_callable=lambda: custom_methods.get_process_dag_ids(
                config.parallel_count_process_schedules, 'trigger_create_schedule'),
            show_return_value_in_logs=False
        )

        # Gather logs from all child DAG runs
        gather_schedule_creation_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_schedule_creation_logs',
            dag_runs='{{ result("get_process_schedule_dag_ids") }}',
            dagrun_task_id='create_schedule_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation',
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dag_id,
            conf=lambda: {
                'total_records': rail.result('create_schedule_collection', key='length'),
                'master_log': rail.result('create_log'),
                'child_logs': rail.result('gather_schedule_creation_logs'),
                'job_start_time': rail.result('job_start_time'),
            }
        )

        # Define task dependencies

        # Initial flow
        job_start_time >> is_data_available
        is_data_available >> rail.Label('Yes') >> create_log
        is_data_available >> rail.Label('No') >> send_mail_blank_data

        # Collection and validation
        create_log >> create_schedule_collection >> if_collection_has_records
        if_collection_has_records >> rail.Label(
            'Yes') >> query_invalid_records >> if_invalid_records
        if_collection_has_records >> rail.Label('No') >> send_mail_blank_data

        # Invalid records handling
        if_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> query_valid_records
        if_invalid_records >> rail.Label('No') >> query_valid_records

        query_valid_records >> if_valid_records

        if_valid_records >> rail.Label('No') >> dummy_process_log_generation
        if_valid_records >> rail.Label('Yes') >> get_all_office_schedules

        get_all_office_schedules >> create_replicon_office_schedule_collection >> query_payload_schedules_left_join_existing_schedules \
            >> query_existing_schedules >> if_existing_schedules_to_skip_present

        if_existing_schedules_to_skip_present >> rail.Label(
            'No') >> query_final_office_schedule_to_process
        if_existing_schedules_to_skip_present >> rail.Label(
            'Yes') >> log_existing_schedules_from_payload >> query_final_office_schedule_to_process

        query_final_office_schedule_to_process >> if_final_office_schedules_to_process_present

        if_final_office_schedules_to_process_present >> rail.Label(
            'No') >> dummy_process_log_generation
        if_final_office_schedules_to_process_present >> rail.Label(
            'Yes') >> dummy_trigger_create_schedule >> trigger_create_schedule

        trigger_create_schedule >> get_process_schedule_dag_ids >> gather_schedule_creation_logs >> dummy_process_log_generation

        dummy_process_log_generation >> process_log_generation

    return dag


rail.for_each_instance(create_dag)
