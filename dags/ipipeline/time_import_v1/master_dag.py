"""
iPipeline JIRA-Replicon Time Sync - Master DAG
Orchestrates the complete workflow for syncing time entries from JIRA/Tempo to Replicon
"""

from datetime import timedelta
from pendulum import datetime as dt, now
from airflow.models import Variable
import rail
import json

from ipipeline.time_import_v1.utils import custom_methods, request_payload

null = None
OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"iPipeline JIRA Time Import Master {config.instance}",
        start_date=dt(2025, 10, 1, tz=config.time_zone),
        company_key=config.company_key,
        schedule_interval=config.master_dag_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        log_job_start_time = rail.PythonOperator(
            task_id='log_job_start_time',
            python_callable=lambda: now(
                config.time_zone).strftime(config.EMAIL_TIMESTAMP_FORMAT)
        )

        log_required_timestamps = rail.PythonOperator(
            task_id='log_required_timestamps',
            python_callable=lambda: custom_methods.get_required_timestamps(
                config)
        )

        # TASK 1: Extract time entries from Tempo API v4
        extract_tempo_time_entries = rail.SimpleHttpOperator(
            task_id='extract_tempo_time_entries',
            http_conn_id=config.tempo_conn_id,
            method='GET',
            # endpoint='https://api.tempo.io/4/worklogs',
            headers={
                'Authorization': f'Bearer {OPEN_BRACKETS}var.value.{config.tempo_bearer_token_var}{CLOSE_BRACKETS}',
                'Accept': 'application/json'
            },
            data={
                'updatedFrom': '{{ result("log_required_timestamps").tempo_lookback_timestamp }}',
                'limit': 1000
            }
        )

        transform_tempo_response = rail.PythonOperator(
            task_id='transform_tempo_response',
            python_callable=custom_methods.transform_tempo_api_response
        )

        set_lookback_timestamp = rail.PythonOperator(
            task_id='set_lookback_timestamp',
            python_callable=lambda: Variable.set(
                config.tempo_time_entries_lookback_date, now(config.tempo_time_zone).strftime(config.TEMPO_DATE_FORMAT))
        )

        if_records_exist = rail.IfOperator(
            task_id='if_records_exist',
            test=lambda: len(rail.result('transform_tempo_response')) > 0,
            yes_task="create_tempo_time_entries_collection",
            no_task="send_mail_no_record_to_process",
        )

        send_mail_no_record_to_process = rail.EmailOperator(
            task_id='send_mail_no_record_to_process',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Import from JIRA - No records to process - '
            + '{{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/emails/no_records_to_process.html",
        )

        create_tempo_time_entries_collection = rail.CreateCollectionOperator(
            task_id='create_tempo_time_entries_collection',
            source=lambda: rail.result("transform_tempo_response"),
            name='tempo_time_entries'
        )

        create_master_log = rail.CreateLogOperator(
            task_id='create_master_log'
        )

        query_distinct_issue_ids = rail.QueryCollectionOperator(
            task_id='query_distinct_issue_ids',
            query=f"""SELECT DISTINCT task_jira_issue_id
                FROM tempo_time_entries""",
            name='distinct_issue_ids'
        )

        trigger_jira_metadata_retrieval = rail.trigger_parallel_dagrun(
            task_id="trigger_jira_metadata_retrieval",
            trigger_dag_id=config.process_jira_project_info_child_dag_id,
            parallel_count=config.parallel_count_process_jira_issue_ids,
            items=lambda: rail.result("query_distinct_issue_ids"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'task_issue_id': item['task_jira_issue_id']
            }
        )

        get_task_metadata_retrieval_dag_ids = rail.PythonOperator(
            task_id='get_task_metadata_retrieval_dag_ids',
            python_callable=lambda: custom_methods.get_process_task_metadata_retrieval(
                config.parallel_count_process_jira_issue_ids),
            show_return_value_in_logs=False
        )

        gather_task_metadata_retrieval_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_task_metadata_retrieval_logs',
            dag_runs='{{ result("get_task_metadata_retrieval_dag_ids") }}',
            dagrun_task_id='create_task_log',
            execution_timeout=timedelta(
                hours=config.gather_task_metadata_retrieval_logs_timeout_hours),
            flatten=True
        )

        get_task_metadata_retrieval_records = rail.PythonOperator(
            task_id='get_task_metadata_retrieval_records',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: custom_methods.format_task_metadata_logs(
                rail.result('gather_task_metadata_retrieval_logs')),
            show_return_value_in_logs=False
        )

        create_collection_task_metadata = rail.CreateCollectionOperator(
            task_id='create_collection_task_metadata',
            source=lambda: rail.result('get_task_metadata_retrieval_records'),
            name='task_metadata_collection'
        )

        query_tempo_time_entries_join_task_metadata = rail.QueryCollectionOperator(
            task_id='query_tempo_time_entries_join_task_metadata',
            query="""SELECT *
                FROM
                    tempo_time_entries 
                LEFT JOIN
                    task_metadata_collection ON tempo_time_entries.task_jira_issue_id = task_metadata_collection.task_issue_id""",
            name='tempo_entries_with_task_metadata'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name="invalid_records",
            query=f"""SELECT * FROM tempo_entries_with_task_metadata WHERE NULLIF(task_issue_id, '') IS NULL
                or  NULLIF(time_entry_date, '') IS NULL or NULLIF(time_entry_comment, '') IS NULL
                or NULLIF(author_jira_account_id, '') IS NULL or NULLIF(task_type, '') IS NULL
                or NULLIF(replicon_id, '') IS NULL or NULLIF(task_parent_issue_id, '') IS NULL
                or NULLIF(task_summary, '') IS NULL or NULLIF(hours, '') IS NULL"""
        )

        if_invalid_records = rail.IfOperator(
            task_id='if_invalid_records',
            test='''{{ result('query_invalid_records', 'length') > 0 }}''',
            yes_task="log_invalid_records",
            no_task="query_valid_records",
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_master_log')}}",
            message='Validation exception',
            severity='Exception',
            properties=lambda item: {
                'task_issue_id': item['task_issue_id'],
                'task_type': item['task_type'],
                'time_entry_date': item['time_entry_date'],
                'hours': item['hours'],
                'replicon_id': item['replicon_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": custom_methods.get_validation_details(item, config)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name="valid_records",
            query=f"""SELECT * FROM tempo_entries_with_task_metadata WHERE NULLIF(task_issue_id, '') IS NOT NULL
                and  NULLIF(time_entry_date, '') IS NOT NULL and NULLIF(time_entry_comment, '') IS NOT NULL
                and NULLIF(author_jira_account_id, '') IS NOT NULL and NULLIF(task_type, '') IS NOT NULL
                and NULLIF(replicon_id, '') IS NOT NULL and NULLIF(task_parent_issue_id, '') IS NOT NULL
                and NULLIF(task_summary, '') IS NOT NULL and NULLIF(hours, '') IS NOT NULL"""
        )

        get_all_projects_in_replicon = rail.RepliconServicePageOperator(
            task_id='get_all_projects_in_replicon',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=request_payload.payload_to_get_all_replicon_projects,
            page_handler=custom_methods.page_handler,
            all_result_data_handler=custom_methods.filter_project_data
        )

        create_replicon_project_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_project_collection',
            source=lambda: rail.result('get_all_projects_in_replicon'),
            name='replicon_projects'
        )

        query_tempo_time_entries_join_replicon_project_details = rail.QueryCollectionOperator(
            task_id='query_tempo_time_entries_join_replicon_project_details',
            query="""SELECT *
                FROM
                    valid_records 
                LEFT JOIN
                    replicon_projects ON valid_records.replicon_id = replicon_projects.project_code""",
            name='final_valid_records_to_process'
        )

        query_distinct_user_in_records_to_process = rail.QueryCollectionOperator(
            task_id='query_distinct_user_in_records_to_process',
            query=f"""SELECT DISTINCT author_jira_account_id
                FROM final_valid_records_to_process""",
            name='distinct_author_ids'
        )

        trigger_time_entries_creation_group_by_user = rail.trigger_parallel_dagrun(
            task_id="trigger_time_entries_creation_group_by_user",
            trigger_dag_id=config.process_each_user_time_entries_child_dag_id,
            parallel_count=config.parallel_count_process_each_user_time_entries,
            items=lambda: rail.result(
                "query_distinct_user_in_records_to_process"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'author_jira_account_id': item['author_jira_account_id']
            }
        )

        get_user_time_entries_dag_ids = rail.PythonOperator(
            task_id='get_user_time_entries_dag_ids',
            python_callable=lambda: custom_methods.get_process_user_time_entries_dag_ids(
                config.parallel_count_process_each_user_time_entries),
            show_return_value_in_logs=False
        )

        gather_time_entry_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_entry_logs',
            dag_runs='{{ result("get_user_time_entries_dag_ids") }}',
            dagrun_task_id='create_user_time_entries_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_child_dag_id,
            conf=lambda: {
                'total_records': rail.result('create_tempo_time_entries_collection', key='length'),
                'master_log': rail.result('create_master_log'),
                'child_logs': rail.result('gather_time_entry_logs'),
                'job_start_time': rail.result('log_job_start_time'),
            }
        )

        wait_for_process_log_generation = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_log_generation',
            dag_runs="{{ result('process_log_generation') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        log_job_start_time >> log_required_timestamps >> extract_tempo_time_entries >> transform_tempo_response >> set_lookback_timestamp \
            >> if_records_exist

        if_records_exist >> rail.Label("No") >> send_mail_no_record_to_process
        if_records_exist >> rail.Label(
            "Yes") >> create_tempo_time_entries_collection >> create_master_log

        create_master_log >> query_distinct_issue_ids

        query_distinct_issue_ids >> trigger_jira_metadata_retrieval \
            >> get_task_metadata_retrieval_dag_ids

        get_task_metadata_retrieval_dag_ids >> gather_task_metadata_retrieval_logs >> get_task_metadata_retrieval_records \
            >> create_collection_task_metadata >> query_tempo_time_entries_join_task_metadata >> query_invalid_records >> if_invalid_records

        if_invalid_records >> rail.Label(
            "No") >> query_valid_records
        if_invalid_records >> rail.Label(
            "Yes") >> log_invalid_records >> query_valid_records

        query_valid_records >> get_all_projects_in_replicon >> create_replicon_project_collection \
            >> query_tempo_time_entries_join_replicon_project_details >> query_distinct_user_in_records_to_process

        query_distinct_user_in_records_to_process >> trigger_time_entries_creation_group_by_user \
            >> get_user_time_entries_dag_ids >> gather_time_entry_logs >> process_log_generation >> wait_for_process_log_generation

    return dag


rail.for_each_instance(create_master_dag)
