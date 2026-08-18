from datetime import timedelta
from pendulum import now
import rail

from tsystems.activity_type_import.utils import custom_methods

def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='T-Systems Activity Type Import - MASTER',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={'sftp_conn_id': config.sftp_conn_id}
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        log_start_time = rail.PythonOperator(
            task_id = "log_start_time",
            python_callable=lambda: now(config.timezone).isoformat()
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_csv_file',
            no_task='send_bad_file_format_email',
        )

        download_csv_file = rail.SFTPDownloadFileOperator(
            task_id='download_csv_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            subject="{{ get_company_key() }} | Replicon Activity Type Import - Invalid Format | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/invalid_format_email.html"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
            yes_task='archive_file'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')


        # Archive original file
        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=f"{config.archive_filepath}/{{{{ dag_run_ecid() | replace(':', '-')}}}}_{{{{ result('new_file_sensor') | file_name }}}}"
        )

        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document="{{ result('download_csv_file') }}",
            delimiter=";",
            encoding="utf-8-sig"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_csv_data') }}",
            name='employee_records',
            columns={
                'Employee ID': 'employee_id',
                'Activity Type': 'activity_type',
                'Valid-from date for Activity Type': 'effective_date_for_activity_type',
                'Cost Rate': 'cost_rate',
                'Valid-from date for Cost Rate': 'effective_date_for_cost_rate',
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_master_log',
            no_task='send_blank_file_email',
        )

        create_master_log = rail.CreateLogOperator(
            task_id='create_master_log'
        )

        send_blank_file_email = rail.EmailOperator(
            task_id='send_blank_file_email',
            to=config.tenant_email,
            subject="{{ get_company_key() }} | Replicon Activity Type Import - No records | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/blank_feed_file_email.html",
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            name='valid_records',
            query='''SELECT * FROM employee_records WHERE NULLIF(employee_id, '') IS NOT NULL AND (
                    NUllIF(activity_type, '') IS NOT NUll OR NUllIF(effective_date_for_activity_type, '') IS NOT NUll OR
                    NUllIF(cost_rate, '') IS NOT NUll OR NUllIF(effective_date_for_cost_rate, '') IS NOT NUll)'''
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            name='invalid_records',
            query='''SELECT * FROM employee_records WHERE NULLIF(employee_id, '') IS NULL OR (
                    NUllIF(activity_type, '') IS NUll AND NUllIF(effective_date_for_activity_type, '') IS NUll AND
                    NUllIF(cost_rate, '') IS NUll AND NUllIF(effective_date_for_cost_rate, '') IS NUll)'''
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{result('create_master_log')}}",
            items='{{ result("query_invalid_records") }}',
            message=lambda item: custom_methods.get_invalid_records_msg(item),
            severity='Exception',
            properties=lambda item:{
                'employee_id': item['employee_id'],
                'action': 'Validation',
                'status': 'Exception',
                'details': custom_methods.get_invalid_records_msg(item)
            }
        )

        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test='{{ result("query_valid_records","length") > 0  }}',
            yes_task='get_all_currencies_in_replicon',
            no_task='format_logs'
        )

        get_all_currencies_in_replicon = rail.RepliconServiceOperator(
            task_id='get_all_currencies_in_replicon',
            endpoint='/services/CurrencyService2.svc/GetAllCurrencies',  
        )

        query_distinct_divisions = rail.QueryCollectionOperator(
            task_id='query_distinct_divisions',
            name='distinct_divisions',
            query="""SELECT DISTINCT activity_type FROM valid_records WHERE NULLIF(activity_type, '') IS NOT NULL"""
        )

        get_all_divisions_details = rail.RepliconServiceOperator(
            task_id='get_all_divisions_details',
            endpoint='/services/DivisionService1.svc/GetAllDivisions',
        )

        create_replicon_division_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_division_collection',
            columns=['displayText', 'parameterCorrelationId', 'slug', 'uri'],
            name="replicon_divisions",
            source="{{ result('get_all_divisions_details') | to_json }}",
        )

        query_divisions_to_create = rail.QueryCollectionOperator(
            task_id='query_divisions_to_create',
            query="""SELECT DISTINCT activity_type FROM valid_records where LOWER(activity_type) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_divisions) and NULLIF(activity_type, '') IS NOT NULL""",
            name='new_divisions'
        )

        has_new_divisions = rail.IfOperator(
            task_id='has_new_divisions',
            test="{{ result('query_divisions_to_create','length') > 0 }}",
            yes_task='process_new_divisions',
            no_task='process_each_records'
        )

        process_new_divisions = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_divisions',
            items=lambda: rail.result('query_divisions_to_create'),
            trigger_dag_id=config.process_new_division_dagid,
            conf={
                "division_name": "{{ item.activity_type }}",
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_process_new_divisions = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_divisions",
            dag_runs="{{result('process_new_divisions')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_each_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_records',
            items="{{ result('query_valid_records') }}",
            trigger_dag_id=config.process_each_record_dagid,
            conf=lambda item: {
                **item,
                "integration_run_date": now().format("DD.MM.YYYY"),
                "cost_rate_amount": ((item['cost_rate']).replace(",",".")).split(' ')[0] if item['cost_rate'] else None,
                "currency_symbol": item['cost_rate'].split(' ')[1] if item['cost_rate'] else None,
                "currency_symbol_available_in_replicon":('Yes' if rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies_in_replicon'),
                    'symbol', item['cost_rate'].split(' ')[1]) else 'No' ) if item['cost_rate'] else 'No'
                }
        )

        wait_for_process_each_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_records',
            dag_runs='{{ result("process_each_records") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs='{{ result("process_each_records") }}',
            dagrun_task_id='create_log',
            flatten=True
        )
        
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        generate_csv_file = rail.WriteCSVFileOperator(
            task_id='generate_csv_file',
            source=lambda: rail.result('format_logs'),
            header=[
                'Employee ID',
                'Action',
                'Status',
                'ProcessInfo',
                'Jobid'
            ],
            row=[   '{{ item.employee_id }}',
                    '{{ item.action }}',
                    '{{ item.status }}',
                    '{{ item.details }}', 
                    '{{ item.jobid }}'],

            footer=['Number of records found:{{ result("format_logs", key="total_record_count")}}',
                    'Number of records processed:'+'{{- result("format_logs", key="exception_record_count") + result("format_logs",key="error_record_count")+ \
                        result("format_logs", key="success_record_count")}}',
                    'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                    'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                    'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
                ]
        )

        get_email_details = rail.PythonOperator(
            task_id = "get_email_details",
            python_callable=lambda : custom_methods.get_email_details_callable(config.timezone,config.log_filepath)
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('generate_csv_file')}}",
            output_file_name="{{ result('get_email_details').log_file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('generate_csv_file') }}",
            remote_filepath=config.log_filepath + "/{{ result('get_email_details').log_file_name }}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0  -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject="""{{ get_company_key() + ' | Replicon Activity Type Import - '}} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ ' | '+  result('get_email_details').email_timestamp }}""",
            html_content="templates/emails/import_complete_mail.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        # Sumologic logging
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Error handling
        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
            trigger_rule='all_done'
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> log_start_time >> is_csv
        is_csv >> rail.Label("No") >> send_bad_file_format_email
        is_csv >> rail.Label("Yes") >> download_csv_file >> was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_csv_file >> load_csv_data

        load_csv_data  >>  create_input_data_collection
        create_input_data_collection >> has_input_data >> rail.Label('Yes') >> create_master_log >> [query_valid_records, query_invalid_records]
        has_input_data >> rail.Label('No') >> send_blank_file_email
        query_invalid_records >> log_invalid_records >> format_logs
        query_valid_records >> has_valid_records
        has_valid_records >> rail.Label("No") >> format_logs
        has_valid_records >> rail.Label("Yes") >> get_all_currencies_in_replicon >> query_distinct_divisions >> get_all_divisions_details >> create_replicon_division_collection >> query_divisions_to_create >> has_new_divisions
        has_new_divisions >> rail.Label("Yes") >> process_new_divisions >> wait_process_new_divisions >> process_each_records
        has_new_divisions >> rail.Label("No") >> process_each_records
        process_each_records >> wait_for_process_each_records >> gather_logs
        gather_logs >> format_logs >> generate_csv_file >> get_email_details >> generate_download_link >> upload_log_to_sftp >> send_import_complete_email

        send_import_complete_email >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag

rail.for_each_instance(create_master_dag)
