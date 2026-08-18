"""
T-Systems ICT India Time Off Import - Master DAG
Imports approved time off bookings from Darwinbox JSON files via SFTP to Replicon
"""

from datetime import timedelta
from pendulum import now
import rail
from tsystems.timeoff_import_india_v1.utils import custom_methods, request_payload, response_filter

def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='T-Systems India Time Off Import - MASTER',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={'sftp_conn_id': config.sftp_conn_id}
    ) as dag:

        # Monitor SFTP for JSON files
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        log_start_time = rail.PythonOperator(
            task_id="log_start_time",
            python_callable=lambda: now(config.timezone).isoformat()
        )

        # Validate JSON file format
        is_valid_json_file = rail.IfOperator(
            task_id='is_valid_json_file',
            test='{{ result("new_file_sensor") | file_ext | lower == "json" }}',
            yes_task='download_json_file',
            no_task='send_invalid_file_format_email'
        )

        send_invalid_file_format_email = rail.EmailOperator(
            task_id='send_invalid_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon TimeOff Import for India - Invalid Format | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/invalid_file_format.html"
        )

        # Download JSON file
        download_json_file = rail.SFTPDownloadFileOperator(
            task_id='download_json_file',
            remote_filepath="{{ result('new_file_sensor') }}"
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

        parse_json_data = rail.PythonOperator(
            task_id='parse_json_data',
            python_callable=lambda: rail.load_json_artifact(
                rail.result('download_json_file')
            )
        )

        create_timeoff_collection = rail.CreateCollectionOperator(
            task_id='create_timeoff_collection',
            source=lambda: rail.result('parse_json_data')['TimeOff'],
            name='input_timeoff_data_collection',
            columns={
                'CID' : "employee_id",
                'Transaction ID' : "transaction_id",
                'Start Time' : 'booking_start_time',
                'Number of Hours' : 'duration_hours',
                'Start Date' : 'booking_start_date',
                'End Date' : 'booking_end_date',
                'Time Off Type' : 'time_off_type'       
            },
        )

        # Validate parsed data
        has_timeoff_data = rail.IfOperator(
            task_id='has_timeoff_data',
            test='{{ result("create_timeoff_collection","length") > 0}}',
            yes_task='create_master_log',
            no_task='send_empty_file_email'
        )

        create_master_log = rail.CreateLogOperator(
            task_id='create_master_log'
        )

        query_valid_timeoff_records = rail.QueryCollectionOperator(
            task_id='query_valid_timeoff_records',
            query="""SELECT * FROM input_timeoff_data_collection 
                        WHERE
                            NULLIF(employee_id,'') IS NOT NULL AND
                            NULLIF(transaction_id,'') IS NOT NULL AND
                            NULLIF(booking_start_date,'') IS NOT NULL AND
                            NULLIF(booking_end_date,'') IS NOT NULL AND
                            NULLIF(time_off_type,'') IS NOT NULL""",
            name='valid_timeoff_records'
        )

        has_valid_timeoff_records = rail.IfOperator(
            task_id='has_valid_timeoff_records',
            test='{{ result("query_valid_timeoff_records","length") > 0 }}',
            yes_task='get_hidden_oef_value',
            no_task='no_valid_timeoff_records_present'
        )

        no_valid_timeoff_records_present = rail.EmptyOperator(
            task_id='no_valid_timeoff_records_present'
        )

        query_invalid_timeoff_records = rail.QueryCollectionOperator(
            task_id='query_invalid_timeoff_records',
            query="""SELECT * FROM input_timeoff_data_collection
                        WHERE
                            NULLIF(employee_id,'') IS NULL OR
                            NULLIF(transaction_id,'') IS NULL OR
                            NULLIF(booking_start_date,'') IS NULL OR
                            NULLIF(booking_end_date,'') IS NULL OR
                            NULLIF(time_off_type,'') IS NULL""",
            name='invalid_timeoff_records'
        )

        has_invalid_timeoff_records = rail.IfOperator(
            task_id='has_invalid_timeoff_records',
            test='{{ result("query_invalid_timeoff_records","length") > 0 }}',
            yes_task='log_invalid_timeoff_records',
            no_task='no_invalid_timeoff_records_present'
        )

        no_invalid_timeoff_records_present = rail.EmptyOperator(
            task_id='no_invalid_timeoff_records_present'
        )

        log_invalid_timeoff_records = rail.WriteLogOperator(
            task_id='log_invalid_timeoff_records',
            items='{{result("query_invalid_timeoff_records")}}',
            log = "{{result('create_master_log')}}",
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'transaction_id': item['transaction_id'],
                'action':'Validation',
                'status': 'Exception',
                "details": request_payload.get_mandatory_fields_exception_message(item)
            }

        )

        send_empty_file_email = rail.EmailOperator(
            task_id='send_empty_file_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon TimeOff Import for India - No records -{{ current_time_in_specified_tz() }}",
            html_content="templates/emails/empty_file.html"
        )

        get_hidden_oef_value = rail.RepliconServiceOperator(
            task_id='get_hidden_oef_value',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data= {
                    "page": "1",
                    "pagesize": "100",
                    "columnUris": [
                        "urn:replicon:object-extension-tag-definition-list-column:name",
                        "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
                    ],
                    "sort": [],
                    "filterExpression": None
                },
            data_handler=response_filter.get_hidden_oef_value
        )

        # Get Replicon time off types
        get_replicon_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_replicon_timeoff_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=lambda response: list(map(lambda row: row['uri'], response))
        )

        get_timeoff_type_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_type_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=lambda: {"timeOffTypeUris": rail.result('get_replicon_timeoff_types')},
            data_handler=response_filter.get_filtered_timeoff_details
        )

        # Process valid records in parallel
        process_timeoff_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff_records',
            items= '{{ result("query_valid_timeoff_records") }}',
            trigger_dag_id=config.process_timeoff_child_dag_id,
            conf=lambda item: {
                **item,
                'timeoff_type_detail': custom_methods.get_required_timeoff_details(item, config.TIMEOFF_TYPE_MAPPER),
                'hidden_oef_value': rail.result('get_hidden_oef_value')['hidden_oef_value']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_timeoff_processing = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_processing',
            dag_runs='{{ result("process_timeoff_records") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Gather logs from child DAGs
        gather_timeoff_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_logs',
            dag_runs='{{ result("process_timeoff_records") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(hours=config.gather_timeoff_logs_timeout_hours),
            flatten=True
        )

        # Format logs for output
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        # Generate CSV log file
        generate_csv_log = rail.WriteCSVFileOperator(
            task_id='generate_csv_log',
            source=lambda: rail.result('format_logs'),
            header=[
                'CID',
                'Transaction ID',
                'Action',
                'Status',
                'ProcessInfo',
                'Job ID'
            ],
            row=[
                '{{ item.employee_id }}',
                '{{ item.transaction_id }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.job_id }}'
            ],
            footer=[
                'Total Records: {{ result("format_logs") | length }}',
                'Successful: {{ result("format_logs", key="success_record_count") }}',
                'Errors: {{ result("format_logs", key="error_record_count") }}',
                'Exceptions: {{ result("format_logs", key="exception_record_count") }}'
            ]
        )

        # Get email details for notifications
        get_email_details = rail.PythonOperator(
            task_id='get_email_details',
            python_callable=lambda : custom_methods.get_email_details(config.timezone, config.log_filepath)
        )

        # Generate download link for log file
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('generate_csv_log') }}",
            output_file_name="{{ result('get_email_details').log_file_name }}",
            expires_in_seconds=7*24*60*60
        )

        # Upload log to SFTP
        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('generate_csv_log') }}",
            remote_filepath=config.log_filepath + "/{{ result('get_email_details').log_file_name }}"
        )

        # Send completion email
        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject="""{{ get_company_key() + ' | Replicon TimeOff Import for India - '}} \
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
            html_content="templates/emails/import_complete.html"
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

        # Task dependencies
        new_file_sensor >> log_start_time >> is_valid_json_file
        
        # Invalid file path
        is_valid_json_file >> rail.Label("No") >> send_invalid_file_format_email
        
        # Valid file processing path
        is_valid_json_file >> rail.Label("Yes") >> download_json_file >> parse_json_data >> create_timeoff_collection
        download_json_file >> was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        create_timeoff_collection >> has_timeoff_data
        
        # Empty file path
        has_timeoff_data >> rail.Label("No") >> send_empty_file_email
        
        # Data processing path
        has_timeoff_data >> rail.Label("Yes") >> create_master_log >> [query_valid_timeoff_records, query_invalid_timeoff_records]
        query_invalid_timeoff_records >> has_invalid_timeoff_records >> rail.Label("No") >> no_invalid_timeoff_records_present >> format_logs
        has_invalid_timeoff_records >> rail.Label("Yes") >> log_invalid_timeoff_records >> format_logs
        query_valid_timeoff_records >> has_valid_timeoff_records >> rail.Label("No") >> no_valid_timeoff_records_present >> format_logs
        has_valid_timeoff_records >> rail.Label("Yes") >>  get_hidden_oef_value >> get_replicon_timeoff_types >> get_timeoff_type_details
        
        # Processing and completion
        get_timeoff_type_details >> process_timeoff_records >> wait_for_timeoff_processing >> gather_timeoff_logs
        gather_timeoff_logs >> format_logs >> generate_csv_log >> get_email_details
        get_email_details >> [generate_download_link, upload_log_to_sftp] >> send_completion_email
        
        # Final steps
        send_completion_email >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag

rail.for_each_instance(create_master_dag)