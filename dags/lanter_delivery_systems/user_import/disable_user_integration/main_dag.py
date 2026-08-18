from datetime import timedelta
import rail

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='Lanter Delivery Systems User Import - Disable User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Login Name': 'loginname',
                'Enabled': 'enabled'
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task=['query_valid_records','query_invalid_records'],
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Disable Users - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_records',
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(loginname, '') IS NOT NULL
                and loginname != 'admin' and enabled ='No'"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='get_report_details',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalid_records',
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(loginname, '') IS NULL
                    or loginname = 'admin' or enabled !='No'"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="no_invalid_records_present"
        )

        no_invalid_records_present = rail.EmptyOperator(
            task_id='no_invalid_records_present'
        )

        def get_invalid_records_exception_message(item):
            if item['enabled'] != 'No':
                return "Skipped as 'Enabled' field value is othen than 'No'"
            if item['loginname'] =='admin':
                return f"This account - {item['loginname']} is used for integration execution"
            return "Login Name not present in feed file"

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            message=get_invalid_records_exception_message,
            severity='Exception',
            properties={
                "loginname": "{{ item.loginname }}",
                "action": "Validation",
                'status': 'Exception',
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report_generation',
            report_params={
                'reportParameters': [
                    {
                        'reportUri': "{{ result('get_report_details').uri }}",
                        'filterValues': [],
                        'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ result('run_report_generation.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ result('run_report_generation.get_report_result').reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('run_report_generation.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='No Data found For Users'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report_generation.get_report_result').reportGenerationResults[0].payload }}"
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id='report_data_collection',
            source="{{ result('load_report_data') }}",
            name='users_data',
            columns={
                'Login Name': 'loginname',
                'User Uri': 'useruri',
                'User Status': 'status',
            }
        )

        query_users_data_to_skip_disable = rail.QueryCollectionOperator(
            task_id='query_users_data_to_skip_disable',
            query="SELECT * FROM valid_records WHERE loginname NOT IN (Select DISTINCT loginname from users_data)"
        )

        log_user_not_available_disabled = rail.WriteLogOperator(
            task_id="log_user_not_available_disabled",
            message="User not found/already disabled in Replicon",
            items="{{result('query_users_data_to_skip_disable')}}",
            severity='Exception',
            properties={
                "loginname": "{{ item.loginname }}",
                "action": "Validation",
                'status': 'Exception',
            }
        )

        query_users_data_to_disable = rail.QueryCollectionOperator(
            task_id='query_users_data_to_disable',
            query="SELECT * FROM users_data WHERE loginname IN (Select DISTINCT loginname from valid_records)"
        )

        process_disable_users = rail.TriggerDagRunForEachItemOperator(
            task_id="process_disable_users",
            trigger_dag_id=config.child_dagid,
            items= '{{ result("query_users_data_to_disable") }}',
            conf={
                "loginname": "{{ item.loginname }}",
                "current_status":"{{ item.status }}",
                "user_uri": "{{ item.useruri }}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_disable_users = rail.WaitForDagRunsSensor(
            task_id="wait_process_disable_users",
            dag_runs="{{ result('process_disable_users') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors', 'get_logged_exceptions', 'get_logged_success'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error'
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            severity='Exception'
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='Success'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            # pylint: disable=line-too-long
            header=[
                'Login Name',
                'Action',
                'Status',
                'Details',
                'JobID',
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                ],
            row=[
                '{{ item.properties.loginname }}',
                '{{  item.properties.action }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'
                ],
            footer=['Number of records:' + '{{- result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length")+ result("get_logged_success", key="length")}}',
                    'Number of success: {{ result("get_logged_success", "length")}}',
                    'Number of errors: {{ result("get_logged_errors", "length") }}',
                    'Number of exceptions: {{ result("get_logged_exceptions", "length") }}',
                ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Disable Users - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete_mail.html",
        )

        new_file_sensor >> download_file >> rail.Label('Always') >> was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_file

        download_file >> load_data >> create_input_data_collection >> has_input_data >> rail.Label('No') >> send_blank_payload_email

        has_input_data >> rail.Label('Yes') >> [query_valid_records, query_invalid_records]

        query_invalid_records >> has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> has_any_entries_in_log
        has_invalid_records >> rail.Label('No') >> no_invalid_records_present >> has_any_entries_in_log

        query_valid_records >> has_valid_records >> rail.Label('No') >> no_valid_records_present >> has_any_entries_in_log
        has_valid_records >> rail.Label('Yes') >> get_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed >> rail.Label('Yes') >> fail_report_generation
        is_report_failed >> rail.Label('No') >> report_has_data >> rail.Label('No') >> fail_no_report_data

        report_has_data >> rail.Label('Yes') >> load_report_data >> report_data_collection >> [query_users_data_to_skip_disable, query_users_data_to_disable]
        query_users_data_to_skip_disable >> log_user_not_available_disabled >> has_any_entries_in_log
        query_users_data_to_disable >> process_disable_users >> wait_process_disable_users >> has_any_entries_in_log

        has_any_entries_in_log >> rail.Label('No') >> fail_with_empty_log
        has_any_entries_in_log >> rail.Label('Yes') >> [get_logged_errors, get_logged_exceptions, get_logged_success] >> render_logs_csv >> upload_log_to_sftp
        upload_log_to_sftp >> generate_download_link >> send_import_complete_email

    return dag


rail.for_each_instance(create_main_dag)
