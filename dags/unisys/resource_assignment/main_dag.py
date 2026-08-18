from datetime import timedelta
import pendulum
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split
from airflow.models import Variable
from unisys.resource_assignment.utils import custom_method


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Unisys Resource Assignment - File-based CSV Processing',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date= pendulum.datetime(2025, 9, 1, tz=config.time_zone),
        max_active_runs=config.master_max_active_run,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        # ========== SFTP File Processing ==========
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        is_pgp = rail.IfOperator(
            task_id='is_pgp',
            test="{{result('new_file_sensor') | file_ext | lower == 'pgp' }}",
            yes_task='validate_file_name',
            no_task='send_bad_file_format_email'
        )

        validate_file_name = rail.PythonOperator(
            task_id='validate_file_name',
            python_callable=lambda: custom_method.validate_assignment_file_name(
                rail.result('new_file_sensor'),
                config.file_name_prefix
            )
        )

        is_valid_file_name = rail.IfOperator(
            task_id='is_valid_file_name',
            test=lambda: rail.result('validate_file_name')['is_valid'],
            yes_task='download_file',
            no_task='delete_this_dagrun_non_resource_file'
        )

        delete_this_dagrun_non_resource_file = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun_non_resource_file'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Resource Assignment - File processing is skipped on {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/bad_file_format.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='false').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        dummy_load_data = rail.PythonOperator(
            task_id="dummy_load_data",
            python_callable=lambda: rail.result('decrypt_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='false').lower() == 'true' else rail.result('download_file'),
            show_return_value_in_logs=False
        )

        load_assignment_data = rail.LoadCSVFileOperator(
            task_id='load_assignment_data',
            document="{{ result('dummy_load_data') }}",
            delimiter=','
        )

        # ========== Data Collection Creation ==========
        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_assignment_data') }}",
            name='inputdata',
            columns={
                "Worker Number": "workernumber",
                "Project Number": "projectnumber",
                "Assignment Start Date": "assignmentstartdate",
                "Assignment End Date": "assignmentenddate"
            }
        )

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log'
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_from_csv', 'length') > 0 }}",
            yes_task='query_blank_mandatory_check',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Resource Assignment - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        # ========== Data Validation ==========
        query_blank_mandatory_check = rail.QueryCollectionOperator(
            task_id='query_blank_mandatory_check',
            query="""SELECT * FROM inputdata WHERE
                NULLIF(workernumber,'') IS NULL OR
                NULLIF(projectnumber,'') IS NULL OR
                NULLIF(assignmentstartdate,'') IS NULL"""
        )

        has_blank_mandatory_fields = rail.IfOperator(
            task_id='has_blank_mandatory_fields',
            test="{{ result('query_blank_mandatory_check', 'length') > 0 }}",
            yes_task='write_blank_mandatory_log',
            no_task='query_valid_data'
        )

        write_blank_mandatory_log = rail.WriteLogOperator(
            task_id="write_blank_mandatory_log",
            items="{{ result('query_blank_mandatory_check') }}",
            log="{{ result('create_exception_log') }}",
            severity="Exception",
            message="Mandatory field(s) missing",
            properties=custom_method.get_invalid_logs_property_conf
        )

        # Query valid data for processing
        query_valid_data = rail.QueryCollectionOperator(
            task_id='query_valid_data',
            name='validdata',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM inputdata WHERE
                NULLIF(workernumber,'') IS NOT NULL AND
                NULLIF(projectnumber,'') IS NOT NULL AND
                NULLIF(assignmentstartdate,'') IS NOT NULL"""
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test="{{ result('query_valid_data', 'length') > 0 }}",
            yes_task='get_user_report_details',
            no_task='format_logs'
        )

        # ========== User Validation via Report ==========
        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.user_base_report_name
        )

        generate_user_report = rail.run_report2(
            group_id="generate_user_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_user_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id
        )

        load_user_report_data = rail.LoadCSVFileOperator(
            task_id='load_user_report_data',
            document="{{ result('generate_user_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_report_collection = rail.CreateCollectionOperator(
            task_id="create_user_report_collection",
            source="{{ result('load_user_report_data') }}",
            name="user_report_collection",
            columns= {
                'Employee ID': 'Employee_ID',
                'UserUri': 'UserUri',
                'User Start Date': 'User_Start_Date',
                'User End Date': 'User_End_Date'
            }
        )

        # Validate users exist and are active
        query_invalid_users = rail.QueryCollectionOperator(
            task_id="query_invalid_users",
            query="""SELECT * FROM validdata WHERE workernumber NOT IN
                (SELECT Employee_ID FROM user_report_collection)""",
            name="invalid_users"
        )

        has_invalid_users = rail.IfOperator(
            task_id="has_invalid_users",
            test="{{ result('query_invalid_users', 'length') > 0 }}",
            yes_task="log_invalid_users",
            no_task="query_valid_user_records"
        )

        log_invalid_users = rail.WriteLogOperator(
            task_id="log_invalid_users",
            items="{{result('query_invalid_users')}}",
            log="{{result('create_exception_log')}}",
            severity="Exception",
            message="User not available or inactive in Replicon",
            properties=custom_method.get_user_not_found_log_properties
        )

        query_valid_user_records = rail.QueryCollectionOperator(
            task_id="query_valid_user_records",
            query="""SELECT v.*, u.UserUri as user_uri, u.User_Start_Date as userstartdate, u.User_End_Date as userenddate FROM validdata v
                INNER JOIN user_report_collection u ON v.workernumber = u.Employee_ID""",
            name="valid_user_records"
        )

        # ========== Assignment Processing ==========
        query_distinct_assignments = rail.QueryCollectionOperator(
            task_id='query_distinct_assignments',
            name='distinctassignments',
            query="""SELECT DISTINCT projectnumber, record_id FROM valid_user_records GROUP BY projectnumber"""
        )

        def get_process_assignments_trigger_id(item):
            try:
                modulo = int(item['record_id']) % config.ASSIGNMENT_BATCH_COUNT
            except (ValueError, KeyError, TypeError):
                # Fallback to base DAG if record_id is invalid or missing
                return config.process_assignment_dag_id

            if modulo == 0:
                return config.process_assignment_dag_id
            return f"{config.process_assignment_dag_id}_batch_{str(modulo)}"

        process_assignments = rail.trigger_parallel_dagrun(
            task_id='process_assignments',
            items='{{ result("query_distinct_assignments") }}',
            parallel_count=config.parallel_count,
            trigger_dag_id=get_process_assignments_trigger_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'projectnumber': item['projectnumber'],
                'log': rail.result("create_exception_log")
            }
        )

        # ========== Log Formatting and Output ==========
        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_method.format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{ result('format_logs') }}",
            header=[
                'workernumber',
                'projectnumber',
                'action',
                'status',
                'details',
                'jobid'
            ],
            row= [
                "{{ item.properties.workernumber }}",
                "{{ item.properties.projectnumber }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=lambda dag_run: f'log_{get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0]}.csv'
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('create_csv_log') }}",
            remote_filepath=config.sftp_log_path + "/{{ result('get_log_file_name') }}",
        )

        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Resource Assignment " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html",
            params={
                'log_filepath': config.sftp_log_path
            }
        )

        # ========== Task Dependencies ==========
        new_file_sensor >> is_pgp
        is_pgp >> rail.Label("Yes") >> validate_file_name >> is_valid_file_name
        is_pgp >> rail.Label("No") >> send_bad_file_format_email

        is_valid_file_name >> rail.Label("Yes") >> download_file >> archive_file >> can_decrypt_file
        is_valid_file_name >> rail.Label("No") >> delete_this_dagrun_non_resource_file

        can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> dummy_load_data >> was_new_file_found
        can_decrypt_file >> rail.Label("No") >> dummy_load_data

        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        dummy_load_data >> load_assignment_data >> create_collection_from_csv
        create_collection_from_csv >> create_exception_log >> has_collection_data

        has_collection_data >> rail.Label("Yes") >> query_blank_mandatory_check
        has_collection_data >> rail.Label("No") >> send_blank_payload_email

        query_blank_mandatory_check >> has_blank_mandatory_fields
        has_blank_mandatory_fields >> rail.Label("Yes") >> write_blank_mandatory_log >> query_valid_data
        has_blank_mandatory_fields >> rail.Label("No") >> query_valid_data

        query_valid_data >> has_valid_data
        has_valid_data >> rail.Label("Yes") >> get_user_report_details >> generate_user_report
        has_valid_data >> rail.Label("No") >> format_logs

        generate_user_report >> load_user_report_data >> create_user_report_collection >> query_invalid_users >> has_invalid_users

        has_invalid_users >> rail.Label("Yes") >> log_invalid_users >> query_valid_user_records
        has_invalid_users >> rail.Label("No") >> query_valid_user_records

        query_valid_user_records >> query_distinct_assignments >> process_assignments >> format_logs

        format_logs >> create_csv_log >> get_log_file_name >> upload_log_to_sftp >> send_completion_email

    return dag


rail.for_each_instance(create_main_dag)
