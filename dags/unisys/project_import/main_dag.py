from datetime import timedelta
import pendulum
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split
from airflow.models import Variable
from unisys.project_import.utils import custom_method

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Unisys Project Import - File-based CSV Processing (Phase 1)',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        start_date= pendulum.datetime(2025, 9, 1, tz=config.time_zone),
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
            test="{{ result('new_file_sensor') | file_ext | lower == 'pgp' }}",
            yes_task='validate_file_name',
            no_task='send_bad_file_format_email'
        )

        validate_file_name = rail.PythonOperator(
            task_id='validate_file_name',
            python_callable=lambda: custom_method.validate_project_file_name(
                rail.result('new_file_sensor'),
                config.file_name_prefix
            )
        )

        is_valid_file_name = rail.IfOperator(
            task_id='is_valid_file_name',
            test=lambda: rail.result('validate_file_name')['is_valid'],
            yes_task='download_file',
            no_task='check_file_type'
        )

        check_file_type = rail.IfOperator(
            task_id='check_file_type',
            test=lambda: rail.result('validate_file_name')['is_resource_file'],
            yes_task='delete_this_dagrun_resource_file',
            no_task='archive_unknown_file'
        )

        delete_this_dagrun_resource_file = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun_resource_file'
        )

        archive_unknown_file = rail.SFTPMoveFileOperator(
            task_id='archive_unknown_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/invalid_{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        send_invalid_file_name_email = rail.EmailOperator(
            task_id='send_invalid_file_name_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Project Import - Invalid file name on {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/invalid_file_name.html'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Project Import - File processing is skipped on {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/bad_file_format.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        log_start_time = rail.PythonOperator(
            task_id="log_start_time",
            python_callable=lambda: pendulum.now(config.time_zone)
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
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            no_task = 'delete_this_dagrun'
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

        load_user_data = rail.LoadCSVFileOperator(
            task_id='load_user_data',
            document="{{ result('dummy_load_data') }}",
            delimiter=','
        )

        # ========== Data Collection Creation ==========
        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_user_data') }}",
            name='inputdata',
            columns={
                "Project Number": "projectnumber",
                "Project Name": "projectname",
                "Project Start Date": "projectstartdate",
                "Project End Date": "projectenddate",
                "Project Status": "projectstatus",
                "Task Code": "taskcode",              # Will be swapped to Task Name in Replicon
                "Task Name": "taskname",               # Will be swapped to Task Code in Replicon
                "Task Start Date": "taskstartdate",
                "Task End Date": "taskenddate",
                "Task Paycode": "taskpaycode",
                "Company Code": "companycode",
                "Project Manager": "projectmanager"
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
            subject='{{ get_company_key() }} | Replicon Project Import - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        # ========== Data Validation ==========
        query_blank_mandatory_check = rail.QueryCollectionOperator(
            task_id='query_blank_mandatory_check',
            query="""SELECT * FROM inputdata WHERE
                NULLIF(projectnumber,'') IS NULL OR
                NULLIF(projectname,'') IS NULL OR
                NULLIF(projectstartdate,'') IS NULL OR
                NULLIF(projectstatus,'') IS NULL OR
                NULLIF(taskcode,'') IS NULL OR
                NULLIF(taskname,'') IS NULL OR
                NULLIF(taskstartdate,'') IS NULL OR
                NULLIF(taskenddate,'') IS NULL OR
                NULLIF(companycode,'') IS NULL OR
                UPPER(projectstatus) NOT IN ('ACTIVE', 'CLOSED')"""
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
                NULLIF(projectnumber,'') IS NOT NULL AND
                NULLIF(projectname,'') IS NOT NULL AND
                NULLIF(projectstartdate,'') IS NOT NULL AND
                NULLIF(projectstatus,'') IS NOT NULL AND
                NULLIF(taskcode,'') IS NOT NULL AND
                NULLIF(taskname,'') IS NOT NULL AND
                NULLIF(taskstartdate,'') IS NOT NULL AND
                NULLIF(taskenddate,'') IS NOT NULL AND
                NULLIF(companycode,'') IS NOT NULL AND
                UPPER(projectstatus) IN ('ACTIVE', 'CLOSED')"""
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test="{{ result('query_valid_data', 'length') > 0 }}",
            yes_task='query_distinct_projects',
            no_task='format_logs'
        )

        # ========== Project Processing ==========
        query_distinct_projects = rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT projectnumber, record_id FROM validdata
                GROUP BY projectnumber"""
        )

        # Get Replicon custom field URIs for Unisys
        get_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:project'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Time Type Validation', 'uri', '')
        )

        get_task_custom_fields = rail.RepliconServiceOperator(
            task_id='get_task_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:task'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Paycode', 'uri', '')
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler= lambda resp: [
                uri for uri in [
                    rail.find_first_by_attr_and_get_attr(resp, 'displayText', perm_name, 'uri')
                    for perm_name in config.PROJECT_MANAGER_REQUIRED_PERMISSIONS
                ] if uri
            ]
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id='get_all_divisions',
            endpoint='/services/DivisionService1.svc/GetEnabledDivisions'
        )

        def get_process_projects_trigger_id(item):
            try:
                modulo = int(item['record_id']) % config.PROJECT_BATCH_COUNT
            except (ValueError, KeyError, TypeError):
                # Fallback to base DAG if record_id is invalid or missing
                return config.process_project_dag_id

            if modulo == 0:
                return config.process_project_dag_id
            return f"{config.process_project_dag_id}_batch_{str(modulo)}"

        process_projects = rail.trigger_parallel_dagrun(
            task_id='process_projects',
            items='{{ result("query_distinct_projects") }}',
            parallel_count=config.parallel_count,
            trigger_dag_id=get_process_projects_trigger_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'projectnumber': item['projectnumber'],
                'project_custom_field_uri': rail.result("get_project_custom_fields"),
                'task_custom_field_uri': rail.result("get_task_custom_fields"),
                'get_division_uris': rail.write_json_artifact(rail.result("get_all_divisions")),
                'log': rail.result("create_exception_log"),
                'permission_set_uris': rail.result("get_all_permission_sets")
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
                'projectnumber',
                'projectname',
                'taskcode',
                'taskname',
                'action',
                'status',
                'details',
                'jobid'
            ],
            row= [
                "{{ item.properties.projectnumber }}",
                "{{ item.properties.projectname }}",
                "{{ item.properties.taskcode }}",
                "{{ item.properties.taskname }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ],
        )

        def get_log_file_details(dag_run):
            """Generate log filename and job timing details"""
            # Generate log filename
            log_filename = f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0]}.csv'

            # Calculate job timing
            start_time = rail.result("log_start_time")
            end_time = pendulum.now(config.time_zone)

            return {
                'log_file': log_filename,
                'job_start_time': start_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'job_end_time': end_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            }

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=get_log_file_details
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('create_csv_log') }}",
            remote_filepath=config.sftp_log_path + "/{{ result('get_log_file_name').log_file }}",
        )

        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Project Sync " }} \
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

        is_valid_file_name >> rail.Label("Yes") >> download_file >> archive_file >> log_start_time >> can_decrypt_file
        is_valid_file_name >> rail.Label("No") >> check_file_type

        check_file_type >> rail.Label("Yes") >> delete_this_dagrun_resource_file
        check_file_type >> rail.Label("No") >> archive_unknown_file >> send_invalid_file_name_email

        can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> dummy_load_data >> was_new_file_found
        can_decrypt_file >> rail.Label("No") >> dummy_load_data

        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        dummy_load_data >> load_user_data >> create_collection_from_csv
        create_collection_from_csv >> create_exception_log >> has_collection_data

        has_collection_data >> rail.Label("Yes") >> query_blank_mandatory_check
        has_collection_data >> rail.Label("No") >> send_blank_payload_email

        query_blank_mandatory_check >> has_blank_mandatory_fields
        has_blank_mandatory_fields >> rail.Label("Yes") >> write_blank_mandatory_log >> query_valid_data
        has_blank_mandatory_fields >> rail.Label("No") >> query_valid_data

        query_valid_data >> has_valid_data
        has_valid_data >> rail.Label("Yes") >> query_distinct_projects
        has_valid_data >> rail.Label("No") >> format_logs

        query_distinct_projects >> get_project_custom_fields >> get_task_custom_fields >> get_all_permission_sets >> get_all_divisions >> process_projects >> format_logs
        format_logs >> create_csv_log >> get_log_file_name >> upload_log_to_sftp >> send_completion_email

    return dag

rail.for_each_instance(create_main_dag)
