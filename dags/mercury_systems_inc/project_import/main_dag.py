from datetime import timedelta
from pendulum import now
import rail
from mercury_systems_inc.project_import.utils import request_payload, custom_method


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Mercury Project import master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        _now = now(config.time_zone)
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon Project import - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        log_start_time = rail.PythonOperator(
            task_id="log_start_time",
            python_callable=lambda:now(config.time_zone)
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
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
            document="{{ result('download_file') }}"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Parent Labor Code': 'project_name',
                'Parent Labor Description': 'project_description',
                'Parent ID': 'project_code',
                "Parent Charge Status": "project_status",
                'Parent Start Date': 'project_start_date',
                'Parent End Date': 'project_end_date',
                'Charge Type': 'program',
                'Project Manager': 'project_manager',
                'Assigned OU': 'team_departments',
                'Child ID Code': 'task_name',
                'Child Description': 'task_description',
                'Child ID': 'task_code',
                'Project/Task Hierarchy': 'child_tasks',
                'Child Start Date': 'task_start_date',
                'Child End Date': 'task_end_date',
                'Allow Charges': 'task_allow_time_entry',
                'Child Charge Status': 'task_status'
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_input_data_collection', 'length') > 0 }}",
            yes_task='query_any_blankmandatory_check',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon Project import - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_any_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_blankmandatory_check',
            query="""SELECT * FROM inputdatacollection WHERE
                NULLIF(project_name,'') IS NULL OR
                NULLIF(project_description,'') IS NULL OR
                NULLIF(project_code,'') IS NULL OR
                NULLIF(project_status,'') IS NULL OR
                NULLIF(project_start_date,'') IS NULL OR
                NULLIF(project_end_date,'') IS NULL OR
                NULLIF(program,'') IS NULL OR
                NULLIF(task_name,'') IS NULL OR
                NULLIF(task_description,'') IS NULL OR
                NULLIF(task_code,'') IS NULL OR
                NULLIF(child_tasks,'') IS NULL OR
                NULLIF(task_allow_time_entry,'') IS NULL"""
        )

        has_any_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_blank_mandatory_field',
            test="{{ result('query_any_blankmandatory_check', 'length') > 0 }}",
            yes_task='write_wbs_blankmandatory_field_log',
            no_task='query_valid_data_from_rawdata'
        )

        write_wbs_blankmandatory_field_log = rail.WriteLogOperator(
            task_id="write_wbs_blankmandatory_field_log",
            items="{{result('query_any_blankmandatory_check')}}",
            log="{{ result('create_log') }}",
            severity="Exception",
            message="mandatory field is not present",
            properties=request_payload.get_invalid_logs_property_conf
        )

        query_valid_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_data_from_rawdata',
            name='validwbsdata',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM inputdatacollection WHERE
                NULLIF(project_name,'') IS NOT NULL AND
                NULLIF(project_description,'') IS NOT NULL AND
                NULLIF(project_code,'') IS NOT NULL AND
                NULLIF(project_status,'') IS NOT NULL AND
                NULLIF(project_start_date,'') IS NOT NULL AND
                NULLIF(project_end_date,'') IS NOT NULL AND
                NULLIF(program,'') IS NOT NULL AND
                NULLIF(task_name,'') IS NOT NULL AND
                NULLIF(task_description,'') IS NOT NULL AND
                NULLIF(task_code,'') IS NOT NULL AND
                NULLIF(child_tasks,'') IS NOT NULL AND
                NULLIF(task_allow_time_entry,'') IS NOT NULL"""
        )

        has_valid_projects = rail.IfOperator(
            task_id='has_valid_projects',
            test="{{ result('query_valid_data_from_rawdata', 'length') > 0 }}",
            yes_task='get_project_manager_permission_set',
            no_task='format_logs'
        )

        get_project_manager_permission_set = rail.RepliconServiceOperator(
            task_id="get_project_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda resp: rail.find_first_by_attr_and_get_attr(
                resp, 'displayText', 'Project Manager', 'uri')
        )

        get_all_department_details = rail.RepliconServiceOperator(
            task_id='get_all_department_details',
            endpoint='/services/DepartmentService1.svc/GetEnabledDepartmentHierarchyDetails',
        )

        query_distict_projects = rail.QueryCollectionOperator(
            task_id='query_distict_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT project_code,record_id from validwbsdata GROUP BY project_code"""
        )

        def get_process_projects_trigger_id(item):
            modulo = int(item['record_id']) % config.PROJECT_BATCH_COUNT
            if modulo == 0:
                return config.process_project_dag_id
            return f"{config.process_project_dag_id}_batch_{str(modulo)}"

        process_projects = rail.trigger_parallel_dagrun(
            task_id='process_projects',
            items='{{ result("query_distict_projects") }}',
            parallel_count=config.parallel_count,
            trigger_dag_id=get_process_projects_trigger_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'depaprtment_details': rail.write_json_artifact(rail.result("get_all_department_details")),
                'project_manager_permission_set_uri': rail.result("get_project_manager_permission_set"),
                'log': rail.result("create_log")
            }
        )

        end_processing_projects = rail.EmptyOperator(
            task_id='end_processing_projects',
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_method.format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{result('format_logs')}}",
            header=[
                "projectcode",
                "projectname",
                "program",
                "taskcode",
                "taskname",
                'action',
                "details",
                "status",
                'ecid'
            ],
            row=[
                "{{item.properties.projectcode}}",
                "{{item.properties.projectname}}",
                "{{item.properties.program}}",
                "{{item.properties.taskcode}}",
                "{{item.properties.taskname}}",
                "{{item.properties.action}}",
                "{{item.properties.details}}",
                "{{item.properties.Status}}",
                "{{item.ecid}}"
            ],
        footer=['Number of records found:{{ result("create_input_data_collection","length")}}',
                    'Number of records processed:{{ result("format_logs", key="total_record_count")}}',
                    'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                    'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                    'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
                    ]
        )

        def get_log_details():
            job_end_time = now(config.time_zone)
            _start = rail.result("log_start_time")
            return {
                "log_file": f"{rail.get_company_key()}_Logs_Project_Import_{rail.result('new_file_sensor').split('/')[-1]}",
                "job_start_time": _start.isoformat(),
                "job_end_time": job_end_time.isoformat(),
                "job_duration": ((job_end_time - _start).minutes)
            }

        get_log_file_name = rail.PythonOperator(
            task_id='get_log_file_name',
            python_callable=get_log_details,
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('create_csv_log') }}",
            remote_filepath=config.log_filepath +
            "{{ result('get_log_file_name').log_file }}"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Project import - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y%m%d%H%M%S") }}',
            html_content="templates/emails/email_import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        new_file_sensor >> is_csv >> rail.Label(
            'Yes') >> download_file >> was_new_file_found

        is_csv >> rail.Label(
            'No') >> send_bad_file_format_email

        was_new_file_found >> rail.Label(
            'Yes') >> archive_file

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        download_file >> log_start_time >> create_log >> load_data >> create_input_data_collection >> has_collection_data

        has_collection_data >> rail.Label(
            "No") >> send_blank_payload_email

        has_collection_data >> rail.Label(
            "Yes") >> query_any_blankmandatory_check >> has_any_blank_mandatory_field

        has_any_blank_mandatory_field >> rail.Label(
            "Yes") >> write_wbs_blankmandatory_field_log >> query_valid_data_from_rawdata

        has_any_blank_mandatory_field >> rail.Label(
            "No") >> query_valid_data_from_rawdata >> has_valid_projects

        has_valid_projects >> rail.Label(
            "No") >> format_logs

        has_valid_projects >> rail.Label(
            "Yes") >> get_project_manager_permission_set >> get_all_department_details >> query_distict_projects >> process_projects >> \
            end_processing_projects >> format_logs >> create_csv_log >> get_log_file_name >> \
            upload_logs_to_sftp >> send_import_complete_email

    return dag


rail.for_each_instance(create_child_dag_wbs)
