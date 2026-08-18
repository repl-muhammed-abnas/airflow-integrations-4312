from datetime import timedelta
import itertools
import pendulum
from dataaxle.user_import.utils import custom_methods, request_payload, response_handler
import rail



def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Dataaxle User Import - Master DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2025, 1, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run_master,
        default_args={
            "execution_timeout": timedelta(days=config.execution_timeout_days),
        },
    ) as dag:
        
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.input_file_path,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )
        
        if_new_file_found = rail.IfOperator(
            task_id="if_new_file_found",
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task="is_csv_file",
            no_task="delete_this_dag_run"
        )

        is_csv_file = rail.IfOperator(
            task_id="is_csv_file",
            test=lambda: rail.result("new_file_sensor").lower().endswith("csv"),
            yes_task="user_import_log",
            no_task="send_bad_file_format_email"

        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id="send_bad_file_format_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | User integration to Replicon failed | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/bad_file_format.html",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            sftp_conn_id=config.sftp_conn_id,
            existing_filename = '{{ result("new_file_sensor") }}',
            new_filename=config.archive_file_path
            + "/{{ dag_run_ecid() }}_" + "{{ result('new_file_sensor') | file_name }}",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath='{{ result("new_file_sensor") }}'
        )

        load_input_data = rail.LoadCSVFileOperator(
            task_id="load_input_data",
            document="{{ result('download_file') }}",
            headers=config.INPUT_FILE_HEADERS,
        )

        write_input_data_csv = rail.WriteCSVFileOperator(
            task_id="write_input_data_csv",
            source="{{ result('load_input_data') }}",
            header=config.CSV_FILE_HEADERS,
            row=lambda item: [item[col] for col in config.INPUT_FILE_HEADERS]
            + [custom_methods.create_hash(config, item)],
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id="create_input_data_collection",
            source="{{ result('write_input_data_csv') }}",
            name=config.USER_INPUT_TABLE,
            columns=config.CSV_FILE_HEADERS
        )

        list_reference_sftp_files = rail.SFTPListFilesOperator(
            task_id="list_reference_sftp_files",
            sftp_conn_id=config.sftp_conn_id,
            paths=[config.reference_file_path],
        )

        if_reference_files_are_present = rail.IfOperator(
            task_id="if_reference_files_are_present",
            test='{{result("list_reference_sftp_files") | length == 1}}',
            yes_task="get_reference_filename",
            no_task="fail_no_reference_dagrun"
        )

        fail_no_reference_dagrun = rail.FailOperator(
            task_id="fail_no_reference_dagrun",
            message="No Reference File Present"
        )

        get_reference_filename = rail.PythonOperator(
            task_id="get_reference_filename",
            python_callable=lambda: rail.result('list_reference_sftp_files')[config.reference_file_path][0]['name']
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id="download_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_file_path +
            "/{{ result('get_reference_filename') }}"
        )

        parse_user_reference_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_reference_csv",
            document='{{result("download_reference_file")}}',
            headers=config.CSV_FILE_HEADERS,
        )

        create_reference_collection = rail.CreateCollectionOperator(
            task_id="create_reference_collection",
            source='{{result("parse_user_reference_csv")}}',
            name="user_reference",
            columns=config.CSV_FILE_HEADERS
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id="query_unchanged_records",
            name="unchanged_records",
            query=f"""SELECT * FROM {config.USER_INPUT_TABLE} WHERE md5 IN (SELECT md5 FROM user_reference)""",
        )

        if_unchanged_records_found = rail.IfOperator(
            task_id="if_unchanged_records_found",
            test= '{{ result("query_unchanged_records") | load_all_records() | length > 0 }}',
            yes_task="query_unchanged_records_with_empl_id",
            no_task="query_for_changed_records"
        )

        query_unchanged_records_with_empl_id = rail.QueryCollectionOperator(
            task_id="query_unchanged_records_with_empl_id",
            name="unchanged_records_with_empl_id",
            query="SELECT * FROM unchanged_records WHERE empl_id IS NOT NULL AND empl_id != ''",
        )

        user_import_log = rail.CreateLogOperator(
            task_id="user_import_log",
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id="log_unchanged_records",
            log='{{ result("user_import_log") }}',
            items='{{ result("query_unchanged_records_with_empl_id") }}',
            severity='Exception',
            message='Unchanged record - skipping import',
            properties=lambda item: custom_methods.build_ignore_list_logs(item, "pre-check", "ignored", "No changes found in the user record"),
        )

        query_for_changed_records = rail.QueryCollectionOperator(
            task_id="query_for_changed_records",
            name="changed_records",
            query=f"""SELECT * FROM {config.USER_INPUT_TABLE} WHERE md5 NOT IN (SELECT md5 FROM user_reference)""",
        )

        query_changed_records_without_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_records_without_mandatory_fields",
            name="changed_records_without_mandatory_fields",
            query=custom_methods.build_mandatory_fields_query(config, operator="=", joiner="OR"),
        )

        if_records_without_mandatory_fields_found = rail.IfOperator(
            task_id="if_records_without_mandatory_fields_found",
            test= '{{ result("query_records_without_mandatory_fields") | load_all_records() | length > 0 }}',
            yes_task="query_records_without_mandatory_fields_with_email",
            no_task="query_changed_records_with_mandatory_fields"
        )

        query_records_without_mandatory_fields_with_email = rail.QueryCollectionOperator(
            task_id="query_records_without_mandatory_fields_with_email",
            name="records_without_mandatory_fields_with_email",
            query="SELECT * FROM changed_records_without_mandatory_fields WHERE email_id IS NOT NULL AND email_id != ''",
        )

        log_records_without_mandatory_fields = rail.WriteLogOperator(
            task_id="log_records_without_mandatory_fields",
            log='{{ result("user_import_log") }}',
            items='{{ result("query_records_without_mandatory_fields_with_email") }}',
            severity='Exception',
            message='Record with missing mandatory fields - skipping import',
            properties=lambda item: custom_methods.build_ignore_list_logs(item, "pre-check", "ignored", "One or more mandatory fields value is missing"),
        )

        query_changed_records_with_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_changed_records_with_mandatory_fields",
            name="changed_records_with_mandatory_fields",
            query=custom_methods.build_mandatory_fields_query(config, operator="!=", joiner="AND"),
        )

        if_changed_records_with_mandatory_fields_found = rail.IfOperator(
            task_id="if_changed_records_with_mandatory_fields_found",
            test= '{{ result("query_changed_records_with_mandatory_fields") | load_all_records() | length > 0 }}',
            yes_task="get_replicon_user_list_report_details",
            no_task="archive_input_file"
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id="archive_input_file",
            sftp_conn_id=config.sftp_conn_id,
            existing_filename = '{{ result("new_file_sensor") }}',
            new_filename=config.archive_file_path
            + "/{{ dag_run_ecid() }}_" + "{{ result('new_file_sensor') | file_name }}",
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            existing_filename=config.reference_file_path + "/{{ result('get_reference_filename') }}",
            new_filename=config.archive_file_path + "/{{ result('get_reference_filename') }}"
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_new_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result ('write_input_data_csv') }}",
            remote_filepath=config.reference_file_path + "/userreference_" + "{{ ts_nodash }}.csv"
        )

        # Workato step 83: prepare ignored-records log CSV and generate a presigned download link
        filter_ignored_log_entries = rail.FilterLogEntriesOperator(
            task_id="filter_ignored_log_entries",
            log="{{ result('user_import_log') }}",
        )

        write_ignored_log_csv = rail.WriteCSVFileOperator(
            task_id="write_ignored_log_csv",
            source="{{ result('filter_ignored_log_entries') }}",
            header=["username", "employeeid", "action", "status", "details", "jobid"],
            row=[
                "{{ item.properties.username }}",
                "{{ item.properties.employeeid }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid }}",
            ],
        )

        generate_completion_email_log_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_completion_email_log_link",
            artifact_name="{{ result('write_ignored_log_csv') }}",
            output_file_name="dataaxleuserimportlogs_{{ current_time_in_specified_tz() }}.csv",
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        # Workato step 83: sent when no changed records with mandatory fields are found
        send_process_completion_email = rail.EmailOperator(
            task_id="send_process_completion_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon user import completed successfully | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/process_completion.html",
        )

        get_replicon_user_list_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_replicon_user_list_report_details",
            report_name=config.REPLICON_USER_LIST_REPORT_NAME,
        )

        if_uri_present_in_report_details = rail.IfOperator(
            task_id="if_uri_present_in_report_details",
            test="{{ result('get_replicon_user_list_report_details').uri | is_truthy }}",
            yes_task="generate_replicon_user_list_report.create_report_run",
            no_task="failed_dag_with_error"
        )

        failed_dag_with_error = rail.FailOperator(
            task_id="failed_dag_with_error",
            message=f"Report not found {config.REPLICON_USER_LIST_REPORT_NAME}"
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id="generate_replicon_user_list_report",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_replicon_user_list_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        if_report_generation_failed = rail.IfOperator(
            task_id='if_report_generation_failed',
            test="{{ result('generate_replicon_user_list_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='if_report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ result('generate_replicon_user_list_report.get_report_result').reportGenerationResults[0].error }}"
        )

        if_report_has_data = rail.IfOperator(
            task_id='if_report_has_data',
            test="{{ result('generate_replicon_user_list_report.get_report_result','has_data') }}",
            yes_task='if_report_has_expected_columns',
            no_task='fail_report_with_no_data'
        )

        fail_report_with_no_data = rail.FailOperator(
            task_id='fail_report_with_no_data',
            message='No data found in the generated report'
        )

        if_report_has_expected_columns = rail.IfOperator(
            task_id='if_report_has_expected_columns',
            test="{{ result('generate_replicon_user_list_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.REPLICON_USER_LIST_REPORT_EXPECTED_COLUMNS,
            yes_task='process_report_data',
            no_task='fail_report_with_unexpected_columns',
        )

        fail_report_with_unexpected_columns = rail.FailOperator(
            task_id='fail_report_with_unexpected_columns',
            message='No Data in the base report'
        )

        process_report_data = rail.EmptyOperator(
            task_id='process_report_data'
        )

        load_replicon_user_list_csv = rail.LoadCSVFileOperator(
            task_id='load_replicon_user_list_csv',
            document="{{ result('generate_replicon_user_list_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=config.REPLICON_USER_LIST_COLUMNS
        )

        get_custom_fields = rail.RepliconServiceOperator(
            task_id="get_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=response_handler.get_custom_fields
        )

        query_all_supervisor_from_input = rail.QueryCollectionOperator(
            task_id="query_all_supervisor_from_input",
            name="supervisors_list",
            query=f"""SELECT * FROM {config.USER_INPUT_TABLE} WHERE empl_id IN (
            SELECT DISTINCT reports_to_manager_id FROM {config.USER_INPUT_TABLE}
            )"""
        )

        load_all_supervisors_from_input = rail.PythonOperator(
            task_id="load_all_supervisors_from_input",
            python_callable=lambda: rail.load_all_records(rail.result("query_all_supervisor_from_input"))
        )

        query_job_title_from_input = rail.QueryCollectionOperator(
            task_id="query_job_title_from_input",
            name="job_title_list",
            query=f"""SELECT DISTINCT job_title, job_code FROM {config.USER_INPUT_TABLE}
            WHERE job_title != ''"""
        )

        get_enabled_service_centers = rail.RepliconServiceOperator(
            task_id="get_enabled_service_centers",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters"
        )

        load_job_title = rail.PythonOperator(
            task_id="load_job_title",
            python_callable=lambda: rail.load_all_records(rail.result("query_job_title_from_input"))
        )

        get_new_job_titles_and_codes = rail.PythonOperator(
            task_id="get_new_job_titles_and_codes",
            python_callable=custom_methods.get_new_job_titles_and_codes,
        )

        if_new_job_titles_found = rail.IfOperator(
            task_id="if_new_job_titles_found",
            test=lambda: len(rail.result("get_new_job_titles_and_codes")) > 0,
            yes_task="trigger_create_job_titles_start",
            no_task="query_payroll_dept_number"
        )

        trigger_create_job_titles_start = rail.EmptyOperator(
            task_id="trigger_create_job_titles_start"
        )

        trigger_create_job_titles = rail.trigger_parallel_dagrun(
            task_id="trigger_create_job_titles",
            items=lambda: rail.result("get_new_job_titles_and_codes"),
            trigger_dag_id=config.child_create_job_title_dag_id,
            conf=lambda item: {
                **item
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_parallel_dagrun_count_create_job_title_child,
        )

        query_payroll_dept_number = rail.QueryCollectionOperator(
            task_id="query_payroll_dept_number",
            name="payroll_dept_no_values",
            query=f"""SELECT DISTINCT payroll_dept_no FROM {config.USER_INPUT_TABLE} WHERE payroll_dept_no != ''"""
        )

        # Create new custom fields for payroll department number
        trigger_create_new_custom_fields_for_payroll_dept_no = rail.TriggerDagRunOperator(
            task_id='trigger_create_new_custom_fields_for_payroll_dept_no',
            trigger_dag_id=config.child_create_custom_fields_dag_id,
            wait_for_completion=True,
            conf=lambda: {
                "custom_field_uri": rail.result("get_custom_fields").get("payroll_department_number_uri"),
                "input_file_custom_field_values": rail.load_all_records(rail.result("query_payroll_dept_number")),
                "column_name": "payroll_dept_no"
            }
        )

        query_payroll_dept_name = rail.QueryCollectionOperator(
            task_id="query_payroll_dept_name",
            name="payroll_dept_name",
            query=f"""SELECT DISTINCT payroll_dept_name FROM {config.USER_INPUT_TABLE} WHERE payroll_dept_name != ''"""
        )

        # Create new custom fields for payroll department names
        trigger_create_new_custom_fields_for_payroll_dept_name = rail.TriggerDagRunOperator(
            task_id='trigger_create_new_custom_fields_for_payroll_dept_name',
            trigger_dag_id=config.child_create_custom_fields_dag_id,
            wait_for_completion=True,
            conf=lambda: {
                "custom_field_uri": rail.result("get_custom_fields").get("payroll_department_uri"),
                "input_file_custom_field_values": rail.load_all_records(rail.result("query_payroll_dept_name")),
                "column_name": "payroll_dept_name"
            }
        )

        query_executive_level = rail.QueryCollectionOperator(
            task_id="query_executive_level",
            name="executive_level",
            query=f"""SELECT DISTINCT executive_level FROM {config.USER_INPUT_TABLE} WHERE executive_level != ''"""
        )

        # Create new custom fields for executive levels
        trigger_create_new_custom_fields_for_executive_level = rail.TriggerDagRunOperator(
            task_id='trigger_create_new_custom_fields_for_executive_level',
            trigger_dag_id=config.child_create_custom_fields_dag_id,
            wait_for_completion=True,
            conf=lambda: {
                "custom_field_uri": rail.result("get_custom_fields").get("executive_level_uri"),
                "input_file_custom_field_values": rail.load_all_records(rail.result("query_executive_level")),
                "column_name": "executive_level"
            }
        )

        query_report_to_names = rail.QueryCollectionOperator(
            task_id="query_report_to_names",
            name="report_to_names",
            query=f"""SELECT DISTINCT report_to_name FROM {config.USER_INPUT_TABLE} WHERE report_to_name != ''"""
        )

        # Create new custom fields for User's Supervisor Name
        trigger_create_new_custom_fields_for_user_supervisor_name = rail.TriggerDagRunOperator(
            task_id='trigger_create_new_custom_fields_for_user_supervisor_name',
            trigger_dag_id=config.child_create_custom_fields_dag_id,
            wait_for_completion=True,
            conf=lambda: {
                "custom_field_uri": rail.result("get_custom_fields").get("user_supervisor_name_uri"),
                "input_file_custom_field_values": rail.load_all_records(rail.result("query_report_to_names")),
                "column_name": "report_to_name"
            }
        )

        query_standard_hours = rail.QueryCollectionOperator(
            task_id="query_standard_hours",
            name="standard_hours",
            query=f"""SELECT DISTINCT standard_hours FROM {config.USER_INPUT_TABLE} WHERE standard_hours != ''"""
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_new_office_schedules = rail.PythonOperator(
            task_id="get_new_office_schedules",
            python_callable=custom_methods.get_new_office_schedules
        )

        if_new_office_schedules_found = rail.IfOperator(
            task_id="if_new_office_schedules_found",
            test=lambda: len(rail.result("get_new_office_schedules")) > 0,
            yes_task="trigger_create_office_schedules_start",
            no_task="start_custom_field_lookups"
        )

        trigger_create_office_schedules_start = rail.EmptyOperator(
            task_id="trigger_create_office_schedules_start"
        )

        trigger_create_office_schedules_child = rail.trigger_parallel_dagrun(
            task_id="trigger_create_office_schedules_child",
            items=lambda: rail.result("get_new_office_schedules"),
            trigger_dag_id=config.child_create_office_schedule_dag_id,
            conf=lambda item: {
                **item
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_parallel_dagrun_count_create_office_schedules_child
        )

        get_all_custom_fields_drop_down_options_payroll_dept_no = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_drop_down_options_payroll_dept_no",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_custom_fields").get("payroll_department_number_uri")
            },
        )

        get_all_custom_fields_drop_down_options_payroll_dept_name = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_drop_down_options_payroll_dept_name",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_custom_fields").get("payroll_department_uri")
            }
        )

        get_all_custom_fields_drop_down_options_executive_level = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_drop_down_options_executive_level",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_custom_fields").get("executive_level_uri")
            }
        )

        get_all_custom_fields_drop_down_options_user_supervisor_name = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_drop_down_options_user_supervisor_name",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_custom_fields").get("user_supervisor_name_uri")
            }
        )

        start_custom_field_lookups = rail.EmptyOperator(
            task_id="start_custom_field_lookups"
        )

        finish_custom_field_lookups = rail.EmptyOperator(
            task_id="finish_custom_field_lookups"
        )

        get_company_department_uri = rail.RepliconServiceOperator(
            task_id="get_company_department_uri",
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment",
            data_handler=lambda response: response["uri"]
        )

        get_children_department_details = rail.RepliconServiceOperator(
            task_id="get_children_department_details",
            endpoint="/services/DepartmentService1.svc/GetChildrenDepartmentDetails",
            data=lambda: {
                "parentDepartmentUri": rail.result("get_company_department_uri")
            }
        )

        get_enabled_currencies = rail.RepliconServiceOperator(
            task_id="get_enabled_currencies",
            endpoint="/services/CurrencyService2.svc/GetEnabledCurrencies"
        )

        get_employee_type_group = rail.RepliconServiceOperator(
            task_id="get_employee_type_group",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.build_employee_type_group_request_body,
            data_handler=lambda response: response_handler.get_employee_type_group(response)
        )

        # Workato equivalent: "user check" ruby step.
        # Builds a dict {Employee ID -> UserUri} from load_replicon_user_list_csv once,
        # before the foreach loop (Workato: Lines Step 38, column_2=Employee ID, column_3=UserUri).
        build_replicon_user_lookup = rail.PythonOperator(
            task_id='build_replicon_user_lookup',
            python_callable=custom_methods.build_replicon_user_lookup
        )

        trigger_process_users = rail.trigger_parallel_dagrun(
            task_id='trigger_process_users',
            items="{{ result('query_changed_records_with_mandatory_fields') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.child_process_users_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: custom_methods.build_process_user_conf(config, item),
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *[
                    rail.result(f'trigger_process_users_{x + 1}') or []
                    for x in range(config.trigger_parallel_dagrun_count_process_users)
                ]
            )),
            show_return_value_in_logs=False,
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_process_users_dag_ids') | to_json }}"
        )

        # ── Yes-path: log generation after child processing ────────────────────
        write_user_import_log_csv = rail.WriteCSVFileOperator(
            task_id="write_user_import_log_csv",
            source="{{ result('user_import_log') }}",
            header=["username", "employeeid", "action", "status", "details", "jobid"],
            row=[
                "{{ item.properties.username }}",
                "{{ item.properties.employeeid }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid }}",
            ],
        )

        generate_user_import_log_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_user_import_log_download_link",
            artifact_name="{{ result('write_user_import_log_csv') }}",
            output_file_name="dataaxleuserimportlogs_{{ current_time_in_specified_tz() }}.csv",
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        # Workato send_logs recipe col4 == "failed" check
        check_failed_records = rail.PythonOperator(
            task_id="check_failed_records",
            python_callable=lambda: any(
                entry.get("properties", {}).get("status") == "failed"
                for entry in (rail.load_all_records(rail.result("user_import_log")) or [])
            ),
        )

        if_failed_records_present = rail.IfOperator(
            task_id="if_failed_records_present",
            test="{{ result('check_failed_records') | is_truthy }}",
            yes_task="send_completion_with_failures_email",
            no_task="send_completion_success_email",
        )

        send_completion_with_failures_email = rail.EmailOperator(
            task_id="send_completion_with_failures_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon user import completed with failed records | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/process_completion_with_failures.html",
        )

        send_completion_success_email = rail.EmailOperator(
            task_id="send_completion_success_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon user import completed successfully | {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/process_completion_success.html",
        )

        # Join point after either email branch before archiving
        post_processing_archive_join = rail.EmptyOperator(
            task_id="post_processing_archive_join",
            trigger_rule="one_success",
        )

        archive_input_file_after_processing = rail.SFTPMoveFileOperator(
            task_id="archive_input_file_after_processing",
            sftp_conn_id=config.sftp_conn_id,
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_file_path
            + "/{{ dag_run_ecid() }}_" + "{{ result('new_file_sensor') | file_name }}",
        )

        archive_reference_file_after_processing = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file_after_processing",
            sftp_conn_id=config.sftp_conn_id,
            existing_filename=config.reference_file_path + "/{{ result('get_reference_filename') }}",
            new_filename=config.archive_file_path + "/{{ result('get_reference_filename') }}"
        )

        upload_new_reference_file_after_processing = rail.SFTPUploadFileOperator(
            task_id="upload_new_reference_file_after_processing",
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('write_input_data_csv') }}",
            remote_filepath=config.reference_file_path + "/userreference_" + "{{ ts_nodash }}.csv"
        )

        delete_this_dag_run = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dag_run"
        )

        finish = rail.EmptyOperator(
            task_id= "finish"
        )

        # ── File detection ─────────────────────────────────────────────────────
        new_file_sensor >> if_new_file_found
        if_new_file_found >> rail.Label("No") >> delete_this_dag_run
        if_new_file_found >> rail.Label("Yes") >> is_csv_file

        # ── File format check ──────────────────────────────────────────────────
        is_csv_file >> rail.Label("No") >> send_bad_file_format_email >> archive_file >> finish
        is_csv_file >> rail.Label("Yes") >> user_import_log
        (
            user_import_log
            >> download_file
            >> load_input_data
            >> write_input_data_csv
            >> create_input_data_collection
            >> list_reference_sftp_files
            >> if_reference_files_are_present
        )

        # ── Reference file download ────────────────────────────────────────────
        if_reference_files_are_present >> rail.Label("No") >> fail_no_reference_dagrun
        if_reference_files_are_present >> rail.Label("Yes") >> get_reference_filename
        (
            get_reference_filename
            >> download_reference_file
            >> parse_user_reference_csv
            >> create_reference_collection
            >> query_unchanged_records
            >> if_unchanged_records_found
        )

        # ── Unchanged records ──────────────────────────────────────────────────
        if_unchanged_records_found >> rail.Label("Yes") >> query_unchanged_records_with_empl_id >> log_unchanged_records >> query_for_changed_records
        if_unchanged_records_found >> rail.Label("No") >> query_for_changed_records

        # ── Changed records validation ─────────────────────────────────────────
        (
            query_for_changed_records
            >> query_changed_records_without_mandatory_fields
            >> if_records_without_mandatory_fields_found
        )
        if_records_without_mandatory_fields_found >> rail.Label("Yes") >> query_records_without_mandatory_fields_with_email >> log_records_without_mandatory_fields >> query_changed_records_with_mandatory_fields
        if_records_without_mandatory_fields_found >> rail.Label("No") >> query_changed_records_with_mandatory_fields

        query_changed_records_with_mandatory_fields >> if_changed_records_with_mandatory_fields_found

        # ── No changed records — archive and finish ────────────────────────────
        if_changed_records_with_mandatory_fields_found >> rail.Label("No") >> archive_input_file
        (
            archive_input_file
            >> archive_reference_file
            >> upload_new_reference_file
            >> filter_ignored_log_entries
            >> write_ignored_log_csv
            >> generate_completion_email_log_link
            >> send_process_completion_email
        )

        # ── Replicon user list report ──────────────────────────────────────────
        if_changed_records_with_mandatory_fields_found >> rail.Label("Yes") >> get_replicon_user_list_report_details >> if_uri_present_in_report_details
        if_uri_present_in_report_details >> rail.Label("No") >> failed_dag_with_error
        if_uri_present_in_report_details >> rail.Label("Yes") >> run_report_entry

        run_report_exit >> if_report_generation_failed
        if_report_generation_failed >> rail.Label("Yes") >> fail_report_generation
        if_report_generation_failed >> rail.Label("No") >> if_report_has_data

        if_report_has_data >> rail.Label("No") >> fail_report_with_no_data
        if_report_has_data >> rail.Label("Yes") >> if_report_has_expected_columns

        if_report_has_expected_columns >> rail.Label("No") >> fail_report_with_unexpected_columns
        if_report_has_expected_columns >> rail.Label("Yes") >> process_report_data
        (
            process_report_data
            >> load_replicon_user_list_csv
            >> build_replicon_user_lookup
            >> get_custom_fields
            >> query_all_supervisor_from_input
            >> load_all_supervisors_from_input
            >> query_job_title_from_input
            >> get_enabled_service_centers
            >> load_job_title
            >> get_new_job_titles_and_codes
            >> if_new_job_titles_found
        )

        # ── Job titles creation ────────────────────────────────────────────────
        if_new_job_titles_found >> rail.Label("No") >> query_payroll_dept_number
        if_new_job_titles_found >> rail.Label("Yes") >> trigger_create_job_titles_start >> trigger_create_job_titles >> query_payroll_dept_number

        # ── Custom field provisioning ──────────────────────────────────────────
        (
            query_payroll_dept_number
            >> trigger_create_new_custom_fields_for_payroll_dept_no
            >> query_payroll_dept_name
            >> trigger_create_new_custom_fields_for_payroll_dept_name
            >> query_executive_level
            >> trigger_create_new_custom_fields_for_executive_level
            >> query_report_to_names
            >> trigger_create_new_custom_fields_for_user_supervisor_name
            >> query_standard_hours
            >> get_all_office_schedules
            >> get_new_office_schedules
            >> if_new_office_schedules_found
        )

        # ── Office schedules creation ──────────────────────────────────────────
        if_new_office_schedules_found >> rail.Label("No") >> start_custom_field_lookups
        if_new_office_schedules_found >> rail.Label("Yes") >> trigger_create_office_schedules_start >> trigger_create_office_schedules_child >> start_custom_field_lookups

        start_custom_field_lookups >> [get_all_custom_fields_drop_down_options_payroll_dept_no, get_all_custom_fields_drop_down_options_payroll_dept_name, get_all_custom_fields_drop_down_options_executive_level, get_all_custom_fields_drop_down_options_user_supervisor_name] >> finish_custom_field_lookups

        # ── Lookup data assembly + process users ──────────────────────────────
        (
            finish_custom_field_lookups
            >> get_company_department_uri
            >> get_children_department_details
            >> get_enabled_currencies
            >> get_employee_type_group
            >> trigger_process_users
            >> get_process_users_dag_ids
            >> wait_for_child_dags
        )

        # ── Yes-path: log generation and email after child processing ─────────────
        (
            wait_for_child_dags
            >> write_user_import_log_csv
            >> generate_user_import_log_download_link
            >> check_failed_records
            >> if_failed_records_present
        )
        if_failed_records_present >> rail.Label("Yes") >> send_completion_with_failures_email >> post_processing_archive_join
        if_failed_records_present >> rail.Label("No") >> send_completion_success_email >> post_processing_archive_join
        (
            post_processing_archive_join
            >> archive_input_file_after_processing
            >> archive_reference_file_after_processing
            >> upload_new_reference_file_after_processing
        )

        return dag

rail.for_each_instance(create_master_dag)