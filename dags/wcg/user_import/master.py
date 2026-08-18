from datetime import timedelta, datetime as dt
from pendulum import datetime
from wcg.user_import.utils import custom_methods
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"WCG User Import Master Process {config.instance}",
        start_date=datetime(2025, 1, 1, tz=config.time_zone),
        schedule_interval=timedelta(seconds=config.schedule_interval_seconds),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_path,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task='download_input_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | User Import - Invalid file format - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/bad_file_format.html"
        )

        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_file_found = rail.IfOperator(
            task_id='was_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_input_file',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id="archive_input_file",
            new_filename=config.sftp_archive_path + '/archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename='{{ result("new_file_sensor") }}'
        )

        load_csv_file = rail.LoadCSVFileOperator(
            task_id="load_csv_file",
            document="{{ result('download_input_file') }}",
            delimiter=','
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id='create_supervisor_log'
        )

        create_users_payload_collection = rail.CreateCollectionOperator(
            task_id="create_users_payload_collection",
            source='{{ result("load_csv_file") }}',
            name='users_payload_data',
            columns={
                "Internal ID": "employeeid",
                "First Name": "firstname",
                "Middle Name": "middlename",
                "Last Name": "lastname",
                "Email": "email",
                "Employee Status": "employee_status",
                "Hire Date": "hire_date",
                "Termination/Release Date": "release_date",
                "Supervisor": "supervisor_name",
                "Supervisor Internal Id": "supervisorempid",
                "Department": "department",
                "Department Internal Id": "department_id",
                "ADP Work Location": "location",
                "Employee Type": "employee_type",
                "Inactive": "inactive",
                "ADP Employee ID": "adp_employee_id",
                "Labor Cost": "labor_cost",
                "Subsidiary": "subsidiary",
                "Subsidiary Internal Id": "subsidiary_id",
                "LOB": "lob",
                "LOB Internal Id": "lob_id",
                "Work Category Description": "work_category",
                "Billing Country": "billing_country"
            }
        )

        if_user_payload_exists = rail.IfOperator(
            task_id='if_user_payload_exists',
            test='{{ result("create_users_payload_collection", "length") > 0 }}',
            yes_task='query_blank_mandatory_fields',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Import - No valid records to process - {{ current_time_in_specified_tz() }}',
            html_content="/templates/emails/blank_payload.html"
        )

        query_blank_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_blank_mandatory_fields",
            query="""SELECT * FROM users_payload_data
                     WHERE NULLIF("email","") IS NULL
                     OR NULLIF("employeeid","") IS NULL
                     OR NULLIF("firstname","") IS NULL
                     OR NULLIF("lastname","") IS NULL
                     OR NULLIF("subsidiary","") IS NULL
                     OR (NULLIF("department","") IS NULL AND NULLIF("department_id","") IS NULL)
                     OR NULLIF("employee_type","") IS NULL""",
        )

        if_blank_mandatory_fields = rail.IfOperator(
            task_id="if_blank_mandatory_fields",
            test='{{result("query_blank_mandatory_fields", "length") > 0}}',
            yes_task="write_blank_mandatory_fields_log",
            no_task="query_mandatory_fields"
        )

        write_blank_mandatory_fields_log = rail.WriteLogOperator(
            task_id="write_blank_mandatory_fields_log",
            log='{{ result("create_log") }}',
            items='{{result("query_blank_mandatory_fields")}}',
            message="Invalid user data - Mandatory field(s) missing",
            severity="Exception",
            properties=custom_methods.get_invalid_user_log_properties
        )

        query_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_mandatory_fields",
            query="""SELECT * FROM users_payload_data
                     WHERE NULLIF("email","") IS NOT NULL
                       AND NULLIF("employeeid","") IS NOT NULL
                       AND NULLIF("firstname","") IS NOT NULL
                       AND NULLIF("lastname","") IS NOT NULL
                       AND NULLIF("subsidiary","") IS NOT NULL
                       AND (NULLIF("department","") IS NOT NULL OR NULLIF("department_id","") IS NOT NULL)
                       AND NULLIF("employee_type","") IS NOT NULL""",
            name= 'valid_users_payload_data'
        )

        if_mandatory_fields_data_exists = rail.IfOperator(
            task_id="if_mandatory_fields_data_exists",
            test='{{result("query_mandatory_fields", "length") > 0}}',
            yes_task="get_all_departments",
            no_task="get_pending_supervisor_logs"
        )

        get_all_departments = rail.RepliconServiceOperator(
            task_id='get_all_departments',
            endpoint='/services/DepartmentService1.svc/GetEnabledDepartments'
        )

        get_all_required_oefs = rail.RepliconServiceOperator(
            task_id='get_all_required_oefs',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'middle_name': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Middle Name', 'uri'),
                'user_subsidiary': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'User Subsidiary', 'uri'),
                'netsuite_internal_id': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'NetSuite Internal ID', 'uri')
            }
        )

        query_unique_subsidiaries = rail.QueryCollectionOperator(
            task_id='query_unique_subsidiaries',
            name='unique_subsidiaries',
            query="""SELECT DISTINCT subsidiary FROM valid_users_payload_data
                     WHERE subsidiary IS NOT NULL AND subsidiary != ''"""
        )

        has_unique_subsidiaries = rail.IfOperator(
            task_id='has_unique_subsidiaries',
            test='{{ result("query_unique_subsidiaries", "length") > 0 }}',
            yes_task='process_subsidiary_dropdowns',
            no_task='query_unique_locations'
        )

        process_subsidiary_dropdowns = rail.TriggerDagRunForEachItemOperator(
            task_id='process_subsidiary_dropdowns',
            retries=0,
            items='{{ result("query_unique_subsidiaries") }}',
            trigger_dag_id=config.process_oef_dropdown_value_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "subsidary_oef_uri": rail.result("get_all_required_oefs")['user_subsidiary'],
                "field_value": item.get("subsidiary"),
                "log_artifact": rail.result("create_log")
            }
        )

        wait_for_subsidiary_dropdowns = rail.WaitForDagRunsSensor(
            task_id='wait_for_subsidiary_dropdowns',
            dag_runs='{{ result("process_subsidiary_dropdowns") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_unique_locations = rail.QueryCollectionOperator(
            task_id='query_unique_locations',
            name='unique_locations',
            query="""SELECT DISTINCT location FROM valid_users_payload_data
                     WHERE location IS NOT NULL AND location != ''"""
        )

        has_unique_locations = rail.IfOperator(
            task_id='has_unique_locations',
            test='{{ result("query_unique_locations", "length") > 0 }}',
            yes_task='get_enabled_locations',
            no_task='get_user_report_details'
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id="get_enabled_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        process_location_groups = rail.TriggerDagRunForEachItemOperator(
            task_id='process_location_groups',
            retries=0,
            items='{{ result("query_unique_locations") }}',
            trigger_dag_id=config.process_location_group_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "location_name": item.get("location"),
                "location_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_enabled_locations"),"displayText",item.get("location"),"uri", None),
                "log_artifact": rail.result("create_log")
            }
        )

        wait_for_location_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_location_groups',
            dag_runs='{{ result("process_location_groups") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.report_name,
        )

        get_all_replicon_users = rail.RepliconServiceOperator(
            task_id='get_all_replicon_users',
            endpoint='/services/ReportService1.svc/GenerateReport',
            data={
                "reportUri": '{{ result("get_user_report_details").uri }}',
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document='{{ result("get_all_replicon_users").payload }}'
        )

        create_replicon_users_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_users_collection',
            source='{{ result("report_payload_to_csv") }}',
            name='replicon_users',
            columns={
                "User Name": "User_Name",
                "User Email": "User_Email",
                "User Status": "User_Status",
                "NetSuite Internal ID": "Internal_ID",
                "uri": "uri",
                "Location (Current)": "location"
            }
        )

        get_all_employee_types = rail.RepliconServiceOperator(
            task_id='get_all_employee_types',
            endpoint='/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails'
        )

        query_new_users = rail.QueryCollectionOperator(
            task_id='query_new_users',
            name='new_users_to_create',
            query="""SELECT * FROM valid_users_payload_data WHERE employeeid NOT IN (SELECT DISTINCT Internal_ID FROM replicon_users)"""
        )

        has_new_users = rail.IfOperator(
            task_id='has_new_users',
            test='{{ result("query_new_users", "length") > 0 }}',
            yes_task='start_processing_new_users',
            no_task='query_disabled_users_in_replicon'
        )

        start_processing_new_users = rail.EmptyOperator(
            task_id='start_processing_new_users'
        )

        process_add_users = rail.trigger_parallel_dagrun(
            task_id="process_add_users",
            items='{{ result("query_new_users") }}',
            trigger_dag_id=config.process_add_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count= config.parallel_count,
            conf=lambda item: {
                **item,
                "log_artifact": rail.result("create_log"),
                "supervisor_log": rail.result("create_supervisor_log"),
                "subsidiary_field_uri": rail.result("get_all_required_oefs")['user_subsidiary'],
                "netsuite_internal_id_oef_uri": rail.result("get_all_required_oefs")['netsuite_internal_id'],
                "middle_name_oef_uri": rail.result("get_all_required_oefs")['middle_name'],
                "department_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_departments"), "displayText", item.get("department").split(":")[-1].strip(), "uri", None),
                "employee_type_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_employee_types"), "displayText", item.get("employee_type"), "uri", None),
            }
        )

        query_disabled_users_in_replicon = rail.QueryCollectionOperator(
            task_id='query_disabled_users_in_replicon',
            name='disabled_users_in_replicon',
            query="""SELECT valid_users_payload_data.employeeid,
                            valid_users_payload_data.firstname,
                            valid_users_payload_data.lastname,
                            valid_users_payload_data.email,
                            replicon_users.User_Status
                    FROM valid_users_payload_data
                    INNER JOIN replicon_users
                    ON LOWER(NULLIF(valid_users_payload_data.employeeid, '')) = LOWER(NULLIF(replicon_users.Internal_ID, ''))
                    WHERE LOWER(TRIM(replicon_users.User_Status)) != 'enabled'"""
        )

        has_disabled_users = rail.IfOperator(
            task_id='has_disabled_users',
            test='{{ result("query_disabled_users_in_replicon", "length") > 0 }}',
            yes_task='log_disabled_users',
            no_task='query_existing_users'
        )

        log_disabled_users = rail.WriteLogOperator(
            task_id='log_disabled_users',
            log='{{ result("create_log") }}',
            message='Users skipped - disabled in Replicon',
            severity='Exception',
            items='{{ result("query_disabled_users_in_replicon") }}',
            properties={
                'employeeid': '{{ item.employeeid }}',
                'firstname': '{{ item.firstname }}',
                'lastname': '{{ item.lastname }}',
                'action': 'Skip',
                'status': 'Exception',
                'details': 'User skipped - User is disabled in Replicon'
            }
        )

        query_existing_users = rail.QueryCollectionOperator(
            task_id='query_existing_users',
            name='existing_users_to_update',
            query="""SELECT valid_users_payload_data.*,
                            replicon_users.uri as user_uri,
                            replicon_users.location as replicon_location,
                            supervisor.uri as desired_supervisor_uri
                    FROM valid_users_payload_data
                    INNER JOIN replicon_users
                    ON LOWER(NULLIF(valid_users_payload_data.employeeid, '')) = LOWER(NULLIF(replicon_users.Internal_ID, ''))
                    LEFT JOIN replicon_users AS supervisor
                    ON LOWER(NULLIF(valid_users_payload_data.supervisorempid, '')) = LOWER(NULLIF(supervisor.Internal_ID, ''))
                    WHERE LOWER(TRIM(replicon_users.User_Status)) = 'enabled'"""
        )

        has_existing_users = rail.IfOperator(
            task_id='has_existing_users',
            test='{{ result("query_existing_users", "length") > 0 }}',
            yes_task='start_processing_update_users',
            no_task='get_pending_supervisor_logs'
        )

        start_processing_update_users = rail.EmptyOperator(
            task_id='start_processing_update_users'
        )

        process_update_users = rail.trigger_parallel_dagrun(
            task_id="process_update_users",
            items='{{ result("query_existing_users") }}',
            trigger_dag_id=config.process_update_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count= config.parallel_count,
            conf=lambda item: {
                **item,
                "log_artifact": rail.result("create_log"),
                "supervisor_log": rail.result("create_supervisor_log"),
                "subsidiary_field_uri": rail.result("get_all_required_oefs")['user_subsidiary'],
                "netsuite_internal_id_oef_uri": rail.result("get_all_required_oefs")['netsuite_internal_id'],
                "middle_name_oef_uri": rail.result("get_all_required_oefs")['middle_name'],
                "department_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_departments"), "displayText", item.get("department"), "uri", None),
                "employee_type_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_employee_types"), "displayText", item.get("employee_type"), "uri", None),
            }
        )

        get_pending_supervisor_logs = rail.FilterLogEntriesOperator(
            task_id='get_pending_supervisor_logs',
            log='{{ result("create_supervisor_log") }}',
            severity='Pending',
            remove_filtered_entries=True
        )

        has_pending_supervisors = rail.IfOperator(
            task_id='has_pending_supervisors',
            test='{{ result("get_pending_supervisor_logs", "length") > 0 }}',
            yes_task='refresh_user_report_details',
            no_task='trigger_log_generation'
        )

        refresh_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='refresh_user_report_details',
            report_name=config.report_name,
        )

        get_refreshed_replicon_users = rail.RepliconServiceOperator(
            task_id='get_refreshed_replicon_users',
            endpoint='/services/ReportService1.svc/GenerateReport',
            data={
                "reportUri": '{{ result("refresh_user_report_details").uri }}',
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        refreshed_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="refreshed_report_payload_to_csv",
            document='{{ result("get_refreshed_replicon_users").payload }}'
        )

        create_refreshed_replicon_users_collection = rail.CreateCollectionOperator(
            task_id='create_refreshed_replicon_users_collection',
            source='{{ result("refreshed_report_payload_to_csv") }}',
            name='refreshed_replicon_users',
            columns={
                "User Name": "User_Name",
                "User Email": "User_Email",
                "User Status": "User_Status",
                "NetSuite Internal ID": "Internal_ID",
                "uri": "uri",
                "Location (Current)": "location"
            }
        )

        process_pending_supervisors = rail.trigger_parallel_dagrun(
            task_id='process_pending_supervisors',
            items='{{ result("get_pending_supervisor_logs") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_supervisor_child_dag_id,
            parallel_count= config.parallel_count,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'user_log': rail.result("create_log")
            }
        )

        finish_import = rail.EmptyOperator(
            task_id='finish_import'
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_child_dag_id,
            conf=lambda: {
                'log': rail.result("create_log"),
                'log_filename': rail.get_company_key() + '_logs_' + dt.now().strftime('%H%M%S') + "_" + rail.render_template("{{result('new_file_sensor') | file_name}}"),
            }
        )


        new_file_sensor >> is_csv

        is_csv >> rail.Label("Yes") >> download_input_file >> was_file_found
        is_csv >> rail.Label("No") >> send_bad_file_format_email

        was_file_found >> rail.Label("Yes") >> archive_input_file
        was_file_found >> rail.Label("No") >> delete_this_dagrun

        download_input_file >> load_csv_file >> create_log >> create_supervisor_log >> create_users_payload_collection >> if_user_payload_exists

        if_user_payload_exists >> rail.Label("Yes") >> query_blank_mandatory_fields >> if_blank_mandatory_fields
        if_user_payload_exists >> rail.Label("No") >> send_blank_payload_email

        if_blank_mandatory_fields >> rail.Label("Yes") >> write_blank_mandatory_fields_log >> query_mandatory_fields
        if_blank_mandatory_fields >> rail.Label("No") >> query_mandatory_fields >> if_mandatory_fields_data_exists

        if_mandatory_fields_data_exists >> rail.Label("Yes") >> get_all_departments
        if_mandatory_fields_data_exists >> rail.Label("No") >> get_pending_supervisor_logs >> has_pending_supervisors

        get_all_departments >> get_all_required_oefs >> query_unique_subsidiaries >> has_unique_subsidiaries

        has_unique_subsidiaries >> rail.Label("Yes") >> process_subsidiary_dropdowns >> wait_for_subsidiary_dropdowns >> query_unique_locations
        has_unique_subsidiaries >> rail.Label("No") >> query_unique_locations >> has_unique_locations

        has_unique_locations >> rail.Label("Yes") >> get_enabled_locations >> process_location_groups >> wait_for_location_groups >> get_user_report_details
        has_unique_locations >> rail.Label("No") >> get_user_report_details

        get_user_report_details >> get_all_replicon_users >> report_payload_to_csv >> create_replicon_users_collection
        create_replicon_users_collection >> get_all_employee_types >> query_new_users >> has_new_users

        has_new_users >> rail.Label("Yes") >> start_processing_new_users >> process_add_users >> query_disabled_users_in_replicon
        has_new_users >> rail.Label("No") >> query_disabled_users_in_replicon >> has_disabled_users

        has_disabled_users >> rail.Label("Yes") >> log_disabled_users >> query_existing_users
        has_disabled_users >> rail.Label("No") >> query_existing_users >> has_existing_users

        has_existing_users >> rail.Label("Yes") >> start_processing_update_users >> process_update_users >> get_pending_supervisor_logs >> has_pending_supervisors
        has_existing_users >> rail.Label("No") >> get_pending_supervisor_logs

        has_pending_supervisors >> rail.Label("Yes") >> refresh_user_report_details >> get_refreshed_replicon_users >> refreshed_report_payload_to_csv >> create_refreshed_replicon_users_collection >> process_pending_supervisors >> finish_import >> trigger_log_generation
        has_pending_supervisors >> rail.Label("No") >> trigger_log_generation

    return dag


rail.for_each_instance(create_main_dag)
