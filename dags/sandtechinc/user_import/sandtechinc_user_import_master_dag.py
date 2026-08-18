"""
Sand Tech Inc - User Import Master DAG
HiBob HR -> Replicon Polaris PSA Integration
"""

from datetime import timedelta
import hashlib
import pendulum
from airflow.models import Variable
import rail
from rail.lib.log import get_master_log_artifact_name
from rail.lib.ecid import get_dagrun_ecid
import chardet
from rail.lib.artifact import existing_artifact

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.main_dagid,
        description=f'Sand Tech Inc - User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        # ========== FILE SENSOR ==========
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now(
                config.est_timezone).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='validate_file_extension'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='validate_file_extension',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ========== FILE VALIDATION ==========
        validate_file_extension = rail.IfOperator(
            task_id="validate_file_extension",
            test='{{ result("new_file_sensor").split(".")[-1] | lower == "csv" if result("new_file_sensor") else False }}',
            yes_task="download_input_file",
            no_task="archive_incorrect_file",
        )

        archive_incorrect_file = rail.SFTPMoveFileOperator(
            task_id='archive_incorrect_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Invalid_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        send_mail_incorrect_file = rail.EmailOperator(
            task_id='send_mail_incorrect_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Import - Invalid File Format {{ current_time_in_specified_tz("US/Eastern") }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong></p>
            <p>Hello,</p>
            <p>The Replicon user import for {{ get_company_key() }} was skipped because the file format is incorrect.</p>
            <p>File name: {{ result('new_file_sensor') | file_name }}</p>
            <p>Please provide the input file in CSV format.</p>
            <p>For any queries, please contact our support team at https://support.deltek.com</p>
            <p>Regards,<br/>Deltek Inc.</p>''',
        )

        # ========== DOWNLOAD AND PARSE INPUT FILE ==========
        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )
        
        def find_file_encoding_callable(task_id):
            feed_file = rail.result(task_id)
            with existing_artifact(feed_file) as ff:
                return chardet.detect_all(ff.file.read())

        find_file_encoding = rail.PythonOperator(
            task_id="find_file_encoding",
            python_callable=find_file_encoding_callable,
            op_args=[download_input_file.task_id]
        )

        parse_input_csv = rail.LoadCSVFileOperator(
            task_id="parse_input_csv",
            document="{{ result('download_input_file') }}",
            encoding="{{ result('find_file_encoding')[0].encoding }}"
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        # ========== MD5 HASH GENERATION ==========
        def get_formatted_user_row(item):
            """Generate MD5 hash for each user record"""
            # Concatenate all fields for MD5 calculation
            hash_string = '_'.join([
                str(item.get("Employee ID", "") or "").strip(),
                str(item.get("First name", "") or "").strip(),
                str(item.get("Last name", "") or "").strip(),
                str(item.get("Display name", "") or "").strip(),
                str(item.get("Email", "") or "").strip(),
                str(item.get("Start date", "") or "").strip(),
                str(item.get("Last day of work", "") or "").strip(),
                str(item.get("Job title", "") or "").strip(),
                str(item.get("Job title/Effective date", "") or "").strip(),
                str(item.get("Manager's email", "") or "").strip(),
                str(item.get("Reports to/Effective date", "") or "").strip(),
                str(item.get("Department", "") or "").strip(),
                str(item.get("Department/Effective date", "") or "").strip(),
                str(item.get("Site", "") or "").strip(),
                str(item.get("Site/Effective date", "") or "").strip(),
                str(item.get("Is a manager", "") or "").strip()
            ])
            user_md5 = hashlib.md5(hash_string.encode()).hexdigest()

            return {
                "employee_id": str(item.get("Employee ID", "") or "").strip(),
                "first_name": str(item.get("First name", "") or "").strip(),
                "last_name": str(item.get("Last name", "") or "").strip(),
                "display_name": str(item.get("Display name", "") or "").strip(),
                "email": str(item.get("Email", "") or "").strip(),
                "start_date": str(item.get("Start date", "") or "").strip(),
                "last_day_of_work": str(item.get("Last day of work", "") or "").strip(),
                "job_title": str(item.get("Job title", "") or "").strip(),
                "job_title_effective_date": str(item.get("Job title/Effective date", "") or "").strip(),
                "manager_email": str(item.get("Manager's email", "") or "").strip(),
                "reports_to_effective_date": str(item.get("Reports to/Effective date", "") or "").strip(),
                "department": str(item.get("Department", "") or "").strip(),
                "department_effective_date": str(item.get("Department/Effective date", "") or "").strip(),
                "site": str(item.get("Site", "") or "").strip(),
                "site_effective_date": str(item.get("Site/Effective date", "") or "").strip(),
                "is_a_manager": str(item.get("Is a manager", "") or "").strip(),
                "md5": user_md5
            }.values()

        create_input_with_md5 = rail.WriteCSVFileOperator(
            task_id='create_input_with_md5',
            source="{{ result('parse_input_csv') }}",
            header=[
                'employee_id', 'first_name', 'last_name', 'display_name', 'email',
                'start_date', 'last_day_of_work', 'job_title', 'job_title_effective_date',
                'manager_email', 'reports_to_effective_date', 'department',
                'department_effective_date', 'site', 'site_effective_date',
                'is_a_manager', 'md5'
            ],
            row=get_formatted_user_row
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id='create_input_collection',
            source="{{ result('create_input_with_md5') }}",
            name="input_users",
            columns={
                'employee_id': 'employee_id',
                'first_name': 'first_name',
                'last_name': 'last_name',
                'display_name': 'display_name',
                'email': 'email',
                'start_date': 'start_date',
                'last_day_of_work': 'last_day_of_work',
                'job_title': 'job_title',
                'job_title_effective_date': 'job_title_effective_date',
                'manager_email': 'manager_email',
                'reports_to_effective_date': 'reports_to_effective_date',
                'department': 'department',
                'department_effective_date': 'department_effective_date',
                'site': 'site',
                'site_effective_date': 'site_effective_date',
                'is_a_manager': 'is_a_manager',
                'md5': 'md5'
            }
        )

        query_input_records = rail.QueryCollectionOperator(
            task_id='query_input_records',
            query="SELECT * FROM input_users",
        )

        has_input_records = rail.IfOperator(
            task_id='has_input_records',
            test='{{ result("query_input_records", "length") > 0 }}',
            yes_task="list_reference_files",
            no_task="log_to_sumo",
        )

        # ========== REFERENCE FILE HANDLING ==========
        list_reference_files = rail.SFTPListFilesOperator(
            task_id="list_reference_files",
            paths=[config.reference_filepath],
        )

        def has_reference_file(result_task_id, file_path):
            data = rail.result(result_task_id)
            if not data or file_path not in data:
                return False
            return len(data[file_path]) > 0

        has_reference_file_check = rail.IfOperator(
            task_id="has_reference_file_check",
            test=lambda: has_reference_file("list_reference_files", config.reference_filepath),
            yes_task="download_reference_file",
            no_task="process_all_as_new"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_filepath + "/" + config.reference_file_name
        )

        parse_reference_csv = rail.LoadCSVFileOperator(
            task_id="parse_reference_csv",
            document="{{ result('download_reference_file') }}"
        )

        create_reference_collection = rail.CreateCollectionOperator(
            task_id='create_reference_collection',
            source="{{ result('parse_reference_csv') }}",
            name="reference_users",
            columns={
                'employee_id': 'employee_id',
                'first_name': 'first_name',
                'last_name': 'last_name',
                'display_name': 'display_name',
                'email': 'email',
                'start_date': 'start_date',
                'last_day_of_work': 'last_day_of_work',
                'job_title': 'job_title',
                'job_title_effective_date': 'job_title_effective_date',
                'manager_email': 'manager_email',
                'reports_to_effective_date': 'reports_to_effective_date',
                'department': 'department',
                'department_effective_date': 'department_effective_date',
                'site': 'site',
                'site_effective_date': 'site_effective_date',
                'is_a_manager': 'is_a_manager',
                'md5': 'md5'
            }
        )

        # ========== IDENTIFY CHANGED RECORDS ==========
        query_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="SELECT * FROM input_users WHERE input_users.md5 IN (SELECT reference_users.md5 FROM reference_users)",
        )

        has_unchanged_records = rail.IfOperator(
            task_id='has_unchanged_records',
            test='{{ result("query_unchanged_records", "length") > 0 }}',
            yes_task="log_unchanged_records",
            no_task="query_changed_records",
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id='log_unchanged_records',
            message="Unchanged Records - Skipped",
            items="{{ result('query_unchanged_records') }}",
            severity="Ignored",
            properties={
                "Empid": "{{ item.employee_id }}",
                "Username": "{{ item.first_name }} {{ item.last_name }}",
                "Action": "Pre-check",
                "Status": "Ignored",
                "Details": "No changes detected in user record",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        query_changed_records = rail.QueryCollectionOperator(
            task_id='query_changed_records',
            query="SELECT * FROM input_users WHERE input_users.md5 NOT IN (SELECT reference_users.md5 FROM reference_users)",
        )

        # ========== PROCESS ALL AS NEW (First Run) ==========
        process_all_as_new = rail.QueryCollectionOperator(
            task_id='process_all_as_new',
            query="SELECT * FROM input_users",
        )

        # ========== MANDATORY FIELD VALIDATION ==========
        create_changed_records_collection = rail.CreateCollectionOperator(
            task_id='create_changed_records_collection',
            source="{{ result('query_changed_records') if result('query_changed_records') else result('process_all_as_new') }}",
            name="changed_records",
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query="""SELECT * FROM changed_records WHERE 
                (changed_records.employee_id = "" OR changed_records.employee_id IS NULL OR
                 changed_records.first_name = "" OR changed_records.first_name IS NULL OR
                 changed_records.last_name = "" OR changed_records.last_name IS NULL OR
                 changed_records.email = "" OR changed_records.email IS NULL OR
                 changed_records.start_date = "" OR changed_records.start_date IS NULL)""",
        )

        has_invalid_records = rail.IfOperator(
            task_id='has_invalid_records',
            test='{{ result("query_invalid_records", "length") > 0 }}',
            yes_task="log_invalid_records",
            no_task="query_valid_records",
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            message="Invalid Records - Missing Mandatory Fields",
            items="{{ result('query_invalid_records') }}",
            severity="Exception",
            properties={
                "Empid": "{{ item.employee_id }}",
                "Username": "{{ item.first_name }} {{ item.last_name }}",
                "Action": "Pre-check",
                "Status": "Exception",
                "Details": "Missing mandatory fields (Employee ID, First name, Last name, Email, or Start date)",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM changed_records WHERE 
                (changed_records.employee_id != "" AND changed_records.employee_id IS NOT NULL AND
                 changed_records.first_name != "" AND changed_records.first_name IS NOT NULL AND
                 changed_records.last_name != "" AND changed_records.last_name IS NOT NULL AND
                 changed_records.email != "" AND changed_records.email IS NOT NULL AND
                 changed_records.start_date != "" AND changed_records.start_date IS NOT NULL)""",
        )

        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test='{{ result("query_valid_records", "length") > 0 }}',
            yes_task="gather_replicon_metadata.get_all_permissions",
            no_task="update_reference_file",
        )

        # ========== HELPER FUNCTION FOR GetActiveRoles RESPONSE ==========
        def extract_roles_from_response(response):
            """Extract roles list from GetActiveRoles response which returns {"d": [...]}"""
            data = response.json()
            if isinstance(data, dict) and 'd' in data:
                return data['d']
            if isinstance(data, list):
                return data
            return []

        # ========== GATHER REPLICON METADATA ==========
        with rail.TaskGroup(group_id='gather_replicon_metadata') as gather_replicon_metadata:

            get_all_permissions = rail.RepliconServiceOperator(
                task_id='get_all_permissions',
                endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
                data=None
            )

            get_all_timezones = rail.RepliconServiceOperator(
                task_id='get_all_timezones',
                endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
                data=None
            )

            get_all_holiday_calendars = rail.RepliconServiceOperator(
                task_id='get_all_holiday_calendars',
                endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
                data=None
            )

            get_all_policy_sets = rail.RepliconServiceOperator(
                task_id='get_all_policy_sets',
                endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
                data=None
            )

            get_all_approval_paths = rail.RepliconServiceOperator(
                task_id='get_all_approval_paths',
                endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
                data=None
            )

            get_all_timeoff_approval_paths = rail.RepliconServiceOperator(
                task_id='get_all_timeoff_approval_paths',
                endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
                data=None
            )

            get_active_roles = rail.RepliconServiceOperator(
                task_id='get_active_roles',
                endpoint="/services/ProjectRoleService1.svc/GetActiveRoles",
                data=None,
                response_filter=extract_roles_from_response
            )

            def get_filtered_departments(response):
                data = response.json()['d']['rows']
                return [{
                    "name": item['cells'][0].get('textValue'),
                    "uri": item['cells'][0].get('uri'),
                } for item in data] if data else []

            get_all_departments = rail.RepliconServiceOperator(
                task_id='get_all_departments',
                endpoint="/services/DepartmentGroupListService1.svc/GetData",
                data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris": ["urn:replicon:department-group-list-column:department-group"],
                    "sort": [],
                    "filterExpression": null
                },
                response_filter=get_filtered_departments
            )

            get_all_locations = rail.RepliconServiceOperator(
                task_id='get_all_locations',
                endpoint="/services/LocationService1.svc/GetEnabledLocations",
                data=None
            )

            get_all_employee_types = rail.RepliconServiceOperator(
                task_id='get_all_employee_types',
                endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
                data=None
            )

            def get_filtered_timesheet_periods(response):
                data = response.json()['d']['rows']
                return [{
                    "name": item['cells'][0]['textValue'],
                    "uri": item['cells'][0].get('uri'),
                } for item in data] if data else []

            get_all_timesheet_periods = rail.RepliconServiceOperator(
                task_id='get_all_timesheet_periods',
                endpoint="/services/TimesheetPeriodListService1.svc/GetData",
                data={
                    "page": 1,
                    "pagesize": 1000,
                    "columnUris": ["urn:replicon:timesheet-period-list-column:timesheet-period"],
                    "sort": [],
                    "filterExpression": null
                },
                response_filter=get_filtered_timesheet_periods
            )

            get_all_timeoff_types = rail.RepliconServiceOperator(
                task_id='get_all_timeoff_types',
                endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
                data=None,
                response_filter=extract_roles_from_response
            )

            get_all_permissions >> get_all_timezones >> get_all_holiday_calendars >> \
                get_all_policy_sets >> get_all_approval_paths >> get_all_timeoff_approval_paths >> \
                get_active_roles >> get_all_departments >> get_all_locations >> \
                get_all_employee_types >> get_all_timesheet_periods >> get_all_timeoff_types

        # ========== PROCESS ROLES (CREATE MISSING) ==========
        query_unique_job_titles = rail.QueryCollectionOperator(
            task_id='query_unique_job_titles',
            query="SELECT DISTINCT changed_records.job_title FROM changed_records WHERE changed_records.job_title != '' AND changed_records.job_title IS NOT NULL",
        )

        def get_missing_roles():
            """Identify job titles that don't exist in Replicon"""
            input_titles = []
            job_titles_doc = rail.result('query_unique_job_titles')
            
            def get_data_from_document(document):
                with rail.lib.readers.get_data_reader(document) as reader:
                    return list(reader)
            
            job_title_records = get_data_from_document(job_titles_doc)
            for record in job_title_records:
                if record.get('job_title'):
                    input_titles.append(record['job_title'])
            
            existing_roles = rail.result('gather_replicon_metadata.get_active_roles')
            existing_role_names = [role.get('displayText', '').lower() for role in existing_roles] if existing_roles else []
            
            missing_roles = []
            for title in input_titles:
                if title.lower() not in existing_role_names:
                    missing_roles.append({"name": title})
            
            return missing_roles

        identify_missing_roles = rail.PythonOperator(
            task_id='identify_missing_roles',
            python_callable=get_missing_roles
        )

        has_missing_roles = rail.IfOperator(
            task_id='has_missing_roles',
            test='{{ result("identify_missing_roles") | length > 0 }}',
            yes_task="trigger_create_roles",
            no_task="get_existing_users_report",
        )

        trigger_create_roles = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_create_roles',
            retries=0,
            items="{{ result('identify_missing_roles') | to_json }}",
            trigger_dag_id=config.create_role_child_dagid,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "role_name": item['name']
            }
        )

        wait_for_role_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_role_creation',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_create_roles") }}'
        )

        # Refresh roles after creation
        refresh_active_roles = rail.RepliconServiceOperator(
            task_id='refresh_active_roles',
            endpoint="/services/ProjectRoleService1.svc/GetActiveRoles",
            data=None,
            response_filter=extract_roles_from_response
        )

        # ========== GET EXISTING USERS FROM REPLICON ==========
        get_existing_users_report = rail.RepliconReportDetailsOperator(
            task_id='get_existing_users_report',
            report_name='User list - For Integration',
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_users_report',
            report_params={
                "reportParameters": [{
                    "reportUri": "{{ result('get_existing_users_report').uri }}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_users_report.get_report_result', 'has_data') }}",
            yes_task='parse_users_report',
            no_task='create_empty_users_collection'
        )

        parse_users_report = rail.LoadCSVFileOperator(
            task_id='parse_users_report',
            document="{{ result('run_users_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_users_collection = rail.CreateCollectionOperator(
            task_id='create_users_collection',
            name='existing_users',
            source="{{ result('parse_users_report') }}",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'UserUri': 'useruri',
                'User Status': 'status'
            }
        )

        create_empty_users_collection = rail.CreateCollectionOperator(
            task_id='create_empty_users_collection',
            name='existing_users',
            source="[]",
        )

        # ========== SUPERVISOR PROCESSING LOG ==========
        supervisor_processing_log = rail.CreateLogOperator(
            task_id='supervisor_processing_log',
        )

        declare_user_dag_runs = rail.SetVariableOperator(
            task_id='declare_user_dag_runs',
            name='user_process_dag_runs',
            value=[]
        )

        # ========== PROCESS EACH USER ==========
        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_existing_user_info(user_record):
            """Check if user exists by Employee ID, then by Login Name (email)"""
            employee_id = user_record['employee_id']
            email = user_record['email']
            
            users_collection = get_data_from_document(
                rail.result('create_users_collection') if rail.result('create_users_collection') else rail.result('create_empty_users_collection')
            )
            
            # First search by Employee ID
            user_by_empid = None
            for user in users_collection:
                if user.get('employeeid') == employee_id:
                    user_by_empid = user
                    break
            
            if user_by_empid:
                return {
                    "found": True,
                    "useruri": user_by_empid.get('useruri'),
                    "loginname": user_by_empid.get('loginname'),
                    "match_type": "employee_id"
                }
            
            # If not found by Employee ID, check by Login Name (email)
            user_by_email = None
            for user in users_collection:
                if user.get('loginname', '').lower() == email.lower():
                    user_by_email = user
                    break
            
            if user_by_email:
                # User exists with same email but different Employee ID - this is an exception
                return {
                    "found": True,
                    "useruri": user_by_email.get('useruri'),
                    "loginname": user_by_email.get('loginname'),
                    "existing_empid": user_by_email.get('employeeid'),
                    "match_type": "email_mismatch"
                }
            
            # User does not exist
            return {
                "found": False,
                "useruri": None,
                "loginname": None,
                "match_type": "new"
            }

        foreach_valid_record = rail.ForEachOperator(
            task_id='foreach_valid_record',
            items="{{ result('query_valid_records') }}",
            start_task='check_user_exists',
            end_task='foreach_valid_record_end'
        )

        check_user_exists = rail.PythonOperator(
            task_id='check_user_exists',
            python_callable=lambda: get_existing_user_info(rail.result('foreach_valid_record'))
        )

        is_email_mismatch = rail.IfOperator(
            task_id='is_email_mismatch',
            test='{{ result("check_user_exists").match_type == "email_mismatch" }}',
            yes_task="log_email_mismatch_exception",
            no_task="is_new_user",
        )

        log_email_mismatch_exception = rail.WriteLogOperator(
            task_id='log_email_mismatch_exception',
            message="Exception - Email exists with different Employee ID",
            severity="Exception",
            properties={
                "Empid": "{{ result('foreach_valid_record').employee_id }}",
                "Username": "{{ result('foreach_valid_record').first_name }} {{ result('foreach_valid_record').last_name }}",
                "Action": "Add",
                "Status": "Exception",
                "Details": "User not added - Login name {{ result('foreach_valid_record').email }} already exists with Employee ID {{ result('check_user_exists').existing_empid }}",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        is_new_user = rail.IfOperator(
            task_id='is_new_user',
            test='{{ result("check_user_exists").found == False }}',
            yes_task="trigger_add_user",
            no_task="trigger_update_user",
        )

        # ========== TRIGGER ADD USER ==========
        def get_uri_by_name(items, name_field, name_value, uri_field='uri'):
            """Helper to find URI by name in a list"""
            if not items or not name_value:
                return None
            for item in items:
                if item.get(name_field, '').lower() == name_value.lower():
                    return item.get(uri_field)
                if item.get('displayText', '').lower() == name_value.lower():
                    return item.get('uri')
            return None

        trigger_add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_add_user',
            retries=0,
            items=[-1],
            trigger_dag_id=config.add_user_child_dagid,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "employee_id": rail.result('foreach_valid_record')['employee_id'],
                "first_name": rail.result('foreach_valid_record')['first_name'],
                "last_name": rail.result('foreach_valid_record')['last_name'],
                "display_name": rail.result('foreach_valid_record')['display_name'],
                "email": rail.result('foreach_valid_record')['email'],
                "start_date": rail.result('foreach_valid_record')['start_date'],
                "last_day_of_work": rail.result('foreach_valid_record')['last_day_of_work'],
                "job_title": rail.result('foreach_valid_record')['job_title'],
                "job_title_effective_date": rail.result('foreach_valid_record')['job_title_effective_date'],
                "manager_email": rail.result('foreach_valid_record')['manager_email'],
                "reports_to_effective_date": rail.result('foreach_valid_record')['reports_to_effective_date'],
                "department": rail.result('foreach_valid_record')['department'],
                "department_effective_date": rail.result('foreach_valid_record')['department_effective_date'],
                "site": rail.result('foreach_valid_record')['site'],
                "site_effective_date": rail.result('foreach_valid_record')['site_effective_date'],
                "is_a_manager": rail.result('foreach_valid_record')['is_a_manager'],
                # URIs resolved from metadata
                "department_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_departments'),
                    'name', rail.result('foreach_valid_record')['department']),
                "location_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_locations'),
                    'displayText', rail.result('foreach_valid_record')['site']),
                "role_uri": get_uri_by_name(
                    rail.result('refresh_active_roles') if rail.result('refresh_active_roles') else rail.result('gather_replicon_metadata.get_active_roles'),
                    'displayText', rail.result('foreach_valid_record')['job_title']),
                "holiday_calendar_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_holiday_calendars'),
                    'displayText', rail.result('foreach_valid_record')['site']),
                "timezone_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_timezones'),
                    'displayText', config.default_timezone),
                "timesheet_template_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_policy_sets'),
                    'displayText', config.default_timesheet_template),
                "timeoff_template_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_policy_sets'),
                    'displayText', config.default_timeoff_template),
                "timesheet_approval_path_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_approval_paths'),
                    'displayText', config.default_timesheet_approval_path),
                "timeoff_approval_path_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_timeoff_approval_paths'),
                    'displayText', config.default_timeoff_approval_path),
                "timesheet_period_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_timesheet_periods'),
                    'name', config.default_timesheet_period),
                "employee_type_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_employee_types'),
                    'displayText', config.default_employee_type),
                "project_resource_permission_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_permissions'),
                    'name', config.project_resource_permission),
                "supervisor_permission_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_permissions'),
                    'name', config.supervisor_permission),
                "supervisor_processing_log": rail.result('supervisor_processing_log'),
                "date_format": config.date_format
            }
        )

        # ========== TRIGGER UPDATE USER ==========
        trigger_update_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_update_user',
            retries=0,
            items=[-1],
            trigger_dag_id=config.update_user_child_dagid,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "employee_id": rail.result('foreach_valid_record')['employee_id'],
                "first_name": rail.result('foreach_valid_record')['first_name'],
                "last_name": rail.result('foreach_valid_record')['last_name'],
                "display_name": rail.result('foreach_valid_record')['display_name'],
                "email": rail.result('foreach_valid_record')['email'],
                "start_date": rail.result('foreach_valid_record')['start_date'],
                "last_day_of_work": rail.result('foreach_valid_record')['last_day_of_work'],
                "job_title": rail.result('foreach_valid_record')['job_title'],
                "job_title_effective_date": rail.result('foreach_valid_record')['job_title_effective_date'],
                "manager_email": rail.result('foreach_valid_record')['manager_email'],
                "reports_to_effective_date": rail.result('foreach_valid_record')['reports_to_effective_date'],
                "department": rail.result('foreach_valid_record')['department'],
                "department_effective_date": rail.result('foreach_valid_record')['department_effective_date'],
                "site": rail.result('foreach_valid_record')['site'],
                "site_effective_date": rail.result('foreach_valid_record')['site_effective_date'],
                "is_a_manager": rail.result('foreach_valid_record')['is_a_manager'],
                "useruri": rail.result('check_user_exists')['useruri'],
                # URIs resolved from metadata
                "department_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_departments'),
                    'name', rail.result('foreach_valid_record')['department']),
                "location_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_locations'),
                    'displayText', rail.result('foreach_valid_record')['site']),
                "role_uri": get_uri_by_name(
                    rail.result('refresh_active_roles') if rail.result('refresh_active_roles') else rail.result('gather_replicon_metadata.get_active_roles'),
                    'displayText', rail.result('foreach_valid_record')['job_title']),
                "holiday_calendar_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_holiday_calendars'),
                    'displayText', rail.result('foreach_valid_record')['site']),
                "supervisor_permission_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_permissions'),
                    'name', config.supervisor_permission),
                "supervisor_processing_log": rail.result('supervisor_processing_log'),
                "date_format": config.date_format
            }
        )

        insert_to_dag_run_list = rail.SetVariableOperator(
            task_id='insert_to_dag_run_list',
            append=True,
            name='{{ result("declare_user_dag_runs").name }}',
            value='{{ (result("trigger_update_user") or result("trigger_add_user"))[0] }}'
        )

        foreach_valid_record_end = rail.EmptyOperator(
            task_id='foreach_valid_record_end',
        )

        # ========== WAIT FOR USER PROCESSING ==========
        has_user_dag_runs = rail.IfOperator(
            task_id='has_user_dag_runs',
            test='{{ result("insert_to_dag_run_list") | is_truthy }}',
            yes_task="wait_for_user_processing",
            no_task="process_pending_supervisors",
        )

        wait_for_user_processing = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_processing',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_dag_run_list").value | to_json }}'
        )

        # ========== PROCESS PENDING SUPERVISORS ==========
        def get_supervisor_entries():
            supervisor_details = []
            log_doc = rail.result('supervisor_processing_log')
            
            def get_data_from_document(document):
                with rail.lib.readers.get_data_reader(document) as reader:
                    return list(reader)
            
            supervisor_log_entries = get_data_from_document(log_doc)
            for entry in supervisor_log_entries:
                if entry.get('properties'):
                    supervisor_details.append({
                        "employee_id": entry['properties'].get('employee_id'),
                        "username": entry['properties'].get('username'),
                        "manager_email": entry['properties'].get('manager_email'),
                        "useruri": entry['properties'].get('useruri'),
                        "action": entry['properties'].get('action'),
                        "effective_date": entry['properties'].get('effective_date')
                    })
            return supervisor_details

        process_pending_supervisors = rail.PythonOperator(
            task_id='process_pending_supervisors',
            python_callable=get_supervisor_entries
        )

        has_pending_supervisors = rail.IfOperator(
            task_id='has_pending_supervisors',
            test='{{ result("process_pending_supervisors") | length > 0 }}',
            yes_task="declare_supervisor_dag_runs",
            no_task="update_reference_file",
        )

        declare_supervisor_dag_runs = rail.SetVariableOperator(
            task_id='declare_supervisor_dag_runs',
            name='supervisor_dag_runs',
            value=[]
        )

        foreach_pending_supervisor = rail.ForEachOperator(
            task_id='foreach_pending_supervisor',
            items="{{ result('process_pending_supervisors') | to_json }}",
            start_task='trigger_supervisor_assignment',
            end_task='foreach_pending_supervisor_end'
        )

        trigger_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_supervisor_assignment',
            retries=0,
            items=[-1],
            trigger_dag_id=config.supervisor_child_dagid,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "employee_id": rail.result('foreach_pending_supervisor')['employee_id'],
                "username": rail.result('foreach_pending_supervisor')['username'],
                "manager_email": rail.result('foreach_pending_supervisor')['manager_email'],
                "useruri": rail.result('foreach_pending_supervisor')['useruri'],
                "action": rail.result('foreach_pending_supervisor')['action'],
                "effective_date": rail.result('foreach_pending_supervisor')['effective_date'],
                "supervisor_permission_uri": get_uri_by_name(
                    rail.result('gather_replicon_metadata.get_all_permissions'),
                    'name', config.supervisor_permission),
                "date_format": config.date_format
            }
        )

        insert_supervisor_dag_run = rail.SetVariableOperator(
            task_id='insert_supervisor_dag_run',
            append=True,
            name='{{ result("declare_supervisor_dag_runs").name }}',
            value='{{ result("trigger_supervisor_assignment")[0] }}'
        )

        foreach_pending_supervisor_end = rail.EmptyOperator(
            task_id='foreach_pending_supervisor_end',
        )

        wait_for_supervisor_processing = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_processing',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_supervisor_dag_run").value | to_json }}'
        )

        # ========== UPDATE REFERENCE FILE ==========
        update_reference_file = rail.IfOperator(
            task_id='update_reference_file',
            test=lambda: has_reference_file("list_reference_files", config.reference_filepath),
            yes_task="archive_old_reference",
            no_task="upload_new_reference",
        )

        archive_old_reference = rail.SFTPMoveFileOperator(
            task_id='archive_old_reference',
            existing_filename=config.reference_filepath + "/" + config.reference_file_name,
            new_filename=config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-') }}_Old_" + config.reference_file_name
        )

        upload_new_reference = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference',
            content="{{ result('create_input_with_md5') }}",
            remote_filepath=config.reference_filepath + "/" + config.reference_file_name,
        )

        # ========== GENERATE AND UPLOAD LOGS ==========
        def format_logs():
            context = get_master_log_artifact_name(rail.get_current_context())
            user_import_log = rail.load_all_records(context)
            
            unique_employees = list(set(
                entry['properties'].get('Empid', '') for entry in user_import_log if entry.get('properties')
            ))
            
            logs = []
            for emp_id in unique_employees:
                if emp_id:
                    emp_logs = [e for e in user_import_log if e.get('properties', {}).get('Empid') == emp_id]
                    if emp_logs:
                        first = emp_logs[0]
                        details = " | ".join(set(e['properties'].get('Details', '') for e in emp_logs if e.get('properties', {}).get('Details')))
                        status = ";".join(set(e['properties'].get('Status', '') for e in emp_logs if e.get('properties', {}).get('Status')))
                        logs.append({
                            "Empid": emp_id,
                            "Username": first['properties'].get('Username', ''),
                            "Action": first['properties'].get('Action', ''),
                            "Status": status,
                            "Details": details,
                            "Jobid": first.get('ecid', '')
                        })
            return logs

        format_import_logs = rail.PythonOperator(
            task_id='format_import_logs',
            python_callable=format_logs
        )

        create_log_csv = rail.WriteCSVFileOperator(
            task_id='create_log_csv',
            source="{{ result('format_import_logs') | to_json }}",
            header=['Empid', 'Username', 'Action', 'Status', 'Details', 'Jobid'],
            row=[
                '{{ item.Empid }}',
                '{{ item.Username }}',
                '{{ item.Action }}',
                '{{ item.Status }}',
                '{{ item.Details }}',
                '{{ item.Jobid }}'
            ]
        )

        upload_log_file = rail.SFTPUploadFileOperator(
            task_id='upload_log_file',
            content="{{ result('create_log_csv') }}",
            remote_filepath=config.log_filepath + "/{{ dag_run_ecid() | replace(':', '-') }}_UserImportLogs_{{ result('get_time_for_file') }}.csv",
        )

        # ========== SEND COMPLETION EMAIL ==========
        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            severity='Exception',
        )

        def get_email_subject():
            has_errors = rail.render_template('{{ result("get_logged_errors", key="length") > 0 }}')
            has_exceptions = rail.render_template('{{ result("get_logged_exceptions", key="length") > 0 }}')
            if has_errors == 'True':
                return "completed with errors"
            elif has_exceptions == 'True':
                return "completed with exceptions"
            return "completed successfully"

        determine_email_subject = rail.PythonOperator(
            task_id='determine_email_subject',
            python_callable=get_email_subject
        )

        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', key='length') > 0 -%}{{ params.alert_email }}{%- else -%}{{ params.internal_email }}{%- endif -%}",
            subject='{{ get_company_key() }} | Replicon User Import {{ result("determine_email_subject") }} - {{ current_time_in_specified_tz("US/Eastern") }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong></p>
            <p>Hello,</p>
            <p>The Replicon user import {{ result("determine_email_subject") }} on {{ current_time_in_specified_tz("US/Eastern") }}.</p>
            <p>Log file details:</p>
            <ul>
                <li>File path: {{ params.log_filepath }}</li>
                <li>File name: {{ dag_run_ecid() | replace(':', '-') }}_UserImportLogs_{{ result('get_time_for_file') }}.csv</li>
            </ul>
            <p>For any queries, please contact our support team at https://support.deltek.com</p>
            <p>Regards,<br/>Deltek Inc.</p>''',
            params={
                'log_filepath': config.log_filepath,
                'alert_email': config.alert_email,
                'internal_email': config.internal_logs_email
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ========== TASK DEPENDENCIES ==========
        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> validate_file_extension

        validate_file_extension >> rail.Label('No') >> archive_incorrect_file >> send_mail_incorrect_file >> log_to_sumo
        validate_file_extension >> rail.Label('Yes') >> download_input_file >> find_file_encoding >> parse_input_csv >> archive_input_file >> \
            create_input_with_md5 >> create_input_collection >> query_input_records >> has_input_records

        has_input_records >> rail.Label('No') >> log_to_sumo
        has_input_records >> rail.Label('Yes') >> list_reference_files >> has_reference_file_check

        has_reference_file_check >> rail.Label('Yes') >> download_reference_file >> parse_reference_csv >> \
            create_reference_collection >> query_unchanged_records >> has_unchanged_records
        has_reference_file_check >> rail.Label('No') >> process_all_as_new >> create_changed_records_collection

        has_unchanged_records >> rail.Label('Yes') >> log_unchanged_records >> query_changed_records
        has_unchanged_records >> rail.Label('No') >> query_changed_records >> create_changed_records_collection

        create_changed_records_collection >> query_invalid_records >> has_invalid_records
        has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> query_valid_records
        has_invalid_records >> rail.Label('No') >> query_valid_records >> has_valid_records

        has_valid_records >> rail.Label('No') >> update_reference_file
        has_valid_records >> rail.Label('Yes') >> gather_replicon_metadata >> query_unique_job_titles >> \
            identify_missing_roles >> has_missing_roles

        has_missing_roles >> rail.Label('Yes') >> trigger_create_roles >> wait_for_role_creation >> \
            refresh_active_roles >> get_existing_users_report
        has_missing_roles >> rail.Label('No') >> get_existing_users_report >> run_report_group_entry

        run_report_group_exit >> report_has_data
        report_has_data >> rail.Label('Yes') >> parse_users_report >> create_users_collection >> \
            supervisor_processing_log >> declare_user_dag_runs >> foreach_valid_record
        report_has_data >> rail.Label('No') >> create_empty_users_collection >> supervisor_processing_log

        foreach_valid_record >> check_user_exists >> is_email_mismatch
        is_email_mismatch >> rail.Label('Yes') >> log_email_mismatch_exception >> foreach_valid_record_end
        is_email_mismatch >> rail.Label('No') >> is_new_user

        is_new_user >> rail.Label('Yes') >> trigger_add_user >> insert_to_dag_run_list >> foreach_valid_record_end
        is_new_user >> rail.Label('No') >> trigger_update_user >> insert_to_dag_run_list >> foreach_valid_record_end

        foreach_valid_record >> foreach_valid_record_end >> has_user_dag_runs
        has_user_dag_runs >> rail.Label('Yes') >> wait_for_user_processing >> process_pending_supervisors
        has_user_dag_runs >> rail.Label('No') >> process_pending_supervisors >> has_pending_supervisors

        has_pending_supervisors >> rail.Label('Yes') >> declare_supervisor_dag_runs >> foreach_pending_supervisor >> \
            trigger_supervisor_assignment >> insert_supervisor_dag_run >> foreach_pending_supervisor_end
        has_pending_supervisors >> rail.Label('No') >> update_reference_file

        foreach_pending_supervisor >> foreach_pending_supervisor_end >> wait_for_supervisor_processing >> update_reference_file

        update_reference_file >> rail.Label('Yes') >> archive_old_reference >> upload_new_reference
        update_reference_file >> rail.Label('No') >> upload_new_reference >> format_import_logs >> \
            create_log_csv >> upload_log_file >> get_logged_errors >> get_logged_exceptions >> \
            determine_email_subject >> send_completion_email >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)