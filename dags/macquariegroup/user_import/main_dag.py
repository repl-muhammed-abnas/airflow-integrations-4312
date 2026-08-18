from datetime import timedelta
import rail
from macquariegroup.user_import.utils.data_handlers import get_value, get_holiday_date_list
from macquariegroup.user_import.utils import custom_methods
from macquariegroup.user_import.tasks.run_base_report import run_base_report
from macquariegroup.user_import.tasks.gather_details import get_gather_details_task
from macquariegroup.user_import.tasks.send_logs import get_send_logs
from airflow.models import Variable

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_user_import_master_{config.instance}',
        description=f'Macquarie User Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_file_encrypted_csv = rail.IfOperator(
            task_id='is_file_encrypted_csv',
            test='{{ result("new_file_sensor") | file_name | lower | ends_with("csv.pgp") }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | User import - File processing is skipped - {{ current_time('%H%M%S') }}",
            html_content='templates/emails/email_bad_file_format.html'
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
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        decrypt_file = rail.PGPDecryptionOperator(
            task_id="decrypt_file",
            source="{{ result('download_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        load_user_data = rail.LoadCSVFileOperator(
            task_id='load_user_data',
            document="{{ result('decrypt_file') }}",
            encoding="utf-8-sig"
        )

        create_raw_data_collection = rail.CreateCollectionOperator(
            task_id='create_raw_data_collection',
            source="{{ result('load_user_data') }}",
            name='raw_input_data',
            columns={
                "EMPLID": "emp_id",
                "FIRST_NAME": "first_name",
                "LAST_NAME": "last_name",
                "EMAIL_ADDR": "email",
                "PREF_FIRST_NAME": "display_name",
                "MB_SHORTNAME": "login_name",
                "IMMDT_MGR/ASSGND_TO": "supervisor",
                "Group": "groups",
                "Division": "division",
                "DEPARTMENT": "department",
                "OFFICE": "office",
                "MB_GL_REP_ENTITY": "mb_gl_rep_entity",
                "MB_GL_BU": "mb_gl_bu",
                "MB_GL_LOCATION": "mb_gl_location",
                "MB_GL_DEPTID": "mb_gl_deptid",
                "MB_GL_PROJECT": "mb_gl_project",
                "BUSINESS_TITLE": "business_title",
                "GRADE": "grade",
                "REGION": "region",
                "FTE": "fte"
            }
        )

        has_user_data = rail.IfOperator(
            task_id='has_user_data',
            test="{{ result('create_raw_data_collection', 'length') > 0 }}",
            yes_task='process_start',
            no_task='send_blank_payload_email'
        )

        process_start = rail.EmptyOperator(
            task_id="process_start"
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | User import - no records in file - {{ current_time('%d%m%Y%H%M%S') }}",
            html_content='templates/emails/email_blank_file_user_import.html'
        )

        query_invalid_input_data = rail.QueryCollectionOperator(
            task_id="query_invalid_input_data",
            query="""SELECT * FROM raw_input_data WHERE
                     NULLIF(emp_id,'') IS NULL OR NULLIF(first_name,'') IS NULL OR
                     NULLIF(last_name,'') IS NULL OR NULLIF(email,'') IS NULL OR
                     NULLIF(display_name,'') IS NULL OR NULLIF(login_name,'') IS NULL OR NULLIF(groups,'') IS NULL OR
                     NULLIF(division,'') IS NULL OR NULLIF(department,'') IS NULL OR
                     NULLIF(office,'') IS NULL OR NULLIF(business_title,'') IS NULL""",
            name="invalid_input_records"
        )

        has_any_invalid_records = rail.IfOperator(
            task_id="has_any_invalid_records",
            test="{{ result('query_invalid_input_data', 'length') > 0}}",
            yes_task="log_invalid_input_records",
            no_task='query_valid_input_data'
        )

        def get_missing_fields_log_message(item):
            mandatory_field_keys = [('emp_id', 'EMPLID'), ('first_name', 'FIRST_NAME'), ('last_name', 'LAST_NAME'),
                                    ('email', "EMAIL_ADDR"), ('display_name',
                                                              "PREF_FIRST_NAME"), ('login_name', "MB_SHORTNAME"),
                                    ('groups', "Group"), ('division',
                                                          "Division"), ('department', "DEPARTMENT"),
                                    ('office', 'OFFICE'), ('business_title', 'BUSINESS_TITLE')]
            missing_fields = []
            for key, message in mandatory_field_keys:
                if not item[key]:
                    missing_fields.append(message)

            return ";".join(missing_fields)

        def get_log_properties(item):
            log_message = get_missing_fields_log_message(item)
            return {
                'userloginname': item['login_name'],
                'user_name': item['first_name'] + "." + item['last_name'],
                'employee_id': item['emp_id'],
                'action': 'Validation',
                'status': 'Skipped',
                'details': log_message + " missing in Feed File"
            }

        log_invalid_input_records = rail.WriteLogOperator(
            task_id="log_invalid_input_records",
            severity='Skipped',
            items="{{result('query_invalid_input_data')}}",
            message="mandatory field missing",
            properties=get_log_properties
        )

        query_valid_input_data = rail.QueryCollectionOperator(
            task_id="query_valid_input_data",
            query="""SELECT * FROM raw_input_data WHERE
                     NULLIF(emp_id,'') IS NOT NULL AND NULLIF(first_name,'') IS NOT NULL AND
                     NULLIF(last_name,'') IS NOT NULL AND NULLIF(email,'') IS NOT NULL AND
                     NULLIF(display_name,'') IS NOT NULL AND NULLIF(login_name,'') IS NOT NULL AND
                     NULLIF(groups,'') IS NOT NULL AND
                     NULLIF(division,'') IS NOT NULL AND NULLIF(department,'') IS NOT NULL AND
                     NULLIF(office,'') IS NOT NULL AND NULLIF(business_title,'') IS NOT NULL""",
            name="mandatory_valid_input_records"
        )

        has_any_valid_records = rail.IfOperator(
            task_id="has_any_valid_records",
            test="{{ result('query_valid_input_data', 'length') > 0}}",
            yes_task="filter_invalid_input_data",
            no_task="load_master_log"
        )

        filter_input_data = rail.QueryCollectionOperator(
            task_id="filter_input_data",
            query="""SELECT * FROM mandatory_valid_input_records WHERE lower(groups) in ('financial management group', 'risk management group')""",
            name="valid_input_records"
        )
        has_any_records_to_process = rail.IfOperator(
            task_id="has_any_records_to_process",
            test="{{result('filter_input_data','length') > 0}}",
            yes_task="dummy_run_report",
            no_task="load_master_log"
        )

        dummy_run_report = rail.EmptyOperator(
            task_id="dummy_run_report"
        )

        filter_invalid_input_data = rail.QueryCollectionOperator(
            task_id="filter_invalid_input_data",
            query="""SELECT * FROM mandatory_valid_input_records WHERE lower(groups) not in ('financial management group', 'risk management group')""",
            name="invalid_input_data"
        )

        log_invalid_input_data = rail.WriteLogOperator(
            task_id="log_invalid_input_data",
            severity='Skipped',
            items="{{result('filter_invalid_input_data')}}",
            message="mandatory field missing",
            properties={
                'userloginname': '{{ item.login_name }}',
                'user_name': "{{item.first_name}}" + "." + "{{item.last_name}}",
                'employee_id': "{{item.emp_id}}",
                'action': 'Validation',
                'status': 'Skipped',
                'details': "`{{item.groups}}` group is not allowed"
            }
        )

        get_report_details, load_report_data = run_base_report(config)

        download_recon_reference_file = rail.SFTPDownloadFileOperator(
            task_id="download_recon_reference_file",
            remote_filepath=config.recovery_reconciliation_reference_filepath +
            config.recovery_reconciliation_reference_filename
        )

        load_recon_ref_data = rail.LoadCSVFileOperator(
            task_id="load_recon_ref_data",
            document="{{ result('download_recon_reference_file') }}"
        )

        process_departments_cost_center = rail.TriggerDagRunOperator(
            task_id="process_departments_cost_center",
            trigger_dag_id=f'macquarie_user_import_process_groups_and_location_{config.instance}',
            conf={
                "file_name": "{{result('new_file_sensor') | file_name}}"
            }
        )

        wait_for_process_departments_cost_center = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_departments_cost_center",
            dag_runs="{{result('process_departments_cost_center')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_cost_center_log_file = rail.GatherResultsFromDagRunsOperator(
            task_id='get_cost_center_log_file',
            dag_runs="{{result('process_departments_cost_center')}}",
            dagrun_task_id="create_cost_center_added_successfully_log"
        )

        get_department_log_file = rail.GatherResultsFromDagRunsOperator(
            task_id='get_department_log_file',
            dag_runs="{{result('process_departments_cost_center')}}",
            dagrun_task_id="create_department_created_successfully_log"
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id="create_supervisor_log"
        )

        create_recon_ref_collection = rail.CreateCollectionOperator(
            task_id="create_recon_ref_collection",
            source='{{result("load_recon_ref_data")}}',
            columns={
                    'employee_type': 'employee_type',
                    'department': 'department',
                    'cost_center': 'cost_center',
                    'group': 'groups',
                    'office': 'office',
                    'timesheet_period': 'timesheet_period',
                    'division': 'division',
                    'md5': 'md5'
            },
            name="recon_reference"
        )

        # using python operator so dont have to load all the collection
        # for each valid feed file record
        create_required_fields = rail.PythonOperator(
            task_id="create_required_fields",
            python_callable=lambda: custom_methods.get_create_required_fields(
                config),
            execution_timeout=timedelta(hours=3)
        )

        input_final_data_collection = rail.CreateCollectionOperator(
            task_id="input_final_data_collection",
            source="{{result('create_required_fields')}}",
            name="final_data"
        )

        gather_details = get_gather_details_task()

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id="download_reference_file",
            remote_filepath=config.user_import_reference_file_path
        )

        load_reference_file = rail.LoadCSVFileOperator(
            task_id="load_reference_file",
            document="{{ result('download_reference_file') }}"
        )

        create_reference_collection = rail.CreateCollectionOperator(
            task_id="create_reference_collection",
            name="reference_file",
            source="{{ result('load_reference_file') }}",
            columns={
                "EMPLID": "emp_id",
                "FIRST_NAME": "first_name",
                "LAST_NAME": "last_name",
                "EMAIL_ADDR": "email",
                "PREF_FIRST_NAME": "display_name",
                "MB_SHORTNAME": "login_name",
                "IMMDT_MGR/ASSGND_TO": "supervisor",
                "Group": "groups",
                "Division": "division",
                "DEPARTMENT": "department",
                "OFFICE": "office",
                "MB_GL_REP_ENTITY": "mb_gl_rep_entity",
                "MB_GL_BU": "mb_gl_bu",
                "MB_GL_LOCATION": "mb_gl_location",
                "MB_GL_DEPTID": "mb_gl_deptid",
                "MB_GL_PROJECT": "mb_gl_project",
                "BUSINESS_TITLE": "business_title",
                "GRADE": "grade",
                "REGION": "region",
                "FTE": "fte",
                "md5": "md5"
            }
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id="create_report_collection",
            source="{{ result('load_report_data') }}",
            name="report_data",
            columns={
                "User Name": "user_name",
                "User Email": "user_email",
                "User First Name": "user_first_name",
                "User Last Name": "user_last_name",
                "User Status": "user_status",
                "Employee ID": "employee_id",
                "Login Name": "login_name",
                "Employee Location": "employee_location",
                "Cost Center (Current)": "assigned_cost_center",
                "Department (Current)": "assigned_department",
                "Department (Current) (Full Path)": "department_fullpath",
                "Group (Current)": "assigned_groups",
                "Employee Type (Current)": "assigned_employee_type",
                "Recovery Enabled (Current)": "recovery_enabled",
                "Timesheet Period (Current)": "assigned_timesheet_period",
                "user_uri": "user_uri",
                "User Start Date": "user_start_date",
                "Recovery Override": "recovery_override",
                "Actual End Date": "actual_end_date"
            }
        )

        start_user_records_processing = rail.EmptyOperator(
            task_id="start_user_records_processing"
        )

        get_default_supervisor = rail.PythonOperator(
            task_id="get_default_supervisor",
            python_callable=lambda: Variable.get(
                config.default_supervisor)
        )

        get_holiday_calender_australia = rail.RepliconServiceOperator(
            task_id="get_holiday_calender_australia",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", config.australia_holiday_calender)
        )

        get_holidays_for_current_month = rail.RepliconServiceOperator(
            task_id="get_holidays_for_current_month",
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data={
                "holidayCalendarUri": "{{result('get_holiday_calender_australia').uri}}",
                "dateRange": {
                    "startDate": custom_methods.get_23rd_of_last_month(),
                    "endDate": custom_methods.get_current_month_end_day()
                }
            },
            data_handler=get_holiday_date_list
        )

        generate_effective_date = rail.PythonOperator(
            task_id="generate_effective_date",
            python_callable=custom_methods.generate_effective_date_callable
        )

        def get_default_supervisor_filter(response):
            if not response['rows']:
                return {}
            res = list(filter(lambda x: x['login_name'] == rail.result("get_default_supervisor") and x['enabled'].lower() == "true",
                              map(lambda data: {
                                  'name': get_value(data['cells'], 0, 'textValue'),
                                  'uri': get_value(data['cells'], 0, 'uri'),
                                  "enabled": get_value(data['cells'], 1, 'textValue'),
                                  "login_name": get_value(data['cells'], 2, 'textValue')
                              }, response['rows'])))
            if not res:
                return {}
            return res[0]

        get_default_supervisor_from_replicon = rail.RepliconServiceOperator(
            task_id="get_default_supervisor_from_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:user-list-column:user-name",
                        "urn:replicon:user-list-column:enabled",
                        "urn:replicon:user-list-column:login-name"
                    ],
                "sort": [],
                "filterExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                        },
                        "operatorUri": "urn:replicon:filter-operator:text-search",
                        "rightExpression": {
                            "value": {
                                # Hard Coded in airflow variable
                                "text": "{{ result('get_default_supervisor') }}"
                            }
                        }
                        }
            },
            data_handler=get_default_supervisor_filter
        )

        query_users_to_disable = rail.QueryCollectionOperator(
            task_id="query_users_to_disable",
            query="""SELECT * FROM report_data WHERE employee_id NOT IN (SELECT DISTINCT emp_id FROM raw_input_data)
                        AND (recovery_override IS NULL or recovery_override != 'Yes')
                        AND lower(user_status) == "enabled"
                        AND lower(assigned_groups) in ('financial management group', 'risk management group')
                        AND NULLIF(actual_end_date, '') IS NULL""",
            name="user_records_to_disable"
        )

        query_users_to_skip_disable = rail.QueryCollectionOperator(
            task_id="query_users_to_skip_disable",
            query="""SELECT * FROM report_data WHERE employee_id NOT IN (SELECT DISTINCT emp_id FROM raw_input_data)
                        AND recovery_override == 'Yes'
                        AND lower(assigned_groups) in ('financial management group', 'risk management group')""",
            name="query_users_to_skip_disable"
        )

        log_disable_user_skipped = rail.WriteLogOperator(
            task_id="log_disable_user_skipped",
            severity='Skipped',
            items="{{result('query_users_to_skip_disable')}}",
            message="User disabled skipped as user's Recovery Override is set to Yes",
            properties={
                'userloginname': '{{ item.login_name }}',
                'user_name': "{{item.user_first_name}}" + "." + "{{item.user_last_name}}",
                'employee_id': "{{item.employee_id}}",
                'action': 'Validation',
                'status': 'Skipped',
                'details': "User disabled skipped as user's Recovery Override is set to Yes"
            }
        )

        has_any_users_to_disable = rail.IfOperator(
            task_id="has_any_users_to_disable",
            test="{{result('query_users_to_disable', 'length') > 0}}",
            yes_task="dummy_disable_users",
            no_task="get_supervisor_process_list"
        )

        dummy_disable_users = rail.EmptyOperator(
            task_id="dummy_disable_users"
        )

        process_users_to_disable = rail.trigger_parallel_dagrun(
            task_id="process_users_to_disable",
            items="{{result('query_users_to_disable')}}",
            trigger_dag_id=f"macquarie_user_import_disable_users_child_{config.instance}",
            conf=custom_methods.get_disable_processing_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.parallel_count
        )

        disable_user_complete = rail.EmptyOperator(
            task_id="disable_user_complete"
        )

        query_users_to_add = rail.QueryCollectionOperator(
            task_id="query_users_to_add",
            query="""SELECT * FROM final_data WHERE emp_id NOT IN (SELECT DISTINCT employee_id FROM report_data)
                     AND NULLIF(timesheet_period, '') IS NOT NULL AND NULLIF(employee_type, '') IS NOT NULL""",
            name="user_records_to_add"
        )

        query_users_to_add_skip = rail.QueryCollectionOperator(
            task_id="query_users_to_add_skip",
            query="""SELECT * FROM final_data WHERE emp_id NOT IN (SELECT DISTINCT employee_id FROM report_data)
                     AND(NULLIF(timesheet_period, '') IS NULL OR NULLIF(employee_type, '') IS NULL)""",
            name="user_records_to_add_skip"
        )

        log_query_users_to_add_skip = rail.WriteLogOperator(
            task_id="log_query_users_to_add_skip",
            severity='Skipped',
            items="{{result('query_users_to_add_skip')}}",
            message="Department + Employee Type + Cost Center is not found in recon file",
            properties={
                'userloginname': '{{ item.login_name }}',
                'user_name': "{{item.first_name}}" + "." + "{{item.last_name}}",
                'employee_id': "{{item.emp_id}}",
                'action': 'Validation',
                'status': 'Skipped',
                'details': "Department + Employee Type + Cost Center is not found in recon file"
            }
        )

        has_any_users_to_add = rail.IfOperator(
            task_id="has_any_users_to_add",
            test="{{result('query_users_to_add', 'length') > 0}}",
            yes_task="process_users_to_add_start",
            no_task='get_supervisor_process_list'
        )


        process_users_to_add_start = rail.EmptyOperator(
            task_id="process_users_to_add_start"
        )

        process_users_to_add = rail.trigger_parallel_dagrun(
            task_id="process_users_to_add",
            items="{{ result('query_users_to_add') }}",
            trigger_dag_id=f'macquarie_user_import_add_users_child_{config.instance}',
            conf=custom_methods.get_add_update_user_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.parallel_count
        )

        get_all_triggered_add_dag_runs = rail.PythonOperator(
            task_id='get_all_triggered_add_dag_runs',
            python_callable=lambda: [rail.result(f"{process_users_to_add.group_id}_{i+1}") for i in range(
                config.parallel_count) if rail.result(f"{process_users_to_add.group_id}_{i+1}")],
            show_return_value_in_logs=False
        )

        gather_all_add_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_all_add_logs",
            dag_runs="{{ result('get_all_triggered_add_dag_runs') }}",
            dagrun_task_id="create_add_log"
        )

        def format_add_update_logs(task_id):
            final_log = []
            for artifact_name in rail.result(task_id):
                final_log.extend(rail.load_all_records(artifact_name))
            return final_log

        format_add_logs = rail.PythonOperator(
            task_id="format_add_logs",
            python_callable=format_add_update_logs,
            op_args=[gather_all_add_logs.task_id]
        )

        add_user_complete = rail.EmptyOperator(
            task_id="add_user_complete"
        )

        query_users_for_update = rail.QueryCollectionOperator(
            task_id="query_users_for_update",
            query="""SELECT * FROM final_data
                    WHERE emp_id IN (SELECT DISTINCT employee_id FROM report_data WHERE (recovery_override is NULL or recovery_override != 'Yes'))""",
            name="user_records_for_update"
        )

        query_ignore_users_for_update = rail.QueryCollectionOperator(
            task_id="query_ignore_users_for_update",
            query="SELECT * FROM final_data WHERE emp_id IN (SELECT DISTINCT employee_id FROM report_data WHERE recovery_override == 'Yes')",
            name="recovery_override_yes_update"
        )

        log_ignored_update_user = rail.WriteLogOperator(
            task_id="log_ignored_update_user",
            severity='Skipped',
            items="{{result('query_ignore_users_for_update')}}",
            message="User update skipped user's Recovery Override is set to Yes",
            properties={
                'userloginname': '{{ item.login_name }}',
                'user_name': "{{item.first_name}}" + "." + "{{item.last_name}}",
                'employee_id': "{{item.emp_id}}",
                'action': 'Validation',
                'status': 'Skipped',
                'details': "User update skipped as user's Recovery Override is set to Yes"
            }
        )

        has_any_users_for_update = rail.IfOperator(
            task_id="has_any_users_for_update",
            test="{{result('query_users_for_update', 'length') > 0}}",
            yes_task=['query_valid_users_to_update',
                      'query_invalid_users_for_update'],
            no_task='get_supervisor_process_list'
        )

        query_invalid_users_for_update = rail.QueryCollectionOperator(
            task_id="query_invalid_users_for_update",
            query="SELECT * FROM user_records_for_update WHERE NULLIF(timesheet_period, '') IS NULL",
            name="invalid_records_for_update"
        )

        log_invalid_records_for_update = rail.WriteLogOperator(
            task_id="log_invalid_records_for_update",
            severity='Skipped',
            items="{{result('query_invalid_users_for_update')}}",
            message="department + Employee Type + Cost center is not found in recon file",
            properties={
                'userloginname': '{{ item.login_name }}',
                'user_name': "{{item.first_name}}" + "." + "{{item.last_name}}",
                'employee_id': "{{item.emp_id}}",
                'action': 'Validation',
                'status': 'Skipped',
                'details': "Department + Employee Type + Cost Center is not found in recon file"
            }
        )

        query_valid_users_to_update = rail.QueryCollectionOperator(
            task_id="query_valid_users_to_update",
            name="user_records_to_update",
            query="""SELECT * FROM user_records_for_update WHERE NULLIF(timesheet_period, '') IS NOT NULL"""
        )

        has_any_records_to_update = rail.IfOperator(
            task_id="has_any_records_to_update",
            test="{{result('query_valid_users_to_update', 'length') > 0}}",
            yes_task=['query_valid_update_records_for_skip',
                      'query_valid_update_records_to_process'],
            no_task="get_supervisor_process_list"
        )

        query_valid_update_records_for_skip = rail.QueryCollectionOperator(
            task_id="query_valid_update_records_for_skip",
            query="SELECT * FROM user_records_to_update WHERE md5 IN (SELECT DISTINCT md5 FROM reference_file)",
            name="skipped_records"
        )

        log_skipped_records = rail.WriteLogOperator(
            task_id="log_skipped_records",
            severity='Skipped',
            items="{{result('query_valid_update_records_for_skip')}}",
            message="No change in the record",
            properties={
                'userloginname': '{{ item.login_name }}',
                'user_name': "{{item.first_name}}" + "." + "{{item.last_name}}",
                'employee_id': "{{item.emp_id}}",
                'action': 'Validation',
                'status': 'Skipped',
                'details': "No change in the record"
            }
        )

        query_valid_update_records_to_process = rail.QueryCollectionOperator(
            task_id="query_valid_update_records_to_process",
            query="SELECT * FROM user_records_to_update WHERE md5 NOT IN (SELECT DISTINCT md5 FROM reference_file)",
            name="update_records"
        )

        process_users_to_update = rail.trigger_parallel_dagrun(
            task_id="process_users_to_update",
            items="{{ result('query_valid_update_records_to_process') }}",
            trigger_dag_id=f'macquarie_user_import_update_users_child_{config.instance}',
            conf=custom_methods.get_add_update_user_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.parallel_count
        )

        get_all_triggered_update_dag_runs = rail.PythonOperator(
            task_id='get_all_triggered_update_dag_runs',
            python_callable=lambda: [rail.result(f"{process_users_to_update.group_id}_{i+1}") for i in range(
                config.parallel_count) if rail.result(f"{process_users_to_update.group_id}_{i+1}")],
            show_return_value_in_logs=False
        )

        gather_all_update_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_all_update_logs",
            dag_runs="{{ result('get_all_triggered_update_dag_runs') }}",
            dagrun_task_id="create_update_user_log"
        )

        format_update_logs = rail.PythonOperator(
            task_id="format_update_logs",
            python_callable=format_add_update_logs,
            op_args=[gather_all_update_logs.task_id]
        )

        update_user_complete = rail.EmptyOperator(
            task_id="update_user_complete"
        )

        get_supervisor_process_list = rail.FilterLogEntriesOperator(
            task_id="get_supervisor_process_list",
            log="{{result('create_supervisor_log')}}"
        )

        process_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id="process_supervisor_assignment",
            trigger_dag_id=f'macquarie_user_import_process_supervisors_child_{config.instance}',
            items="{{result('get_supervisor_process_list')}}",
            conf=lambda item: item['properties'],
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_supervisor_assignment = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_supervisor_assignment",
            dag_runs="{{result('process_supervisor_assignment')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ get_master_log() | load_all_records | to_json }}"
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_methods.do_format_logs
        )

        send_logs_start, send_logs_end = get_send_logs(config)

        can_update_reference_file = rail.IfOperator(
            task_id="can_update_reference_file",
            test="{{ result('filter_input_data', 'length') | is_truthy and result('filter_input_data', 'length') > 0}}",
            yes_task="create_reference_file"
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source="{{result('input_final_data_collection')}}",
            header=['EMPLID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL_ADDR', 'PREF_FIRST_NAME',
                    'MB_SHORTNAME', 'IMMDT_MGR/ASSGND_TO', 'Group', 'Division', 'DEPARTMENT',
                    'OFFICE', 'MB_GL_REP_ENTITY', 'MB_GL_BU', 'MB_GL_LOCATION', 'MB_GL_DEPTID',
                    'MB_GL_PROJECT', 'BUSINESS_TITLE', 'GRADE', 'REGION', 'FTE', 'md5'],
            row=[
                '{{item.emp_id}}',
                '{{item.first_name}}',
                '{{item.last_name}}',
                '{{item.email}}',
                '{{item.display_name}}',
                '{{item.login_name}}',
                '{{item.supervisor}}',
                '{{item.groups}}',
                '{{item.division}}',
                '{{item.department}}',
                '{{item.office}}',
                '{{item.mb_gl_rep_entity}}',
                '{{item.mb_gl_bu}}',
                '{{item.mb_gl_location}}',
                '{{item.mb_gl_deptid}}',
                '{{item.mb_gl_project}}',
                '{{item.business_title}}',
                '{{item.grade}}',
                '{{item.region}}',
                '{{item.fte}}',
                '{{item.md5}}'
            ]
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            new_filename=config.user_import_reference_file_archive_filepath +
            "user_import_reference_file_{{current_time_in_specified_tz('Australia/Sydney','%Y-%m-%dT%H%M%S%z')}}.csv",
            existing_filename=config.user_import_reference_file_path
        )

        update_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="update_new_reference_file",
            content="{{result('create_reference_file')}}",
            remote_filepath=config.user_import_reference_file_path
        )

        new_file_sensor >> is_file_encrypted_csv >> rail.Label(
            "No") >> send_bad_file_format_email
        is_file_encrypted_csv >> rail.Label(
            "Yes") >> download_file >> decrypt_file >> load_user_data >> create_raw_data_collection >> has_user_data
        download_file >> was_new_file_found >> rail.Label(
            "Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        has_user_data >> rail.Label("No") >> send_blank_payload_email

        has_user_data >> rail.Label(
            "Yes") >> process_start >> query_invalid_input_data

        query_invalid_input_data >> has_any_invalid_records >> rail.Label(
            "Yes") >> log_invalid_input_records >> query_valid_input_data
        has_any_invalid_records >> rail.Label("No") >> query_valid_input_data

        query_valid_input_data >> has_any_valid_records >> rail.Label(
            "Yes") >> filter_invalid_input_data >> log_invalid_input_data >>\
            filter_input_data >> has_any_records_to_process >> rail.Label(
                "Yes") >> dummy_run_report >> get_report_details
        has_any_records_to_process >> rail.Label("No") >> load_master_log
        has_any_valid_records >> rail.Label(
            "No") >> load_master_log
        load_report_data >> create_report_collection >> download_recon_reference_file >> load_recon_ref_data >> create_recon_ref_collection >>\
            create_required_fields >> input_final_data_collection >> process_departments_cost_center \
            >> wait_for_process_departments_cost_center >> get_cost_center_log_file >> get_department_log_file \
            >> create_supervisor_log >> gather_details >> start_user_records_processing
        load_report_data >> download_reference_file >> load_reference_file >> create_reference_collection >> start_user_records_processing
        start_user_records_processing >> get_default_supervisor >> get_holiday_calender_australia \
            >> get_holidays_for_current_month >> generate_effective_date >> get_default_supervisor_from_replicon >> [
            query_users_to_disable, query_users_to_add, query_users_for_update, query_ignore_users_for_update,
            query_users_to_add_skip, query_users_to_skip_disable]

        query_ignore_users_for_update >> log_ignored_update_user >> get_supervisor_process_list
        query_users_to_skip_disable >> log_disable_user_skipped >> get_supervisor_process_list
        query_users_to_add_skip >> log_query_users_to_add_skip >> get_supervisor_process_list

        query_users_to_disable >> has_any_users_to_disable >> rail.Label(
            "Yes") >> dummy_disable_users >> process_users_to_disable >> disable_user_complete >> get_supervisor_process_list
        has_any_users_to_disable >> rail.Label(
            "No") >> get_supervisor_process_list

        query_users_to_add >> has_any_users_to_add >> rail.Label(
            "Yes") >>\
            process_users_to_add_start >> process_users_to_add >> get_all_triggered_add_dag_runs\
            >> gather_all_add_logs >> format_add_logs >> add_user_complete >> get_supervisor_process_list
        has_any_users_to_add >> rail.Label("No") >> get_supervisor_process_list

        query_users_for_update >> has_any_users_for_update >> rail.Label(
            "No") >> get_supervisor_process_list
        has_any_users_for_update >> rail.Label(
            "Yes") >> [query_invalid_users_for_update, query_valid_users_to_update]

        query_invalid_users_for_update >> log_invalid_records_for_update >> get_supervisor_process_list
        query_valid_users_to_update >> has_any_records_to_update >> rail.Label(
            "Yes") >> [query_valid_update_records_for_skip, query_valid_update_records_to_process]
        has_any_records_to_update >> rail.Label(
            "No") >> get_supervisor_process_list

        query_valid_update_records_for_skip >> log_skipped_records >> get_supervisor_process_list
        query_valid_update_records_to_process >> process_users_to_update >> get_all_triggered_update_dag_runs\
            >> gather_all_update_logs >> format_update_logs >> update_user_complete \
            >> get_supervisor_process_list >> process_supervisor_assignment >> wait_for_process_supervisor_assignment

        wait_for_process_supervisor_assignment >> load_master_log >> format_logs >> send_logs_start
        send_logs_end >> can_update_reference_file >> rail.Label(
            "Yes") >> create_reference_file >> archive_reference_file >> update_new_reference_file

    return dag


rail.for_each_instance(create_main_dag)
