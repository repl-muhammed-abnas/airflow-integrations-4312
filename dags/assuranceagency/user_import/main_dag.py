# pylint: disable=too-many-statements
from datetime import datetime, timedelta
import rail
from assuranceagency.user_import.utils import python_callable
from assuranceagency.user_import.utils.python_callable import get_ref_file_name
from assuranceagency.user_import.utils import request_payload
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'assuranceagency_user_import_master_{config.instance}',
        description=f'assuranceagency_user_import_master_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success" }}',
            yes_task="logger_list",
            no_task="delete_dagrun"
        )

        logger_list = rail.CreateLogOperator(
            task_id = "logger_list"
        )

        supervisor_logger_list = rail.CreateLogOperator(
            task_id = "supervisor_logger_list"
        )

        get_current_datetime = rail.PythonOperator(
            task_id="get_current_datetime",
            python_callable=python_callable.get_current_date_time
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task="send_incorrect_file_format_mail",
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id='send_incorrect_file_format_mail',
            to=config.to_email,
            bcc=config.alert_email,
            subject=f'{config.company_key} |User import has been skipped - {datetime.now().strftime("%d-%m-%Y")}',
            html_content="templates/email/incorrect_file_format_email.html",
        )

        archive_skipped_file = rail.SFTPMoveFileOperator(
            task_id='archive_skipped_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/Skipped_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        parse_user_import_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_import_csv",
            document='{{result("download_file")}}',
            delimiter=","
        )

        write_user_import_csv = rail.WriteCSVFileOperator(
            task_id="write_user_import_csv",
            source='{{result("parse_user_import_csv")}}',
            header=["loginname","firstname","lastname","employeetype","department","location","authenticationtype", \
                     "enabled","employeeid","startdate","enddate","emailaddress","initialsupervisorloginname", \
                        "permissionsets","timesheettemplate","timesheetperiodtype","timesheetapprovalpath","timezone", \
                            "workweek","holidaycalendar","initialschedulename","timeofftemplate","timeoffapprovalpath", \
                                "initialpayrulename","workdayid","position","workercategory","manager","encoded"],
            row=request_payload.user_import_csv_data
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        check_csv_has_data = rail.IfOperator(
            task_id = "check_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_user_import_csv'))) > 0,
            yes_task = "create_collection_from_csv",
            no_task = "send_no_data_to_import_mail"
        )

        send_no_data_to_import_mail = rail.EmailOperator(
            task_id='send_no_data_to_import_mail',
            to=config.to_email,
            bcc=config.alert_email,
            subject=f'{config.company_key} |User import has been skipped - {datetime.now().strftime("%d-%m-%Y")}',
            html_content="templates/email/no_records_to_process_email.html",
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('write_user_import_csv') }}",
            name="input_csv_data"
        )

        query_blank_records = rail.QueryCollectionOperator(
            task_id="query_blank_records",
            query="""SELECT * FROM input_csv_data WHERE
                            (NULLIF(loginname, '') IS NULL
                            OR NULLIF(employeeid, '') IS NULL
                            OR NULLIF(startdate, '') IS NULL)   
                    """,
            name="blank_records"
        )

        has_any_invalid_records = rail.IfOperator(
            task_id="has_any_invalid_records",
            test="{{ result('query_blank_records', 'length') > 0 }}",
            yes_task="compose_blank_data_csv",
            no_task="query_input_on_loginame_employeeid"
        )

        compose_blank_data_csv = rail.WriteCSVFileOperator(
            task_id='compose_blank_data_csv',
            source="{{ result('query_blank_records') }}",
            header=[
                'username', 'login_name','emplid', 'action','status', 'details'
            ],
            row=python_callable.get_blank_fields_conf
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content="{{ result('compose_blank_data_csv') }}",
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        query_input_on_loginame_employeeid = rail.QueryCollectionOperator(
            task_id="query_input_on_loginame_employeeid",
            query="""SELECT * FROM input_csv_data WHERE
                            (NULLIF(loginname, '') IS NOT NULL
                            AND NULLIF(employeeid, '') IS NOT NULL)   
                    """,
            name="validatedinputlist"
        )

        is_validated_input_list_present = rail.IfOperator(
            task_id="is_validated_input_list_present",
            test="{{ result('query_input_on_loginame_employeeid', 'length') > 0 }}",
            yes_task="get_report_uri",
            no_task="log_to_sumo"
        )

        get_report_uri = rail.RepliconServiceOperator(
            task_id="get_report_uri",
            endpoint="/services/reportservice1.svc/GetAllReports",
            data_handler=lambda response:
            {
                "userlist_report_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', '**userlistfromreplicon_prod', 'uri', '')
            }
        )

        run_userlist_report_entry, run_userlist_report_exit = rail.run_report(
            group_id='run_report_userlist',
            report_params=request_payload.get_userlist_report_params
        )

        is_userlist_report_failed = rail.IfOperator(
            task_id="is_userlist_report_failed",
            test="{{ result('run_report_userlist.get_report_result').reportGenerationResults[0].error | is_truthy or \
                result('run_report_userlist.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="log_to_sumo",
            no_task="get_all_payrule_scripts"
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        get_all_enabled_timesheet_period = rail.RepliconServiceOperator(
            task_id="get_all_enabled_timesheet_period",
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data = request_payload.get_enabled_timesheet_period
        )

        get_all_timeoff_approval = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_approval",
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_timesheet_approval = rail.RepliconServiceOperator(
            task_id="get_all_timesheet_approval",
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_enabled_dept_list = rail.RepliconServiceOperator(
            task_id="get_all_enabled_dept_list",
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups"
        )

        get_all_employee_type = rail.RepliconServiceOperator(
            task_id="get_all_employee_type",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )

        get_all_enabled_location = rail.RepliconServiceOperator(
            task_id="get_all_enabled_location",
            endpoint="/services/LocationlistService1.svc/GetData",
            data = request_payload.get_enabled_locations
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        get_custom_field_data_uri = rail.RepliconServiceOperator(
            task_id='get_custom_field_data_uri',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data = {
                    "objectUri": "urn:replicon:object-type:user"
                    },
            data_handler=lambda response: {
                'manageruri' : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Manager' , 'uri', ''),
                'workdayidudfuri' : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Work day ID' , 'uri', ''),
                'positionudf' : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Position' , 'uri', ''),
            }
        )

        get_all_custom_fields_for_required_group = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_for_required_group',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_custom_field_data_uri').manageruri }}"
                }
        )

        get_all_timeoff_type = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_type",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        load_replicon_user_data = rail.LoadCSVFileOperator(
            task_id='load_replicon_user_data',
            document="{{ result('run_report_userlist.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_collection_from_replicon_userdata = rail.CreateCollectionOperator(
            task_id='create_collection_from_replicon_userdata',
            source="{{ result('load_replicon_user_data') }}",
            name="replicon_user_data",
            columns={
                "User Name" : "username",
                "Login Name" : "loginname",
                "Employee ID" : "employeeid",
                "User Status" : "enabled",
                "useruri" : "useruri",
                "User Email" : "emailid",
                "User Start Date" : "startdate"
            }
        )

        query_replicon_userdata = rail.QueryCollectionOperator(
            task_id="query_replicon_userdata",
            query="""SELECT * FROM replicon_user_data WHERE (NULLIF(employeeid, '') IS NOT NULL) """,
            name="validateduserlistfromreplicon"
        )

        query_replicon_enabled_user = rail.QueryCollectionOperator(
            task_id="query_replicon_enabled_user",
            query="""SELECT * FROM replicon_user_data WHERE (NULLIF(employeeid, '') IS NOT NULL AND enabled == 'Enabled' )""",
            name="enabledrepliconuserlist"
        )

        query_replicon_disabled_user = rail.QueryCollectionOperator(
            task_id="query_replicon_disabled_user",
            query="""SELECT * FROM replicon_user_data WHERE (NULLIF(employeeid, '') IS NOT NULL AND enabled == 'Disabled' )""",
            name="disabledrepliconuserlist"
        )

        query_already_disabled_users_for_disable = rail.QueryCollectionOperator(
            task_id="query_already_disabled_users_for_disable",
            query="""SELECT * FROM validatedinputlist WHERE employeeid IN (\
                SELECT DISTINCT employeeid FROM disabledrepliconuserlist) AND (LOWER(enabled) == 'no')""",
            name="re_disabledrepliconuserlist"
        )

        is_disable_users_present_to_be_disabled = rail.IfOperator(
            task_id="is_disable_users_present_to_be_disabled",
            test="{{ result('query_already_disabled_users_for_disable', 'length') > 0 }}",
            yes_task="compose_disabled_skip_csv",
            no_task="query_users_to_be_disabled_without_end_date"
        )

        compose_disabled_skip_csv = rail.WriteCSVFileOperator(
            task_id='compose_disabled_skip_csv',
            source="{{ result('query_already_disabled_users_for_disable') }}",
            header=[
                'username', 'login_name','emplid', 'action','status', 'details'
            ],
            row=python_callable.get_disabled_skip_conf
        )

        upload_disabled_skip_csv_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_disabled_skip_csv_to_sftp',
            content="{{ result('compose_disabled_skip_csv') }}",
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        query_users_to_be_disabled_without_end_date = rail.QueryCollectionOperator(
            task_id="query_users_to_be_disabled_without_end_date",
            query="""SELECT * FROM validatedinputlist WHERE employeeid IN \
                (SELECT DISTINCT employeeid FROM enabledrepliconuserlist) AND (LOWER(enabled) == 'no') AND (NULLIF(enddate, '') IS NULL)""",
            name="disabled_users_without_enddate"
        )

        is_enable_users_without_enddate_present_to_be_disabled = rail.IfOperator(
            task_id="is_enable_users_without_enddate_present_to_be_disabled",
            test="{{ result('query_users_to_be_disabled_without_end_date', 'length') > 0 }}",
            yes_task="compose_disabled_skip_for_no_end_date_csv",
            no_task="query_users_to_be_disabled_with_end_date"
        )

        compose_disabled_skip_for_no_end_date_csv = rail.WriteCSVFileOperator(
            task_id='compose_disabled_skip_for_no_end_date_csv',
            source="{{ result('query_users_to_be_disabled_without_end_date') }}",
            header=[
                'username', 'login_name','emplid', 'action','status', 'details'
            ],
            row=python_callable.get_disabled_skip_for_no_enddate_conf
        )

        upload_disabled_skip_for_no_end_date_csv_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_disabled_skip_for_no_end_date_csv_to_sftp',
            content="{{ result('compose_disabled_skip_for_no_end_date_csv') }}",
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        query_users_to_be_disabled_with_end_date = rail.QueryCollectionOperator(
            task_id="query_users_to_be_disabled_with_end_date",
            query="""SELECT * FROM validatedinputlist WHERE employeeid IN \
                (SELECT DISTINCT employeeid FROM enabledrepliconuserlist) AND (LOWER(enabled) == 'no') and (NULLIF(enddate, '') IS NOT NULL)""",
            name="disable_users_with_enddate"
        )

        is_enable_users_with_enddate_present_to_be_disabled = rail.IfOperator(
            task_id="is_enable_users_with_enddate_present_to_be_disabled",
            test="{{ result('query_users_to_be_disabled_with_end_date', 'length') > 0 }}",
            yes_task="process_each_data_to_disable_user",
            no_task="query_new_users_enable_false_or_blank_names"
        )

        process_each_data_to_disable_user = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_data_to_disable_user",
            items = "{{ result('query_users_to_be_disabled_with_end_date')}}",
            trigger_dag_id = f'assuranceagency_user_import_disable_user_child_{config.instance}',
            execution_timeout = timedelta(config.execution_timeout_days),
            conf = request_payload.process_user_to_disable_with_enddate
        )

        wait_process_to_disable_user = rail.WaitForDagRunsSensor(
            task_id="wait_process_to_disable_user",
            dag_runs="{{result('process_each_data_to_disable_user')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_new_users_enable_false_or_blank_names = rail.QueryCollectionOperator(
            task_id="query_new_users_enable_false_or_blank_names",
            query="""SELECT * FROM validatedinputlist WHERE employeeid NOT IN \
                (SELECT DISTINCT employeeid FROM validateduserlistfromreplicon) AND \
                    (LOWER(enabled) == 'no') OR (NULLIF(firstname, '') IS NULL) OR (NULLIF(lastname, '') IS NULL)""",
            name="new_users_with_blank_names"
        )

        is_new_user_with_disable_n_blanknames_present = rail.IfOperator(
            task_id="is_new_user_with_disable_n_blanknames_present",
            test="{{ result('query_new_users_enable_false_or_blank_names', 'length') > 0 }}",
            yes_task="compose_add_skip_for_new_user_with_invalid_data_csv",
            no_task="query_new_users_with_enable_true"
        )

        compose_add_skip_for_new_user_with_invalid_data_csv = rail.WriteCSVFileOperator(
            task_id='compose_add_skip_for_new_user_with_invalid_data_csv',
            source="{{ result('query_new_users_enable_false_or_blank_names') }}",
            header=[
                'username', 'login_name','emplid', 'action','status', 'details'
            ],
            row=python_callable.get_add_skip_for_new_user_conf
        )

        upload_add_skip_for_new_user_csv_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_add_skip_for_new_user_csv_to_sftp',
            content="{{ result('compose_add_skip_for_new_user_with_invalid_data_csv') }}",
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        query_new_users_with_enable_true = rail.QueryCollectionOperator(
            task_id="query_new_users_with_enable_true",
            query="""SELECT * FROM validatedinputlist WHERE employeeid NOT IN \
                (SELECT DISTINCT employeeid FROM validateduserlistfromreplicon) AND \
                    (LOWER(enabled) == 'yes') AND (NULLIF(firstname, '') IS NOT NULL) AND (NULLIF(lastname, '') IS NOT NULL)""",
            name="new_users_with_enable_true"
        )

        is_new_users_with_enable_true_present = rail.IfOperator(
            task_id="is_new_users_with_enable_true_present",
            test="{{ result('query_new_users_with_enable_true', 'length') > 0 }}",
            yes_task="process_each_data_to_add_user",
            no_task="query_user_to_be_updated"
        )

        process_each_data_to_add_user = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_data_to_add_user",
            items = "{{ result('query_new_users_with_enable_true')}}",
            trigger_dag_id = f'assuranceagency_user_import_add_user_child_{config.instance}',
            execution_timeout = timedelta(config.execution_timeout_days),
            conf = request_payload.process_user_to_add
        )

        wait_process_to_add_user = rail.WaitForDagRunsSensor(
            task_id="wait_process_to_add_user",
            dag_runs="{{result('process_each_data_to_add_user')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_user_to_be_updated = rail.QueryCollectionOperator(
            task_id="query_user_to_be_updated",
            query="""SELECT * FROM validatedinputlist WHERE employeeid IN \
                (SELECT DISTINCT employeeid FROM validateduserlistfromreplicon) AND (LOWER(enabled) == 'yes')""",
            name="updateuserslist"
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            paths=[config.reference_filepath]
        )

        get_ref_filepath_name = rail.PythonOperator(
            task_id = "get_ref_filepath_name",
            python_callable=lambda: get_ref_file_name(config.reference_filepath)
        )

        is_users_for_update_present = rail.IfOperator(
            task_id="is_users_for_update_present",
            test="{{ result('query_user_to_be_updated', 'length') > 0 }}",
            yes_task="is_use_reference_file_allowed",
            no_task="write_supervisor_checker_log_file"
        )

        is_use_reference_file_allowed = rail.IfOperator(
            task_id="is_use_reference_file_allowed",
            test=lambda: Variable.get(
                config.can_use_reference_file, default_var='true').lower() == 'true',
            yes_task="download_reference_file",
            no_task="write_supervisor_checker_log_file"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath= "{{ result('get_ref_filepath_name')}}"
        )

        load_reference_csv = rail.LoadCSVFileOperator(
            task_id = "load_reference_csv",
            delimiter=",",
            document="{{ result('download_reference_file') }}",
            headers=["loginname","firstname","lastname","employeetype","department","location","authenticationtype", \
                     "enabled","employeeid","startdate","enddate","emailaddress","initialsupervisorloginname", \
                        "permissionsets","timesheettemplate","timesheetperiodtype","timesheetapprovalpath","timezone", \
                            "workweek","holidaycalendar","initialschedulename","timeofftemplate","timeoffapprovalpath", \
                                "initialpayrulename","workdayid","position","workercategory","manager","encoded"]
        )

        create__ref_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create__ref_collection_from_csv',
            source="{{ result('load_reference_csv') }}",
            name="userreferencedata"
        )

        query_for_unchanged_records = rail.QueryCollectionOperator(
            task_id="query_for_unchanged_records",
            query="""SELECT * FROM updateuserslist WHERE encoded IN (SELECT DISTINCT encoded FROM userreferencedata)""",
            name="unchanged_records"
        )

        has_any_unchanged_records = rail.IfOperator(
            task_id="has_any_unchanged_records",
            test="{{ result('query_for_unchanged_records', 'length') > 0 }}",
            yes_task="compose_unchanged_data_csv",
            no_task="query_for_changed_records"
        )

        compose_unchanged_data_csv = rail.WriteCSVFileOperator(
            task_id='compose_unchanged_data_csv',
            source="{{ result('query_for_unchanged_records') }}",
            header=[
                'username', 'login_name','emplid', 'action','status', 'details'
            ],
            row=python_callable.get_unchanged_data_conf
        )

        upload_unchanged_csv_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_unchanged_csv_to_sftp',
            content="{{ result('compose_unchanged_data_csv') }}",
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        query_for_changed_records = rail.QueryCollectionOperator(
            task_id="query_for_changed_records",
            query="""SELECT * FROM updateuserslist WHERE encoded NOT IN (SELECT DISTINCT encoded FROM userreferencedata)""",
            name="changed_records"
        )

        is_changed_records_present = rail.IfOperator(
            task_id="is_changed_records_present",
            test="{{ result('query_for_changed_records', 'length') > 0 }}",
            yes_task="process_each_data_to_update_user",
            no_task="write_supervisor_checker_log_file"
        )

        process_each_data_to_update_user = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_data_to_update_user",
            items = "{{ result('query_for_changed_records')}}",
            trigger_dag_id = f'assuranceagency_user_import_update_user_child_{config.instance}',
            execution_timeout = timedelta(config.execution_timeout_days),
            conf = request_payload.process_user_to_update
        )

        wait_process_to_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_process_to_update_user",
            dag_runs="{{result('process_each_data_to_update_user')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        write_supervisor_checker_log_file = rail.WriteCSVFileOperator(
            task_id="write_supervisor_checker_log_file",
            source="{{ result('supervisor_logger_list') }}",
            header=['userloginname', 'useruri', 'username', 'supervisorloginname','emplid', 'action', 'status'],
            row=lambda item: [
                item['properties']['userloginname'],
                item['properties']['useruri'],
                item['properties']['username'],
                item['properties']['supervisorloginname'],
                item['properties']['emplid'],
                item['properties']['action'],
                item['properties']['status']
            ]
        )

        check_supervisor_mapper_csv_has_data = rail.IfOperator(
            task_id = "check_supervisor_mapper_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_supervisor_checker_log_file'))) > 0,
            yes_task = "process_each_supervisor_log_data",
            no_task = "write_log_user_import"
        )

        process_each_supervisor_log_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_supervisor_log_data',
            items = "{{ result('write_supervisor_checker_log_file')}}",
            trigger_dag_id=f'assuranceagency_user_import_update_supervisor_from_logs_child_{config.instance}',
            conf=request_payload.process_supervisor_mapper_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_to_update_supervisor = rail.WaitForDagRunsSensor(
            task_id="wait_process_to_update_supervisor",
            dag_runs="{{result('process_each_supervisor_log_data')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        write_log_user_import = rail.WriteCSVFileOperator(
            task_id='write_log_user_import',
            source="{{ result('logger_list') }}",
            header=['username', 'login_name', 'emplid', 'action','status', 'details'],
            row=lambda item: [
                item['properties']['username'],
                item['properties']['login_name'],
                item['properties']['emplid'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details']
            ]
        )

        check_user_log_has_data = rail.IfOperator(
            task_id = "check_user_log_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_log_user_import'))) > 0,
            yes_task = "upload_logs_to_sftp",
            no_task = "download_log_file"
        )

        upload_logs_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('write_log_user_import') }}",
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        check_if_upload_success = rail.IfOperator(
            task_id='check_if_upload_success',
            test="{{ get_task_state('upload_logs_to_sftp') == 'success' }}",
            yes_task='download_log_file',
            no_task='send_error_in_upload_mail'
        )

        send_error_in_upload_mail = rail.EmailOperator(
            task_id='send_error_in_upload_mail',
            to=config.to_email,
            bcc=config.alert_email,
            subject=f'{config.company_key} |User Import - Uploading Logs to SFTP failed - {datetime.now().strftime("%d-%m-%Y")}',
            html_content="templates/email/send_error_in_upload.html",
            params={
                'username': config.user_name,
                'company_key': config.company_key,
                'today': datetime.now().strftime("%d-%m-%Y")
            }
        )

        download_log_file = rail.SFTPDownloadFileOperator(
            task_id='download_log_file',
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        parse_user_import_log_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_import_log_csv",
            document='{{result("download_log_file")}}',
            delimiter=","
        )

        write_user_import_log_csv = rail.WriteCSVFileOperator(
            task_id="write_user_import_log_csv",
            source='{{result("parse_user_import_log_csv")}}',
            header=["username","login_name","emplid","action","status","details"],
            row=request_payload.user_import_log_csv_data
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log = "{{ result('logger_list') }}",
            severity='Error'
        )

        get_logged_exception = rail.FilterLogEntriesOperator(
            task_id='get_logged_exception',
            log = "{{ result('logger_list') }}",
            severity='Exception',
        )

        email_subject_line = rail.PythonOperator(
            task_id='email_subject_line',
            python_callable=python_callable.get_subject_line
        )

        email_body = rail.PythonOperator(
            task_id='email_body',
            python_callable=python_callable.get_email_body
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('write_user_import_log_csv')}}",
            output_file_name="logs_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        send_cshare_mail = rail.EmailOperator(
            task_id='send_cshare_mail',
            to=config.to_email,
            bcc="{%- if result('get_logged_errors', key='length') > 0 -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject=f'{config.company_key} | User import - ' + "{{ result('email_subject_line') }} -" + datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            html_content="templates/email/send_cshare_email.html",
            params={
                'today': datetime.now().strftime("%d-%m-%Y")
            }
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            existing_filename='{{ result("get_ref_filepath_name") }}',
            new_filename=config.archive_filepath + "/Old_{{ result('get_current_datetime') }}_{{ result('get_ref_filepath_name') | file_name }}"
        )

        upload_reference_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reference_csv_to_sftp',
            content="{{ result('write_user_import_csv') }}",
            remote_filepath=config.reference_filepath + "/Reference_{{ result('get_current_datetime') }}_{{ result('new_file_sensor') | file_name }}"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> was_new_file_found

        was_new_file_found >> rail.Label("No") >> delete_dagrun
        was_new_file_found >> rail.Label("Yes") >> logger_list >> supervisor_logger_list >> get_current_datetime >> is_csv

        is_csv >> rail.Label("No") >> send_incorrect_file_format_mail >> archive_skipped_file
        is_csv >> rail.Label("Yes") >> download_file >> parse_user_import_csv >> write_user_import_csv >> \
            archive_input_file >> check_csv_has_data

        check_csv_has_data >> rail.Label("No") >> send_no_data_to_import_mail >> archive_skipped_file
        check_csv_has_data >> rail.Label("Yes") >> create_collection_from_csv >> query_blank_records >> has_any_invalid_records

        has_any_invalid_records >> rail.Label("Yes") >> compose_blank_data_csv >> upload_csv_to_sftp >> query_input_on_loginame_employeeid
        has_any_invalid_records >> rail.Label("No") >> query_input_on_loginame_employeeid

        query_input_on_loginame_employeeid >> is_validated_input_list_present
        is_validated_input_list_present >> rail.Label("No") >> log_to_sumo
        is_validated_input_list_present >> rail.Label("Yes") >> get_report_uri >> run_userlist_report_entry
        run_userlist_report_exit >> is_userlist_report_failed

        is_userlist_report_failed >> rail.Label("Yes") >> log_to_sumo
        is_userlist_report_failed >> rail.Label("No") >> get_all_payrule_scripts >> get_all_office_schedules >> \
        get_all_holiday_calendars >> get_all_enabled_timesheet_period >> get_all_timeoff_approval >> \
        get_all_timesheet_approval >> get_all_enabled_dept_list >> get_all_employee_type >> get_all_enabled_location >> \
        get_all_policy_sets >> get_all_permission_set >> get_custom_field_data_uri >> get_all_custom_fields_for_required_group >> \
        get_all_timeoff_type

        get_all_timeoff_type >> load_replicon_user_data >> create_collection_from_replicon_userdata >> query_replicon_userdata >> \
        query_replicon_enabled_user >> query_replicon_disabled_user >> query_already_disabled_users_for_disable >> \
            is_disable_users_present_to_be_disabled

        is_disable_users_present_to_be_disabled >> rail.Label("Yes") >> compose_disabled_skip_csv >> upload_disabled_skip_csv_to_sftp >> \
        query_users_to_be_disabled_without_end_date
        is_disable_users_present_to_be_disabled >> rail.Label("No") >> query_users_to_be_disabled_without_end_date

        query_users_to_be_disabled_without_end_date >> is_enable_users_without_enddate_present_to_be_disabled

        is_enable_users_without_enddate_present_to_be_disabled >> rail.Label("Yes") >> compose_disabled_skip_for_no_end_date_csv >> \
        upload_disabled_skip_for_no_end_date_csv_to_sftp >> query_users_to_be_disabled_with_end_date
        is_enable_users_without_enddate_present_to_be_disabled >> rail.Label("No") >> query_users_to_be_disabled_with_end_date

        query_users_to_be_disabled_with_end_date >> is_enable_users_with_enddate_present_to_be_disabled

        is_enable_users_with_enddate_present_to_be_disabled >> rail.Label("Yes") >> process_each_data_to_disable_user >> \
            wait_process_to_disable_user >> query_new_users_enable_false_or_blank_names >> is_new_user_with_disable_n_blanknames_present
        is_enable_users_with_enddate_present_to_be_disabled >> rail.Label("No") >> query_new_users_enable_false_or_blank_names >> \
        is_new_user_with_disable_n_blanknames_present

        is_new_user_with_disable_n_blanknames_present >> rail.Label("Yes") >> compose_add_skip_for_new_user_with_invalid_data_csv >> \
        upload_add_skip_for_new_user_csv_to_sftp >> query_new_users_with_enable_true
        is_new_user_with_disable_n_blanknames_present >> rail.Label("No") >> query_new_users_with_enable_true

        query_new_users_with_enable_true >> is_new_users_with_enable_true_present

        is_new_users_with_enable_true_present >> rail.Label("Yes") >> process_each_data_to_add_user >> \
            wait_process_to_add_user >> query_user_to_be_updated
        is_new_users_with_enable_true_present >> rail.Label("No") >> query_user_to_be_updated

        query_user_to_be_updated >> list_reference_files >> get_ref_filepath_name >> is_users_for_update_present

        is_users_for_update_present >> rail.Label("Yes") >> is_use_reference_file_allowed
        is_users_for_update_present >> rail.Label("No") >> write_supervisor_checker_log_file

        is_use_reference_file_allowed >> rail.Label("Yes") >> download_reference_file >> load_reference_csv >> \
            create__ref_collection_from_csv >> query_for_unchanged_records >> has_any_unchanged_records

        has_any_unchanged_records >> rail.Label("Yes") >> compose_unchanged_data_csv >> upload_unchanged_csv_to_sftp >> query_for_changed_records
        has_any_unchanged_records >> rail.Label("No") >> query_for_changed_records

        query_for_changed_records >> is_changed_records_present

        is_changed_records_present >> rail.Label("Yes") >> process_each_data_to_update_user >> wait_process_to_update_user >> \
        write_supervisor_checker_log_file
        is_changed_records_present >> rail.Label("No") >> write_supervisor_checker_log_file

        is_use_reference_file_allowed >> rail.Label("No") >> write_supervisor_checker_log_file

        write_supervisor_checker_log_file >> check_supervisor_mapper_csv_has_data

        check_supervisor_mapper_csv_has_data >> rail.Label("Yes") >> process_each_supervisor_log_data >> \
            wait_process_to_update_supervisor >> write_log_user_import
        check_supervisor_mapper_csv_has_data >> rail.Label("No") >> write_log_user_import

        write_log_user_import >> check_user_log_has_data
        check_user_log_has_data >> rail.Label("No") >> download_log_file
        check_user_log_has_data >> rail.Label("Yes") >> upload_logs_to_sftp >> check_if_upload_success

        check_if_upload_success >> rail.Label("Yes") >> download_log_file

        download_log_file >> parse_user_import_log_csv >> write_user_import_log_csv >> \
            get_logged_errors >> get_logged_exception >> email_subject_line >> email_body >> generate_downloadlink >> \
                send_cshare_mail >> archive_reference_file >> upload_reference_csv_to_sftp >> log_to_sumo

        check_if_upload_success >> rail.Label("No") >> send_error_in_upload_mail >> log_to_sumo

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
