
from datetime import timedelta
import itertools
import pendulum
from airflow.models import Variable
from rail.lib.log import get_master_log_artifact_name
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_user_import_dtna_dtna_user_import_prod_{config.instance}',
        description=f'Live|DTNA_User Import_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%m-%d-%Y')
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
            no_task='download_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='download_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        download_2 = rail.SFTPDownloadFileOperator(
            task_id='download_2',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        parse_csv_3 = rail.LoadCSVFileOperator(
            task_id='parse_csv_3',
            document="{{result('download_2')}}"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}"
        )

        def get_formated_user_row(item):
            return {
                "Employee_ID": item["Employee ID"].strip() if item["Employee ID"] else "",
                "Login_Name": item["Login Name"].strip() if item["Login Name"] else "",
                "First_Name": item["First Name"].strip() if item["First Name"] else "",
                "Last_Name": item["Last Name"].strip() if item["Last Name"] else "",
                "Email": item["E-mail"].strip() if item["E-mail"] else "",
                "Employee_Type": item["Employee Type"].strip() if item["Employee Type"] else "",
                "Start_Date": item["Start Date"].strip() if item["Start Date"] else "",
                "End_Date": item["End Date"].strip() if item["End Date"] else "",
                "Is_Login_Enabled": item["Is Login Enabled"].strip() if item["Is Login Enabled"] else "",
                "Authentication_Type": item["Authentication Type"].strip() if item["Authentication Type"] else "",
                "Password": item["Password"].strip() if item["Password"] else "",
                "TimeSheet_Template": item["TimeSheet Template"].strip() if item["TimeSheet Template"] else "",
                "Work_Week": item["Work Week"].strip() if item["Work Week"] else "",
                "Department_Name": item["Department Name"].strip() if item["Department Name"] else "",
                "Holiday_Calendar": item["Holiday Calendar"].strip() if item["Holiday Calendar"] else "",
                "Supervisor_Login_Name": item["Supervisor Login Name"].strip() if item["Supervisor Login Name"] else "",
                "Supervisor_Start_Date": item["Supervisor Start Date"].strip() if item["Supervisor Start Date"] else "",
                "Time_Zone": item["Time Zone"].strip() if item["Time Zone"] else "",
                "Timesheet_Approval_Path": item["Timesheet Approval Path"].strip() if item["Timesheet Approval Path"] else "",
                "Timesheet_Period_Type": item["Timesheet Period Type"].strip() if item["Timesheet Period Type"] else "",
                "Add_User_Permission1": item["Add User Permission1"].strip() if item["Add User Permission1"] else "",
                "Add_User_Permission2": item["Add User Permission2"].strip() if item["Add User Permission2"] else "",
                "Add_User_Permission3": item["Add User Permission3"].strip() if item["Add User Permission3"] else "",
                "Add_User_Permission4": item["Add User Permission4"].strip() if item["Add User Permission4"] else "",
                "Add_User_Permission5": item["Add User Permission5"].strip() if item["Add User Permission5"] else "",
                "Add_User_Permission6": item["Add User Permission6"].strip() if item["Add User Permission6"] else "",
                "Add_User_Permission7": item["Add User Permission7"].strip() if item["Add User Permission7"] else "",
                "Schedule_Type": item["Schedule Type"].strip() if item["Schedule Type"] else "",
                "Group_COST_CENTER_NAME": item["Group:COST_CENTER_NAME"].strip() if item["Group:COST_CENTER_NAME"] else "",
                "Group_COST_CENTER_NAME_Effective_Date": item["Group:COST_CENTER_NAME Effective Date"].strip() if item["Group:COST_CENTER_NAME Effective Date"] else "",
                "Group__Manager_ENG": item["Group: Manager- ENG"].strip() if item["Group: Manager- ENG"] else "",
                "Group_Manager_ENG_Effective_Date": item["Group:Manager- ENG Effective Date"].strip() if item["Group:Manager- ENG Effective Date"] else "",
                "License_Seats": item["License Seats"].strip() if item["License Seats"] else "",
                "Custom_Field_WRKR_ID": item["Custom Field:WRKR_ID"].strip() if item["Custom Field:WRKR_ID"] else "",
                "Custom_Field_CLNT_WRKR_ID": item["Custom Field:CLNT_WRKR_ID"].strip() if item["Custom Field:CLNT_WRKR_ID"] else "",
                "Custom_Field_SUPPLIER_ID": item["Custom Field:SUPPLIER_ID"].strip() if item["Custom Field:SUPPLIER_ID"] else "",
                "Custom_Field_JOB_CODE": item["Custom Field:JOB_CODE"].strip() if item["Custom Field:JOB_CODE"] else "",
                "Custom_Field_HIRING_MANAGER_ID": item["Custom Field:HIRING_MANAGER_ID"].strip() if item["Custom Field:HIRING_MANAGER_ID"] else "",
                "Custom_Field_APPR_ID": item["Custom Field:APPR_ID"].strip() if item["Custom Field:APPR_ID"] else "",
                "Custom_Field_Initials_-_ENG": item["Custom Field:Initials - ENG"].strip() if item["Custom Field:Initials - ENG"] else ""
            }.values()

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('parse_csv_3') }}",
            header=['Employee_ID',
                    'Login_Name',
                    'First_Name',
                    'Last_Name',
                    'Email',
                    'Employee_Type',
                    'Start_Date',
                    'End_Date',
                    'Is_Login_Enabled',
                    'Authentication_Type',
                    'Password',
                    'TimeSheet_Template',
                    'Work_Week',
                    'Department_Name',
                    'Holiday_Calendar',
                    'Supervisor_Login_Name',
                    'Supervisor_Start_Date',
                    'Time_Zone',
                    'Timesheet_Approval_Path',
                    'Timesheet_Period_Type',
                    'Add_User_Permission1',
                    'Add_User_Permission2',
                    'Add_User_Permission3',
                    'Add_User_Permission4',
                    'Add_User_Permission5',
                    'Add_User_Permission6',
                    'Add_User_Permission7',
                    'Schedule_Type',
                    'Group_COST_CENTER_NAME',
                    'Group_COST_CENTER_NAME_Effective_Date',
                    'Group__Manager_ENG',
                    'Group_Manager_ENG_Effective_Date',
                    'License_Seats',
                    'Custom_Field_WRKR_ID',
                    'Custom_Field_CLNT_WRKR_ID',
                    'Custom_Field_SUPPLIER_ID',
                    'Custom_Field_JOB_CODE',
                    'Custom_Field_HIRING_MANAGER_ID',
                    'Custom_Field_APPR_ID',
                    'Custom_Field_Initials_-_ENG'],
            row=get_formated_user_row
        )

        create_collection_create_list_from_csv_8 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_8',
            source="{{ result('create_csv_lines') }}",
            name="inputfileuser",
            columns={
                'Employee_ID': 'Employee_ID',
                'Login_Name': 'Login_Name',
                'First_Name': 'First_Name',
                'Last_Name': 'Last_Name',
                'Email': 'Email',
                'Employee_Type': 'Employee_Type',
                'Start_Date': 'Start_Date',
                'End_Date': 'End_Date',
                'Is_Login_Enabled': 'Is_Login_Enabled',
                'Authentication_Type': 'Authentication_Type',
                'Password': 'Password',
                'TimeSheet_Template': 'TimeSheet_Template',
                'Work_Week': 'Work_Week',
                'Department_Name': 'Department_Name',
                'Holiday_Calendar': 'Holiday_Calendar',
                'Supervisor_Login_Name': 'Supervisor_Login_Name',
                'Supervisor_Start_Date': 'Supervisor_Start_Date',
                'Time_Zone': 'Time_Zone',
                'Timesheet_Approval_Path': 'Timesheet_Approval_Path',
                'Timesheet_Period_Type': 'Timesheet_Period_Type',
                'Add_User_Permission1': 'Add_User_Permission1',
                'Add_User_Permission2': 'Add_User_Permission2',
                'Add_User_Permission3': 'Add_User_Permission3',
                'Add_User_Permission4': 'Add_User_Permission4',
                'Add_User_Permission5': 'Add_User_Permission5',
                'Add_User_Permission6': 'Add_User_Permission6',
                'Add_User_Permission7': 'Add_User_Permission7',
                'Schedule_Type': 'Schedule_Type',
                'Group_COST_CENTER_NAME': 'Group_COST_CENTER_NAME',
                'Group_COST_CENTER_NAME_Effective_Date': 'Group_COST_CENTER_NAME_Effective_Date',
                'Group__Manager_ENG': 'Group__Manager_ENG',
                'Group_Manager_ENG_Effective_Date': 'Group_Manager_ENG_Effective_Date',
                'License_Seats': 'License_Seats',
                'Custom_Field_WRKR_ID': 'Custom_Field_WRKR_ID',
                'Custom_Field_CLNT_WRKR_ID': 'Custom_Field_CLNT_WRKR_ID',
                'Custom_Field_SUPPLIER_ID': 'Custom_Field_SUPPLIER_ID',
                'Custom_Field_JOB_CODE': 'Custom_Field_JOB_CODE',
                'Custom_Field_HIRING_MANAGER_ID': 'Custom_Field_HIRING_MANAGER_ID',
                'Custom_Field_APPR_ID': 'Custom_Field_APPR_ID',
                'Custom_Field_Initials_-_ENG':  'Custom_Field_Initials_ENG'
            }
        )

        if_parse_csv_3_3_lines_less_than_1_8 = rail.IfOperator(
            task_id='if_parse_csv_3_3_lines_less_than_1_8',
            test='''{{result("create_collection_create_list_from_csv_8", key="length") == 0}}''',
            yes_task="send_mail_9",
            no_task="declare_list_dag_runs_11",
        )

        send_mail_9 = rail.EmailOperator(
            task_id='send_mail_9',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''DaimlerTrucks- Replicon user import is completed ''',
            html_content='''<p><strong>This is a automated mail, please don't reply&nbsp;</strong></p>
            <p>Hello ,</p>
            <p>The User sync into Replicon is completed successfully. There was no data in the file - {{ result('new_file_sensor') | file_name }} on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}.</p>
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        declare_list_dag_runs_11 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_11',
            name='user_process_dag_runs',
            value=[]
        )

        foreach_parse_csv_11_parse_csv_3_11 = rail.ForEachOperator(
            task_id='foreach_parse_csv_11_parse_csv_3_11',
            items="{{ result('create_collection_create_list_from_csv_8') }}",
            start_task='if_foreach_parse_csv_11_parse_csv_3_11_column_1_present_12',
            end_task='foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end'
        )

        if_foreach_parse_csv_11_parse_csv_3_11_column_1_present_12 = rail.IfOperator(
            task_id='if_foreach_parse_csv_11_parse_csv_3_11_column_1_present_12',
            test='''{{ result('foreach_parse_csv_11_parse_csv_3_11').Login_Name | is_truthy }}''',
            yes_task="search_users_13",
            no_task="foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_13 = rail.RepliconServicePageOperator(
            task_id="search_users_13",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('foreach_parse_csv_11_parse_csv_3_11')['Login_Name']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response: compose_user_details(
                response, rail.result('foreach_parse_csv_11_parse_csv_3_11')['Login_Name'])
        )

        if_login_name_textvalue_present_14 = rail.IfOperator(
            task_id='if_login_name_textvalue_present_14',
            test='''{{ result('search_users_13') | is_truthy }}''',
            yes_task="trigger_dag_run_daimlertrucks_user_import_dtna_child_update_user_dtna_prodasync_17",
            no_task="if_login_name_textvalue_blank_23",
        )

        trigger_dag_run_daimlertrucks_user_import_dtna_child_update_user_dtna_prodasync_17 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_daimlertrucks_user_import_dtna_child_update_user_dtna_prodasync_17',
            retries=0,
            items=[-1],
            trigger_dag_id=f'daimlertrucks_user_import_dtna_child_update_user_dtna_prod_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "useruri": rail.result('search_users_13')['useruri'],
                "EmployeeID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Employee_ID'],
                "LoginName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Login_Name'],
                "FirstName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['First_Name'],
                "LastName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Last_Name'],
                "Email": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Email'],
                "EmployeeType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Employee_Type'],
                "StartDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Start_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Start_Date'] else None,
                "EndDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['End_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['End_Date'] else None,
                "IsLoginEnabled": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Is_Login_Enabled'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Is_Login_Enabled'] else None,
                "AuthenticationType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Authentication_Type'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Authentication_Type'] else None,
                "TimeSheetTemplate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['TimeSheet_Template'],
                "WorkWeek": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Work_Week'],
                "DepartmentName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Department_Name'],
                "HolidayCalendar": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Holiday_Calendar'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Holiday_Calendar'] else None,
                "SupervisorLoginName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Login_Name'],
                "SupervisorStartDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Start_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Start_Date'] else None,
                "TimeZone": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Time_Zone'],
                "TimesheetApprovalPath": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Approval_Path'],
                "TimesheetPeriodType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Period_Type'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Period_Type'] else None,
                "AddUserPermission1": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission1'],
                "AddUserPermission2": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission2'],
                "AddUserPermission3": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission3'],
                "AddUserPermission4": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission4'],
                "AddUserPermission5": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission5'],
                "AddUserPermission6": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission6'],
                "AddUserPermission7": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission7'],
                "ScheduleType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Schedule_Type'],
                "GroupCOST_CENTER_NAME": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME'],
                "GroupCOST_CENTER_NAME_EffectiveDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME_Effective_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME_Effective_Date'] else None,
                "LicenseSeats": rail.result('foreach_parse_csv_11_parse_csv_3_11')['License_Seats'],
                "CustomFieldWRKR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_WRKR_ID'],
                "CustomFieldCLNT_WRKR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_CLNT_WRKR_ID'],
                "CustomFieldSUPPLIER_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_SUPPLIER_ID'],
                "CustomFieldJOB_CODE": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_JOB_CODE'],
                "CustomFieldHIRING_MANAGER_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_HIRING_MANAGER_ID'],
                "CustomFieldAPPR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_APPR_ID'],
                "CustomFieldInitialsENG": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_Initials_ENG'],
                "GroupManagerENG": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group__Manager_ENG'],
                "GroupManagerENGEffectiveDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_Manager_ENG_Effective_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_Manager_ENG_Effective_Date'] else None,
            }
        )

        if_log_13_blank_18 = rail.IfOperator(
            task_id='if_log_13_blank_18',
            test='''{{ result('search_users_13') | is_falsy }}''',
            yes_task="if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_19",
            no_task="if_login_name_textvalue_blank_23",
        )

        if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_19 = rail.IfOperator(
            task_id='if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_19',
            test='''{{ result('foreach_parse_csv_11_parse_csv_3_11').Department_Name == 'DTNA ENG'  or result('foreach_parse_csv_11_parse_csv_3_11').Department_Name == 'DTNA IT' }}''',
            yes_task="trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_20",
            no_task="if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_21",
        )

        trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_20 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_20',
            retries=0,
            items=[-1],
            trigger_dag_id=f'daimlertrucks_user_import_dtna_child_add_user_dtna_prod_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "EmployeeID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Employee_ID'],
                "LoginName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Login_Name'],
                "FirstName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['First_Name'],
                "LastName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Last_Name'],
                "Email": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Email'],
                "EmployeeType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Employee_Type'],
                "StartDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Start_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Start_Date'] else None,
                "EndDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['End_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['End_Date'] else None,
                "IsLoginEnabled": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Is_Login_Enabled'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Is_Login_Enabled'] else None,
                "AuthenticationType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Authentication_Type'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Authentication_Type'] else None,
                "TimeSheetTemplate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['TimeSheet_Template'],
                "WorkWeek": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Work_Week'],
                "DepartmentName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Department_Name'],
                "HolidayCalendar": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Holiday_Calendar'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Holiday_Calendar'] else None,
                "SupervisorLoginName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Login_Name'],
                "SupervisorStartDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Start_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Start_Date'] else None,
                "TimeZone": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Time_Zone'],
                "TimesheetApprovalPath": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Approval_Path'],
                "TimesheetPeriodType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Period_Type'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Period_Type'] else None,
                "AddUserPermission1": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission1'],
                "AddUserPermission2": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission2'],
                "AddUserPermission3": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission3'],
                "AddUserPermission4": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission4'],
                "AddUserPermission5": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission5'],
                "AddUserPermission6": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission6'],
                "AddUserPermission7": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission7'],
                "ScheduleType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Schedule_Type'],
                "GroupCOST_CENTER_NAME": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME'],
                "GroupCOST_CENTER_NAME_EffectiveDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME_Effective_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME_Effective_Date'] else None,
                "LicenseSeats": rail.result('foreach_parse_csv_11_parse_csv_3_11')['License_Seats'],
                "CustomFieldWRKR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_WRKR_ID'],
                "CustomFieldCLNT_WRKR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_CLNT_WRKR_ID'],
                "CustomFieldSUPPLIER_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_SUPPLIER_ID'],
                "CustomFieldJOB_CODE": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_JOB_CODE'],
                "CustomFieldHIRING_MANAGER_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_HIRING_MANAGER_ID'],
                "CustomFieldAPPR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_APPR_ID'],
                "CustomFieldInitialsENG": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_Initials_ENG'],
                "GroupManagerENG": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group__Manager_ENG'],
                "GroupManagerENGEffectiveDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_Manager_ENG_Effective_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_Manager_ENG_Effective_Date'] else None,
            }
        )

        if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_21 = rail.IfOperator(
            task_id='if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_21',
            test='''{{ result('foreach_parse_csv_11_parse_csv_3_11').Department_Name != 'DTNA ENG' and result('foreach_parse_csv_11_parse_csv_3_11').Department_Name != 'DTNA IT' }}''',
            yes_task="dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_22",
            no_task="if_login_name_textvalue_blank_23",
        )

        dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_22 = rail.WriteLogOperator(
            task_id='dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_22',
            message="na",
            severity="Not Created or Updated",
            properties={
                "username": "{{ result('foreach_parse_csv_11_parse_csv_3_11').Login_Name }}",
                "status": "Not Created or Updated",
                "failure/reason": "Invalid 'Department name' in the input file"
            }
        )

        if_login_name_textvalue_blank_23 = rail.IfOperator(
            task_id='if_login_name_textvalue_blank_23',
            test='''{{ result('search_users_13') | is_falsy }}''',
            yes_task="if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_24",
            no_task="if_child_dag_trigger_present",
        )

        if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_24 = rail.IfOperator(
            task_id='if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_24',
            test='''{{ result('foreach_parse_csv_11_parse_csv_3_11').Department_Name == 'DTNA ENG' or result('foreach_parse_csv_11_parse_csv_3_11').Department_Name == 'DTNA IT' }}''',
            yes_task="trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_25",
            no_task="if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_26",
        )

        trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_25 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_25',
            retries=0,
            items=[-1],
            trigger_dag_id=f'daimlertrucks_user_import_dtna_child_add_user_dtna_prod_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "EmployeeID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Employee_ID'],
                "LoginName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Login_Name'],
                "FirstName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['First_Name'],
                "LastName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Last_Name'],
                "Email": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Email'],
                "EmployeeType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Employee_Type'],
                "StartDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Start_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Start_Date'] else None,
                "EndDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['End_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['End_Date'] else None,
                "IsLoginEnabled": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Is_Login_Enabled'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Is_Login_Enabled'] else None,
                "AuthenticationType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Authentication_Type'].lower() if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Authentication_Type'] else None,
                "TimeSheetTemplate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['TimeSheet_Template'],
                "WorkWeek": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Work_Week'],
                "DepartmentName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Department_Name'],
                "HolidayCalendar": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Holiday_Calendar'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Holiday_Calendar'] else None,
                "SupervisorLoginName": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Login_Name'],
                "SupervisorStartDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Start_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Supervisor_Start_Date'] else None,
                "TimeZone": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Time_Zone'],
                "TimesheetApprovalPath": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Approval_Path'],
                "TimesheetPeriodType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Period_Type'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Timesheet_Period_Type'] else None,
                "AddUserPermission1": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission1'],
                "AddUserPermission2": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission2'],
                "AddUserPermission3": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission3'],
                "AddUserPermission4": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission4'],
                "AddUserPermission5": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission5'],
                "AddUserPermission6": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission6'],
                "AddUserPermission7": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Add_User_Permission7'],
                "ScheduleType": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Schedule_Type'],
                "GroupCOST_CENTER_NAME": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME'],
                "GroupCOST_CENTER_NAME_EffectiveDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME_Effective_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_COST_CENTER_NAME_Effective_Date'] else None,
                "LicenseSeats": rail.result('foreach_parse_csv_11_parse_csv_3_11')['License_Seats'],
                "CustomFieldWRKR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_WRKR_ID'],
                "CustomFieldCLNT_WRKR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_CLNT_WRKR_ID'],
                "CustomFieldSUPPLIER_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_SUPPLIER_ID'],
                "CustomFieldJOB_CODE": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_JOB_CODE'],
                "CustomFieldHIRING_MANAGER_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_HIRING_MANAGER_ID'],
                "CustomFieldAPPR_ID": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_APPR_ID'],
                "CustomFieldInitialsENG": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Custom_Field_Initials_ENG'],
                "GroupManagerENG": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group__Manager_ENG'],
                "GroupManagerENGEffectiveDate": rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_Manager_ENG_Effective_Date'] if rail.result('foreach_parse_csv_11_parse_csv_3_11')['Group_Manager_ENG_Effective_Date'] else None,
            }
        )

        if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_26 = rail.IfOperator(
            task_id='if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_26',
            test='''{{ result('foreach_parse_csv_11_parse_csv_3_11').Department_Name != 'DTNA ENG' and result('foreach_parse_csv_11_parse_csv_3_11').Department_Name != 'DTNA IT' }}''',
            yes_task="dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_27",
            no_task="if_child_dag_trigger_present",
        )

        dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_27 = rail.WriteLogOperator(
            task_id='dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_27',
            message="Not Created or Updated",
            severity="Not Created or Updated",
            properties={
                "username": "{{ result('foreach_parse_csv_11_parse_csv_3_11').column_1 }}",
                "status": "Not Created or Updated",
                "failure/reason": "Invalid 'Department name' in the input file"
            }
        )

        if_child_dag_trigger_present = rail.IfOperator(
            task_id='if_child_dag_trigger_present',
            test='''{{ result('trigger_dag_run_daimlertrucks_user_import_dtna_child_update_user_dtna_prodasync_17') | is_truthy or result('trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_20') | is_truthy or result('trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_25') | is_truthy}}''',
            yes_task="insert_to_user_dag_run_list_27",
            no_task="foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end",
        )

        insert_to_user_dag_run_list_27 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_27',
            append=True,
            name='{{ result("declare_list_dag_runs_11").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_daimlertrucks_user_import_dtna_child_update_user_dtna_prodasync_17") or result("trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_20") or result("trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_25"))[0]}}'
        )

        foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end = rail.EmptyOperator(
            task_id='foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end',
        )

        is_user_trigger_runs_avaialbale = rail.IfOperator(
            task_id='is_user_trigger_runs_avaialbale',
            test='''{{ result('insert_to_user_dag_run_list_27') | is_truthy }}''',
            yes_task="wait_for_completion_trigger_dag_run_daimlertrucks_user_import_dtna_child",
            no_task="if_dtna_user_import_logs_greater_than_0_32",
        )

        wait_for_completion_trigger_dag_run_daimlertrucks_user_import_dtna_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_daimlertrucks_user_import_dtna_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_27").value | to_json }}'
        )

        def is_log_present():
            try:
                master_log = get_master_log_artifact_name(
                    rail.get_current_context())
                rail.load_all_records(master_log)
                return True
            except:  # pylint: disable=bare-except
                return False

        if_dtna_user_import_logs_greater_than_0_32 = rail.IfOperator(
            task_id='if_dtna_user_import_logs_greater_than_0_32',
            test=is_log_present,
            yes_task="create_csv_lines_33",
            no_task="log_to_sumo",
        )

        create_csv_lines_33 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_33',
            source="{{ get_master_log() }}",
            header=['jobid',
                    'username ',
                    'status',
                    'failure/reason'],
            row=[
                '{{ item.properties | attr_or_default("childjobid", "") }}',
                '{{ item.properties | attr_or_default("username", "") }}',
                '{{ item.properties | attr_or_default("status", "")}}',
                '{{ item.properties | attr_or_default("failure/reason", "") }}'],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_33') }}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        get_logged_errors_33 = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors_33',
            severity='Error',
        )

        if_log_34_blank_35 = rail.IfOperator(
            task_id='if_log_34_blank_35',
            test='''{{result("get_logged_errors_33", key="length") == 0}}''',
            yes_task="send_mail_36",
            no_task="send_mail_39",
        )

        send_mail_36 = rail.EmailOperator(
            task_id='send_mail_36',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''DaimlerTrucks- Replicon user import is completed on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}''',
            html_content='''<p><strong><em><span style="font-family: 'Calibri',sans-serif;">This is a automated mail, please don't reply&nbsp;</span></em></strong></p>
            <p>Hello ,</p>
            <p>The User sync into Replicon is completed successfully based on filename - {{ result('new_file_sensor') | file_name }} on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} </p>
            <p>Please click on the below link to download the logs and review. <br /> <br /><a href="{{result('generate_download_link')}}">Download log</a></p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_39 = rail.EmailOperator(
            task_id='send_mail_39',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''DaimlerTrucks- Replicon user import is completed with errors on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ''',
            html_content='''<p><strong><em><span style="font-family: 'Calibri',sans-serif;">This is a automated mail, please don't reply&nbsp;</span></em></strong></p>
            <p>Hello ,</p>
            <p>The User sync into Replicon is completed with errors for filename - {{ result('new_file_sensor') | file_name }} on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} </p>
            <p>Please click on the below link to download the logs and review. <br /> <br /><a href="{{result('generate_download_link')}}">Download log</a></p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> download_2 >> parse_csv_3 >> archive_file >> \
            create_csv_lines >> create_collection_create_list_from_csv_8 >> if_parse_csv_3_3_lines_less_than_1_8
        if_parse_csv_3_3_lines_less_than_1_8 >> rail.Label(
            'Yes') >> send_mail_9 >> log_to_sumo
        if_parse_csv_3_3_lines_less_than_1_8 >> rail.Label(
            'No') >> declare_list_dag_runs_11 >> foreach_parse_csv_11_parse_csv_3_11 >> if_foreach_parse_csv_11_parse_csv_3_11_column_1_present_12
        if_foreach_parse_csv_11_parse_csv_3_11_column_1_present_12 >> rail.Label(
            'No') >> foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end
        if_foreach_parse_csv_11_parse_csv_3_11_column_1_present_12 >> rail.Label(
            'Yes') >> search_users_13 >> if_login_name_textvalue_present_14
        if_login_name_textvalue_present_14 >> rail.Label(
            'No') >> if_login_name_textvalue_blank_23
        if_login_name_textvalue_present_14 >> rail.Label('Yes') >> trigger_dag_run_daimlertrucks_user_import_dtna_child_update_user_dtna_prodasync_17 >> \
            if_log_13_blank_18
        if_log_13_blank_18 >> rail.Label(
            'No') >> if_login_name_textvalue_blank_23
        if_log_13_blank_18 >> rail.Label(
            'Yes') >> if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_19
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_19 >> rail.Label(
            'No') >> if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_21
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_19 >> rail.Label('Yes') >> \
            trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_20 >> \
            if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_21
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_21 >> rail.Label(
            'No') >> if_login_name_textvalue_blank_23
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_21 >> rail.Label('Yes') >> \
            dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_22 >> if_login_name_textvalue_blank_23
        if_login_name_textvalue_blank_23 >> rail.Label(
            'No') >> if_child_dag_trigger_present
        if_login_name_textvalue_blank_23 >> rail.Label(
            'Yes') >> if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_24
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_24 >> rail.Label(
            'No') >> if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_26
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_equals_to_dtnaeng_24 >> rail.Label('Yes') >> \
            trigger_dag_run_daimlertrucks_user_import_dtna_child_add_user_dtna_prodasync_25 >> \
            if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_26
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_26 >> rail.Label(
            'No') >> if_child_dag_trigger_present
        if_foreach_parse_csv_11_parse_csv_3_11_column_13_not_equals_to_dtnaeng_26 >> rail.Label(
            'Yes') >> dtna_user_import_dtna_user_import_dtna_user_import_add_entry_27_27_27 >> if_child_dag_trigger_present
        if_child_dag_trigger_present >> rail.Label(
            'No') >> foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end
        if_child_dag_trigger_present >> rail.Label(
            'Yes') >> insert_to_user_dag_run_list_27 >> foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end
        foreach_parse_csv_11_parse_csv_3_11 >> foreach_parse_csv_11_parse_csv_3_11_parse_csv_3_3_11_end >> \
            is_user_trigger_runs_avaialbale
        is_user_trigger_runs_avaialbale >> rail.Label(
            'No') >> if_dtna_user_import_logs_greater_than_0_32
        is_user_trigger_runs_avaialbale >> rail.Label('Yes') >> wait_for_completion_trigger_dag_run_daimlertrucks_user_import_dtna_child >> \
            if_dtna_user_import_logs_greater_than_0_32
        if_dtna_user_import_logs_greater_than_0_32 >> rail.Label(
            'No') >> log_to_sumo
        if_dtna_user_import_logs_greater_than_0_32 >> rail.Label(
            'Yes') >> create_csv_lines_33 >> generate_download_link >> get_logged_errors_33 >> if_log_34_blank_35
        if_log_34_blank_35 >> rail.Label('No') >> send_mail_39 >> log_to_sumo
        if_log_34_blank_35 >> rail.Label('Yes') >> send_mail_36 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
