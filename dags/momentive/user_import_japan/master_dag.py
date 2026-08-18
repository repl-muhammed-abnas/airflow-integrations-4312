import json
from datetime import datetime, timedelta
from pendulum import now, datetime as dt
from momentive.user_import_japan.utils import python_callable, request_payload
import rail

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_master_dag_id,
        description=f'Momentive user import Japan - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2026, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
        
        get_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details',
            report_name=config.report_name
        )

        log_report_uri_5 = rail.PythonOperator(
            task_id='log_report_uri_5',
            python_callable=lambda: rail.result('get_specific_report_details')['uri']
        )
        
        log_userfilter_uri_7 = rail.PythonOperator(
            task_id='log_userfilter_uri_7',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_specific_report_details')['filterConfiguration']['enabledFilters'],'displayText', 'UserFilter','uri')
        )

        if_instance_trial = rail.IfOperator(
            task_id='if_instance_trial',
            test=lambda: bool('trial' in config.instance),
            yes_task='new_file_sensor_to_process',
            no_task='get_workdayreport_http_payload'
        )

        get_workdayreport_http_payload = rail.SimpleHttpOperator(
            task_id='get_workdayreport_http_payload',
            method='GET',
            http_conn_id=config.workday_report_http_conn_id,
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        workdayreport_json_load = rail.PythonOperator(
            task_id='workdayreport_json_load',
            python_callable=lambda: json.loads(
                rail.result('get_workdayreport_http_payload'))
        )

        if_first_employee_id_blank_1_8 = rail.IfOperator(
            task_id='if_first_employee_id_blank_1_5',
            test='''{{ result('workdayreport_json_load') | is_falsy or result('workdayreport_json_load')['Report_Entry'] | length == 0}}''',
            yes_task="send_mail_no_change_records",
            no_task="get_write_csv_task_source",
        )

        new_file_sensor_to_process = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_to_process',
            path=config.input_filepath_for_trial,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor_to_process") == "success" }}',
            yes_task="download_sftp_file",
            no_task="delete_dagrun"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        download_sftp_file = rail.SFTPDownloadFileOperator(
            task_id='download_sftp_file',
            remote_filepath="{{ result('new_file_sensor_to_process') }}"
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename='{{ result("new_file_sensor_to_process") }}',
            new_filename=config.archive_filepath +
            "/Processed{{ result('new_file_sensor_to_process') | file_name }}_{{dag_run_ecid()}}"
        )

        parse_user_sync_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_sync_csv",
            document='{{result("download_sftp_file")}}',
            delimiter=","
        )

        get_write_csv_task_source = rail.PythonOperator(
            task_id='get_write_csv_task_source',
            trigger_rule='one_success',
            python_callable=lambda: json.dumps(rail.result('workdayreport_json_load')['Report_Entry']) if rail.result(
                'workdayreport_json_load') else rail.result('parse_user_sync_csv')
        )

        log_todaysdate_2 = rail.PythonOperator(
            task_id='log_todaysdate_2',
            python_callable=lambda:  now(
                tz=config.time_zone).strftime("%Y_%m_%d%H_%M_%S")
        )

        create_csv_lines_12 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_12',
            source="{{ result('get_write_csv_task_source') }}",
            header=['User_ID', 'Worker_Reference_Employee_ID', 'Email_Address', 'First_Name', 'Last_Name', 'Worker_Type', 'Effective_Date_of_Worker_Type',
                'Exemption_Status', 'CF_LRV_Job_Exempt_Eff_Date', 'Gender', 'Hire_Date', 'Termination_Date', 'Active', 'Function',
                'Function_Change_Effective_Date', 'Business_Title', 'CF_LRV_Business_Title_Change_Eff_Date', 'Field_HR', 'Manager_ID',
                'Effective_Date_of_Manager_Change', 'Work_Shift', 'Work_Shift_Change_Effective_Date', 'Location', 'CF_LRV_Location_Change_Effective_Date',
                'Country', 'CF_Date_of_Birth_MM_DD_YYYY', 'CF_LRV_Manager_Email', 'CF_LRV_Manager_First_Name', 'CF_LRV_Manager_Last_Name', 'Legal_entity',
                'Worker_subType', 'Cost_center', 'Worker_cc_change_date', 'Year_of_service', 'Paygroup', 'Japan_special_schedule_flag',
                'continous_service_date', 'timeoff_service_date'],
            row=lambda item: [
            item['User ID'] if item['User ID'] else '',
            item['Worker reference employee ID'] if item['Worker reference employee ID'] else '',
            item['Email address'] if item['Email address'] else '',
            item['First name'] if item['First name'] else '',
            item['Last name'] if item['Last name'] else '',
            item['Worker type'] if item['Worker type'] else '',
            item['Effective date of worker type'] if item['Effective date of worker type'] else '',
            item['Exemption status'] if item['Exemption status'] else '',
            item['Exemption eff date'] if item['Exemption eff date'] else '',
            item['Gender'] if item['Gender'] else '',
            item['Hire date'] if item['Hire date'] else '',
            item['Termination date'] if item['Termination date'] else '',
            item['Active'] if item['Active'] else '',
            item['Function'] if item['Function'] else '',
            item['Function change effective date'] if item['Function change effective date'] else '',
            item['Business title'] if item['Business title'] else '',
            item['CF LRV business title change eff date'] if item['CF LRV business title change eff date'] else '',
            item['Field HR'] if item['Field HR'] else '',
            item['Manager ID'] if item['Manager ID'] else '',
            item['Effective date of manager change'] if item['Effective date of manager change'] else '',
            item['Work shift'] if item['Work shift'] else '',
            item['Work shift change effective date'] if item['Work shift change effective date'] else '',
            item['Location'] if item['Location'] else '',
            item['CF LRV location change effective date'] if item['CF LRV location change effective date'] else '',
            item['Country'] if item['Country'] else '',
            item['CF date of birth MM DD YYYY'] if item['CF date of birth MM DD YYYY'] else '',
            item['CF LRV manager email'] if item['CF LRV manager email'] else '',
            item['CF LRV manager first name'] if item['CF LRV manager first name'] else '',
            item['CF LRV manager last name'] if item['CF LRV manager last name'] else '',
            item['Legal entity'] if item['Legal entity'] else '',
            item['Worker sub type'] if item['Worker sub type'] else '',
            item['Cost center'] if item['Cost center'] else '',
            item['Workers CC change eff date'] if item['Workers CC change eff date'] else '',
            item['Years of service'] if item['Years of service'] else '',
            item['Pay group'] if item['Pay group'] else '',
            item['Japan special schedule flag'] if item['Japan special schedule flag'] else '',
            item['Continuous service date'] if item['Continuous service date'] else '',
            item['Time off service date'] if item['Time off service date'] else '',
            ]
        )

        if_record_count_less_than_1_15 = rail.IfOperator(
            task_id='if_record_count_less_than_1_15',
            test=lambda: bool(
                int(len(rail.load_all_records(rail.result('parse_user_sync_csv')))) < 1),
            yes_task="send_mail_no_change_records",
            no_task="create_collection_create_list_from_csv",
        )

        send_mail_no_change_records = rail.EmailOperator(
            task_id='send_mail_no_change_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key() }} - Japan| User import completed- No change records found - {{ current_time() }} ''',
            html_content='''templates/no_delta_records.html''',
            params=None,
        )

        create_collection_create_list_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv',
            source="{{ result('create_csv_lines_12') }}",
            name="workdayuserdata",
            columns={
                'User_ID': 'User_ID',
                'Worker_Reference_Employee_ID': 'Worker_Reference_Employee_ID',
                'Email_Address': 'Email_Address',
                'First_Name': 'First_Name',
                'Last_Name': 'Last_Name',
                'Worker_Type': 'Worker_Type',
                'Effective_Date_of_Worker_Type': 'Effective_Date_of_Worker_Type',
                'Exemption_Status': 'Exemption_Status',
                'Exemption_Eff_Date': 'Exemption_Eff_Date',
                'Gender': 'Gender',
                'Hire_Date': 'Hire_Date',
                'Termination_Date': 'Termination_Date',
                'Active': 'Active',
                'Function': 'Function',
                'Function_Change_Effective_Date': 'Function_Change_Effective_Date',
                'Business_Title': 'Business_Title',
                'CF_LRV_Business_Title_Change_Eff_Date': 'CF_LRV_Business_Title_Change_Eff_Date',
                'Field_HR': 'Field_HR',
                'Manager_ID': 'Manager_ID',
                'Effective_Date_of_Manager_Change': 'Effective_Date_of_Manager_Change',
                'Work_Shift': 'Work_Shift',
                'Work_Shift_Change_Effective_Date': 'Work_Shift_Change_Effective_Date',
                'Location': 'Location',
                'CF_LRV_Location_Change_Effective_Date': 'CF_LRV_Location_Change_Effective_Date',
                'Country': 'Country',
                'CF_Date_of_Birth_MM_DD_YYYY': 'CF_Date_of_Birth_MM_DD_YYYY',
                'CF_LRV_Manager_Email': 'CF_LRV_Manager_Email',
                'CF_LRV_Manager_First_Name': 'CF_LRV_Manager_First_Name',
                'CF_LRV_Manager_Last_Name': 'CF_LRV_Manager_Last_Name',
                'Legal_entity': 'Legal_entity',
                'Worker_subType': 'Worker_subType',
                'Cost_center': 'Cost_center',
                'Worker_cc_change_date': 'Worker_cc_change_date',
                'Year_of_service': 'Year_of_service',
                'Paygroup': 'Paygroup',
                'Japan_special_schedule_flag': 'Japan_special_schedule_flag',
                'continous_service_date': 'continous_service_date',
                'timeoff_service_date': 'timeoff_service_date',
            }
        )

        query_list_usershereloginnameisblank_20 = rail.QueryCollectionOperator(
            task_id='query_list_usershereloginnameisblank_20',
            query="""SELECT * FROM  workdayuserdata WHERE  (NULLIF(User_ID, '') IS NULL)""",
        )

        create_log_momentive_user_import_log = rail.CreateLogOperator(
            task_id='create_log_momentive_user_import_log'
        )

        create_log_momentive_supervisor_assignment = rail.CreateLogOperator(
            task_id='create_log_momentive_supervisor_assignment'
        )

        create_log_momentive_supervisor_restriction = rail.CreateLogOperator(
            task_id='create_log_momentive_supervisor_restriction'
        )

        momentive_user_import_logs_skipped_entries = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_skipped_entries',
            log="{{result('create_log_momentive_user_import_log')}}",
            items="{{result('query_list_usershereloginnameisblank_20')}}",
            severity='na',
            message='Skipped',
            properties=lambda item: {
                'jobid': rail.render_template("{{ dag_run_ecid() }}"),
                "userid": item['User_ID'],
                "username": item['First_Name'] + "|" + item['Last_Name'],
                "action": 'Validation',
                "status": 'Skipped',
                'details': 'User ID must be present'
            }
        )

        query_list_usershereloginnameispresent_22 = rail.QueryCollectionOperator(
            task_id='query_list_usershereloginnameispresent_22',
            query="""SELECT * FROM  workdayuserdata WHERE  (NULLIF(User_ID, '') IS NOT NULL)""",
        )

        if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23 = rail.IfOperator(
            task_id='if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23',
            test="{{ result('query_list_usershereloginnameispresent_22', 'length') > 0 }}",
            yes_task="get_all_enabled_divisions_25",
            no_task="send_mail_no_change_records",
        )

        get_all_enabled_divisions_25 = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions_25",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions"
        )

        get_all_enabled_service_center_details_26 = rail.RepliconServiceOperator(
            task_id="get_all_enabled_service_center_details_26",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_all_enabled_costcenters_27 = rail.RepliconServiceOperator(
            task_id="get_all_enabled_costcenters_27",
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters"
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'basic_user_with_report_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', "Basic User with Reports", 'uri'),
                'supervisor': rail.find_first_by_attr_and_get_attr(
                    response, 'name', "Supervisor - Edit", 'uri'),
                'schedule_manager': rail.find_first_by_attr_and_get_attr(
                    response, 'name', "Schedule Manager", 'uri')
            }
        )

        getall_enabled_departments_28 = rail.RepliconServiceOperator(
            task_id='getall_enabled_departments_28',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "100000",
            "columnUris": [
                "urn:replicon:department-group-list-column:department-group",
                "urn:replicon:department-group-list-column:effectively-enabled",
                "urn:replicon:department-group-list-column:full-path"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": "true",
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": None,
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None
                },
                "filterDefinitionUri": None
                },
                "value": None,
                "filterDefinitionUri": None
            }
            },
            data_handler=lambda response: [{
            "departmentgroupname": item["cells"][0]["textValue"],
            "departmentgroupuri": item["cells"][0]["uri"],
            "fullpath": " / ".join([cell["textValue"] for cell in item["cells"][2]["cellCollection"]])
            }for item in response['rows']]
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'date_of_birth_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Date of Birth', 'uri', ''),
                'title_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Title', 'uri', ''),
                'workersubtypeuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Worker Sub Type', 'uri', ''),
                'years_of_service_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Years of Service', 'uri', ''),
                'hrm_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'HRM', 'uri', ''),
                'continous_years_of_service_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Continuous Years of Service - YOS', 'uri', ''),
                'timeoffservicedate_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Time off Service Date - YOSS', 'uri', ''),
                'gender_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Gender', 'uri', ''),
                'function_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Function', 'uri', ''),
                'workshift_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Work Shift', 'uri', ''),
                'workertype_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Worker Type', 'uri', '')
            }
        )

        create_child_triggered_list = rail.SetVariableOperator(
            task_id='create_child_triggered_list',
            name='childtriggered',
            append=False,
            value=[]
        )

        foreach_query_list_usershereloginnameispresent_22_31 = rail.ForEachOperator(
            task_id='foreach_query_list_usershereloginnameispresent_22_31',
            items="{{result('query_list_usershereloginnameispresent_22')}}",
            start_task='declare_list',
            end_task='foreach_query_list_usershereloginnameispresent_22_31_end'
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='userlist',
            value=[]
        )

        search_users_33 = rail.RepliconServiceOperator(
            task_id='search_users_33',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: request_payload.get_user_by_search_payload(
                rail.result('foreach_query_list_usershereloginnameispresent_22_31')['User_ID']),
            data_handler=lambda response: response['rows'] if response['rows'] else [
            ]
        )

        if_user_name_textvalue_present = rail.IfOperator(
            task_id='if_user_name_textvalue_present',
            test='''{{result('search_users_33') | is_truthy }}''',
            yes_task="foreach_search_users_33",
            no_task="log_ifuserexistsuseruri_and_departmentgroupuri_36_37",
        )

        foreach_search_users_33 = rail.ForEachOperator(
            task_id='foreach_search_users_33',
            items=lambda: rail.result('search_users_33'),
            start_task='insert_to_list',
            end_task='foreach_search_users_33_end'
        )

        def build_user_list_item():
            # GetData cells omit keys with no value (e.g. list-type:null cells have
            # no textValue; empty date cells have no dateValue) — use .get() and
            # normalize dateValue dicts to the Y-m-d strings downstream tasks parse.
            cells = rail.result('foreach_search_users_33')['cells']

            def cell_date(cell):
                date_value = cell.get('dateValue')
                if cell.get('textValue') and date_value:
                    return f"{date_value['year']}-{date_value['month']:02d}-{date_value['day']:02d}"
                return null

            return {
                "username": cells[0]['textValue'].lower() if cells[0].get('textValue') else null,
                "useruri": cells[0].get('uri'),
                "status": cells[3].get('boolValue'),
                "enddate": cell_date(cells[1]),
                "startdate": cell_date(cells[2]),
                "employee_type": cells[4].get('textValue') if len(cells) > 4 and cells[4].get('textValue') else null,
            }

        insert_to_list = rail.SetVariableOperator(
            task_id='insert_to_list',
            append=True,
            name='{{ result("declare_list").name }}',
            value=build_user_list_item
        )

        foreach_search_users_33_end = rail.EmptyOperator(
            task_id='foreach_search_users_33_end',
        )

        log_ifuserexistsuseruri_and_departmentgroupuri_36_37 = rail.PythonOperator(
            task_id='log_ifuserexistsuseruri_and_departmentgroupuri_36_37',
            python_callable=lambda: {
                'useruri': rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('userlist'), 'username', rail.result(
                    'foreach_query_list_usershereloginnameispresent_22_31')['User_ID'].lower(), 'useruri') if rail.get_dag_run_var('userlist') else null,
                'departmentgroupuri': rail.find_first_by_attr_and_get_attr(rail.result('getall_enabled_departments_28'), 'departmentgroupname',  rail.result(
                    'foreach_query_list_usershereloginnameispresent_22_31')['Location'], 'departmentgroupuri') if rail.result('getall_enabled_departments_28') else null
            }
        )

        log_legalentity_paygroup_and_costcenter_uris_38_39_40 = rail.PythonOperator(
            task_id='log_legalentity_paygroup_and_costcenter_uris_38_39_40',
            python_callable=lambda: {
                'legalentityuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_divisions_25'), 'displayText',  rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Legal_entity'], 'uri', '') if rail.result('get_all_enabled_divisions_25') else null,
                'paygroupuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_service_center_details_26'), 'displayText',  rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Paygroup'], 'uri', '') if rail.result('get_all_enabled_service_center_details_26') else null,
                'costcenteruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_costcenters_27'), 'displayText',  rail.result('foreach_query_list_usershereloginnameispresent_22_31')['Cost_center'], 'uri', '') if rail.result('get_all_enabled_costcenters_27') else null,
            }
        )

        if_log_ifuserexistsuseruri_36_present_41 = rail.IfOperator(
            task_id='if_log_ifuserexistsuseruri_36_present_41',
            test='''{{ result('log_ifuserexistsuseruri_and_departmentgroupuri_36_37').useruri | is_truthy }}''',
            yes_task="log_enddatepresent_and_userstatus_42_43",
            no_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_1_72",
        )

        log_enddatepresent_and_userstatus_42_43 = rail.PythonOperator(
            task_id='log_enddatepresent_and_userstatus_42_43',
            python_callable=lambda: {
                'enddatepresent': rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('userlist'), 'username', rail.result(
                    'foreach_query_list_usershereloginnameispresent_22_31')['User_ID'].lower(), 'enddate', '') if rail.get_dag_run_var('userlist') else null,
                'userstatus': rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('userlist'), 'username', rail.result(
                    'foreach_query_list_usershereloginnameispresent_22_31')['User_ID'].lower(), 'status', '') if rail.get_dag_run_var('userlist') else null
            }
        )

        if_log_userstatus_43_equals_to_false_44 = rail.IfOperator(
            task_id='if_log_userstatus_43_equals_to_false_44',
            test='''{{ result('log_enddatepresent_and_userstatus_42_43').userstatus | is_falsy }}''',
            yes_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_present_45",
            no_task="if_log_userstatus_43_equals_to_true_63",
        )

        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_45 = rail.IfOperator(
            task_id='if_foreach_query_list_usershereloginnameispresent_22_31_active_present_45',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Active | is_truthy  and result('foreach_query_list_usershereloginnameispresent_22_31').Active == '0' }}''',
            yes_task="if_log_enddatepresent_42_present_46",
            no_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60",
        )

        if_log_enddatepresent_42_present_46 = rail.IfOperator(
            task_id='if_log_enddatepresent_42_present_46',
            test='''{{ result('log_enddatepresent_and_userstatus_42_43').enddatepresent | is_truthy }}''',
            yes_task="momentive_user_import_logs_add_entry_47",
            no_task="if_termination_date_is_present_49",
        )

        momentive_user_import_logs_add_entry_47 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_47',
            log="{{result('create_log_momentive_user_import_log') }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
                "userid": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').User_ID }}",
                "username": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').First_Name }} {{ result('foreach_query_list_usershereloginnameispresent_22_31').Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User is already disabled in Replicon with end date"
            }
        )

        if_termination_date_is_present_49 = rail.IfOperator(
            task_id='if_termination_date_is_present_49',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Termination_Date | is_truthy }}''',
            yes_task="if_to_date_to_time_equals_to_todayto_time_50",
            no_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60",
        )

        if_to_date_to_time_equals_to_todayto_time_50 = rail.IfOperator(
            task_id='if_to_date_to_time_equals_to_todayto_time_50',
            test=lambda: bool(datetime.strptime(rail.result('foreach_query_list_usershereloginnameispresent_22_31')[
                              'Termination_Date'], "%Y-%m-%d").date() >= now(tz=config.time_zone).date()),
            yes_task="log_split_dates",
            no_task="momentive_user_import_logs_add_entry_59",
        )

        def startdate_and_enddate_splitter():
            startdate = rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('userlist'), 'username', rail.result(
                'foreach_query_list_usershereloginnameispresent_22_31')['User_ID'].lower(), 'startdate', '') if rail.get_dag_run_var('userlist') else null

            return {
                'start_date': startdate,
                'start_date_split': python_callable.split_date_string(startdate, 'int') if startdate else '',
                'end_date_split': python_callable.split_date_string(rail.result(
                    'log_enddatepresent_and_userstatus_42_43')['enddatepresent'], 'int') if rail.result(
                    'log_enddatepresent_and_userstatus_42_43')['enddatepresent'] else ''
            }

        log_split_dates = rail.PythonOperator(
            task_id='log_split_dates',
            python_callable=startdate_and_enddate_splitter
        )

        if_to_date_to_time_less_than_dataloggerlog_startdatefortheuser_51messageto_dateto_time_54 = rail.IfOperator(
            task_id='if_to_date_to_time_less_than_dataloggerlog_startdatefortheuser_51messageto_dateto_time_54',
            test=lambda: datetime.strptime(rail.result('foreach_query_list_usershereloginnameispresent_22_31')[
                                           'Termination_Date'], "%Y-%m-%d") < datetime.strptime(rail.result('log_split_dates')['start_date'], "%Y-%m-%d"),
            yes_task="momentive_user_import_logs_add_entry_55",
            no_task="trigger_disable_user_child_dag_57",
        )

        momentive_user_import_logs_add_entry_55 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_55',
            log="{{result('create_log_momentive_user_import_log') }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
                "userid": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').User_ID }}",
                "username": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').First_Name }} {{ result('foreach_query_list_usershereloginnameispresent_22_31').Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User was already disabled in Replicon, end date was updated since end date received is in the past"
            }
        )

        momentive_user_import_logs_add_entry_59 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_59',
            log="{{result('create_log_momentive_user_import_log') }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
                "userid": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').User_ID }}",
                "username": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').First_Name }} {{ result('foreach_query_list_usershereloginnameispresent_22_31').Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User not disabled since end date received is in the past"
            }
        )

        trigger_disable_user_child_dag_57 = rail.TriggerDagRunOperator(
            task_id='trigger_disable_user_child_dag_57',
            trigger_dag_id=config.momentive_japan_user_sync_child_disable_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.conf_payload('disablewithenddate')
        )

        insert_childid_to_wait_list_1 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_1',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_disable_user_child_dag_57')}}"
        )

        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60 = rail.IfOperator(
            task_id='if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Active | is_truthy  and result('foreach_query_list_usershereloginnameispresent_22_31').Active == '1' }}''',
            yes_task="log_user_emp_type_61",
            no_task="if_log_userstatus_43_equals_to_true_63",
        )

        log_user_emp_type_61 = rail.PythonOperator(
            task_id='log_user_emp_type_61',
            python_callable=lambda: {
                'employee_type': rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('userlist'), 'username', rail.result(
                    'foreach_query_list_usershereloginnameispresent_22_31')['User_ID'].lower(), 'employee_type', '') if rail.get_dag_run_var('userlist') else null
            }
        )

        trigger_dag_run_live_momentive_user_sync_update_v3_62 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_momentive_user_sync_update_v3_62',
            trigger_dag_id=config.momentive_japan_user_sync_child_update_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.conf_payload('rehire')
        )

        insert_childid_to_wait_list_2 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_2',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_live_momentive_user_sync_update_v3_62')}}"
        )

        if_log_userstatus_43_equals_to_true_63 = rail.IfOperator(
            task_id='if_log_userstatus_43_equals_to_true_63',
            test='''{{ result('log_enddatepresent_and_userstatus_42_43').userstatus | is_truthy }}''',
            yes_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_present_64",
            no_task="foreach_query_list_usershereloginnameispresent_22_31_end",
        )

        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_64 = rail.IfOperator(
            task_id='if_foreach_query_list_usershereloginnameispresent_22_31_active_present_64',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Active | is_truthy  and result('foreach_query_list_usershereloginnameispresent_22_31').Active == '0' }}''',
            yes_task="trigger_dag_child_workflow_to_disable_user_65",
            no_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_present_66",
        )

        trigger_dag_child_workflow_to_disable_user_65 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_child_workflow_to_disable_user_65',
            trigger_dag_id=config.momentive_japan_user_sync_child_disable_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.conf_payload('disable')
        )

        insert_childid_to_wait_list_3 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_3',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_child_workflow_to_disable_user_65')}}"
        )

        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_66 = rail.IfOperator(
            task_id='if_foreach_query_list_usershereloginnameispresent_22_31_active_present_66',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Active | is_truthy  and result('foreach_query_list_usershereloginnameispresent_22_31').Active == '1' }}''',
            yes_task="log_user_emp_type_67",
            no_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_blank_69",
        )

        log_user_emp_type_67 = rail.PythonOperator(
            task_id='log_user_emp_type_67',
            python_callable=lambda: {
                'employee_type': rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('userlist'), 'username', rail.result(
                    'foreach_query_list_usershereloginnameispresent_22_31')['User_ID'].lower(), 'employee_type', '') if rail.get_dag_run_var('userlist') else null
            }
        )

        trigger_dag_run_live_momentive_user_sync_update_v3_68 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_momentive_user_sync_update_v3_68',
            trigger_dag_id=config.momentive_japan_user_sync_child_update_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.conf_payload('update')
        )

        insert_childid_to_wait_list_4 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_4',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_live_momentive_user_sync_update_v3_68')}}"
        )

        if_foreach_query_list_usershereloginnameispresent_22_31_active_blank_69 = rail.IfOperator(
            task_id='if_foreach_query_list_usershereloginnameispresent_22_31_active_blank_69',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Active | is_falsy  or result('foreach_query_list_usershereloginnameispresent_22_31').Active == '-' }}''',
            yes_task="momentive_user_import_logs_add_entry_70",
            no_task="foreach_query_list_usershereloginnameispresent_22_31_end",
        )

        momentive_user_import_logs_add_entry_70 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_70',
            log="{{result('create_log_momentive_user_import_log')}}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
                "userid": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').User_ID }}",
                "username": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').First_Name }} {{ result('foreach_query_list_usershereloginnameispresent_22_31').Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User status (Active) received blank value or '-'"
            }
        )

        if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_1_72 = rail.IfOperator(
            task_id='if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_1_72',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Active == '1' }}''',
            yes_task="trigger_dag_run_live_momentive_user_sync_add_v3_73",
            no_task="if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_0_74",
        )

        trigger_dag_run_live_momentive_user_sync_add_v3_73 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_momentive_user_sync_add_v3_73',
            trigger_dag_id=config.momentive_japan_user_sync_child_add_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.conf_payload('add')
        )

        insert_childid_to_wait_list_5 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_5',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_dag_run_live_momentive_user_sync_add_v3_73')}}"
        )

        if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_0_74 = rail.IfOperator(
            task_id='if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_0_74',
            test='''{{ result('foreach_query_list_usershereloginnameispresent_22_31').Active == '0'  or result('foreach_query_list_usershereloginnameispresent_22_31').Active == '-' }}''',
            yes_task="momentive_user_import_logs_add_entry_75",
            no_task="foreach_query_list_usershereloginnameispresent_22_31_end",
        )

        momentive_user_import_logs_add_entry_75 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_75',
            log="{{result('create_log_momentive_user_import_log')}}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
                "userid": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').User_ID }}",
                "username": "{{ result('foreach_query_list_usershereloginnameispresent_22_31').First_Name }} {{ result('foreach_query_list_usershereloginnameispresent_22_31').Last_Name }}",
                "action": "Add",
                "status": "Skipped",
                "details": "User is  disabled in workday hence not added"
            }
        )

        foreach_query_list_usershereloginnameispresent_22_31_end = rail.EmptyOperator(
            task_id="foreach_query_list_usershereloginnameispresent_22_31_end"
        )

        get_child_dag_ids = rail.PythonOperator(
            task_id='get_child_dag_ids',
            python_callable=lambda: json.dumps(
                rail.get_dag_run_var("childtriggered"))
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("get_child_dag_ids")}}'
        )

        momentive_supervisor_assignment_search_entries_77 = rail.FilterLogEntriesOperator(
            task_id='momentive_supervisor_assignment_search_entries_77',
            log="{{result('create_log_momentive_supervisor_assignment')}}",
            properties={
                'parentjobid': "{{dag_run_ecid()}}",
            }
        )

        trigger_dag_run_momentive_supervisor_assignment_80 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_momentive_supervisor_assignment_80',
            retries=0,
            items="{{result('momentive_supervisor_assignment_search_entries_77')}}",
            trigger_dag_id=config.momentive_japan_user_sync_supervisor_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "loginname": "{{ item.properties.loginid }}",
                "supervisorloginname": "{{ item.properties.supervisorempid}}",
                "useruri": "{{ item.properties.useruri }}",
                "parentjobid": "{{ dag_run_ecid() }}",
                "type": "{{ item.properties.type }}",
                "user_import_logs": "{{ result('create_log_momentive_user_import_log') }}",
                "childjobid": "{{ item.properties.childjobid }}",
                "sup_firstname": "{{ item.properties.sup_firstname}}",
                "sup_lastname": "{{ item.properties.sup_lastname }}",
                "sup_email": "{{ item.properties.sup_email }}",
                "sup_change_effectivedate": "{{item.properties.sup_change_effective_date}}",
                "supervisor": "{{ result('get_all_permissionsets')['supervisor'] }}"
            }
        )

        wait_for_completion_trigger_dag_run_momentive_supervisor_assignment_80 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_momentive_supervisor_assignment_80',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_momentive_supervisor_assignment_80") }}'
        )

        momentive_supervisor_restriction_search_entries_82 = rail.FilterLogEntriesOperator(
            task_id='momentive_supervisor_restriction_search_entries_82',
            log="{{result('create_log_momentive_supervisor_restriction')}}",
            properties={
                'parentjobid': "{{dag_run_ecid()}}",
            }
        )

        trigger_dag_run_momentive_supervisor_restriction_84 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_momentive_supervisor_restriction_84',
            retries=0,
            items="{{result('momentive_supervisor_restriction_search_entries_82')}}",
            trigger_dag_id=config.momentive_japan_user_sync_supervisor_restriction_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "log": "{{result('create_log_momentive_supervisor_restriction')}}",
                "supervisoruri": "{{ item.properties.useruri }}",
                "permissionuri": "{{ item.properties.permissionseturi}}",
                "status": "{{ item.properties.status }}",
                "reporturi": "{{ result('log_report_uri_5') }}",
                "userfilteruri": "{{ result('log_userfilter_uri_7') }}",
                "entryid": "{{ item.entryid }}"
            }
        )

        wait_for_completion_trigger_dag_run_momentive_supervisor_restriction_85 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_momentive_supervisor_restriction_85',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_momentive_supervisor_restriction_84") }}'
        )

        search_log_entries = rail.FilterLogEntriesOperator(
            task_id='search_log_entries',
            log="{{result('create_log_momentive_user_import_log')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id='compose_logs_csv',
            source="{{ result('search_log_entries') }}",
            header=['userid',
                    'username', 'action', 'status', 'details', 'jobid'],
            row=lambda item: [
                item['properties']['userid'],
                item['properties']['username'],
                (item['properties']['action'].split('|'))[
                    0] if '|' in item['properties']['action'] else item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                item['ecid']
            ],
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content='''{{ result('compose_logs_csv') }}''',
            remote_filepath=config.log_filepath +
            '''/japan_userimport_log_{{ result('log_todaysdate_2') }}.csv''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name='''japan_userimport_log_{{ result('log_todaysdate_2') }}.csv''',
            expires_in_seconds=7*24*60*60,
        )

        if_log_upload_successful = rail.IfOperator(
            task_id='if_log_upload_successful',
            test='{{ get_task_state("upload_logs_to_sftp") == "success" }}',
            yes_task='check_for_error_log',
            no_task='send_alert_mail_log_upload_unsuccessful'
        )

        send_alert_mail_log_upload_unsuccessful = rail.EmailOperator(
            task_id='send_alert_mail_log_upload_unsuccessful',
            to='{{ var.value.dagrun_failure_alert_email }}',
            subject='''{{get_company_key() }} -Japan |  Failed while uploading User import Logs to SFTP  - {{ current_time() }} ''',
            html_content='''templates/log_upload_failure.html''',
            params=None,
        )

        check_for_error_log = rail.FilterLogEntriesOperator(
            task_id='check_for_error_log',
            log="{{result('create_log_momentive_user_import_log')}}",
            properties={'status': 'Error'}
        )

        check_for_exception_log = rail.FilterLogEntriesOperator(
            task_id='check_for_exception_log',
            log="{{result('create_log_momentive_user_import_log')}}",
            properties={'status': 'Exception'}
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('check_for_error_log', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User import - " }} \
                {%- if result("check_for_error_log", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("check_for_exception_log", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/import_complete_mail.html",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_specific_report_details >> log_report_uri_5 >> log_userfilter_uri_7 >> if_instance_trial

        if_instance_trial >> rail.Label(
            'No') >> get_workdayreport_http_payload >> workdayreport_json_load >> if_first_employee_id_blank_1_8

        if_first_employee_id_blank_1_8 >> rail.Label(
            'No') >> get_write_csv_task_source

        if_first_employee_id_blank_1_8 >> rail.Label(
            'Yes') >> send_mail_no_change_records >> finish

        if_instance_trial >> rail.Label('Yes') >> new_file_sensor_to_process

        new_file_sensor_to_process >> was_new_file_found

        was_new_file_found >> rail.Label('No') >> delete_dagrun
        was_new_file_found >> rail.Label(
            'Yes') >> download_sftp_file >> parse_user_sync_csv >> get_write_csv_task_source
        download_sftp_file >> archive_input_file

        get_write_csv_task_source >> log_todaysdate_2 >> create_csv_lines_12

        create_csv_lines_12 >> if_record_count_less_than_1_15

        if_record_count_less_than_1_15 >> rail.Label(
            'Yes') >> send_mail_no_change_records >> finish
        if_record_count_less_than_1_15 >> rail.Label('No') >> create_collection_create_list_from_csv \
            >> query_list_usershereloginnameisblank_20 >> create_log_momentive_user_import_log >> create_log_momentive_supervisor_assignment>> create_log_momentive_supervisor_restriction \
            >> momentive_user_import_logs_skipped_entries \
            >> query_list_usershereloginnameispresent_22 >> if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23

        if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23 >> rail.Label(
            'No') >> send_mail_no_change_records >> finish

        if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23 >> rail.Label("Yes") >> get_all_enabled_divisions_25 >> get_all_enabled_service_center_details_26 >> get_all_enabled_costcenters_27 >> get_all_permissionsets >>\
        getall_enabled_departments_28 >> get_required_user_customfields >> create_child_triggered_list >> foreach_query_list_usershereloginnameispresent_22_31 >>  declare_list >> search_users_33 >> if_user_name_textvalue_present

        if_user_name_textvalue_present >> rail.Label(
            'Yes') >> foreach_search_users_33 >> insert_to_list >> foreach_search_users_33_end
        foreach_search_users_33 >> foreach_search_users_33_end >> log_ifuserexistsuseruri_and_departmentgroupuri_36_37
        if_user_name_textvalue_present >> rail.Label(
            'No') >> log_ifuserexistsuseruri_and_departmentgroupuri_36_37

        log_ifuserexistsuseruri_and_departmentgroupuri_36_37 >> log_legalentity_paygroup_and_costcenter_uris_38_39_40 >> if_log_ifuserexistsuseruri_36_present_41

        if_log_ifuserexistsuseruri_36_present_41 >> rail.Label('Yes') >> log_enddatepresent_and_userstatus_42_43 >> if_log_userstatus_43_equals_to_false_44
        if_log_ifuserexistsuseruri_36_present_41 >> rail.Label('No') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_1_72

        if_log_userstatus_43_equals_to_false_44 >> rail.Label('Yes') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_45 
        if_log_userstatus_43_equals_to_false_44 >> rail.Label('No') >> if_log_userstatus_43_equals_to_true_63
        
        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_45 >> rail.Label('Yes') >> if_log_enddatepresent_42_present_46
        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_45 >> rail.Label('No') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60
        
        if_log_enddatepresent_42_present_46 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_47 >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60
        if_log_enddatepresent_42_present_46 >> rail.Label('No') >> if_termination_date_is_present_49

        if_termination_date_is_present_49 >> rail.Label('Yes') >> if_to_date_to_time_equals_to_todayto_time_50
        if_termination_date_is_present_49 >> rail.Label('No') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60

        if_to_date_to_time_equals_to_todayto_time_50 >> rail.Label('Yes') >> log_split_dates >> if_to_date_to_time_less_than_dataloggerlog_startdatefortheuser_51messageto_dateto_time_54
        if_to_date_to_time_equals_to_todayto_time_50 >> rail.Label('No') >> momentive_user_import_logs_add_entry_59 >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60

        if_to_date_to_time_less_than_dataloggerlog_startdatefortheuser_51messageto_dateto_time_54 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_55>> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60
        if_to_date_to_time_less_than_dataloggerlog_startdatefortheuser_51messageto_dateto_time_54 >> rail.Label('No') >> trigger_disable_user_child_dag_57 >> insert_childid_to_wait_list_1

        insert_childid_to_wait_list_1 >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60 >> rail.Label('Yes') >> log_user_emp_type_61 >> trigger_dag_run_live_momentive_user_sync_update_v3_62 >> insert_childid_to_wait_list_2
        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_rehire_60 >> rail.Label('No') >> if_log_userstatus_43_equals_to_true_63

        insert_childid_to_wait_list_2 >> if_log_userstatus_43_equals_to_true_63 >> rail.Label('Yes') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_64
        if_log_userstatus_43_equals_to_true_63 >> rail.Label('No') >> foreach_query_list_usershereloginnameispresent_22_31_end

        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_64 >> rail.Label('Yes') >> trigger_dag_child_workflow_to_disable_user_65 >> insert_childid_to_wait_list_3
        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_64 >> rail.Label('No') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_66

        insert_childid_to_wait_list_3 >> if_foreach_query_list_usershereloginnameispresent_22_31_active_present_66 >> rail.Label('Yes') >> log_user_emp_type_67 >> trigger_dag_run_live_momentive_user_sync_update_v3_68 >> insert_childid_to_wait_list_4
        if_foreach_query_list_usershereloginnameispresent_22_31_active_present_66 >> rail.Label('No') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_blank_69

        insert_childid_to_wait_list_4 >> if_foreach_query_list_usershereloginnameispresent_22_31_active_blank_69 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_70 >> foreach_query_list_usershereloginnameispresent_22_31_end
        if_foreach_query_list_usershereloginnameispresent_22_31_active_blank_69 >> rail.Label('No') >> foreach_query_list_usershereloginnameispresent_22_31_end

        if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_1_72 >> rail.Label('Yes') >> trigger_dag_run_live_momentive_user_sync_add_v3_73 >> insert_childid_to_wait_list_5
        if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_1_72 >> rail.Label('No') >> if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_0_74

        insert_childid_to_wait_list_5 >> if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_0_74 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_75 >> foreach_query_list_usershereloginnameispresent_22_31_end
        if_foreach_query_list_usershereloginnameispresent_22_31_active_equals_to_0_74 >> rail.Label('No') >> foreach_query_list_usershereloginnameispresent_22_31_end

        foreach_query_list_usershereloginnameispresent_22_31 >> foreach_query_list_usershereloginnameispresent_22_31_end >> get_child_dag_ids >> wait_for_child_dags \
            >> momentive_supervisor_assignment_search_entries_77 >> trigger_dag_run_momentive_supervisor_assignment_80 \
            >> wait_for_completion_trigger_dag_run_momentive_supervisor_assignment_80 >> momentive_supervisor_restriction_search_entries_82 >> trigger_dag_run_momentive_supervisor_restriction_84 >> wait_for_completion_trigger_dag_run_momentive_supervisor_restriction_85 >> search_log_entries
    
        search_log_entries >> compose_logs_csv >> upload_logs_to_sftp >> generate_download_link >> if_log_upload_successful
        if_log_upload_successful >> rail.Label(
            'Yes') >> check_for_error_log >> check_for_exception_log >> send_import_complete_email >> finish

        if_log_upload_successful >> rail.Label(
            'No') >> send_alert_mail_log_upload_unsuccessful >> finish

        return dag

rail.for_each_instance(create_dag)
