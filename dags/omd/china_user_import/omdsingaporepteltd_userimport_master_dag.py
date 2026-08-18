import hashlib
from datetime import timedelta, datetime
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'omdsingaporepteltd_china_user_import_master_{config.instance}',
        description=f'Omdsingaporepteltd_UserImport_Master_{config.instance}',
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
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
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
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath + "{{dag_run_ecid()}}" +
            "_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        if_name_ends_with_csv=rail.IfOperator(
            task_id='if_name_ends_with_csv',
            test='''{{ result('new_file_sensor') | ends_with('.csv')}}''',
            yes_task="parse_csv",
            no_task="send_mail_incorrect_file_format",
        )

        send_mail_incorrect_file_format=rail.EmailOperator(
            task_id='send_mail_incorrect_file_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key()}}" + "|" + "User import - File processing is skipped -" + "{{current_time('%d%m%Y%H%M%S')}}",
            html_content='templates/incorrect_file_format_mail.html',
        )

        rename_archived_file=rail.SFTPMoveFileOperator(
            task_id='rename_archived_file',
            new_filename= config.archive_filepath + "Skipped_" + "{{result('new_file_sensor') | file_name}}",
            existing_filename= config.archive_filepath + "{{dag_run_ecid()}}" +
            "_{{ result('new_file_sensor') | file_name }}",
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{result('download_file')}}",
            delimiter=',',
        )

        compose_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('parse_csv') }}",
            delimiter=',',
            header=['loginname',
                    'firstname',
                    'lastname',
                    'department',
                    'loginstatus',
                    'empid',
                    'employeetype',
                    'supervisorid',
                    'startdate',
                    'enddate',
                    'email',
                    'location',
                    'division',
                    'legalentity',
                    'costcenter',
                    'md5'],
            row=lambda item:
            [
                item['Login_Name'],
                item['First_name'],
                item['Last_name'],
                item['Department'],
                item['Login_status'],
                item['Emp_id'],
                item['Employee_type'],
                item['Supervisor_id'],
                item['Startdate'],
                item['Enddate'],
                item['Email'],
                item['Location'],
                item['Division'],
                item['Legal_Entity'],
                item['Cost_Center'],
                hashlib.md5((str(item['Login_Name'])+str(item['First_name'])+str(item['Last_name'])+str(item['Department'])+
                             str(item['Login_status'])+
                             str(item['Emp_id']) +
                             str(item['Employee_type']) +
                             str(item['Supervisor_id']) +
                             str(item['Startdate']) +
                             str(item['Enddate']) +
                             str(item['Email']) +
                             str(item['Location']) +
                             str(item['Division']) +
                             str(item['Legal_Entity']) +
                             str(item['Cost_Center'])).encode('utf-8')).hexdigest(),
            ]
        )

        create_rawdata_collection = rail.CreateCollectionOperator(
            task_id='create_rawdata_collection',
            source = "{{ result('compose_csv') }}",
            name = "rawdatafile",
        )

        create_log_lookup_table = rail.CreateLogOperator(
            task_id = 'create_log_lookup_table'
        )

        if_no_data_in_input_file=rail.IfOperator(
            task_id='if_no_data_in_input_file',
            test='''{{ result('create_rawdata_collection','length') < 1 }}''',
            yes_task="send_mail_no_data_in_file",
            no_task="query_users_with_blank_loginname_or_employeenumber",
        )

        send_mail_no_data_in_file=rail.EmailOperator(
            task_id='send_mail_no_data_in_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}}" + "|" + "User import - no records in file" + "{{current_time('%d%m%Y%H%M%S')}}",
            html_content="templates/no_data_mail.html",
            params=None,
        )

        rename_archived_file_when_no_data=rail.SFTPMoveFileOperator(
            task_id='rename_archived_file_when_no_data',
            new_filename= config.archive_filepath + "Skipped_" + "{{result('new_file_sensor') | file_name}}",
            existing_filename= config.archive_filepath + "{{dag_run_ecid()}}" +
            "_{{ result('new_file_sensor') | file_name }}",
        )

        query_users_with_blank_loginname_or_employeenumber=rail.QueryCollectionOperator(
            task_id='query_users_with_blank_loginname_or_employeenumber',
            query="""SELECT * FROM  rawdatafile WHERE NULLIF(loginname,'') IS NULL OR NULLIF(firstname,'') IS NULL OR NULLIF(lastname,'') IS NULL""",
        )

        if_there_are_users_with_blank_loginname=rail.IfOperator(
            task_id='if_there_are_users_with_blank_loginname',
            test="{{ result('query_users_with_blank_loginname_or_employeenumber','length') > 0}}",
            yes_task="log_loginname_not_present",
            no_task="get_users_with_valid_loginname_and_employeenumber",
        )

        log_loginname_not_present=rail.WriteLogOperator(
            task_id='log_loginname_not_present',
            log="{{ result('create_log_lookup_table') }}",
            message="na",
            severity="Skipped",
            items="{{result('query_users_with_blank_loginname_or_employeenumber')}}",
            properties=lambda item:{
                "loginname": item['loginname'],
                "action": "validation",
                "status": "skipped",
                "details": "loginname or firstname or lastname is not present",
                "jobid": rail.render_template('{{dag_run_ecid()}}'),
                "childjobid": ''
            }
        )

        get_users_with_valid_loginname_and_employeenumber=rail.QueryCollectionOperator(
            task_id='get_users_with_valid_loginname_and_employeenumber',
            query="""SELECT * FROM  rawdatafile WHERE NULLIF(loginname,'') IS NOT NULL AND NULLIF(firstname,'') IS NOT NULL AND
                    NULLIF(lastname,'') IS NOT NULL""",
        )

        create_validated_input_list_collection = rail.CreateCollectionOperator(
            task_id='create_validated_input_list_collection',
            source = "{{ result('get_users_with_valid_loginname_and_employeenumber')}}",
            name = "validatedinputlist",
        )

        get_user_list_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_user_list_report_details',
            report_name=config.user_list_report
        )

        run_user_list_report = rail.run_report2(
            group_id='run_user_list_report',
            report_params={
                "reportParameters": [
                    {
                    "reportUri": "{{result('get_user_list_report_details').uri}}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        load_csv_from_report_result=rail.LoadCSVFileOperator(
            task_id="load_csv_from_report_result",
            document="{{ (result('run_user_list_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_userlist_from_replicon_collection = rail.CreateCollectionOperator(
            task_id='create_userlist_from_replicon_collection',
            source = "{{ result('load_csv_from_report_result') }}",
            name = "userlistfromreplicon",
        )

        query_list_getallusersfrom_replicon=rail.QueryCollectionOperator(
            task_id='query_list_getallusersfrom_replicon',
            query="""SELECT * FROM  userlistfromreplicon""",
        )

        def get_location_list(response):
            return [{
                "name": item['cells'][0].get('textValue'),
                "uri": item['cells'][0].get('uri'),
                "fullpath": "|".join([location.get('textValue') for location in (item['cells'][1].get('cellCollection'))])
            } for item in response['rows'] ]

        get_location_details=rail.RepliconServiceOperator(
            task_id='get_location_details',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "1000000",
            "columnUris": [
                "urn:replicon:location-list-column:location",
                "urn:replicon:location-list-column:full-path"
            ],
            "sort": [],
            "filterExpression": null
            },
            data_handler=get_location_list
        )

        def get_employee_type_list(response):
            return [{
                "name": item['cells'][0].get('textValue'),
                "uri": item['cells'][0].get('uri'),
                "fullpath": "|".join([employee.get('textValue') for employee in (item['cells'][1].get('cellCollection'))])
            } for item in response['rows'] ]

        get_employee_type_group_details=rail.RepliconServiceOperator(
            task_id='get_employee_type_group_details',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "1000000",
            "columnUris": [
                "urn:replicon:employee-type-group-list-column:employee-type-group",
                "urn:replicon:location-list-column:full-path"
            ],
            "sort": [],
            "filterExpression": null
            },
            data_handler=get_employee_type_list
        )

        def create_cost_center_list(response):
            return [{
                "name": item['cells'][0].get('textValue'),
                "uri": item['cells'][0].get('uri'),
                "fullpath": "|".join([costcentre.get('textValue') for costcentre in (item['cells'][1].get('cellCollection'))])
            } for item in response['rows'] ]

        get_cost_center_list=rail.RepliconServiceOperator(
            task_id='get_cost_center_list',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "1000000",
            "columnUris": [
                "urn:replicon:cost-center-list-column:cost-center",
                "urn:replicon:cost-center-list-column:full-path"
            ],
            "sort": [],
            "filterExpression": null
            },
            data_handler=create_cost_center_list
        )

        def create_entity_list(response):
            return [{
                "name": item['cells'][0].get('textValue'),
                "uri": item['cells'][0].get('uri'),
                "fullpath": "|".join([servicecentre.get('textValue') for servicecentre in (item['cells'][1].get('cellCollection'))])
            } for item in response['rows'] ]

        get_service_center_list=rail.RepliconServiceOperator(
            task_id='get_service_center_list',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "100000",
            "columnUris": [
                "urn:replicon:service-center-list-column:service-center",
                "urn:replicon:service-center-list-column:full-path"
            ],
            "sort": [],
            "filterExpression": null
            },
            data_handler=create_entity_list
        )

        def create_department_group_list(response):
            print(response)
            return [{
                "name": item['cells'][0].get('textValue'),
                "uri": item['cells'][0].get('uri'),
                "fullpath": "|".join([department.get('textValue') for department in (item['cells'][1].get('cellCollection'))])
            } for item in response['rows'] ]

        get_department_group_list=rail.RepliconServiceOperator(
            task_id='get_department_group_list',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "1000000",
            "columnUris": [
                "urn:replicon:department-group-list-column:department-group",
                "urn:replicon:department-group-list-column:full-path"
            ],
            "sort": [],
            "filterExpression": null
            },
            data_handler=create_department_group_list
        )

        def create_division_list(response):
            return [{
                "name": item['cells'][0].get('textValue'),
                "uri": item['cells'][0].get('uri'),
                "fullpath": "|".join([division.get('textValue') for division in (item['cells'][1].get('cellCollection'))])
            } for item in response['rows'] ]

        get_division_list=rail.RepliconServiceOperator(
            task_id='get_division_list',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "100000",
            "columnUris": [
                "urn:replicon:division-list-column:division",
                "urn:replicon:division-list-column:full-path"
            ],
            "sort": [],
            "filterExpression": null
            },
            data_handler=create_division_list
        )

        get_all_permission_sets=rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_policy_sets=rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_time_off_types=rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_all_timesheet_periods=rail.RepliconServiceOperator(
            task_id='get_all_timesheet_periods',
            endpoint="/services/TimesheetPeriodService2.svc/GetPageOfTimesheetPeriodsBySearchParameter",
            data={
            "page": "1",
            "pageSize": "1000",
            "timesheetPeriodSearch": {
                "statusOptionUri": "urn:replicon:timesheet-period-status-option:include-all-timesheet-periods",
                "textSearch": null
            }
            }
        )

        get_all_office_schedules=rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_all_timesheet_approval_paths=rail.RepliconServiceOperator(
            task_id='get_all_timesheet_approval_paths',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
        )

        get_all_time_off_approval_paths=rail.RepliconServiceOperator(
            task_id='get_all_time_off_approval_paths',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
        )

        query_new_users_to_process=rail.QueryCollectionOperator(
            task_id='query_new_users_to_process',
            query="""SELECT * FROM  validatedinputlist WHERE
                    validatedinputlist.loginname NOT IN (SELECT DISTINCT  userlistfromreplicon.Login_Name FROM  userlistfromreplicon)""",
        )

        def get_add_user_payload(item):
            startdate = datetime.strptime(item['startdate'],'%d-%m-%Y') if item['startdate'] else null
            return {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['email'],
                "employeeid": item['empid'],
                "loginname": item['loginname'],
                "employeetype": item['employeetype'],
                "officeschedule": "8 Hours/day;Mon-Fri",
                "startdate": {
                    "day": startdate.day if startdate else null,
                    "month": startdate.month if startdate else null,
                    "year": startdate.year if startdate else null,
                },
                "officescheduleuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_office_schedules'),'displayText','8 hours/day; Mon-Fri',
                    'uri',null) if rail.result('get_all_office_schedules')[0] and rail.result('get_all_office_schedules')[0]['displayText'] else null,
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_employee_type_group_details'),'fullpath',item['employeetype'],
                    'uri',null) if rail.result('get_employee_type_group_details') else null,
                "timesheettemplateuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policy_sets'),'displayText','OMG China Timesheet Template','uri',null)
                    if rail.result('get_all_policy_sets') else null,
                "timeofftemplateuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policy_sets'),'displayText','Time Off','uri',null) if rail.result('get_all_policy_sets') else null,
                "location": item['location'],
                "timesheetapprovalpathuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timesheet_approval_paths'),'displayText','OMG China Timesheet Approver',
                    'uri',null) if rail.result('get_all_timesheet_approval_paths') else null,
                "timeoffapprovalpathuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_time_off_approval_paths'),'displayText','Supervisor','uri',null)
                    if rail.result('get_all_time_off_approval_paths') else null,
                "supervisoremployeeid": item['supervisorid'],
                "department": item['department'],
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_department_group_list'),'fullpath',item['department'],'uri',null)
                    if rail.result('get_department_group_list') else null,
                "employeestatus": item['loginstatus'],
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_location_details'),'fullpath',item['location'],'uri',null)
                    if rail.result('get_location_details') else null,
                "permissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'displayText','China - Employee','uri',null)
                    if rail.result('get_all_permission_sets') else null,
                "timesheetperioduri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timesheet_periods'),'displayText','OMG China Timesheet Period','uri',null)
                    if rail.result('get_all_timesheet_periods') else null,
                "holidaytimeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),'displayText','Holiday','uri',null)
                    if rail.result('get_all_time_off_types') else null,
                "division": item['division'],
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_division_list'),'fullpath',item['division'],'uri',null)
                    if rail.result('get_division_list') else null,
                "costcenter": item['costcenter'],
                "costcenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_cost_center_list'),'fullpath',item['costcenter'],'uri',null)
                    if rail.result('get_cost_center_list') else null,
                "legalentity": item['legalentity'],
                "legalentityuri": rail.find_first_by_attr_and_get_attr(rail.result('get_service_center_list'),'fullpath',item['legalentity'],'uri',null)
                    if rail.result('get_service_center_list') else null,
                "lookuptable": rail.result('create_log_lookup_table'),
                "callerjobid": rail.render_template('{{dag_run_ecid()}}')
            }

        trigger_child_to_add_user=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_to_add_user',
            retries=0,
            items="{{ result('query_new_users_to_process') }}",
            trigger_dag_id=f'omdsingaporepteltd_china_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_add_user_payload
        )

        if_add_child_triggered = rail.IfOperator(
            task_id = 'if_add_child_triggered',
            test="{{result('trigger_child_to_add_user') | is_truthy}}",
            yes_task='wait_for_add_user_child',
            no_task='query_existing_users'
        )

        wait_for_add_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_user_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_add_user") }}'
        )

        query_existing_users=rail.QueryCollectionOperator(
            task_id='query_existing_users',
            query="""SELECT * FROM  validatedinputlist WHERE
                    validatedinputlist.loginname IN (SELECT DISTINCT  userlistfromreplicon.Login_Name FROM  userlistfromreplicon)""",
        )

        create_updateuserlist_collection = rail.CreateCollectionOperator(
            task_id='create_updateuserlist_collection',
            source = "{{ result('query_existing_users') }}",
            name = "updateuserlist",
        )

        if_updateuserlist_has_data=rail.IfOperator(
            task_id='if_updateuserlist_has_data',
            test='''{{ result('create_updateuserlist_collection','length') > 0 }}''',
            yes_task="list_reference_file",
            no_task="search_log_entries",
        )

        list_reference_file = rail.SFTPListFilesOperator(
            task_id='list_reference_file',
            paths=[config.reference_filepath]
        )

        get_reference_filename = rail.PythonOperator(
            task_id= 'get_reference_filename',
            python_callable=lambda: rail.result('list_reference_file')[config.reference_filepath][0]['name']
                if rail.result('list_reference_file') else None
        )

        download_reference_file=rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath= config.reference_filepath + "{{ result('get_reference_filename')}}"
        )

        if_file_ends_with_csv=rail.IfOperator(
            task_id='if_file_ends_with_csv',
            test="{{result('get_reference_filename') | ends_with('.csv')}}",
            yes_task='archive_reference_file',
            no_task='load_csv_from_reference_file'
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            existing_filename=config.reference_filepath + "{{ result('get_reference_filename')}}",
            new_filename=config.archive_filepath + "Old_Ref_{{dag_run_ecid()}}" +
            "{{result('get_reference_filename')}}"
        )

        load_csv_from_reference_file=rail.LoadCSVFileOperator(
            task_id="load_csv_from_reference_file",
            delimiter=',',
            document="{{result('download_reference_file') }}",
        )

        create_userreferencedata_collection = rail.CreateCollectionOperator(
            task_id='create_userreferencedata_collection',
            source = "{{ result('load_csv_from_reference_file') }}",
            name = "userreferencedata",
            columns = {
                'loginname':'loginname', 
                'firstname':'firstname', 
                'lastname':'lastname', 
                'department':'department', 
                'loginstatus':'loginstatus', 
                'empid':'empid', 
                'employeetype':'employeetype', 
                'supervisorid':'supervisorid', 
                'startdate':'startdate', 
                'enddate':'enddate', 
                'email':'email', 
                'location':'location', 
                'division':'division', 
                'legalentity':'legalentity', 
                'costcenter':'costcenter', 
                'md5':'md5'
            }
        )

        query_unchanged_records=rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM  updateuserlist WHERE  updateuserlist.md5 IN (SELECT DISTINCT  userreferencedata.md5 FROM  userreferencedata )""",
        )

        if_file_has_unchanged_records=rail.IfOperator(
            task_id='if_file_has_unchanged_records',
            test='''{{ result('query_unchanged_records','length') > 0 }}''',
            yes_task="log_user_skipped",
            no_task="query_changed_records",
        )

        log_user_skipped=rail.WriteLogOperator(
            task_id='log_user_skipped',
            log="{{ result('create_log_lookup_table') }}",
            message="na",
            severity="Skipped",
            items="{{result('query_unchanged_records')}}",
            properties=lambda item:{
                "loginname": item['loginname'],
                "action": "Update",
                "status": "Skipped",
                "details": "No change in user record",
                "jobid": rail.render_template('{{dag_run_ecid()}}'),
                "childjobid": ''
            }
        )

        query_changed_records=rail.QueryCollectionOperator(
            task_id='query_changed_records',
            query="""SELECT * FROM  updateuserlist WHERE  updateuserlist.md5 NOT IN (SELECT DISTINCT  userreferencedata.md5 FROM  userreferencedata )""",
        )

        if_file_has_changed_records=rail.IfOperator(
            task_id='if_file_has_changed_records',
            test='''{{ result('query_changed_records','length') > 0 }}''',
            yes_task="query_users_tobe_updated",
            no_task="search_log_entries",
        )

        query_users_tobe_updated = rail.QueryCollectionOperator(
            task_id = 'query_users_tobe_updated',
            query="SELECT * FROM userlistfromreplicon JOIN query_changed_records ON userlistfromreplicon.Login_Name = query_changed_records.loginname"
        )

        load_all_users_tobe_updated = rail.PythonOperator(
            task_id = 'load_all_users_tobe_updated',
            python_callable=lambda: rail.load_all_records(rail.result('query_users_tobe_updated'))
        )

        def get_update_user_payload(item):
            startdate = datetime.strptime(item['startdate'],'%d-%m-%Y') if item['startdate'] else null
            enddate = datetime.strptime(item['enddate'],'%d-%m-%Y') if item['enddate'] else null
            return {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['email'],
                "employeeid": item['empid'],
                "startdate": {
                    "day": startdate.day if startdate else null,
                    "month": startdate.month if startdate else null,
                    "year": startdate.year if startdate else null,
                },
                "loginname": item['loginname'],
                "employeetype": item['employeetype'],
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_employee_type_group_details'),'fullpath',item['employeetype'],
                    'uri',null) if rail.result('get_employee_type_group_details') else null,
                "location": item['location'],
                "supervisoremployeeid": item['supervisorid'],
                "department": item['department'],
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_department_group_list'),'fullpath',item['department'],'uri',null)
                    if rail.result('get_department_group_list') else null,
                "employeestatus": item['loginstatus'],
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_location_details'),'fullpath',item['location'],'uri',null)
                    if rail.result('get_location_details') else null,
                "useruri": rail.find_first_by_attr_and_get_attr(rail.result('load_all_users_tobe_updated'),'loginname',item['loginname'],'useruri',null),
                "today": {
                    "day": datetime.now().strftime("%d"),
                    "month": datetime.now().strftime("%m"),
                    "year": datetime.now().strftime("%Y"),
                },
                "userstartdate": item['startdate'],
                "userenddate": item['enddate'],
                "costcenter": item['costcenter'],
                "costcenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_cost_center_list'),'fullpath',item['costcenter'],'uri',null)
                    if rail.result('get_cost_center_list') else null,
                "division": item['division'],
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_division_list'),'fullpath',item['division'],'uri',null)
                    if rail.result('get_division_list') else null,
                "legalentity": item['legalentity'],
                "legalentityuri": rail.find_first_by_attr_and_get_attr(rail.result('get_service_center_list'),'fullpath',item['legalentity'],'uri',null)
                    if rail.result('get_service_center_list') else null,
                "enddate": {
                    "day": enddate.day if enddate else null,
                    "month": enddate.month if enddate else null,
                    "year": enddate.year if enddate else null,
                },
                "lookuptable": rail.result('create_log_lookup_table'),
                "callerjobid": rail.render_template('{{dag_run_ecid()}}')
            }

        trigger_update_user_child=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_update_user_child',
            retries=0,
            items="{{ result('query_changed_records') }}",
            trigger_dag_id=f'omdsingaporepteltd_china_user_import_update_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_update_user_payload
        )

        wait_for_update_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_update_user_child") }}'
        )

        search_log_entries = rail.FilterLogEntriesOperator(
            task_id = 'search_log_entries',
            log = "{{result('create_log_lookup_table')}}",
            properties={
                "jobid": "{{ dag_run_ecid()}}"
            }
        )

        compose_logs=rail.WriteCSVFileOperator(
            task_id='compose_logs',
            source="{{ result('search_log_entries')}}",
            header=['loginname',
                    'Action',
                    'Status',
                    'Details',
                    'JobID',
                    'Child job ID'],
            row= [
                    "{{ item.properties.loginname }}",
                    "{{ item.properties.action }}",
                    "{{ item.properties.status }}",
                    "{{ item.properties.details }}",
                    "{{ item.properties.jobid }}",
                    "{{ item.properties.childjobid }}"],
        )

        upload_logs=rail.SFTPUploadFileOperator(
            task_id='upload_logs',
            content='''{{ result('compose_logs') }}''',
            remote_filepath=config.log_filepath + "Logs_{{ dag_run_ecid() }}{{ result('new_file_sensor') | file_name}}",
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs')}}",
            output_file_name="Logs_{{ dag_run_ecid() }}{{ result('new_file_sensor') | file_name}}",
            expires_in_seconds=7*24*60*60,
        )

        def create_status_object():
            logs = [ item['properties'] for item in rail.load_all_records(rail.result('search_log_entries'))]
            return {
                "errorcheck": rail.find_first_by_attr_and_get_attr(logs,'status','error','details',null),
                "exceptioncheck": rail.find_first_by_attr_and_get_attr(logs,'status','exception','details',null),
                "subject": "completed with errors" if rail.find_first_by_attr_and_get_attr(logs,'status','error','details',null) else (
                            "completed with exceptions" if rail.find_first_by_attr_and_get_attr(
                            logs,'status','exception','details',null) else "completed successfully"),
                #pylint: disable=line-too-long
                "body": "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if rail.find_first_by_attr_and_get_attr(logs,'status','error','details',null) else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>",
            }

        get_status_check_object = rail.PythonOperator(
            task_id = 'get_status_check_object',
            python_callable=create_status_object
        )

        send_mail = rail.EmailOperator(
            task_id='send_mail',
            to=config.tenant_email,
            bcc="{%- if result('get_status_check_object')['errorcheck'] -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject="{{ get_company_key() }} | User import {{result('get_status_check_object').subject}}-{{ current_time() }}",
            html_content='templates/success_email.html',
        )

        upload_new_reference_file=rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('compose_csv') }}''',
            remote_filepath= config.reference_filepath + "Ref_{{result('new_file_sensor') | file_name}}",
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "total_rows":"{{result('get_users_with_valid_loginname_and_employeenumber','length')}}",
                "skipped_in_validation":"{{result('query_users_with_blank_loginname_or_employeenumber','length')}}",
                "add|update":"{{result('query_new_users_to_process','length')}}|{{result('query_changed_records','length')}}"
            }
        )

        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >> if_name_ends_with_csv >> rail.Label('No')  >> send_mail_incorrect_file_format >> rename_archived_file >> finish
        if_name_ends_with_csv >> rail.Label(
            'Yes') >> parse_csv >> compose_csv >> create_rawdata_collection >> create_log_lookup_table >> if_no_data_in_input_file
        if_no_data_in_input_file >> rail.Label('Yes')  >> send_mail_no_data_in_file >> rename_archived_file_when_no_data >> finish
        if_no_data_in_input_file >> rail.Label('No') >> query_users_with_blank_loginname_or_employeenumber >> if_there_are_users_with_blank_loginname
        if_there_are_users_with_blank_loginname >> rail.Label('Yes')  >> log_loginname_not_present >> get_users_with_valid_loginname_and_employeenumber
        if_there_are_users_with_blank_loginname >> rail.Label(
            'No') >> get_users_with_valid_loginname_and_employeenumber >> create_validated_input_list_collection >> get_user_list_report_details
        get_user_list_report_details >> run_user_list_report >> load_csv_from_report_result >> create_userlist_from_replicon_collection
        create_userlist_from_replicon_collection >> query_list_getallusersfrom_replicon >> get_location_details >> get_employee_type_group_details
        get_employee_type_group_details >> get_cost_center_list >> get_service_center_list >> get_department_group_list >> get_division_list
        get_division_list >> get_all_permission_sets >> get_all_policy_sets >> get_all_time_off_types >> get_all_timesheet_periods >> get_all_office_schedules
        get_all_office_schedules >> get_all_timesheet_approval_paths >> get_all_time_off_approval_paths >> query_new_users_to_process
        query_new_users_to_process >> trigger_child_to_add_user >> if_add_child_triggered >> rail.Label(
            'Yes') >> wait_for_add_user_child >> query_existing_users
        if_add_child_triggered >> rail.Label('No') >> query_existing_users >> create_updateuserlist_collection >> if_updateuserlist_has_data
        if_updateuserlist_has_data >> rail.Label(
            'Yes') >> list_reference_file >> get_reference_filename >> download_reference_file >> if_file_ends_with_csv >> rail.Label(
            'Yes') >> archive_reference_file >> load_csv_from_reference_file >> create_userreferencedata_collection >> query_unchanged_records
        query_unchanged_records >> if_file_has_unchanged_records
        if_file_ends_with_csv >> rail.Label('No') >> load_csv_from_reference_file
        if_file_has_unchanged_records >> rail.Label('Yes')  >> log_user_skipped >> query_changed_records
        if_file_has_unchanged_records >> rail.Label('No') >> query_changed_records >> if_file_has_changed_records
        if_file_has_changed_records >> rail.Label(
            'Yes')  >> query_users_tobe_updated >> load_all_users_tobe_updated >> trigger_update_user_child >> wait_for_update_user_child >> search_log_entries
        search_log_entries >> compose_logs >> upload_logs >> generate_download_link >> get_status_check_object >> send_mail >> upload_new_reference_file
        if_file_has_changed_records >> rail.Label('No') >> search_log_entries
        if_updateuserlist_has_data >> rail.Label('No') >> search_log_entries
        upload_new_reference_file >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
