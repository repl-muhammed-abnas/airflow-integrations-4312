
from datetime import timedelta, datetime
import hashlib
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'baylorcollegeofmedicine_userimport_master_{config.instance}',
        description=f'BaylorCollegeOfMedicine_UserImport_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
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
            new_filename=config.archive_filepath +
            "{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}",
            existing_filename="{{ result('new_file_sensor') }}",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_file_name_ends_with_csv_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_file_name_ends_with_csv_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_file_name_ends_with_csv_2 = rail.IfOperator(
            task_id='if_file_name_ends_with_csv_2',
            test='''{{ result('new_file_sensor') | ends_with('csv') }}''',
            yes_task="parse_csv_7",
            no_task="send_mail_notificationforincorrectfileformat_3",
        )

        send_mail_notificationforincorrectfileformat_3 = rail.EmailOperator(
            task_id='send_mail_notificationforincorrectfileformat_3',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''"{{get_company_key()}} | User import - File processing is skipped - {{current_time("%d%m%Y%H%M%S")}} ''',
            html_content='''templates/incorrect_fileformat_mail.html''',
        )

        parse_csv_7 = rail.LoadCSVFileOperator(
            task_id='parse_csv_7',
            document="{{result('download_file')}}",
            delimiter=','
        )

        write_csv_with_encoded = rail.WriteCSVFileOperator(
            task_id='write_csv_with_encoded',
            source="{{result('parse_csv_7')}}",
            header=[
                'First Name',
                'Last Name',
                'Email',
                'Employee ID',
                'Start Date',
                'Login Name',
                'Supervisor',
                'Time Approver',
                'Department Level 2',
                'Department Level 3',
                'Employee Type',
                'Places',
                'Schedule Type',
                'encoded'
            ],
            row=lambda item: [
                item['First Name'].strip() if item['First Name'] else '',
                item['Last Name'].strip() if item['Last Name'] else '',
                item['Email'].strip() if item['Email'] else '',
                item['Employee ID'].strip() if item['Employee ID'] else '',
                (datetime.strptime(item['Start Date'].strip(
                ), '%m/%d/%Y')).strftime("%Y-%m-%d") if item['Start Date'] else '',
                item['Login Name'].strip() if item['Login Name'] else '',
                item['Supervisor'].strip() if item['Supervisor'] else '',
                item['Time Approver'].strip() if item['Time Approver'] else '',
                item['Department Level 2'].strip(
                ) if item['Department Level 2'] else '',
                item['Department Level 3'].strip(
                ) if item['Department Level 3'] else '',
                item['Employee Type'].strip() if item['Employee Type'] else '',
                item['Places'].strip() if item['Places'] else '',
                item['Schedule Type'].strip() if item['Schedule Type'] else '',
                hashlib.md5((str(str(item['First Name']) + str(item['Last Name']) + str(item['Email']) + str(item['Employee ID']) + str(item['Start Date']) +
                str(item['Login Name']) + str(item['Login Name']) + str(item['Supervisor']) + str(item['Supervisor']) + str(item['Time Approver']) +
                str(item['Department Level 2']) + str(item['Department Level 3']) + str(item['Employee Type']) +
                str(item['Places']) +
                str(item['Schedule Type'])
                )).encode('utf-8')).hexdigest()
            ]
        )

        create_collection_create_list_from_csv_raw_data_file_8 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_raw_data_file_8',
            source="{{ result('write_csv_with_encoded') }}",
            name="inputfile",
            columns={
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Email': 'email',
                'Employee ID': 'employeeid',
                'Start Date': 'startdate',
                'Login Name': 'loginname',
                'Supervisor': 'supervisor',
                'Time Approver': 'timeapprover',
                'Department Level 2': 'departmentlevel2',
                'Department Level 3': 'departmentlevel3',
                'Employee Type': 'employeetype',
                'Places': 'places',
                'Schedule Type': 'scheduletype',
                'encoded': 'encoded'
            }
        )

        if_create_list_from_csv_raw_data_file_8_row_count_less_than_1_9 = rail.IfOperator(
            task_id='if_create_list_from_csv_raw_data_file_8_row_count_less_than_1_9',
            test='''{{ result('create_collection_create_list_from_csv_raw_data_file_8','length') < 1 }}''',
            yes_task="send_mail_notificationfornorecords_blank_data_10",
            no_task="query_list_getuserwithblankloginname_17",
        )

        send_mail_notificationfornorecords_blank_data_10 = rail.EmailOperator(
            task_id='send_mail_notificationfornorecords_blank_data_10',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''"{{get_company_key()}} | User import - no records in file" {{ current_time("%d%m%Y%H%M%S")}} ''',
            html_content='''templates/no_records_in_file_mail.html''',
        )

        query_list_getuserwithblankloginname_17 = rail.QueryCollectionOperator(
            task_id='query_list_getuserwithblankloginname_17',
            query="""SELECT * FROM  inputfile WHERE NULLIF(loginname,'') IS NULL """,
        )

        create_user_import_logs_lookuptable = rail.CreateLogOperator(
            task_id='create_user_import_logs_lookuptable'
        )

        create_supervisor_assignment_lookup = rail.CreateLogOperator(
            task_id='create_supervisor_assignment_lookup'
        )

        create_groups_update_logs_lookup = rail.CreateLogOperator(
            task_id='create_groups_update_logs_lookup'
        )

        if_query_list_getuserwithblankloginname_17_rows_greater_than_0_18 = rail.IfOperator(
            task_id='if_query_list_getuserwithblankloginname_17_rows_greater_than_0_18',
            test='''{{ result('query_list_getuserwithblankloginname_17','length') > 0 }}''',
            yes_task="baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_19",
            no_task="query_list_getuserwithvalidloginname_20",
        )

        baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_19 = rail.WriteLogOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_19',
            log="{{ result('create_user_import_logs_lookuptable')}}",
            items="{{result('query_list_getuserwithblankloginname_17')}}",
            message='na',
            severity='na',
            properties={
                "loginname": "{{item.loginname}}",
                "action": "Validation",
                "status": "Skipped",
                "details": "Employee loginname is not present in the file",
                "jobid": "{{dag_run_ecid()}}",
                "childjobid": "",
                "firstname": "{{item.firstname}}",
                "lastname": "{{item.lastname}}"
            }
        )

        query_list_getuserwithvalidloginname_20 = rail.QueryCollectionOperator(
            task_id='query_list_getuserwithvalidloginname_20',
            name="validatedinputlist",
            query="""SELECT * FROM  inputfile WHERE NULLIF(loginname,'') IS NOT NULL """,
        )

        query_list_groups_data_22 = rail.QueryCollectionOperator(
            task_id='query_list_groups_data_22',
            name='groupsdata',
            query="""SELECT DISTINCT  validatedinputlist.timeapprover, validatedinputlist.departmentlevel2, validatedinputlist.departmentlevel3,
                validatedinputlist.employeetype FROM  validatedinputlist """,
        )

        if_query_list_groups_data_22_rows_greater_than_0_23 = rail.IfOperator(
            task_id='if_query_list_groups_data_22_rows_greater_than_0_23',
            test='''{{ result('query_list_groups_data_22','length') > 0 }}''',
            yes_task="trigger_groups_update_child",
            no_task="get_userlist_report_details",
        )

        trigger_groups_update_child = rail.TriggerDagRunOperator(
            task_id='trigger_groups_update_child',
            retries=0,
            trigger_dag_id=f'baylorcollegeofmedicine_groups_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "filepath": "{{result('new_file_sensor')}}",
                "groupsupdatelookup": "{{result('create_groups_update_logs_lookup')}}"
            }
        )

        wait_for_groups_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_groups_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_groups_update_child") }}'
        )

        get_userlist_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userlist_report_details',
            report_name=config.user_list_report

        )

        generate_userlist_report = rail.run_report2(
            group_id='generate_userlist_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_userlist_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv_27 = rail.LoadCSVFileOperator(
            task_id='parse_csv_27',
            document="{{(result('generate_userlist_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}",
            delimiter=','
        )

        write_csv_with_loginname_in_downcase = rail.WriteCSVFileOperator(
            task_id='write_csv_with_loginname_in_downcase',
            source="{{ result('parse_csv_27') }}",
            header=['User Name',
                    'User First Name',
                    'User Last Name',
                    'Login Name',
                    'User Status',
                    'Admin Use Only',
                    'useruri'
                    ],
            row=lambda item: [
                item['User Name'],
                item['User First Name'],
                item['User Last Name'],
                (item['Login Name']).lower(),
                item['User Status'],
                item['Admin Use Only'],
                item['useruri']
            ],
        )

        create_collection_create_list_from_csv_29 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_29',
            source="{{ result('write_csv_with_loginname_in_downcase') }}",
            name="userlistfromreplicon",
            columns={
                'User Name': 'username',
                'User First Name': 'firstname',
                'User Last Name': 'lastname',
                'Login Name': 'loginname',
                'User Status': 'userstatus',
                'Admin Use Only': 'adminuseonly',
                'useruri': 'useruri'
            }
        )

        load_allusers_in_replicon = rail.PythonOperator(
            task_id='load_allusers_in_replicon',
            python_callable=lambda: rail.load_all_records(
                rail.result('create_collection_create_list_from_csv_29'))
        )

        query_list_getallenabledusersfrom_repliconwhoareeligibleforprocessing_31 = rail.QueryCollectionOperator(
            task_id='query_list_getallenabledusersfrom_repliconwhoareeligibleforprocessing_31',
            name='enabledusers',
            query="""SELECT * FROM  userlistfromreplicon WHERE  userlistfromreplicon.userstatus = 'Enabled' AND  userlistfromreplicon.adminuseonly= ''""",
        )

        query_list_usertodisable_33 = rail.QueryCollectionOperator(
            task_id='query_list_usertodisable_33',
            name="userstodisable",
            query="""SELECT * FROM  enabledusers WHERE (LOWER( enabledusers.loginname) NOT IN
                (SELECT LOWER( validatedinputlist.loginname) FROM  validatedinputlist)) AND  enabledusers.userstatus = 'Enabled' AND
                enabledusers.adminuseonly= ''""",
        )

        if_rows_to_i_equals_to_dataworkatojob_contextparametersdisablethresholdto_i_34 = rail.IfOperator(
            task_id='if_rows_to_i_equals_to_dataworkatojob_contextparametersdisablethresholdto_i_34',
            test=lambda: rail.result(
                'query_list_usertodisable_33', 'length') <= config.disable_threshold,
            yes_task="query_enabled_users_tobe_disabled",
            no_task="send_mail_send_notificationfornumberofuserstobedisabledmorethanthethreshold_39",
        )

        query_enabled_users_tobe_disabled = rail.QueryCollectionOperator(
            task_id='query_enabled_users_tobe_disabled',
            query="SELECT * FROM userstodisable WHERE userstodisable.userstatus = 'Enabled'"
        )


        trigger_child_disable_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_disable_user',
            retries=0,
            items="{{ result('query_enabled_users_tobe_disabled') }}",
            trigger_dag_id=f'baylorcollegeofmedicine_workflow_to_disable_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "userloginname": item['loginname'],
                "useruri": item['useruri'],
                "username": ((item['username'].split(','))[-1]).strip() + " " + ((item['username'].split(','))[0]).strip(),
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "userimportlogslookup": rail.result('create_user_import_logs_lookuptable')
            }
        )

        wait_for_child_disable_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_disable_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_disable_user") }}'
        )

        send_mail_send_notificationfornumberofuserstobedisabledmorethanthethreshold_39 = rail.EmailOperator(
            task_id='send_mail_send_notificationfornumberofuserstobedisabledmorethanthethreshold_39',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} + "| User import has been skipped -" {{current_time("%H%M%S")}} ''',
            html_content='''templates/userstodisable_large_mail.html''',
            params={
                'disablethreshold': config.disable_threshold
            },
        )

        def get_group_list(response):
            groupdata = response['rows']
            return [{
                'name': data['cells'][0].get('textValue'),
                'uri': data['cells'][0].get('uri'),
                'fullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in data['cells'][1]['cellCollection']], '/')
            } for data in groupdata]

        get_department_group_details_42 = rail.RepliconServiceOperator(
            task_id='get_department_group_details_42',
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
            data_handler=get_group_list
        )

        get_location_details_43 = rail.RepliconServiceOperator(
            task_id='get_location_details_43',
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
            data_handler=get_group_list
        )

        get_employee_type_group_details_44 = rail.RepliconServiceOperator(
            task_id='get_employee_type_group_details_44',
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
            data_handler=get_group_list
        )

        get_all_permission_sets_45 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_45',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_office_schedules_46 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_46',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_all_active_places_47 = rail.RepliconServiceOperator(
            task_id='get_all_active_places_47',
            endpoint="/services/PlaceService1.svc/GetPageOfPlaceDetails",
            data={
                "page": "1",
                "pageSize": "100",
                "searchParameter": {
                    "isEnabled": "1"
                }
            }
        )

        get_all_policy_sets_48 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_48',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_timesheet_period_for_new_users_49 = rail.RepliconServiceOperator(
            task_id='get_timesheet_period_for_new_users_49',
            endpoint="/services/TimesheetPeriodService2.svc/GetTimesheetPeriodForNewUsers",
        )

        query_list_newuserstoprocess_53 = rail.QueryCollectionOperator(
            task_id='query_list_newuserstoprocess_53',
            query="""SELECT * FROM  validatedinputlist WHERE LOWER( validatedinputlist.loginname) NOT IN
              (SELECT DISTINCT LOWER( userlistfromreplicon.loginname) FROM  userlistfromreplicon)""",
        )

        query_list_updateuserstoprocess_54 = rail.QueryCollectionOperator(
            task_id='query_list_updateuserstoprocess_54',
            name="updateuserslist",
            query="""SELECT * FROM  validatedinputlist WHERE LOWER( validatedinputlist.loginname) IN
                (SELECT DISTINCT LOWER( userlistfromreplicon.loginname) FROM  userlistfromreplicon)""",
        )

        create_add_child_triggered_list = rail.SetVariableOperator(
            task_id='create_add_child_triggered_list',
            name='childtriggered',
            append=False,
            value=[]
        )

        foreach_query_list_newuserstoprocess_53_56 = rail.ForEachOperator(
            task_id='foreach_query_list_newuserstoprocess_53_56',
            items="{{ result('query_list_newuserstoprocess_53') }}",
            start_task='if_foreach_query_list_newuserstoprocess_53_56_firstname_present_a_57',
            end_task='foreach_query_list_newuserstoprocess_53_56_end'
        )

        if_foreach_query_list_newuserstoprocess_53_56_firstname_present_a_57 = rail.IfOperator(
            task_id='if_foreach_query_list_newuserstoprocess_53_56_firstname_present_a_57',
            #pylint: disable = line-too-long
            test='''{{ result('foreach_query_list_newuserstoprocess_53_56').firstname | is_truthy and result('foreach_query_list_newuserstoprocess_53_56').lastname | is_truthy  and result('foreach_query_list_newuserstoprocess_53_56').loginname | is_truthy }}''',
            yes_task="trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58",
            no_task="baylorcollegeofmedicine_user_import_logs_add_entry_60",
        )

        def get_today_date():
            now = datetime.now()
            return {
                'year': now.year,
                'month': now.month,
                'day': now.day
            }

        def get_add_user_payload():
            user = rail.result('foreach_query_list_newuserstoprocess_53_56')
            departmentfullpath = rail.smartjoin_by_delim(
                ("Baylor" + "|" + str(user['departmentlevel2']) + "|" + str(user['departmentlevel3'])).split("|"), '/')
            return {
                "firstname": user['firstname'],
                "lastname": user['lastname'],
                "emailaddress": user['email'],
                "employeeid": user['employeeid'],
                "startdate": rail.get_replicon_date(datetime.strptime(user['startdate'],'%Y-%m-%d')),
                "loginname": user['loginname'],
                "supervisor": user['supervisor'],
                "timeapprover": user['timeapprover'],
                "departmentlevel2": user['departmentlevel2'] + ".pre",
                "departmentlevel3": user['departmentlevel3'],
                "departmentfullpath": departmentfullpath,
                "employeetype": user['employeetype'],
                "places": user['places'],
                "officeschedule": user['scheduletype'],
                "place": user['places'],
                "placeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_active_places_47'), 'name', user['places'], 'uri', ''),
                "departmentgroupuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_department_group_details_42'), 'fullpath', departmentfullpath, 'uri', ''),
                "timeapproveruri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_location_details_43'), 'fullpath', user['timeapprover'], 'uri', ''),
                "officescheduleuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_office_schedules_46'), 'displayText', user['scheduletype'], 'uri', ''),
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_employee_type_group_details_44'), 'fullpath', user['employeetype'], 'uri', ''),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Supervisor", 'uri', ''),
                "basicwithreportpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Basic user with Report Access", 'uri', ''),
                "basicpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Basic User", 'uri', ''),
                "rundate": get_today_date(),
                "timesheettemplateuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_48'), 'displayText', "Cloud Clock Timesheet", 'uri', ''),
                "punchentrypolicyuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_48'), 'displayText', "Timesheet Access", 'uri', ''),
                "timesheetperioduri": rail.result('get_timesheet_period_for_new_users_49')['uri'],

                "userimportlogslookup": rail.result('create_user_import_logs_lookuptable'),
                "supervisorlookup": rail.result('create_supervisor_assignment_lookup'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }

        trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58',
            retries=0,
            trigger_dag_id=f'baylorcollegeofmedicine_user_add_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_add_user_payload
        )

        insert_child_triggered_in_list = rail.SetVariableOperator(
            task_id='insert_child_triggered_in_list',
            name='childtriggered',
            append=True,
            value="{{result('trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58')}}"
        )

        baylorcollegeofmedicine_user_import_logs_add_entry_60 = rail.WriteLogOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_add_entry_60',
            log="{{ result('create_user_import_logs_lookuptable') }}",
            message="na",
            severity="Exception",
            properties=lambda: {
                "loginname": rail.result('foreach_query_list_newuserstoprocess_53_56')['loginname'],
                "action": "Add",
                "status": "Exception",
                "details": rail.smartjoin_by_delim((("" if rail.result('foreach_query_list_newuserstoprocess_53_56')['lastname'] else
                    'Last name is not present') + ',' +
                    ("" if rail.result('foreach_query_list_newuserstoprocess_53_56')['firstname'] else 'First name is not present') + ',' +
                    ("" if rail.result('foreach_query_list_newuserstoprocess_53_56')['loginname'] else 'loginname is not present')).split(','), ', '),
                "jobid": rail.render_template("{{ dag_run_ecid() }}"),
                "childjobid": '',
                "firstname": rail.result('foreach_query_list_newuserstoprocess_53_56')['firstname'],
                "lastname": rail.result('foreach_query_list_newuserstoprocess_53_56')['lastname']
            }
        )

        foreach_query_list_newuserstoprocess_53_56_end = rail.EmptyOperator(
            task_id='foreach_query_list_newuserstoprocess_53_56_end',
        )

        if_add_user_triggered = rail.IfOperator(
            task_id='if_add_user_triggered',
            test=lambda: bool(rail.get_dag_run_var('childtriggered')),
            yes_task='wait_for_completion_trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58',
            no_task='dir_getthereferencefiledetails_63'
        )

        wait_for_completion_trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58") }}',
        )

        dir_getthereferencefiledetails_63 = rail.SFTPListFilesOperator(
            task_id='dir_getthereferencefiledetails_63',
            paths=[config.reference_filepath],
        )

        if_create_list_updateuserstoprocess_62_row_count_greater_than_0_64 = rail.IfOperator(
            task_id='if_create_list_updateuserstoprocess_62_row_count_greater_than_0_64',
            test=lambda: bool(rail.result(
                'dir_getthereferencefiledetails_63')),
            yes_task="get_reference_filename",
            no_task="baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75",
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: config.reference_filepath +
            (rail.result('dir_getthereferencefiledetails_63')
             [config.reference_filepath])[0]['name']
        )

        download_downloadthereferencefile_65 = rail.SFTPDownloadFileOperator(
            task_id='download_downloadthereferencefile_65',
            remote_filepath="{{result('get_reference_filename')}}"
        )

        load_csv_create_list_from_csv_66 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_66",
            document="{{result('download_downloadthereferencefile_65')}}",
        )

        create_collection_create_list_from_csv_66 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_66',
            source="{{ result('load_csv_create_list_from_csv_66') }}",
            name="userreferencedata",
            columns={
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Email': 'email',
                'Employee ID': 'employeeid',
                'Start Date': 'startdate',
                'Login Name': 'loginname',
                'Supervisor': 'supervisor',
                'Time Approver': 'timeapprover',
                'Department Level 2': 'departmentlevel2',
                'Department Level 3': 'departmentlevel3',
                'Employee Type': 'employeetype',
                'Places': 'places',
                'Schedule Type': 'scheduletype',
                'encoded': 'encoded'
            }
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM  updateuserslist WHERE  updateuserslist.encoded IN (SELECT DISTINCT  userreferencedata.encoded FROM  userreferencedata )""",
        )

        if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_67_rows_greater_than_0_68 = rail.IfOperator(
            task_id='if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_67_rows_greater_than_0_68',
            test='''{{ result('query_unchanged_records','length') > 0 }}''',
            yes_task="baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_69",
            no_task="query_changed_records",
        )

        baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_69 = rail.WriteLogOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_69',
            items="{{result('query_unchanged_records')}}",
            log="{{result('create_user_import_logs_lookuptable')}}",
            message='na',
            severity='na',
            properties={
                'loginname': "{{item.loginname}}",
                'action': 'Update',
                'status': 'Skipped',
                'details': 'No change in user record',
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
                'firstname': "{{item.firstname}}",
                'lastname': "{{item.lastname}}"
            }
        )

        query_changed_records = rail.QueryCollectionOperator(
            task_id='query_changed_records',
            query="""SELECT * FROM  updateuserslist WHERE  updateuserslist.encoded NOT IN
                (SELECT DISTINCT  userreferencedata.encoded FROM  userreferencedata )""",
        )

        if_changed_records_present = rail.IfOperator(
            task_id='if_changed_records_present',
            test='''{{ result('query_changed_records','length') > 0 }}''',
            yes_task="trigger_child_user_update",
            no_task="baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75",
        )

        trigger_child_user_update = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_user_update',
            retries=0,
            items="{{ result('query_changed_records') }}",
            trigger_dag_id=f'baylorcollegeofmedicine_user_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['email'],
                "employeeid": item['employeeid'],
                "startdate": rail.get_replicon_date(datetime.strptime(item['startdate'],'%Y-%m-%d')),
                "loginname": item['loginname'],
                "supervisor": item['supervisor'],
                "timeapprover": item['timeapprover'],
                "departmentlevel2": item['departmentlevel2'],
                "departmentlevel3": item['departmentlevel3'],
                "departmentfullpath": rail.smartjoin_by_delim(("Baylor" + "|" + str(item['departmentlevel2']) + "|" +
                    str(item['departmentlevel3'])).split("|"), '/'),
                "employeetype": item['employeetype'],
                "officeschedule": item['scheduletype'],
                "place": item['places'],
                "placeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_active_places_47'), 'name', item['places'], 'uri', ''),
                "departmentgroupuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_department_group_details_42'), 'fullpath', (rail.smartjoin_by_delim(
                    ("Baylor" + "|" + str(item['departmentlevel2']) + "|" + str(item['departmentlevel3'])).split("|"), '/')), 'uri', ''),
                "timeapproveruri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_location_details_43'), 'fullpath', item['timeapprover'], 'uri', ''),
                "officescheduleuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_office_schedules_46'), 'displayText', item['scheduletype'], 'uri', ''),
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_employee_type_group_details_44'), 'fullpath', item['employeetype'], 'uri', ''),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Supervisor", 'uri', ''),
                "basicwithreportpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Basic user with Report Access", 'uri', ''),
                "basicpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Basic user with Report Access", 'uri', ''),
                "useruri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_allusers_in_replicon'), 'loginname', item['loginname'], 'useruri', ''),
                "rundate": get_today_date(),
                "userstartdate": item['startdate'],
                "timesheettemplateuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_48'), 'displayText', "Cloud Clock Timesheet", 'uri', ''),
                "punchentrypolicyuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_policy_sets_48'), 'displayText', "Timesheet Access", 'uri', ''),
                "userimportlogslookup": rail.result('create_user_import_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}"),
                "supervisorlookup": rail.result('create_supervisor_assignment_lookup')
            }
        )

        wait_for_child_user_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_user_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_user_update") }}'
        )

        baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75 = rail.FilterLogEntriesOperator(
            task_id='baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75',
            log="{{result('create_supervisor_assignment_lookup')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        if_entry_col1_present_76 = rail.IfOperator(
            task_id='if_entry_col1_present_76',
            test='''{{ result('baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75','length') > 0 | is_truthy }}''',
            yes_task="trigger_child_assign_supervisor",
            no_task="baylorcollegeofmedicine_user_import_logs_search_entries_4",
        )

        trigger_child_assign_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_assign_supervisor',
            retries=0,
            items="{{ result('baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75') }}",
            trigger_dag_id=f'baylorcollegeofmedicine_assign_supervisor_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['properties']['username'],
                "supervisorloginname": item['properties']['supervisorloginname'],
                "parentjobid": item['properties']['jobid'],
                "childjobid": item['properties']['childjobid'],
                "useruri": item['properties']['useruri'],
                "action": item['properties']['action'],
                "supervisorpermission": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Supervisor", 'uri', ''),
                "basicuserwithreports": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_45'), 'displayText', "*Gen3 - Basic user with Report Access", 'uri', ''),
                "today": get_today_date(),
                "userimportlogslookup": rail.result('create_user_import_logs_lookuptable'),
                "supervisorlookup": rail.result('create_supervisor_assignment_lookup')
            }
        )

        waitfor_child_assign_supervisor = rail.WaitForDagRunsSensor(
            task_id='waitfor_child_assign_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_assign_supervisor") }}'
        )

        baylorcollegeofmedicine_user_import_logs_search_entries_4 = rail.FilterLogEntriesOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_search_entries_4',
            log="{{result('create_user_import_logs_lookuptable')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        create_csv_lines_5 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_5',
            source="{{ result('baylorcollegeofmedicine_user_import_logs_search_entries_4') }}",
            header=['FirstName',
                    'LastName',
                    'Loginname',
                    'Action',
                    'Status',
                    'Details',
                    'JobID',
                    'Child job ID'],
            row=[
                "{{ item.properties.firstname }}",
                "{{ item.properties.lastname }}",
                "{{ item.properties.loginname }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid }}",
                "{{ item.properties.childjobid }}"
            ],
        )

        upload_logs_upload_7 = rail.SFTPUploadFileOperator(
            task_id='upload_logs_upload_7',
            content='''{{ result('create_csv_lines_5') }}''',
            remote_filepath=config.log_filepath +
            'Logs_{{dag_run_ecid()}}_{{result("new_file_sensor") | file_name }}',
        )

        def get_error_and_email_subject():
            logs = rail.load_all_records(rail.result(
                'baylorcollegeofmedicine_user_import_logs_search_entries_4'))
            iserrorpresent = rail.find_first_by_attr_and_get_attr(
                logs, 'status', 'Error', 'status', '')
            isexceptionpresent = rail.find_first_by_attr_and_get_attr(
                logs, 'status', 'Exception', 'status', '')
            return {
                "errorcheck": iserrorpresent,
                "exceptioncheck": isexceptionpresent,
                "subject": 'completed with errors' if iserrorpresent else ('completed with exceptions' if isexceptionpresent else 'completed successfully'),
                # pylint: disable = line-too-long
                "body": "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if iserrorpresent else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
            }

        get_error_exception_checks = rail.PythonOperator(
            task_id='get_error_exception_checks',
            python_callable=get_error_and_email_subject
        )

        send_mail_send_jobcompletionnotification_12 = rail.EmailOperator(
            task_id='send_mail_send_jobcompletionnotification_12',
            to=config.tenant_email,
            bcc="{%- if result('get_error_exception_checks').errorcheck -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='''{{ get_company_key() }}| User import {{ result('get_error_exception_checks').subject }}- {{ current_time() }} ''',
            html_content='''templates/completion_mail.html''',
            params={
                'logfilepath': config.log_filepath
            },
        )

        rename_archivethereferenceinputfile_84 = rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferenceinputfile_84',
            new_filename=config.archive_filepath +
            'Old_Ref_{{dag_run_ecid()}}{{result("get_reference_filename") | file_name}}',
            existing_filename="{{result('get_reference_filename')}}",
        )

        upload_uploadreferencefile_85 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadreferencefile_85',
            content='''{{ result('write_csv_with_encoded') }}''',
            remote_filepath=config.reference_filepath +
            "Ref_{{dag_run_ecid()}}_{{result('new_file_sensor') | file_name }}",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label(
            "Always") >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_file_name_ends_with_csv_2
        if_file_name_ends_with_csv_2 >> rail.Label(
            'No') >> send_mail_notificationforincorrectfileformat_3 >> log_to_sumo
        if_file_name_ends_with_csv_2 >> rail.Label(
            'Yes') >> parse_csv_7 >> write_csv_with_encoded >> create_collection_create_list_from_csv_raw_data_file_8
        create_collection_create_list_from_csv_raw_data_file_8 >> if_create_list_from_csv_raw_data_file_8_row_count_less_than_1_9
        if_create_list_from_csv_raw_data_file_8_row_count_less_than_1_9 >> rail.Label(
            'Yes') >> send_mail_notificationfornorecords_blank_data_10 >> log_to_sumo
        if_create_list_from_csv_raw_data_file_8_row_count_less_than_1_9 >> rail.Label(
            'No') >> query_list_getuserwithblankloginname_17 >> create_user_import_logs_lookuptable >> create_supervisor_assignment_lookup
        create_supervisor_assignment_lookup >> create_groups_update_logs_lookup >> if_query_list_getuserwithblankloginname_17_rows_greater_than_0_18
        if_query_list_getuserwithblankloginname_17_rows_greater_than_0_18 >> rail.Label(
            'Yes') >> baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_19 >> query_list_getuserwithvalidloginname_20
        if_query_list_getuserwithblankloginname_17_rows_greater_than_0_18 >> rail.Label(
            'No') >> query_list_getuserwithvalidloginname_20 >> query_list_groups_data_22 >> if_query_list_groups_data_22_rows_greater_than_0_23
        if_query_list_groups_data_22_rows_greater_than_0_23 >> rail.Label(
            'Yes') >> trigger_groups_update_child
        trigger_groups_update_child >> wait_for_groups_update_child >> get_userlist_report_details
        if_query_list_groups_data_22_rows_greater_than_0_23 >> rail.Label(
            'No') >> get_userlist_report_details >> generate_userlist_report >> parse_csv_27 >> write_csv_with_loginname_in_downcase
        write_csv_with_loginname_in_downcase >> create_collection_create_list_from_csv_29 >> load_allusers_in_replicon
        load_allusers_in_replicon >> query_list_getallenabledusersfrom_repliconwhoareeligibleforprocessing_31 >> query_list_usertodisable_33
        query_list_usertodisable_33 >> if_rows_to_i_equals_to_dataworkatojob_contextparametersdisablethresholdto_i_34
        if_rows_to_i_equals_to_dataworkatojob_contextparametersdisablethresholdto_i_34 >> rail.Label(
            'Yes') >> query_enabled_users_tobe_disabled
        query_enabled_users_tobe_disabled >> trigger_child_disable_user >> wait_for_child_disable_user >> get_department_group_details_42
        if_rows_to_i_equals_to_dataworkatojob_contextparametersdisablethresholdto_i_34 >> rail.Label(
            'No') >> send_mail_send_notificationfornumberofuserstobedisabledmorethanthethreshold_39 >> log_to_sumo
        get_department_group_details_42 >> get_location_details_43 >> get_employee_type_group_details_44 >> get_all_permission_sets_45
        get_all_permission_sets_45 >> get_all_office_schedules_46 >> get_all_active_places_47 >> get_all_policy_sets_48
        get_all_policy_sets_48 >> get_timesheet_period_for_new_users_49 >> query_list_newuserstoprocess_53 >> query_list_updateuserstoprocess_54
        query_list_updateuserstoprocess_54 >> create_add_child_triggered_list >> foreach_query_list_newuserstoprocess_53_56
        foreach_query_list_newuserstoprocess_53_56 >> if_foreach_query_list_newuserstoprocess_53_56_firstname_present_a_57
        if_foreach_query_list_newuserstoprocess_53_56_firstname_present_a_57 >> rail.Label(
            'Yes') >> trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58 >> insert_child_triggered_in_list
        insert_child_triggered_in_list >> foreach_query_list_newuserstoprocess_53_56_end
        if_foreach_query_list_newuserstoprocess_53_56_firstname_present_a_57 >> rail.Label(
            'No') >> baylorcollegeofmedicine_user_import_logs_add_entry_60 >> foreach_query_list_newuserstoprocess_53_56_end
        foreach_query_list_newuserstoprocess_53_56 >> foreach_query_list_newuserstoprocess_53_56_end >> if_add_user_triggered
        if_add_user_triggered >> rail.Label(
            'Yes') >> wait_for_completion_trigger_dag_run_live_baylorcollegeofmedicine_user_add_v1_0async_58 >> dir_getthereferencefiledetails_63
        if_add_user_triggered >> rail.Label(
            'No') >> dir_getthereferencefiledetails_63 >> if_create_list_updateuserstoprocess_62_row_count_greater_than_0_64
        if_create_list_updateuserstoprocess_62_row_count_greater_than_0_64 >> rail.Label(
            'Yes') >> get_reference_filename >> download_downloadthereferencefile_65 >> load_csv_create_list_from_csv_66
        load_csv_create_list_from_csv_66 >> create_collection_create_list_from_csv_66 >> query_unchanged_records
        query_unchanged_records >> if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_67_rows_greater_than_0_68
        if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_67_rows_greater_than_0_68 >> rail.Label(
            'Yes') >> baylorcollegeofmedicine_user_import_logs_add_batch_of_entries_69 >> query_changed_records
        if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_67_rows_greater_than_0_68 >> rail.Label(
            'No') >> query_changed_records >> if_changed_records_present
        if_changed_records_present >> rail.Label(
            'Yes') >> trigger_child_user_update >> wait_for_child_user_update
        wait_for_child_user_update >> baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75
        if_changed_records_present >> rail.Label(
            'No') >> baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75
        if_create_list_updateuserstoprocess_62_row_count_greater_than_0_64 >> rail.Label(
            'No') >> baylorcollegeofmedicine_supervisor_assignment_logs_search_entries_75 >> if_entry_col1_present_76
        if_entry_col1_present_76 >> rail.Label(
            'Yes') >> trigger_child_assign_supervisor >> waitfor_child_assign_supervisor >> baylorcollegeofmedicine_user_import_logs_search_entries_4
        if_entry_col1_present_76 >> rail.Label(
            'No') >> baylorcollegeofmedicine_user_import_logs_search_entries_4 >> create_csv_lines_5 >> upload_logs_upload_7 >> get_error_exception_checks
        get_error_exception_checks >> send_mail_send_jobcompletionnotification_12 >> rename_archivethereferenceinputfile_84
        rename_archivethereferenceinputfile_84 >> upload_uploadreferencefile_85 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
