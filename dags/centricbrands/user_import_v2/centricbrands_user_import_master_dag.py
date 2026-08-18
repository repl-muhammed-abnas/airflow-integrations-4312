import hashlib
from datetime import timedelta, datetime
import pytz
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long unnecessary-lambda
    with rail.create_airflow_dag(
        dag_id=f'centricbrands_user_import_master_{config.instance}_v2',
        description=f'CentricBrands_User_Import_Master {config.instance}_v2',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
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
            new_filename=config.archive_filepath +
            "{{dag_run_ecid()}}_{{result('new_file_sensor') | file_name}}",
            existing_filename="{{ result('new_file_sensor') }}",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        get_user_import_logs_lookup_table = rail.CreateLogOperator(
            task_id='get_user_import_logs_lookup_table',
        )

        create_supervisor_assignment_lookuptable = rail.CreateLogOperator(
            task_id='create_supervisor_assignment_lookuptable',
        )

        create_centric_brands_groups_logs_lookuptable = rail.CreateLogOperator(
            task_id='create_centric_brands_groups_logs_lookuptable'
        )

        log_current_time = rail.PythonOperator(
            task_id='log_current_time',
            python_callable=lambda: datetime.now(pytz.timezone(
                'America/New_York')).strftime('%Y-%m-%dT%H:%M:%S')
        )

        if_file_ends_with_csv = rail.IfOperator(
            task_id='if_file_ends_with_csv',
            test='''{{ result('new_file_sensor').lower() | ends_with('.csv') }}''',
            yes_task="parse_csv",
            no_task="send_mail_incorrect_file_format",
        )

        send_mail_incorrect_file_format = rail.EmailOperator(
            task_id='send_mail_incorrect_file_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User Import - Incorrect file name {{ result('log_current_time') }}''',
            html_content='templates/incorrect_file_format.html',
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{result('download_file')}}",
            delimiter=',',
            encoding='utf-8-sig',
        )

        compose_csv_with_encode = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_encode',
            source="{{ result('parse_csv') }}",
            header=['First Name',
                    'Last Name',
                    'Login Name',
                    'Employee ID',
                    'Email',
                    'EmployeeType',
                    'Authentication Type',
                    'Department Name',
                    'License Seats',
                    'Is Login Enabled',
                    'Start Date',
                    'Integration Date',
                    'End Date',
                    'China Career Start Date',
                    'Hong Kong Levels',
                    'User Permission',
                    'Supervisor Permission',
                    'Team Manager Permission',
                    'Payroll Manager Permission',
                    'Administrator Permission',
                    'Location',
                    'Location Effective Date',
                    'Team',
                    'Team Effective Date',
                    'State/Province',
                    'Supervisor Login Name',
                    'Supervisor Start Date',
                    'TimeOff Template',
                    'TimeOff Approval Path',
                    'Holiday Calendar',
                    'Schedule',
                    'Schedule effective date',
                    'encoded',
                    'departmentfullname'],
            row=lambda item:
            [
                item['First_Name'],
                item['Last Name'],
                item['Login Name'],
                item['Employee ID'],
                item['Email'],
                item['EmployeeType'],
                item['Authentication Type'],
                item['Department Name'],
                item['License Seats'],
                item['Is Login Enabled'],
                item['Start Date'],
                item['Integration Date'],
                item['End Date'],
                item['China Career Start Date'],
                item['Hong Kong Levels'],
                item['User Permission'],
                item['Supervisor Permission'],
                item['Team Manager Permission'],
                item['Payroll Manager Permission'],
                item['Administrator Permission'],
                item['Location'],
                item['Location Effective Date'],
                item['Team'],
                item['Team Effective Date'],
                item['State/Province'],
                item['Supervisor Login Name'],
                item['Supervisor Start Date'],
                item['TimeOff Template'],
                item['TimeOff Approval Path'],
                item['Holiday Calendar'],
                item['Schedule'],
                item['Schedule effective date'],
                hashlib.md5((
                    str(item['First_Name']) + ',' + str(item['Last Name']) + ',' + str(item['Login Name']) + ',' + str(item['Employee ID']) + ',' +
                    str(item['Email']) + ',' + str(item['EmployeeType']) + ',' + str(item['Authentication Type']) + ',' + str(item['Department Name']) + ',' +
                    str(item['License Seats']) + ',' + str(item['Is Login Enabled']) + ',' + str(item['Start Date']) + ',' +
                    str(item['Integration Date']) + ',' + str(item['End Date']) + ',' + str(item['China Career Start Date']) + ',' + str(item['Hong Kong Levels']) + ',' + str(item['User Permission']) + ',' +
                    str(item['Supervisor Permission']) + ',' + str(item['Team Manager Permission']) + ',' + str(item['Payroll Manager Permission']) + ',' +
                    str(item['Administrator Permission']) + ',' + str(item['Location']) + ',' + str(item['Location Effective Date']) + ',' +
                    str(item['Team']) + ',' + str(item['Team Effective Date']) + ',' + str(item['State/Province']) + ',' +
                    str(item['Supervisor Login Name']) + ',' + str(item['Supervisor Start Date']) + ',' + str(item['TimeOff Template']) + ',' +
                    str(item['TimeOff Approval Path']) + ',' + str(item['Holiday Calendar']) + ',' + str(item['Schedule']) + ',' +
                    str(item['Schedule effective date'])
                ).encode('utf-8')).hexdigest(),
                item['Department Name'].replace(
                    '|', ' / ') if item['Department Name'] else null
            ],
        )

        create_inputfile_collection = rail.CreateCollectionOperator(
            task_id='create_inputfile_collection',
            source="{{ result('compose_csv_with_encode') }}",
            name="inputfile",
        )

        if_rawinput_has_no_data = rail.IfOperator(
            task_id='if_rawinput_has_no_data',
            test="{{result('create_inputfile_collection','length') < 1 }}",
            yes_task="send_mail_no_data_in_file",
            no_task="list_reference_file",
        )

        send_mail_no_data_in_file = rail.EmailOperator(
            task_id='send_mail_no_data_in_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | User Import - No data in file {{ result('log_current_time') }}''',
            html_content='templates/no_data_in_file.html',
        )

        list_reference_file = rail.SFTPListFilesOperator(
            task_id='list_reference_file',
            paths=[config.reference_filepath],
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: rail.result('list_reference_file')[
                config.reference_filepath][0]['name']
            if rail.result('list_reference_file') else None
        )

        if_file_not_present_or_doesnt_end_with_csv = rail.IfOperator(
            task_id='if_file_not_present_or_doesnt_end_with_csv',
            test=lambda: bool((not rail.result('get_reference_filename')) or (
                rail.result('get_reference_filename').split('.')[-1] != 'csv')),
            yes_task="fail_with_reference_file_missing",
            no_task="download_reference_file",
        )

        fail_with_reference_file_missing = rail.FailOperator(
            task_id='fail_with_reference_file_missing',
            message='''Reference file missing'''
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath +
            "{{ result('get_reference_filename')}}"
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
            delimiter=','
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source="{{ result('parse_reference_file') }}",
            name="referencefile",
        )

        query_delta_records = rail.QueryCollectionOperator(
            task_id='query_delta_records',
            query="""SELECT * FROM  inputfile WHERE  inputfile.encoded NOT IN (SELECT DISTINCT  referencefile.encoded FROM  referencefile)""",
        )

        if_no_delta_records = rail.IfOperator(
            task_id='if_no_delta_records',
            test="{{result('query_delta_records','length') < 1}}",
            yes_task="send_mail_no_changes_found",
            no_task="query_unchanged_records",
        )

        send_mail_no_changes_found = rail.EmailOperator(
            task_id='send_mail_no_changes_found',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | User Import - No change in values {{ result('log_current_time') }} ''',
            html_content='templates/no_change_mail.html',
        )

        archive_old_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_old_reference_file',
            new_filename=config.archive_filepath +
            "{{ dag_run_ecid() }}_{{ result('get_reference_filename')}}",
            existing_filename=config.reference_filepath +
            "{{ result('get_reference_filename')}}",
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('compose_csv_with_encode') }}''',
            remote_filepath=config.reference_filepath +
            "Reference_{{ result('new_file_sensor') | file_name }}",
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM  inputfile WHERE  inputfile.encoded IN (SELECT DISTINCT referencefile.encoded FROM  referencefile)""",
        )

        log_no_change_in_user_record = rail.WriteLogOperator(
            task_id='log_no_change_in_user_record',
            log='{{ result("get_user_import_logs_lookup_table") }}',
            items='{{ result("query_unchanged_records") }}',
            message='na',
            severity="Skipped",
            properties={
                "loginname": "{{item.Login_Name}}",
                "empid": "{{item.Employee_ID}}",
                "email": "{{item.Email}}",
                "isloginenabled": "{{item.Is_Login_Enabled}}",
                "status": "Skipped",
                "details": "No change in user records",
                "jobid": "{{dag_run_ecid()}}",
                "childjobid": '',
                "department|location|team": '||'
            }
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_report
        )

        run_user_report = rail.run_report2(
            group_id='run_user_report',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_user_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_report_has_data = rail.IfOperator(
            task_id='if_report_has_data',
            # pylint: disable = line-too-long
            test="{{(result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_falsy and result('run_user_report.get_report_result', 'has_data') | is_truthy}}",
            yes_task='get_department_report_details',
            no_task='fail_with_report_has_no_data'
        )

        fail_with_report_has_no_data = rail.FailOperator(
            task_id='fail_with_report_has_no_data',
            # pylint: disable = line-too-long
            message="Error fetching report data {{(result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}-{{(result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}"
        )

        get_department_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_department_report_details',
            report_name=config.department_report
        )

        trigger_child_add_department = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_department',
            retries=0,
            trigger_dag_id=f'centricbrands_user_import_department_add_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "reporturi": "{{ result('get_department_report_details').uri }}",
                "downloadedfile": "{{ result('parse_csv') }}",
                "groupslogslookuptable": "{{result('create_centric_brands_groups_logs_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        wait_for_child_add_department = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_department',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_department") }}'
        )

        trigger_child_add_cost_centers_locations = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_cost_centers_locations',
            retries=0,
            trigger_dag_id=f'centricbrands_user_import_cost_centers_locations_add_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "downloadedfile": "{{ result('parse_csv') }}",
                "groupslogslookuptable": "{{result('create_centric_brands_groups_logs_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        wait_for_child_add_cost_centers_loactions = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_cost_centers_loactions',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_cost_centers_locations") }}'
        )

        trigger_child_add_locations_teams = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_locations_teams',
            retries=0,
            trigger_dag_id=f'centricbrands_user_import_locations_teams_add_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "downloadedfile": "{{ result('parse_csv') }}",
                "groupslogslookuptable": "{{result('create_centric_brands_groups_logs_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        wait_for_child_add_locations_teams = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_locations_teams',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_locations_teams") }}'
        )

        generate_report_existing_departments = rail.RepliconServiceOperator(
            task_id='generate_report_existing_departments',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('get_department_report_details').uri }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            },
            target='artifact'
        )

        parse_csv_existing_departments = rail.LoadCSVFileOperator(
            task_id='parse_csv_existing_departments',
            document="{{(result('generate_report_existing_departments') | load_json_artifact).payload}}",
        )

        load_existing_departments = rail.PythonOperator(
            task_id='load_existing_departments',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_existing_departments'))
        )

        get_all_cost_centers_locations = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers_locations',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        get_all_locations_teams = rail.RepliconServiceOperator(
            task_id='get_all_locations_teams',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        def create_existing_cost_center_list():
            cost_center_list = rail.result(
                'get_all_cost_centers_locations')['rows']
            costcenterlist = []
            for row in cost_center_list:
                for cell in row['cells']:
                    costcenterlist.append({
                        'costcenter': cell['cellCollection'][-1]['textValue'] if cell['cellCollection'][-1]['dataType'] else null,
                        'costcenterfullname': ('|'.join([costcenter['textValue'] for costcenter in cell['cellCollection']]) if len(
                            cell['cellCollection']) > 1 else cell['cellCollection'][-1]['textValue']) if cell['cellCollection'][-1]['dataType'] else null,
                        'uri': cell['cellCollection'][-1]['uri'] if cell['cellCollection'][-1]['dataType'] else null
                    })
            return costcenterlist

        create_existing_costcenter_list = rail.PythonOperator(
            task_id='create_existing_costcenter_list',
            python_callable=create_existing_cost_center_list
        )

        def create_existing_teamslist():
            team_list = rail.result('get_all_locations_teams')['rows']
            teamlist = []
            for row in team_list:
                for cell in row['cells']:
                    teamlist.append({
                        'location': cell['cellCollection'][-1]['textValue'] if cell['cellCollection'][-1]['dataType'] else null,
                        'locationfullname': ('|'.join([location['textValue'] for location in cell['cellCollection']]) if len(
                            cell['cellCollection']) > 1 else cell['cellCollection'][-1]['textValue']) if cell['cellCollection'][-1]['dataType'] else null,
                        'uri': cell['cellCollection'][-1]['uri'] if cell['cellCollection'][-1]['dataType'] else null
                    })
            return teamlist

        create_existing_teams_list = rail.PythonOperator(
            task_id='create_existing_teams_list',
            python_callable=create_existing_teamslist
        )

        load_user_report_csv = rail.LoadCSVFileOperator(
            task_id="load_user_report_csv",
            document="{{(result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}",
        )

        create_allusers_in_replicon_collection = rail.CreateCollectionOperator(
            task_id='create_allusers_in_replicon_collection',
            source="{{ result('load_user_report_csv') }}",
            name="allusersinreplicon",
            columns={
                'Login Name': 'loginname',
                'User Name': 'username',
                'Employee ID': 'employeeid',
                'User Status': 'userstatus',
                'useruri': 'useruri'
            }
        )

        load_all_users_in_replicon = rail.PythonOperator(
            task_id='load_all_users_in_replicon',
            python_callable=lambda: rail.load_all_records(
                rail.result('create_allusers_in_replicon_collection'))
        )

        create_child_triggered_list = rail.SetVariableOperator(
            task_id='create_child_triggered_list',
            name='childtriggered',
            append=False,
            value=[]
        )

        foreach_delta_record = rail.ForEachOperator(
            task_id='foreach_delta_record',
            items="{{ result('query_delta_records') }}",
            start_task='get_user_uri',
            end_task='foreach_delta_record_end'
        )

        get_user_uri = rail.PythonOperator(
            task_id='get_user_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('load_all_users_in_replicon'), 'loginname', rail.result(
                'foreach_delta_record')['Login_Name'], 'useruri', '') if rail.result('load_all_users_in_replicon') else null
        )

        if_user_exists = rail.IfOperator(
            task_id='if_user_exists',
            test='''{{ result('get_user_uri') | is_truthy }}''',
            yes_task="log_location_fullname",
            no_task="foreach_delta_record_end",
        )

        log_location_fullname = rail.PythonOperator(
            task_id='log_location_fullname',
            python_callable=lambda: ('|'.join(rail.result('foreach_delta_record')['Location'].split('|')) if '|' in rail.result('foreach_delta_record')[
                'Location'] else rail.result('foreach_delta_record')['Location']) if rail.result(
                'foreach_delta_record')['Location'] else ''
        )

        log_team_fullname = rail.PythonOperator(
            task_id='log_team_fullname',
            python_callable=lambda: (('|'.join(rail.result('foreach_delta_record')['Team'].split('|')) if '|' in rail.result('foreach_delta_record')[
                                     'Team'] else rail.result('foreach_delta_record')['Team']) if rail.result('foreach_delta_record')['Team'] else '').strip()
        )

        def get_add_or_update_user_payload(user, locationfullname, teamfullname, action):
            conf = {
                "firstname": user['First_Name'].strip() if user['First_Name'] else '',
                "lastname": user['Last_Name'].strip() if user['Last_Name'] else '',
                "loginname": user['Login_Name'].strip() if user['Login_Name'] else '',
                "employeeid": user['Employee_ID'].strip() if user['Employee_ID'] else '',
                "email": user['Email'].strip() if user['Email'] else '',
                "employeetype": user['EmployeeType'].strip() if user['EmployeeType'] else '',
                "authenticationtype": user['Authentication_Type'].strip() if user['Authentication_Type'] else '',
                "departmentname": user['Department_Name'].strip() if user['Department_Name'] else '',
                "licenseseats": user['License_Seats'].strip() if user['License_Seats'] else '',
                "isloginenabled": user['Is_Login_Enabled'].strip() if user['Is_Login_Enabled'] else '',
                "startdate": user['Start_Date'].strip().replace("-", "/") if user['Start_Date'] else '',
                "integrationdate": user['Integration_Date'].strip().replace("-", "/") if user['Integration_Date'] else '',
                "enddate": user['End_Date'].strip().replace("-", "/") if user['End_Date'] else '',
                "userpermission": user['User_Permission'].strip() if user['User_Permission'] else '',
                "supervisorpermission": user['Supervisor_Permission'].strip() if user['Supervisor_Permission'] else '',
                "teammanager": user['Team_Manager_Permission'].strip() if user['Team_Manager_Permission'] else '',
                "payrollmanager": user['Payroll_Manager_Permission'].strip() if user['Payroll_Manager_Permission'] else '',
                "administratorpermission": user['Administrator_Permission'].strip() if user['Administrator_Permission'] else '',
                "location": user['Location'].strip() if user['Location'] else '',
                "locationeffectivedate": user['Location_Effective_Date'].strip().replace("-", "/") if user['Location_Effective_Date'] else '',
                "team": user['Team'].strip() if user['Team'] else '',
                "teameffectivedate": user['Team_Effective_Date'].strip().replace("-", "/") if user['Team_Effective_Date'] else '',
                "stateprovince": user['State_Province'].strip() if user['State_Province'] else '',
                "supervisorloginname": user['Supervisor_Login_Name'].strip() if user['Supervisor_Login_Name'] else '',
                "supervisorstartdate": user['Supervisor_Start_Date'].strip().replace("-", "/") if user['Supervisor_Start_Date'] else '',
                "timeofftemplate": user['TimeOff_Template'].strip() if user['TimeOff_Template'] else '',
                "timeoffapprovalpath": user['TimeOff_Approval_Path'].strip() if user['TimeOff_Approval_Path'] else '',
                "holidaycalendar": user['Holiday_Calendar'].strip() if user['Holiday_Calendar'] else '',
                "schedule": user['Schedule'].strip() if user['Schedule'] else '',
                "scheduleeffectivedate": user['Schedule_effective_date'].strip().replace("-", "/") if user['Schedule_effective_date'] else '',
                "inputdepartmenturi": rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_existing_departments'), 'Department Full Name', user['departmentfullname'], 'department uri', ''),
                "inputlocationuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'create_existing_costcenter_list'), 'costcenterfullname', locationfullname, 'uri', ''),
                "inputteamuri": rail.find_first_by_attr_and_get_attr(rail.result('create_existing_teams_list'), 'locationfullname', teamfullname, 'uri', ''),
                "slug": rail.get_tenant_slug(),

                "userimportlogslookuptable": rail.result('get_user_import_logs_lookup_table'),
                "callerjobid": rail.render_template('{{dag_run_ecid()}}'),
                "supervisorlookuptable": rail.result('create_supervisor_assignment_lookuptable'),
                "chinacareerstartdate": user["China_Career_Start_Date"].strip().replace("-", "/") if user["China_Career_Start_Date"] else '',
                "hongkonglevels": user["Hong_Kong_Levels"] if user["Hong_Kong_Levels"] else ''
            }
            if action == 'update':
                conf.update({'uri': rail.result('get_user_uri')}),
            return conf

        trigger_child_update_user_dag = rail.TriggerDagRunOperator(
            task_id='trigger_child_update_user_dag',
            retries=0,
            trigger_dag_id=f'centricbrands_user_import_update_user_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_or_update_user_payload(rail.result('foreach_delta_record'), rail.result(
                'log_location_fullname'), rail.result('log_team_fullname'), 'update')
        )

        insert_childid_to_wait_list = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list',
            name="{{result('create_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_child_update_user_dag')}}"
        )

        foreach_delta_record_end = rail.EmptyOperator(
            task_id='foreach_delta_record_end',
        )

        if_update_child_tobe_awaited = rail.IfOperator(
            task_id='if_update_child_tobe_awaited',
            test=lambda: bool(rail.get_dag_run_var('childtriggered')),
            yes_task='wait_for_update_user_child_dag',
            no_task='query_list_users_tobe_added'
        )

        wait_for_update_user_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('insert_childid_to_wait_list').value | to_json}}"
        )

        query_list_users_tobe_added = rail.QueryCollectionOperator(
            task_id='query_list_users_tobe_added',
            query="""SELECT * FROM inputfile WHERE
                inputfile.Login_Name NOT IN (SELECT allusersinreplicon.loginname FROM allusersinreplicon) ORDER BY inputfile.Supervisor_Permission DESC""",
        )

        create_add_child_triggered_list = rail.SetVariableOperator(
            task_id='create_add_child_triggered_list',
            name='addchildtriggered',
            append=False,
            value=[]
        )

        foreach_user_tobe_added = rail.ForEachOperator(
            task_id='foreach_user_tobe_added',
            items="{{ result('query_list_users_tobe_added') }}",
            start_task='get_location_fullname',
            end_task='foreach_user_tobe_added_end'
        )

        get_location_fullname = rail.PythonOperator(
            task_id='get_location_fullname',
            python_callable=lambda: ('|'.join(rail.result('foreach_user_tobe_added')['Location'].split('|')) if '|' in rail.result('foreach_user_tobe_added')[
                'Location'] else rail.result('foreach_user_tobe_added')['Location']) if rail.result(
                'foreach_user_tobe_added')['Location'] else ''
        )

        get_team_fullname = rail.PythonOperator(
            task_id='get_team_fullname',
            python_callable=lambda: (('|'.join(rail.result('foreach_user_tobe_added')['Team'].split('|')) if '|' in rail.result('foreach_user_tobe_added')[
                                     'Team'] else rail.result('foreach_user_tobe_added')['Team']) if rail.result(
                                         'foreach_user_tobe_added')['Team'] else '').strip()
        )

        trigger_child_add_user_dag = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_user_dag',
            retries=0,
            trigger_dag_id=f'centricbrands_user_import_add_user_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_or_update_user_payload(rail.result('foreach_user_tobe_added'), rail.result(
                'get_location_fullname'), rail.result('get_team_fullname'), 'add')
        )

        insert_addchildid_to_wait_list = rail.SetVariableOperator(
            task_id='insert_addchildid_to_wait_list',
            name="{{result('create_add_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_child_add_user_dag')}}"
        )

        foreach_user_tobe_added_end = rail.EmptyOperator(
            task_id='foreach_user_tobe_added_end',
        )

        if_add_child_tobe_awaited = rail.IfOperator(
            task_id='if_add_child_tobe_awaited',
            test=lambda: bool(rail.get_dag_run_var('addchildtriggered')),
            yes_task='wait_for_add_user_child_dag',
            no_task='search_unassignedstatus_entries_in_supervisor_lookup'
        )

        wait_for_add_user_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_user_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('insert_addchildid_to_wait_list').value | to_json}}"
        )

        search_unassignedstatus_entries_in_supervisor_lookup = rail.FilterLogEntriesOperator(
            task_id='search_unassignedstatus_entries_in_supervisor_lookup',
            log="{{result('create_supervisor_assignment_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}",
                'assignedstatus': 'Not assigned'
            }
        )

        trigger_child_assign_supervisor_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_assign_supervisor_dag',
            retries=0,
            items="{{ result('search_unassignedstatus_entries_in_supervisor_lookup')}}",
            trigger_dag_id=f'centricbrands_user_import_assign_supervisor_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "loginname": "{{ item.properties.loginname }}",
                "supervisorloginname": "{{ item.properties.supervisorloginname }}",
                "supervisorstartdate": "{{ item.properties.supervisorstartdate }}",
                "parentjobid": "{{ item.properties.jobid }}",
                "childjobid": "{{ item.properties.childjobid }}",
                "useruri": "{{ item.properties.useruri }}",
                "action": "{{ item.properties.action }}",
                "userimportlogslookuptable": "{{result('get_user_import_logs_lookup_table')}}",
                "supervisorlookuptable": "{{result('create_supervisor_assignment_lookuptable')}}"
            }
        )

        if_unassigned_status_supervisor_entries_present = rail.IfOperator(
            task_id='if_unassigned_status_supervisor_entries_present',
            test='''{{ result('search_unassignedstatus_entries_in_supervisor_lookup','length') > 0 }}''',
            yes_task="wait_for_assign_supervisor_child_dag",
            no_task="search_log_entries",
        )

        wait_for_assign_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_assign_supervisor_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_assign_supervisor_dag") }}'
        )

        search_log_entries = rail.FilterLogEntriesOperator(
            task_id='search_log_entries',
            log="{{result('get_user_import_logs_lookup_table')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test='''{{ result('search_log_entries','length') > 0 }}''',
            yes_task="check_for_error_log",
            no_task="archive_old_referencefile",
        )

        check_for_error_log = rail.PythonOperator(
            task_id='check_for_error_log',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.load_all_records(
                rail.result('search_log_entries')), 'properties.status', 'Error', 'properties.status', '')
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id='compose_logs_csv',
            source="{{ result('search_log_entries') }}",
            header=['Login name',
                    'Employee ID',
                    'Email',
                    'Is Login Enabled',
                    'Department',
                    'Location',
                    'Team',
                    'Status',
                    'Details',
                    'JobID'],
            row=lambda item: [
                item['properties']['loginname'],
                item['properties']['empid'],
                item['properties']['email'],
                item['properties']['isloginenabled'],
                (item['properties']['department|location|team'].split('|'))[
                    0] if '|' in item['properties']['department|location|team'] else item['properties']['department|location|team'],
                (item['properties']['department|location|team'].split('|'))[
                    1] if '|' in item['properties']['department|location|team'] else item['properties']['department|location|team'],
                (item['properties']['department|location|team'].split(
                    '|'))[-1] if '|' in item['properties']['department|location|team'] else item['properties']['department|location|team'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['jobid'] + '|' +
                item['properties']['childjobid']
            ],
        )

        log_time_now = rail.PythonOperator(
            task_id='log_time_now',
            python_callable=lambda: datetime.now(pytz.timezone(
                'America/New_York')).strftime('%m%d%Y-%H%M%S')
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content='''{{ result('compose_logs_csv') }}''',
            remote_filepath=config.log_filepath +
            '''UserImport_Logs_{{ result('log_time_now') }}{{ result('new_file_sensor') | file_name }}''',
        )

        log_user_import_logs_location = rail.PythonOperator(
            task_id='log_user_import_logs_location',
            python_callable=lambda:  "UserImport Logs location: " + config.log_filepath + "UserImport_Logs_" +
            rail.result('log_time_now') +
            rail.render_template("{{result('new_file_sensor') | file_name }}")
        )

        search_log_entries_in_groups_logs = rail.FilterLogEntriesOperator(
            task_id='search_log_entries_in_groups_logs',
            log="{{result('create_centric_brands_groups_logs_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_entry_present = rail.IfOperator(
            task_id='if_entry_present',
            test='''{{ result('search_log_entries_in_groups_logs','length') > 0 }}''',
            yes_task="check_for_any_error_log",
            no_task="if_no_error_in_user_import_logs",
        )

        check_for_any_error_log = rail.PythonOperator(
            task_id='check_for_any_error_log',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.load_all_records(rail.result(
                'search_log_entries_in_groups_logs')), 'properties.status', 'Error', 'properties.status', '')
        )

        compose_csv_groups_import_logs = rail.WriteCSVFileOperator(
            task_id='compose_csv_groups_import_logs',
            source="{{ result('search_log_entries_in_groups_logs') }}",
            header=['Department',
                    'Location',
                    'Team',
                    'Status',
                    'Details',
                    'JobID'],
            row=[
                "{{ item.properties.department }}",
                "{{ item.properties.location }}",
                "{{ item.properties.team }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid }}|{{ item.properties.childjobid }}"
            ],
        )

        upload_groups_import_csv_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_groups_import_csv_logs_to_sftp',
            content='''{{ result('compose_csv_groups_import_logs') }}''',
            remote_filepath=config.log_filepath +
            '''GroupsImport_Logs_{{ result('log_time_now') }}{{ result('new_file_sensor') | file_name }}''',
        )

        log_groups_import_logs_location = rail.PythonOperator(
            task_id='log_groups_import_logs_location',
            python_callable=lambda:  config.log_filepath + "GroupsImport_Logs_" +
            rail.result('log_time_now') +
            rail.render_template("{{ result('new_file_sensor') | file_name }}")
        )

        if_no_error_in_user_import_logs = rail.IfOperator(
            task_id='if_no_error_in_user_import_logs',
            test='''{{ result('check_for_error_log') | is_falsy }}''',
            yes_task="check_for_exception_in_user_import_logs",
            no_task="send_mail_completed_with_errors",
        )

        check_for_exception_in_user_import_logs = rail.PythonOperator(
            task_id='check_for_exception_in_user_import_logs',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.load_all_records(
                rail.result('search_log_entries')), 'properties.status', 'Exception', 'properties.status', '')
        )

        if_no_exception_in_user_import_logs = rail.IfOperator(
            task_id='if_no_exception_in_user_import_logs',
            test='''{{ result('check_for_exception_in_user_import_logs') | is_falsy }}''',
            yes_task="send_mail_completed_successfully",
            no_task="send_mail_completed_with_exceptions",
        )

        send_mail_completed_successfully = rail.EmailOperator(
            task_id='send_mail_completed_successfully',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key() }} | User Import - User Import Completed successfully {{ result('log_current_time') }} ''',
            html_content='''templates/completed_successfully_mail.html''',
        )

        send_mail_completed_with_exceptions = rail.EmailOperator(
            task_id='send_mail_completed_with_exceptions',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key() }} | User Import - User Import Completed with Exceptions {{ result('log_current_time') }} ''',
            html_content='''templates/completed_with_exceptions_mail.html''',
        )

        send_mail_completed_with_errors = rail.EmailOperator(
            task_id='send_mail_completed_with_errors',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{get_company_key() }} | User Import - User Import Completed with Errors {{ result('log_current_time') }} ''',
            html_content='''templates/completed_with_errors_mail.html''',
        )

        archive_old_referencefile = rail.SFTPMoveFileOperator(
            task_id='archive_old_referencefile',
            new_filename=config.archive_filepath +
            "{{ dag_run_ecid() }}_{{ result('get_reference_filename')}}",
            existing_filename=config.reference_filepath +
            "{{ result('get_reference_filename')}}",
        )

        upload_new_referencefile = rail.SFTPUploadFileOperator(
            task_id='upload_new_referencefile',
            content='''{{ result('compose_csv_with_encode') }}''',
            remote_filepath=config.reference_filepath +
            "Reference_{{ result('new_file_sensor') | file_name }}",
        )

        if_users_tobe_added_or_users_in_replicon_present = rail.IfOperator(
            task_id='if_users_tobe_added_or_users_in_replicon_present',
            test='''{{ result('query_list_users_tobe_added','length') > 0  or result('create_allusers_in_replicon_collection','length') > 0}}''',
            yes_task="if_log_entries_not_present",
            no_task="log_to_sumo",
        )

        if_log_entries_not_present = rail.IfOperator(
            task_id='if_log_entries_not_present',
            test='''{{ result('search_log_entries','length') == 0 }}''',
            yes_task="fail_dag_with_error",
            no_task="log_to_sumo",
        )

        fail_dag_with_error = rail.FailOperator(
            task_id='fail_dag_with_error',
            message='''Files processed however logs generation failed'''
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label(
            "Always") >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> get_user_import_logs_lookup_table >> create_supervisor_assignment_lookuptable >> create_centric_brands_groups_logs_lookuptable
        create_centric_brands_groups_logs_lookuptable >> log_current_time >> if_file_ends_with_csv
        if_file_ends_with_csv >> rail.Label(
            'No') >> send_mail_incorrect_file_format >> log_to_sumo
        if_file_ends_with_csv >> rail.Label(
            'Yes') >> parse_csv >> compose_csv_with_encode >> create_inputfile_collection >> if_rawinput_has_no_data
        if_rawinput_has_no_data >> rail.Label(
            'Yes') >> send_mail_no_data_in_file >> log_to_sumo
        if_rawinput_has_no_data >> rail.Label(
            'No') >> list_reference_file >> get_reference_filename >> if_file_not_present_or_doesnt_end_with_csv
        if_file_not_present_or_doesnt_end_with_csv >> rail.Label(
            'Yes') >> fail_with_reference_file_missing >> log_to_sumo
        if_file_not_present_or_doesnt_end_with_csv >> rail.Label(
            'No') >> download_reference_file >> parse_reference_file >> create_referencefile_collection >> query_delta_records >> if_no_delta_records
        if_no_delta_records >> rail.Label(
            'Yes') >> send_mail_no_changes_found >> archive_old_reference_file >> upload_new_reference_file >> log_to_sumo
        if_no_delta_records >> rail.Label(
            'No') >> query_unchanged_records >> log_no_change_in_user_record >> get_user_report_details >> run_user_report >> if_report_has_data
        if_report_has_data >> rail.Label(
            'Yes') >> get_department_report_details >> trigger_child_add_department >> wait_for_child_add_department
        wait_for_child_add_department >> trigger_child_add_cost_centers_locations >> wait_for_child_add_cost_centers_loactions
        wait_for_child_add_cost_centers_loactions >> trigger_child_add_locations_teams >> wait_for_child_add_locations_teams
        wait_for_child_add_locations_teams >> generate_report_existing_departments >> parse_csv_existing_departments >> load_existing_departments
        load_existing_departments >> get_all_cost_centers_locations >> get_all_locations_teams >> create_existing_costcenter_list >> create_existing_teams_list
        create_existing_teams_list >> load_user_report_csv >> create_allusers_in_replicon_collection >> load_all_users_in_replicon
        load_all_users_in_replicon >> create_child_triggered_list >> foreach_delta_record >> get_user_uri >> if_user_exists
        if_user_exists >> rail.Label(
            'Yes') >> log_location_fullname >> log_team_fullname >> trigger_child_update_user_dag >> insert_childid_to_wait_list >> foreach_delta_record_end
        if_user_exists >> rail.Label('No') >> foreach_delta_record_end
        foreach_delta_record >> foreach_delta_record_end >> if_update_child_tobe_awaited >> rail.Label(
            'Yes') >> wait_for_update_user_child_dag >> query_list_users_tobe_added >> create_add_child_triggered_list
        create_add_child_triggered_list >> foreach_user_tobe_added >> get_location_fullname >> get_team_fullname >> trigger_child_add_user_dag
        trigger_child_add_user_dag >> insert_addchildid_to_wait_list >> foreach_user_tobe_added_end
        if_update_child_tobe_awaited >> rail.Label(
            'No') >> query_list_users_tobe_added
        foreach_user_tobe_added >> foreach_user_tobe_added_end >> if_add_child_tobe_awaited >> rail.Label(
            'Yes') >> wait_for_add_user_child_dag >> search_unassignedstatus_entries_in_supervisor_lookup >> trigger_child_assign_supervisor_dag
        trigger_child_assign_supervisor_dag >> if_unassigned_status_supervisor_entries_present
        if_add_child_tobe_awaited >> rail.Label(
            'No') >> search_unassignedstatus_entries_in_supervisor_lookup
        if_unassigned_status_supervisor_entries_present >> rail.Label(
            'Yes') >> wait_for_assign_supervisor_child_dag >> search_log_entries
        if_unassigned_status_supervisor_entries_present >> rail.Label(
            'No') >> search_log_entries >> if_logs_present
        if_logs_present >> rail.Label(
            'Yes') >> check_for_error_log >> compose_logs_csv >> log_time_now >> upload_logs_to_sftp >> log_user_import_logs_location
        log_user_import_logs_location >> search_log_entries_in_groups_logs >> if_entry_present
        if_entry_present >> rail.Label(
            'Yes') >> check_for_any_error_log >> compose_csv_groups_import_logs >> upload_groups_import_csv_logs_to_sftp >> log_groups_import_logs_location
        log_groups_import_logs_location >> if_no_error_in_user_import_logs
        if_entry_present >> rail.Label('No') >> if_no_error_in_user_import_logs
        if_no_error_in_user_import_logs >> rail.Label(
            'Yes') >> check_for_exception_in_user_import_logs >> if_no_exception_in_user_import_logs
        if_no_exception_in_user_import_logs >> rail.Label(
            'Yes') >> send_mail_completed_successfully >> archive_old_referencefile
        if_no_exception_in_user_import_logs >> rail.Label(
            'No') >> send_mail_completed_with_exceptions >> archive_old_referencefile
        if_no_error_in_user_import_logs >> rail.Label(
            'No') >> send_mail_completed_with_errors >> archive_old_referencefile
        if_logs_present >> rail.Label(
            'No') >> archive_old_referencefile >> upload_new_referencefile >> if_users_tobe_added_or_users_in_replicon_present
        if_users_tobe_added_or_users_in_replicon_present >> rail.Label(
            'Yes') >> if_log_entries_not_present
        if_log_entries_not_present >> rail.Label(
            'Yes') >> fail_dag_with_error >> log_to_sumo
        if_log_entries_not_present >> rail.Label('No') >> log_to_sumo
        if_users_tobe_added_or_users_in_replicon_present >> rail.Label(
            'No') >> log_to_sumo
        if_report_has_data >> rail.Label(
            'No') >> fail_with_report_has_no_data >> log_to_sumo
    return dag


rail.for_each_instance(create_dag)
