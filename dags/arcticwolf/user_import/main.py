
from datetime import timedelta, datetime
from pendulum import datetime as dt
import hashlib
import rail
from arcticwolf.user_import.utils import python_callable_methods
from arcticwolf.user_import.utils import response_filter
import json

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Arctic Wolf Master User Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_csv_data_from_workday = rail.SimpleHttpOperator(
            task_id='get_csv_data_from_workday',
            method='GET',
            http_conn_id=config.workday_http_conn_id,
            endpoint='/RPT_-_INT_-_S2_Employee_List_for_Replicon?format=json',
            headers={
                    "Content-Type": "application/json"
            },
            extra_options={
                'verify': False
            }
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id="load_csv",
            document="{{ dag_run.conf.json_data if 'json_data' in dag_run.conf else result('get_csv_data_from_workday') }}",
        )

        write_csv_with_encoded = rail.WriteCSVFileOperator(
            task_id='write_csv_with_encoded',
            source=lambda dag_run: dag_run.conf['json_data'] if 'json_data' in dag_run.conf
                                                            else json.loads(rail.result('get_csv_data_from_workday'))['Report_Entry'],
            header=[
                'First Name',
                'Last Name',
                'Email',
                'Employee ID',
                'Start Date',
                'End Date',
                'Login Name',
                'Supervisor',
                'Supervisor Email',
                'Department Level 2',
                'Department Level 3',
                'Employee Type',
                'Job Code',
                'Position Title',
                'Position Title Code',
                'Login Status',
                'Status',
                'Location Level 1',
                'Location Level 2',
                'FTE',
                'Exemption Status',
                'Worker Type',
                'Division',
                'Cost Center',
                'Cost Center (code)',
                'encoded'
            ],
            row=lambda item: [
                item['name_pref_first'].strip(
                ) if item['name_pref_first'] else '',
                item['name_pref_last'].strip(
                ) if item['name_pref_last'] else '',
                item['email_work'].strip() if item['email_work'] else '',
                item['id_employee'].strip() if item['id_employee'] else '',
                (datetime.strptime(item['date_last_hire'].strip(
                ), "%Y-%m-%d")).strftime("%Y-%m-%d") if item['date_last_hire'] else datetime.now().strftime("%Y-%m-%d"),
                (datetime.strptime(item['term_date_all'].strip(
                ), "%Y-%m-%d")).strftime("%Y-%m-%d") if 'term_date_all' in item and item['term_date_all'] else '',
                item['email_work'].strip() if item['email_work'] else '',
                item['id_manager'].strip() if item['id_manager'] else '',
                item['email_manager'].strip() if item['email_manager'] else '',
                item['company'].strip() if item['company'] else '',
                item['department'].strip() if item['department'] else '',
                item['type_employee'].strip() if item['type_employee'] else '',
                item['id_job'].strip() if item['id_job'] else '',
                item['position_title'].strip(
                ) if item['position_title'] else '',
                item['id_job'].strip() if item['id_job'] else '',
                item['status'].strip() if item['status'] else '',
                item['status'].strip() if item['status'] else '',
                item['country_work'].strip() if item['country_work'] else '',
                item['loca_office'].strip() if item['loca_office'] else '',
                item['full_time_equiv'].strip(
                ) if item['full_time_equiv'] else '',
                item['exempt_status'].strip() if item['exempt_status'] else '',
                item['type_worker'].strip() if item['type_worker'] else '',
                item['division'].strip() if item['division'] else '',
                item['cost_center'].strip() if item['cost_center'] else '',
                item['id_cost_center'].strip(
                ) if item['id_cost_center'] else '',
                hashlib.md5((str(str(item['name_pref_first']) + str(item['name_pref_last']) + str(item['email_work']) + str(item['id_employee']) + str(item['date_last_hire']) +
                                 str(item['email_manager']) + str(item['company']) + str(item['department']) + str(item['type_employee']) + str(item['id_job']) + str(item['position_title']) + str(item['status']) + str(item['country_work']) +
                                 str(item['loca_office']) + str(item['full_time_equiv']) + str(item['exempt_status']) + str(
                    item['division']) + str(item['cost_center']) + str(item['id_cost_center']) + str(item['id_manager'])
                )).encode('utf-8')).hexdigest()
            ]
        )

        create_collection_create_list_from_csv_raw_data = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_raw_data',
            source="{{ result('write_csv_with_encoded') }}",
            name="inputfile",
            columns={
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Email': 'email',
                'Employee ID': 'employeeid',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Login Name': 'loginname',
                'Supervisor': 'supervisor',
                'Supervisor Email': 'supervisor_email',
                'Department Level 2': 'departmentlevel2',
                'Department Level 3': 'departmentlevel3',
                'Employee Type': 'employeetype',
                'Job Code': 'jobcode',
                'Position Title': 'pos_title',
                'Position Title Code': 'pos_title_code',
                'Login Status': 'login_status',
                'Status': 'status',
                'Location Level 1': 'location_level_1',
                'Location Level 2': 'location_level_2',
                'FTE': 'fte',
                'Exemption Status': 'exemption_status',
                'Worker Type': 'type_worker',
                'Division': 'division',
                'Cost Center': 'cost_center',
                'Cost Center (code)': 'cost_center_code',
                'encoded': 'encoded'
            }
        )

        if_csv_has_records = rail.IfOperator(
            task_id='if_csv_has_records',
            test='''{{ result('create_collection_create_list_from_csv_raw_data','length') < 1 }}''',
            yes_task="send_mail_notificationfornorecords_blank_data",
            no_task="query_list_getuserwith_missing_required_fields",
        )

        send_mail_notificationfornorecords_blank_data = rail.EmailOperator(
            task_id='send_mail_notificationfornorecords_blank_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | User import - no records received from workday {{ current_time("%d%m%Y%H%M%S")}} ''',
            html_content='''templates/no_records_in_report_mail.html''',
        )

        query_list_getuserwith_missing_required_fields = rail.QueryCollectionOperator(
            task_id='query_list_getuserwith_missing_required_fields',
            query="""SELECT * FROM  inputfile WHERE NULLIF(firstname,'') IS NULL OR NULLIF(lastname,'') IS NULL OR
              NULLIF(email,'') IS NULL OR NULLIF(employeeid,'') IS NULL OR NULLIF(startdate,'') IS NULL OR NULLIF(loginname,'') IS NULL
                OR NULLIF(supervisor,'') IS NULL OR NULLIF(departmentlevel2,'') IS NULL OR NULLIF(departmentlevel3,'') IS NULL
                  OR NULLIF(employeetype,'') IS NULL OR NULLIF(jobcode,'') IS NULL OR NULLIF(pos_title_code,'') IS NULL
                    OR NULLIF(login_status,'') IS NULL OR NULLIF(status,'') IS NULL OR NULLIF(location_level_1,'') IS NULL
                    OR NULLIF(location_level_2,'') IS NULL""",
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

        if_query_list_getuserwith_missing_required_fields_has_data = rail.IfOperator(
            task_id='if_query_list_getuserwith_missing_required_fields_has_data',
            test='''{{ result('query_list_getuserwith_missing_required_fields','length') > 0 }}''',
            yes_task="user_import_logs_add_batch_of_entries",
            no_task="query_list_valid_records",
        )

        user_import_logs_add_batch_of_entries = rail.WriteLogOperator(
            task_id='user_import_logs_add_batch_of_entries',
            log="{{ result('create_user_import_logs_lookuptable')}}",
            items="{{result('query_list_getuserwith_missing_required_fields')}}",
            message='One or more mandatory field is missing.',
            severity='Info',
            properties=lambda item: {
                "loginname": item['loginname'],
                "action": "Validation",
                "status": "Skipped",
                "details": python_callable_methods.get_missing_field_message(item),
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "childjobid": "",
                "firstname": item["firstname"],
                "lastname": item["lastname"]
            }
        )

        query_list_valid_records = rail.QueryCollectionOperator(
            task_id='query_list_valid_records',
            name="validatedinputlist",
            query="""SELECT * FROM  inputfile WHERE NULLIF(firstname,'') IS NOT NULL AND NULLIF(lastname,'') IS NOT NULL
              AND NULLIF(email,'') IS NOT NULL AND NULLIF(employeeid,'') IS NOT NULL AND NULLIF(startdate,'') IS NOT NULL
                AND NULLIF(loginname,'') IS NOT NULL AND NULLIF(supervisor,'') IS NOT NULL AND NULLIF(departmentlevel2,'') IS NOT NULL
                  AND NULLIF(departmentlevel3,'') IS NOT NULL AND NULLIF(employeetype,'') IS NOT NULL AND NULLIF(jobcode,'') IS NOT NULL
                    AND NULLIF(pos_title_code,'') IS NOT NULL AND NULLIF(login_status,'') IS NOT NULL AND NULLIF(status,'') IS NOT NULL
              AND NULLIF(location_level_1,'') IS NOT NULL AND NULLIF(location_level_2,'') IS NOT NULL """,
        )

        if_has_valid_records = rail.IfOperator(
            task_id='if_has_valid_records',
            test='''{{ result('query_list_valid_records', 'length') > 0 }}''',
            yes_task='query_list_groups_data',
            no_task='user_import_logs_search_entries'
        )

        query_list_groups_data = rail.QueryCollectionOperator(
            task_id='query_list_groups_data',
            name='groupsdata',
            query="""SELECT DISTINCT  validatedinputlist.division, validatedinputlist.departmentlevel2, validatedinputlist.departmentlevel3,
                validatedinputlist.employeetype, validatedinputlist.cost_center, validatedinputlist.cost_center_code,
                 validatedinputlist.pos_title, validatedinputlist.pos_title_code, validatedinputlist.location_level_1, validatedinputlist.location_level_2  FROM  validatedinputlist """,
        )

        if_query_list_has_groups_data = rail.IfOperator(
            task_id='if_query_list_has_groups_data',
            test='''{{ result('query_list_groups_data','length') > 0 }}''',
            yes_task="trigger_groups_update_child",
            no_task="get_userlist_report_details",
        )

        trigger_groups_update_child = rail.TriggerDagRunOperator(
            task_id='trigger_groups_update_child',
            retries=0,
            trigger_dag_id=config.groups_update_child_dagid,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "groupsupdatelookup": "{{result('create_groups_update_logs_lookup')}}"
            }
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

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{(result('generate_userlist_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}",
            delimiter=','
        )

        write_csv_user_list = rail.WriteCSVFileOperator(
            task_id='write_csv_user_list',
            source="{{ result('parse_csv') }}",
            header=['User Name',
                    'User First Name',
                    'User Last Name',
                    'Employee ID',
                    'Login Name',
                    'User Status',
                    'useruri',
                    'User End Date'
                    ],
            row=lambda item: [
                item['User Name'],
                item['User First Name'],
                item['User Last Name'],
                item['Employee ID'],
                item['Login Name'],
                item['User Status'],
                item['useruri'],
                item['User End Date']
            ],
        )

        create_collection_user_list_replicon = rail.CreateCollectionOperator(
            task_id='create_collection_user_list_replicon',
            source="{{ result('write_csv_user_list') }}",
            name="userlistfromreplicon",
            columns={
                'User Name': 'username',
                'User First Name': 'firstname',
                'User Last Name': 'lastname',
                'Employee ID': 'employeeid',
                'Login Name': 'loginname',
                'User Status': 'userstatus',
                'useruri': 'useruri',
                'User End Date': 'enddate'
            }
        )

        def get_group_list(response):
            groupdata = response['rows']
            return [{
                'name': data['cells'][0].get('textValue'),
                'uri': data['cells'][0].get('uri'),
                'fullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in data['cells'][1]['cellCollection']], '/')
            } for data in groupdata]

        get_department_group_details = rail.RepliconServiceOperator(
            task_id='get_department_group_details',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=get_group_list
        )

        get_location_details = rail.RepliconServiceOperator(
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
                "filterExpression": None
            },
            data_handler=response_filter.get_location_list
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        query_list_newuserstoprocess = rail.QueryCollectionOperator(
            task_id='query_list_newuserstoprocess',
            query="""SELECT * FROM  validatedinputlist WHERE validatedinputlist.employeeid NOT IN
              (SELECT DISTINCT userlistfromreplicon.employeeid FROM  userlistfromreplicon)""",
        )

        query_list_updateuserstoprocess = rail.QueryCollectionOperator(
            task_id='query_list_updateuserstoprocess',
            name="updateuserslist",
            query="""SELECT * FROM  validatedinputlist WHERE validatedinputlist.employeeid IN
                (SELECT DISTINCT userlistfromreplicon.employeeid FROM  userlistfromreplicon)""",
        )

        trigger_dag_run_user_add_async = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_user_add_async',
            items="{{ result('query_list_newuserstoprocess')}}",
            retries=0,
            trigger_dag_id=config.user_add_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=python_callable_methods.get_add_user_conf
        )

        wait_for_user_add_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_add_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_user_add_async") }}'
        )

        dir_get_referencefile_details = rail.SFTPListFilesOperator(
            task_id='dir_get_referencefile_details',
            paths=[config.reference_filepath],
        )

        if_has_reference_file = rail.IfOperator(
            task_id='if_has_reference_file',
            test=lambda: bool(rail.result(
                'dir_get_referencefile_details')),
            yes_task="get_reference_filename",
            no_task="add_log_reference_file_missing",
        )

        add_log_reference_file_missing = rail.WriteLogOperator(
            task_id='add_log_reference_file_missing',
            items="{{result('query_list_updateuserstoprocess')}}",
            log="{{result('create_user_import_logs_lookuptable')}}",
            message='na',
            severity='na',
            properties={
                'loginname': "{{item.loginname}}",
                'action': 'Update',
                'status': 'Skipped',
                'details': 'Reference file is Missing',
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
                'firstname': "{{item.firstname}}",
                'lastname': "{{item.lastname}}"
            }
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: config.reference_filepath +
            (rail.result('dir_get_referencefile_details')
             [config.reference_filepath])[0]['name']
        )

        download_referencefile = rail.SFTPDownloadFileOperator(
            task_id='download_referencefile',
            remote_filepath="{{result('get_reference_filename')}}"
        )

        load_csv_from_reference_file = rail.LoadCSVFileOperator(
            task_id="load_csv_from_reference_file",
            document="{{result('download_referencefile')}}",
        )

        create_collection_userreferencedata = rail.CreateCollectionOperator(
            task_id='create_collection_userreferencedata',
            source="{{ result('load_csv_from_reference_file') }}",
            name="userreferencedata",
            columns={
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Email': 'email',
                'Employee ID': 'employeeid',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Login Name': 'loginname',
                'Supervisor': 'supervisor',
                'Supervisor Email': 'supervisor_email',
                'Department Level 2': 'departmentlevel2',
                'Department Level 3': 'departmentlevel3',
                'Employee Type': 'employeetype',
                'Job Code': 'jobcode',
                'Position Title': 'pos_title',
                'Position Title Code': 'pos_title_code',
                'Login Status': 'login_status',
                'Status': 'status',
                'Location Level 1': 'location_level_1',
                'Location Level 2': 'location_level_2',
                'FTE': 'fte',
                'Exemption Status': 'exemption_status',
                'Worker Type': 'type_worker',
                'Division': 'division',
                'Cost Center': 'cost_center',
                'Cost Center (code)': 'cost_center_code',
                'encoded': 'encoded'
            }
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM  updateuserslist WHERE  updateuserslist.encoded IN (SELECT DISTINCT  userreferencedata.encoded FROM  userreferencedata )""",
        )

        if_query_has_unchanged_records = rail.IfOperator(
            task_id='if_query_has_unchanged_records',
            test='''{{ result('query_unchanged_records','length') > 0 }}''',
            yes_task="user_import_logs_add_batch_of_entries_skipped_unchanged",
            no_task="query_changed_records",
        )

        user_import_logs_add_batch_of_entries_skipped_unchanged = rail.WriteLogOperator(
            task_id='user_import_logs_add_batch_of_entries_skipped_unchanged',
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
            no_task="supervisor_assignment_logs_search_entries",
        )

        trigger_child_user_update = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_user_update',
            retries=0,
            items="{{ result('query_changed_records') }}",
            trigger_dag_id=config.user_update_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=python_callable_methods.get_update_user_conf
        )

        wait_for_child_user_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_user_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_user_update") }}'
        )

        supervisor_assignment_logs_search_entries = rail.FilterLogEntriesOperator(
            task_id='supervisor_assignment_logs_search_entries',
            log="{{result('create_supervisor_assignment_lookup')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        if_supervisor_assignment_entries_present = rail.IfOperator(
            task_id='if_supervisor_assignment_entries_present',
            test='''{{ result('supervisor_assignment_logs_search_entries','length') > 0 | is_truthy }}''',
            yes_task="trigger_child_assign_supervisor",
            no_task="archive_old_reference_file",
        )

        trigger_child_assign_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_assign_supervisor',
            retries=0,
            items="{{ result('supervisor_assignment_logs_search_entries') }}",
            trigger_dag_id=config.assign_supervisor_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['properties']['username'],
                "employeeid": item['properties']['employeeid'],
                "supervisor": item['properties']['supervisor'],
                "parentjobid": item['properties']['jobid'],
                "childjobid": item['properties']['childjobid'],
                "firstname": item['properties']['firstname'],
                "lastname": item['properties']['lastname'],
                "useruri": item['properties']['useruri'],
                "action": item['properties']['action'],
                "supervisorpermission": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets'), 'displayText', "Supervisor", 'uri', ''),
                "today": python_callable_methods.get_today_date(),
                "userimportlogslookup": rail.result('create_user_import_logs_lookuptable'),
                "supervisorlookup": rail.result('create_supervisor_assignment_lookup'),
            }
        )

        waitfor_child_assign_supervisor = rail.WaitForDagRunsSensor(
            task_id='waitfor_child_assign_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_assign_supervisor") }}'
        )

        archive_old_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_old_reference_file',
            new_filename=config.archive_filepath +
            'Old_Ref_{{ dag_run_ecid() | replace(":", "-") }}_{{result("get_reference_filename") | file_name}}',
            existing_filename="{{result('get_reference_filename')}}",
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('write_csv_with_encoded') }}''',
            remote_filepath=config.reference_filepath +
            'Reference_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        user_import_logs_search_entries = rail.FilterLogEntriesOperator(
            task_id='user_import_logs_search_entries',
            log="{{result('create_user_import_logs_lookuptable')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('user_import_logs_search_entries') }}",
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

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name='{{result("create_csv_lines")}}',
            output_file_name='user_import_logs_{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=config.log_file_download_link_expiry_in_sec
        )

        get_error_exception_checks = rail.PythonOperator(
            task_id='get_error_exception_checks',
            python_callable=python_callable_methods.get_error_and_email_subject
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc="{%- if result('get_error_exception_checks').errorcheck -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='''{{ get_company_key() }}| User import {{ result('get_error_exception_checks').subject }}- {{ current_time() }} ''',
            html_content='''templates/completion_mail.html''',
            params={
                'generated_link': "{{result('generate_download_link')}}"
            },
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
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

        get_csv_data_from_workday >> load_csv >> write_csv_with_encoded >> create_collection_create_list_from_csv_raw_data >> if_csv_has_records
        if_csv_has_records >> rail.Label(
            'Yes') >> send_mail_notificationfornorecords_blank_data >> finish
        if_csv_has_records >> rail.Label('No') >> query_list_getuserwith_missing_required_fields >> create_user_import_logs_lookuptable \
            >> create_supervisor_assignment_lookup >> create_groups_update_logs_lookup >> if_query_list_getuserwith_missing_required_fields_has_data
        if_query_list_getuserwith_missing_required_fields_has_data >> rail.Label(
            'Yes') >> user_import_logs_add_batch_of_entries >> query_list_valid_records
        if_query_list_getuserwith_missing_required_fields_has_data >> rail.Label(
            'No') >> query_list_valid_records >> if_has_valid_records
        if_has_valid_records >> rail.Label(
            'Yes') >> query_list_groups_data >> if_query_list_has_groups_data
        if_has_valid_records >> rail.Label(
            'No') >> user_import_logs_search_entries
        if_query_list_has_groups_data >> rail.Label(
            'Yes') >> trigger_groups_update_child >> get_userlist_report_details
        if_query_list_has_groups_data >> rail.Label('No') >> get_userlist_report_details >> generate_userlist_report >> parse_csv >>\
            write_csv_user_list >> create_collection_user_list_replicon >> \
            get_department_group_details >> get_location_details >> get_all_permission_sets >> query_list_newuserstoprocess >> query_list_updateuserstoprocess >> \
            trigger_dag_run_user_add_async >> wait_for_user_add_child >> dir_get_referencefile_details >> if_has_reference_file
        if_has_reference_file >> rail.Label('Yes') >> get_reference_filename >> download_referencefile >> load_csv_from_reference_file >>\
            create_collection_userreferencedata >> query_unchanged_records >> if_query_has_unchanged_records
        if_query_has_unchanged_records >> rail.Label(
            'Yes') >> user_import_logs_add_batch_of_entries_skipped_unchanged >> query_changed_records
        if_query_has_unchanged_records >> rail.Label(
            'No') >> query_changed_records >> if_changed_records_present
        if_changed_records_present >> rail.Label(
            'Yes') >> trigger_child_user_update >> wait_for_child_user_update >> supervisor_assignment_logs_search_entries
        if_changed_records_present >> rail.Label(
            'No') >> supervisor_assignment_logs_search_entries >> if_supervisor_assignment_entries_present
        if_supervisor_assignment_entries_present >> rail.Label(
            'Yes') >> trigger_child_assign_supervisor >> waitfor_child_assign_supervisor >> archive_old_reference_file >>\
            upload_new_reference_file >> user_import_logs_search_entries
        if_supervisor_assignment_entries_present >> rail.Label(
            'No') >> archive_old_reference_file
        user_import_logs_search_entries >> create_csv_lines >>\
            generate_download_link >> get_error_exception_checks >> send_success_email >> finish
        if_has_reference_file >> rail.Label(
            'No') >> add_log_reference_file_missing >> supervisor_assignment_logs_search_entries
        finish >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_dag)
