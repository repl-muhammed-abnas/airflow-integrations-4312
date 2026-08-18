import hashlib
from datetime import timedelta, datetime
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'omd_singapore_user_import_master_{config.instance}',
        description=f'OMD Singapore User Import Master {config.instance}',
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

        log_current_time_for_subject = rail.PythonOperator(
            task_id = 'log_current_time_for_subject',
            python_callable= lambda: datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
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
            test='''{{ result('new_file_sensor') | file_ext | lower == 'csv'}}''',
            yes_task="parse_csv",
            no_task="send_mail_incorrect_file_format",
        )

        send_mail_incorrect_file_format=rail.EmailOperator(
            task_id='send_mail_incorrect_file_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key()}}" + "|" + "User import completed file processing is skipped" + "{{ result('log_current_time_for_subject')}}",
            html_content='templates/incorrect_file_format_mail.html',
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
                    'internaltitle',
                    'md5'],
            row=lambda item:
            [
                item['Login_name'],
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
                item['Internal Title'],
                hashlib.md5((str(item['Login_name']) + "," + str(item['First_name']) + "," + str(item['Last_name']) + "," + str(item['Department']) + "," +
                            str(item['Login_status']) + "," + str(item['Emp_id']) + "," + str(item['Employee_type']) + "," +
                            str(item['Supervisor_id']) + "," + str(item['Startdate']) + "," + str(item['Enddate']) + "," + str(item['Email']) + "," +
                            str(item['Internal Title'])).encode('utf-8')).hexdigest(),
            ]
        )

        create_inputfile_collection = rail.CreateCollectionOperator(
            task_id='create_inputfile_collection',
            source = "{{ result('compose_csv') }}",
            name = "resource_details_list",
        )

        if_no_data_in_file=rail.IfOperator(
            task_id='if_no_data_in_file',
            test="{{result('create_inputfile_collection','length') < 1}}",
            yes_task="send_mail_no_data_in_file",
            no_task="download_reference_file",
        )

        send_mail_no_data_in_file=rail.EmailOperator(
            task_id='send_mail_no_data_in_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}}" + "|" + "User import completed file processing is skipped - " + "{{result('log_current_time_for_subject')}}",
            html_content='templates/no_data_in_file.html',
        )

        download_reference_file= rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath
        )

        load_reference_file=rail.LoadCSVFileOperator(
            task_id="load_reference_file",
            document="{{result('download_reference_file')}}",
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source = "{{ result('load_reference_file') }}",
            name = "reference_details_list",
            columns = {
                'Login_name':'loginname',
                'First_name':'firstname',
                'Last_name':'lastname',
                'Department':'department',
                'Login_status':'loginstatus',
                'Emp_id':'empid',
                'Employee_type':'employeetype',
                'Supervisor_id':'supervisorid',
                'Startdate':'startdate',
                'Enddate':'enddate',
                'Email':'email',
                'Internal Title':'internaltitle',
                'md5':'md5'
            }
        )

        query_delta_records=rail.QueryCollectionOperator(
            task_id='query_delta_records',
            query="""SELECT * FROM  resource_details_list WHERE
                    resource_details_list.md5 NOT IN (SELECT DISTINCT  reference_details_list.md5  FROM  reference_details_list)""",
        )

        has_delta_records=rail.IfOperator(
            task_id='has_delta_records',
            test="{{ result('query_delta_records','length') > 0 }}",
            yes_task="get_enabled_employee_type_groups",
            no_task="send_mail_no_delta_records",
        )

        get_enabled_employee_type_groups=rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
        )

        create_userimport_logs_lookuptable = rail.CreateLogOperator(
            task_id = 'create_userimport_logs_lookuptable'
        )

        create_userimport_supervisor_lookuptable = rail.CreateLogOperator(
            task_id = 'create_userimport_supervisor_lookuptable'
        )

        get_all_permission_sets=rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_departments_data=rail.RepliconServiceOperator(
            task_id='get_departments_data',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:code",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": "true",
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null
                    },
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            target='artifact'
        )

        def get_deparments_list():
            departments = rail.load_json_artifact(rail.result('get_departments_data'))['rows']
            departmets_list = []
            departmets_list = [{
                "name": rail.find_first_by_attr_and_get_attr(item['cells'],'dataType','urn:replicon:list-type:object','textValue',null),
                "uri": rail.find_first_by_attr_and_get_attr(item['cells'],'dataType','urn:replicon:list-type:object','uri',null),
                "fullpath": "|".join([ department['textValue'] for department in item['cells'][2]['cellCollection']])
            } for item in departments ]
            return departmets_list

        create_department_list = rail.PythonOperator(
            task_id = 'create_department_list',
            python_callable= get_deparments_list
        )

        get_all_custom_fields=rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        log_custom_field_name=rail.PythonOperator(
            task_id='log_custom_field_name',
            python_callable= lambda: (rail.find_first_by_attr_and_get_attr(rail.result(
                                'get_all_custom_fields'),'displayText',config.customfieldname1,'uri','')) if rail.result('get_all_custom_fields') else null
        )

        foreach_delta_record=rail.ForEachOperator(
            task_id='foreach_delta_record',
            items="{{ result('query_delta_records') }}",
            start_task = 'if_loginname_present',
            end_task = 'foreach_delta_record_end'
        )

        if_loginname_present=rail.IfOperator(
            task_id='if_loginname_present',
            test='''{{ result('foreach_delta_record').loginname | is_truthy }}''',
            yes_task="search_user_by_loginname",
            no_task="log_loginname_not_present",
        )

        search_user_by_loginname=rail.RepliconServiceOperator(
            task_id='search_user_by_loginname',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled",
                  "urn:replicon:user-list-column:hourly-cost"
              ],
              "sort": [],
              "filterExpression": {
                  "leftExpression": {
                      "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                  },
                  "operatorUri": "urn:replicon:filter-operator:text-search",
                  "rightExpression": {
                      "value": {
                          "text": "{{result('foreach_delta_record').loginname}}"
                      }
                  }
              }
            }
        )

        def get_useruri():
            if rail.result('search_user_by_loginname') and rail.result('search_user_by_loginname')['rows']:
                for user in rail.result('search_user_by_loginname')['rows']:
                    if user['cells'][0]['textValue'] == rail.result('foreach_delta_record')['loginname']:
                        return user['cells'][0]['uri']
            return null

        get_user_uri=rail.PythonOperator(
            task_id='get_user_uri',
            python_callable= get_useruri
        )

        if_uri_not_present=rail.IfOperator(
            task_id='if_uri_not_present',
            test='''{{ result('get_user_uri') | is_falsy }}''',
            yes_task="trigger_child_create_user",
            no_task="trigger_update_user",
        )

        def get_create_user_payload():
            user = rail.result('foreach_delta_record')
            return {
                "loginname": user['loginname'].strip() if user['loginname'] else null,
                "firstname": user['firstname'].strip() if user['firstname'] else null,
                "lastname": user['lastname'].strip() if user['lastname'] else null,
                "department": user['department'].strip() if user['department'] else null,
                "enabled": user['loginstatus'].strip() if user['loginstatus'] else null,
                "employeeId": user['empid'].strip() if user['empid'] else null,
                "employeetype": user['employeetype'].strip() if user['employeetype'] else null,
                "supervisor": user['supervisorid'].strip() if user['supervisorid'] else null,
                "startdate": user['startdate'],
                "enddate": user['enddate'],
                "email": user['email'].strip() if user['email'] else null,
                "customfield1": user['internaltitle'].strip() if user['internaltitle'] else null,
                "customfield1_uri": rail.result('log_custom_field_name'),
                "customfiled1type": config.customfieldname1_type,
                "departmenturi": (rail.find_first_by_attr_and_get_attr(
                                    rail.result('create_department_list'),'fullpath',user['department'].strip(),'uri',null)
                                    if rail.result('create_department_list')[0]['uri'] else null) if user['department'] else null,
                "employeetypeuri": ((rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_enabled_employee_type_groups'),'displayText',user['employeetype'].strip(),'uri',null))
                                    if rail.result('get_enabled_employee_type_groups') else null)
                                    if user['employeetype'] else null,
                "identifier": config.supervisor_identifier,
                "customfield1name": config.customfieldname1,
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'name','Supervisor','uri',''),
                "logslookuptable": rail.result('create_userimport_logs_lookuptable'),
                "supervisorlookuptable": rail.result('create_userimport_supervisor_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}"),
            }

        trigger_child_create_user=rail.TriggerDagRunOperator(
            task_id='trigger_child_create_user',
            retries=0,
            trigger_dag_id=f'omd_singapore_user_import_add_user_child{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_create_user_payload
        )

        def get_update_user_payload():
            user = rail.result('foreach_delta_record')
            return {
                "loginname": user['loginname'].strip() if user['loginname'] else null,
                "firstname": user['firstname'].strip() if user['firstname'] else null,
                "lastname": user['lastname'].strip() if user['lastname'] else null,
                "department": user['department'].strip() if user['department'] else null,
                "enabled": user['loginstatus'].strip() if user['loginstatus'] else null,
                "employeeId": user['empid'].strip() if user['empid'] else null,
                "employeetype": user['employeetype'].strip() if user['employeetype'] else null,
                "supervisor": user['supervisorid'].strip() if user['supervisorid'] else null,
                "startdate": user['startdate'],
                "enddate": user['enddate'],
                "email": user['email'].strip() if user['email'] else null,
                "timesheettemplate": '',
                "schedule": '',
                "customfield1": user['internaltitle'].strip() if user['internaltitle'] else null,
                "customfield1_uri": rail.result('log_custom_field_name'),
                "customfiled1type": config.customfieldname1_type,
                "useruri": rail.result('get_user_uri'),
                "departmenturi": (rail.find_first_by_attr_and_get_attr(
                                    rail.result('create_department_list'),'fullpath',user['department'].strip(),'uri',null)
                                    if rail.result('create_department_list')[0]['uri'] else null ) if user['department'] else null,
                "employeetypeuri": ((rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_enabled_employee_type_groups'),'displayText',user['employeetype'].strip(),'uri',null))
                                    if rail.result('get_enabled_employee_type_groups') else null)
                                    if user['employeetype'] else null,
                "identifier": config.supervisor_identifier,
                "customfield1name": config.customfieldname1,
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'name','Supervisor','uri',''),
                "logslookuptable": rail.result('create_userimport_logs_lookuptable'),
                "supervisorlookuptable": rail.result('create_userimport_supervisor_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }

        trigger_update_user=rail.TriggerDagRunOperator(
            task_id='trigger_update_user',
            retries=0,
            trigger_dag_id=f'omd_singapore_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_update_user_payload
        )

        log_loginname_not_present=rail.WriteLogOperator(
            task_id='log_loginname_not_present',
            log="{{ result('create_userimport_logs_lookuptable') }}",
            message="na",
            severity="Skipped ",
            properties={
                "employeeid": 'NA',
                "username": "{{ result('foreach_delta_record').firstname }} {{ result('foreach_delta_record').lastname }}",
                "status": "Skipped",
                "action": "NA",
                "details": "Loginname not present",
                "jobid": "{{ dag_run_ecid() }}",
                "childjobid": "NA"
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ result('create_userimport_logs_lookuptable') }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{result('foreach_delta_record').loginname}}",
                "username": "{{ result('foreach_delta_record').firstname }} {{ result('foreach_delta_record').lastname }}",
                "status": "Error",
                "action": "NA",
                "details": "{{get_error_message()}}",
                "jobid": "{{ dag_run_ecid() }}",
                "childjobid": "NA"
            }
        )

        foreach_delta_record_end=rail.EmptyOperator(
            task_id='foreach_delta_record_end',
        )

        if_create_user_triggered = rail.IfOperator(
            task_id = 'if_add_user_triggered',
            test="{{ get_task_state('trigger_child_create_user') == 'success'}}",
            yes_task='wait_for_completion_create_user',
            no_task='if_update_user_triggered'
        )

        wait_for_completion_create_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_create_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_create_user") }}'
        )

        if_update_user_triggered = rail.IfOperator(
            task_id = 'if_update_user_triggered',
            test="{{ get_task_state('trigger_update_user') == 'success'}}",
            yes_task='wait_for_completion_update_user',
            no_task='search_supervisor_assignment_entries'
        )

        wait_for_completion_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_update_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_update_user") }}'
        )

        search_supervisor_assignment_entries=rail.FilterLogEntriesOperator(
            task_id='search_supervisor_assignment_entries',
            log="{{ result('create_userimport_supervisor_lookuptable') }}",
        )

        if_entries_present=rail.IfOperator(
            task_id='if_entries_present',
            test="{{ result('search_supervisor_assignment_entries','length') > 0 }}",
            yes_task="trigger_add_supervisor_dag",
            no_task="rename_archived_fileas_processed",
        )

        def get_add_supervisor_payload(item):
            item = item['properties']
            return {
                        "loginname": item['username'],
                        "supervisorloginname": item['supervisorloginname'],
                        "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
                        "childjobid": item['childjobid'],
                        "useruri": item['useruri'],
                        "action": item['action'],
                        "identifier": config.supervisor_identifier,
                        "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'name','Supervisor','uri',''),
                        "logslookuptable": rail.result('create_userimport_logs_lookuptable'),
                        "supervisorlookuptable": rail.result('create_userimport_supervisor_lookuptable'),
            }

        trigger_add_supervisor_dag=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_add_supervisor_dag',
            retries=0,
            items="{{ result('search_supervisor_assignment_entries') }}",
            trigger_dag_id=f'omd_singapore_user_import_add_supervisor_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=get_add_supervisor_payload
        )

        wait_for_add_supervisor_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_supervisor_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_add_supervisor_dag") }}'
        )

        rename_archived_fileas_processed=rail.SFTPMoveFileOperator(
            task_id='rename_archived_fileas_processed',
            new_filename= config.archive_filepath + "Processed" + "_{{ result('new_file_sensor') | file_name }}",
            existing_filename= config.archive_filepath + "{{dag_run_ecid()}}" + "_{{ result('new_file_sensor') | file_name }}"
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda: 'logs_' + datetime.now().strftime('%H%M%S') + "_" + rail.render_template("{{result('new_file_sensor') | file_name}}")
        )

        get_log_entries = rail.FilterLogEntriesOperator(
            task_id = 'get_log_entries',
            log="{{ result('create_userimport_logs_lookuptable') }}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        load_log_entries = rail.PythonOperator(
            task_id = 'load_log_entries',
            python_callable=lambda: rail.load_all_records(rail.result('get_log_entries'))
        )

        if_entries_not_present_but_delta_records = rail.IfOperator(
            task_id = 'if_entries_not_present_but_delta_records',
            test="{{ result('get_log_entries','length') < 1 and result('query_delta_records','length') > 0}}",
            yes_task='fail_with_error',
            no_task='compose_logs'
        )

        fail_with_error = rail.FailOperator(
            task_id = 'fail_with_error',
            message="No record found in lookup table"
        )

        compose_logs = rail.WriteCSVFileOperator(
            task_id = 'compose_logs',
            source="{{result('get_log_entries')}}",
            header=[
                'Login Name',
                'User Name',
                'Action',
                'Status',
                'Details',
                'JobId'
            ],
            row=lambda item:
            [
                item['properties']['employeeid'],
                item['properties']['employeeid'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['jobid'] + "|" + item['properties']['childjobid']
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs')}}",
            output_file_name='{{result("get_log_file_name")}}',
            expires_in_seconds=7*24*60*60,
        )

        check_error_exception_entries = rail.PythonOperator(
            task_id = 'check_error_exception_entries',
            python_callable=lambda: {
                'error': rail.find_first_by_attr_and_get_attr(rail.result('load_log_entries'),'properties.status','Error','properties.status',''),
                'exception': rail.find_first_by_attr_and_get_attr(rail.result('load_log_entries'),'properties.status','Exception','properties.status','')
            }
        )

        get_subject_line = rail.PythonOperator(
            task_id = 'get_subject_line',
            python_callable=lambda: "completed with errors" if rail.result('check_error_exception_entries')['error']
                                else ("compelted with exceptions" if rail.result('check_error_exception_entries')['exception'] else "completed successfully")
        )

        get_email_body = rail.PythonOperator(
            task_id = 'get_email_body',
            #pylint: disable =line-too-long
            python_callable=lambda: "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
                                if rail.result('check_error_exception_entries')['error'] else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key()}} | User import - {{result('get_subject_line')}} - {{current_time('%m/%d/%YT%H:%M:%S')}}",
            html_content='templates/completion_mail.html',
        )

        send_mail_no_delta_records=rail.EmailOperator(
            task_id='send_mail_no_delta_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key()}}" + "|" + "User import completed file processing is skipped" + "{{ result('log_current_time_for_subject')}}",
            html_content='templates/no_delta_records_mail.html',
        )

        upload_new_reference_file=rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('compose_csv') }}''',
            remote_filepath= config.reference_filepath
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> log_current_time_for_subject >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >> if_name_ends_with_csv >> rail.Label('No')  >> send_mail_incorrect_file_format >> finish
        if_name_ends_with_csv >> rail.Label(
            'Yes') >> parse_csv >> compose_csv >> create_inputfile_collection >> if_no_data_in_file
        if_no_data_in_file >> rail.Label('Yes') >> send_mail_no_data_in_file >> finish
        if_no_data_in_file >> rail.Label(
            'No') >> download_reference_file >> load_reference_file >> create_referencefile_collection >> query_delta_records >> has_delta_records
        has_delta_records >> rail.Label(
            'Yes')  >> get_enabled_employee_type_groups >> create_userimport_logs_lookuptable >> create_userimport_supervisor_lookuptable
        create_userimport_supervisor_lookuptable >> get_all_permission_sets >> get_departments_data >> create_department_list >> get_all_custom_fields
        get_all_custom_fields >> log_custom_field_name >> foreach_delta_record >> if_loginname_present
        if_loginname_present >> rail.Label('Yes')  >> search_user_by_loginname >> get_user_uri >> if_uri_not_present
        if_uri_not_present >> rail.Label('Yes')  >> trigger_child_create_user >> catch_and_log_error
        if_uri_not_present >> rail.Label('No') >> trigger_update_user >> catch_and_log_error
        if_loginname_present >> rail.Label('No') >> log_loginname_not_present >> catch_and_log_error >> foreach_delta_record_end
        foreach_delta_record >> foreach_delta_record_end >> if_create_user_triggered >> rail.Label(
            'Yes') >> wait_for_completion_create_user >> if_update_user_triggered >> rail.Label(
            'Yes') >> wait_for_completion_update_user >> search_supervisor_assignment_entries >> if_entries_present
        if_create_user_triggered >> rail.Label('No') >> if_update_user_triggered >> rail.Label('No') >> search_supervisor_assignment_entries
        if_entries_present >> rail.Label('Yes')  >> trigger_add_supervisor_dag >> wait_for_add_supervisor_dag >> rename_archived_fileas_processed
        if_entries_present >> rail.Label('No') >> rename_archived_fileas_processed >> get_log_file_name >> get_log_entries >> load_log_entries
        load_log_entries >> if_entries_not_present_but_delta_records >> rail.Label('Yes') >> fail_with_error >> finish
        if_entries_not_present_but_delta_records >> rail.Label('No') >> compose_logs >> generate_download_link >> check_error_exception_entries
        check_error_exception_entries >> get_subject_line >> get_email_body >> send_completion_mail >> upload_new_reference_file >> finish >> log_to_sumo
        has_delta_records >> rail.Label('No') >> send_mail_no_delta_records >> upload_new_reference_file >> finish >> log_to_sumo
    return dag

rail.for_each_instance(create_dag)
