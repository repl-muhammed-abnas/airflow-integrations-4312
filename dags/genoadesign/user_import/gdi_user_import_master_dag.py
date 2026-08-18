
from datetime import timedelta
import hashlib
import pendulum
from airflow.models import Variable
import rail
from rail.lib.log import get_master_log_artifact_name

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_user_import_master_{config.instance}',
        description=f'Live|GDI: User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
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
                config.pacific_timezone).strftime('%Y%m%dT%H%M%S')
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
            no_task='if_name_downcase_not_ends_with_csv_5'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_name_downcase_not_ends_with_csv_5',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_name_downcase_not_ends_with_csv_5 = rail.IfOperator(
            task_id='if_name_downcase_not_ends_with_csv_5',
            test='''{{ not(result('new_file_sensor') | ends_with('csv')) }}''',
            yes_task="send_mail_send_emailfor_incorrect_fileformat_6",
            no_task="if_name_downcase_ends_with_csv_9",
        )

        send_mail_send_emailfor_incorrect_fileformat_6 = rail.EmailOperator(
            task_id='send_mail_send_emailfor_incorrect_fileformat_6',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | User import - Skipped file processing - {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }} ''',
            html_content='''<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
            <p>Hi ,</p>
            <p>The User import is skipped on {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }}. The file - {{ result('new_file_sensor') | file_name }} does not have the prescribed format (.csv).</p>
            <p>For any queries, please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br />Deltek Inc.</p> ''',
            params=None,
        )

        rename_move_input_fileto_archive_7 = rail.SFTPMoveFileOperator(
            task_id='rename_move_input_fileto_archive_7',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            '''/Skipped_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}'''
        )

        if_name_downcase_ends_with_csv_9 = rail.IfOperator(
            task_id='if_name_downcase_ends_with_csv_9',
            test='''{{ result("new_file_sensor").split(".")[-1] | lower == "csv" if result("new_file_sensor") else False }}''',
            yes_task="download_11",
            no_task="finish",
        )

        download_11 = rail.SFTPDownloadFileOperator(
            task_id='download_11',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        parse_csv_12 = rail.LoadCSVFileOperator(
            task_id="parse_csv_12",
            document="{{ result('download_11') }}",
            encoding='ISO-8859-1'
        )

        rename_move_input_fileto_archive_12 = rail.SFTPMoveFileOperator(
            task_id='rename_move_input_fileto_archive_12',
            new_filename=config.archive_filepath +
            '''/Processed_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename="{{ result('new_file_sensor') }}",
        )

        send_mail_send_emailfor_blankfile_filewithnorecords_14 = rail.EmailOperator(
            task_id='send_mail_send_emailfor_blankfile_filewithnorecords_14',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | User import - no records in file {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }} ''',
            html_content='''<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
            <p>Hi ,</p>
            <p>The User import is completed on {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }}. There were no records in the file - {{ result('new_file_sensor') | file_name }} to be processed.</p>
            <p>For any queries, please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br />Deltek Inc.</p> ''',
            params=None,
        )

        def get_formated_user_row(item):
            user_md5 = hashlib.md5((
                item["First Name"]+"," +
                item["LastName"]+"," +
                item['Email Address']+"," +
                item['Employee ID']+"," +
                item['Team']+"," +
                item['Start Date']+"," +
                item['Login Name']+"," +
                item['Department Name']+"," +
                item['Supervisor']+"," +
                item['Supervisor Effective date']+"," +
                item['Department']+"," +
                item['Employee Hourly Cost']+"," +
                item['Employee Hourly cost Effective Date']+"," +
                item['User Hourly cost currency']+"," +
                item['Employee Type']+"," +
                item['Login Status']+"," +
                item['Location']+"," +
                item['Time Zone']+"," +
                item['Holiday Calendar']).encode()).hexdigest()

            return {
                "firstname": item["First Name"].strip() if item["First Name"] else "",
                "lastname": item["LastName"].strip() if item["LastName"] else "",
                "emailaddress": item["Email Address"].strip() if item["Email Address"] else "",
                "employeeid": item["Employee ID"].strip() if item["Employee ID"] else "",
                "team": item["Team"].strip() if item["Team"] else "",
                "startdate": item["Start Date"].strip() if item["Start Date"] else "",
                "loginname": item["Login Name"].strip() if item["Login Name"] else "",
                "departmentname": item["Department Name"].strip() if item["Department Name"] else "",
                "supervisor": item["Supervisor"].strip() if item["Supervisor"] else "",
                "supervisoreffectivedate": item["Supervisor Effective date"].strip() if item["Supervisor Effective date"] else "",
                "department": item["Department"].strip() if item["Department"] else "",
                "employeehourlycost": item["Employee Hourly Cost"].strip() if item["Employee Hourly Cost"] else "",
                "employeehourlycosteffectivedate": item["Employee Hourly cost Effective Date"].strip() if item["Employee Hourly cost Effective Date"] else "",
                "userhourlycostcurrency": item["User Hourly cost currency"].strip() if item["User Hourly cost currency"] else "",
                "employeetype": item["Employee Type"].strip() if item["Employee Type"] else "",
                "loginstatus": item["Login Status"].strip() if item["Login Status"] else "",
                "location": item["Location"].strip() if item["Location"] else "",
                "timezone": item["Time Zone"].strip() if item["Time Zone"] else "",
                "holidaycalendar": item["Holiday Calendar"].strip() if item["Holiday Calendar"] else "",
                "encoded": user_md5
            }.values()

        load_csv_create_list_from_csv_17 = rail.WriteCSVFileOperator(
            task_id='load_csv_create_list_from_csv_17',
            source="{{ result('parse_csv_12') }}",
            header=['firstname',
                    'lastname',
                    'emailaddress',
                    'employeeid',
                    'team',
                    'startdate',
                    'loginname',
                    'departmentname',
                    'supervisor',
                    'supervisoreffectivedate',
                    'department',
                    'employeehourlycost',
                    'employeehourlycosteffectivedate',
                    'userhourlycostcurrency',
                    'employeetype',
                    'loginstatus',
                    'location',
                    'timezone',
                    'holidaycalendar',
                    'encoded'],
            row=get_formated_user_row
        )

        create_collection_create_list_from_csv_17 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_17',
            source="{{ result('load_csv_create_list_from_csv_17') }}",
            name="inputfilerawdata",
            columns={
                'firstname': 'firstname',
                'lastname': 'lastname',
                'emailaddress': 'emailaddress',
                'employeeid': 'employeeid',
                'team': 'team',
                'startdate': 'startdate',
                'loginname': 'loginname',
                'departmentname': 'departmentname',
                'supervisor': 'supervisor',
                'supervisoreffectivedate': 'supervisoreffectivedate',
                'department': 'department',
                'employeehourlycost': 'employeehourlycost',
                'employeehourlycosteffectivedate': 'employeehourlycosteffectivedate',
                'userhourlycostcurrency': 'userhourlycostcurrency',
                'employeetype': 'employeetype',
                'loginstatus': 'loginstatus',
                'location': 'location',
                'timezone': 'timezone',
                'holidaycalendar': 'holidaycalendar',
                'encoded': 'encoded'
            }
        )

        if_parse_csv_12_lines_less_than_1_13 = rail.IfOperator(
            task_id='if_parse_csv_12_lines_less_than_1_13',
            test='{{ result("create_collection_create_list_from_csv_17", "length") == 0 }}',
            yes_task="send_mail_send_emailfor_blankfile_filewithnorecords_14",
            no_task="if_parse_csv_12_lines_greater_than_0_15",
        )

        if_parse_csv_12_lines_greater_than_0_15 = rail.IfOperator(
            task_id='if_parse_csv_12_lines_greater_than_0_15',
            test='{{ result("create_collection_create_list_from_csv_17", "length") > 0 }}',
            yes_task="dir_18",
            no_task="finish",
        )

        dir_18 = rail.SFTPListFilesOperator(
            task_id='dir_18',
            paths=[config.referance_filepath],
        )

        def has_any_file(result_task_id, input_file_path):
            if not result_task_id or not input_file_path:
                raise Exception(
                    "Task_id" if not result_task_id else "input path" + "is not provided")
            data = rail.result(result_task_id)
            if not data:
                return False
            return len(data[input_file_path]) > 0

        if_first_name_blank_19 = rail.IfOperator(
            task_id="if_first_name_blank_19",
            test=lambda: has_any_file(
                "dir_18", config.referance_filepath),
            yes_task="get_referance_file_name_21",
            no_task="finish"
        )

        def get_refrance_file_path():
            referance_info = rail.result('dir_18')
            file_info = referance_info[config.referance_filepath][0]
            return file_info['name']

        get_referance_file_name_21 = rail.PythonOperator(
            task_id='get_referance_file_name_21',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda:  get_refrance_file_path()
        )

        download_21 = rail.SFTPDownloadFileOperator(
            task_id='download_21',
            remote_filepath=config.referance_filepath + "/" +
            '''{{ result('get_referance_file_name_21') }}'''
        )

        parse_referance_csv = rail.LoadCSVFileOperator(
            task_id="parse_referance_csv",
            document="{{ result('download_21') }}"
        )

        def get_formated_user_referance_row(item):
            return {
                "firstname": item["firstname"].strip() if item["firstname"] else "",
                "lastname": item["lastname"].strip() if item["lastname"] else "",
                "emailaddress": item["emailaddress"].strip() if item["emailaddress"] else "",
                "employeeid": item["employeeid"].strip() if item["employeeid"] else "",
                "team": item["team"].strip() if item["team"] else "",
                "startdate": item["startdate"].strip() if item["startdate"] else "",
                "loginname": item["loginname"].strip() if item["loginname"] else "",
                "departmentname": item["departmentname"].strip() if item["departmentname"] else "",
                "supervisor": item["supervisor"].strip() if item["supervisor"] else "",
                "supervisoreffectivedate": item["supervisoreffectivedate"].strip() if item["supervisoreffectivedate"] else "",
                "department": item["department"].strip() if item["department"] else "",
                "employeehourlycost": item["employeehourlycost"].strip() if item["employeehourlycost"] else "",
                "employeehourlycosteffectivedate": item["employeehourlycosteffectivedate"].strip() if item["employeehourlycosteffectivedate"] else "",
                "userhourlycostcurrency": item["userhourlycostcurrency"].strip() if item["userhourlycostcurrency"] else "",
                "employeetype": item["employeetype"].strip() if item["employeetype"] else "",
                "loginstatus": item["loginstatus"].strip() if item["loginstatus"] else "",
                "location": item["location"].strip() if item["location"] else "",
                "timezone": item["timezone"].strip() if item["timezone"] else "",
                "holidaycalendar": item["holidaycalendar"].strip() if item["holidaycalendar"] else "",
                "encoded": item["encoded"].strip() if item["encoded"] else ""
            }.values()

        load_csv_create_list_from_csv_22 = rail.WriteCSVFileOperator(
            task_id='load_csv_create_list_from_csv_22',
            source="{{ result('parse_referance_csv') }}",
            header=['firstname',
                    'lastname',
                    'emailaddress',
                    'employeeid',
                    'team',
                    'startdate',
                    'loginname',
                    'departmentname',
                    'supervisor',
                    'supervisoreffectivedate',
                    'department',
                    'employeehourlycost',
                    'employeehourlycosteffectivedate',
                    'userhourlycostcurrency',
                    'employeetype',
                    'loginstatus',
                    'location',
                    'timezone',
                    'holidaycalendar',
                    'encoded'],
            row=get_formated_user_referance_row
        )

        create_collection_create_list_from_csv_22 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_22',
            source="{{ result('load_csv_create_list_from_csv_22') }}",
            name="referencefiledata",
            columns={
                'firstname': 'firstname',
                'lastname': 'lastname',
                'emailaddress': 'emailaddress',
                'employeeid': 'employeeid',
                'team': 'team',
                'startdate': 'startdate',
                'loginname': 'loginname',
                'departmentname': 'departmentname',
                'supervisor': 'supervisor',
                'supervisoreffectivedate': 'supervisoreffectivedate',
                'department': 'department',
                'employeehourlycost': 'employeehourlycost',
                'employeehourlycosteffectivedate': 'employeehourlycosteffectivedate',
                'userhourlycostcurrency': 'userhourlycostcurrency',
                'employeetype': 'employeetype',
                'loginstatus': 'loginstatus',
                'location': 'location',
                'timezone': 'timezone',
                'holidaycalendar': 'holidaycalendar',
                'encoded': 'encoded'
            }
        )

        query_list_un_changed_profiles_23 = rail.QueryCollectionOperator(
            task_id='query_list_un_changed_profiles_23',
            # fixme use NULLIF(col_name,'') for IS NULL or IS NOT NULL where clause
            query="""SELECT * FROM  inputfilerawdata WHERE  inputfilerawdata.encoded IN (SELECT DISTINCT  referencefiledata.encoded FROM  referencefiledata)""",
        )

        genoadi_user_import_unchanged_records_24 = rail.WriteLogOperator(
            task_id='genoadi_user_import_unchanged_records_24',
            message="No change in user records",
            items="{{ result('query_list_un_changed_profiles_23') }}",
            severity="Skipped",
            properties={
                "username|loginname": "{{ item.firstname }} {{ item.lastname }}|{{ item.loginname }}",
                "status": "Skipped",
                "details": "No change in user records",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        query_list_new_changed_profiles_27 = rail.QueryCollectionOperator(
            task_id='query_list_new_changed_profiles_27',
            # fixme use NULLIF(col_name,'') for IS NULL or IS NOT NULL where clause
            query="""SELECT * FROM  inputfilerawdata WHERE  inputfilerawdata.encoded NOT IN (SELECT DISTINCT  referencefiledata.encoded FROM  referencefiledata)""",
        )

        supervisor_processing_log = rail.CreateLogOperator(
            task_id='supervisor_processing_log',
        )

        process_user_28 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_user_28',
            retries=0,
            items="{{ result('query_list_new_changed_profiles_27') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'genoadesign_user_import_process_child_v1_0_{config.instance}',
            conf=lambda item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "email": item['emailaddress'],
                "employeeid": item['employeeid'],
                "team": item['team'],
                "startdate": item['startdate'],
                "loginname": item['loginname'],
                "departmentname": item['departmentname'],
                "supervisor": item['supervisor'],
                "supervisoreffectivedate": item['supervisoreffectivedate'],
                "department": item['departmentname'],
                "employeehourlycost": item['employeehourlycost'],
                "employeehourlycosteffectivedate": item['employeehourlycosteffectivedate'],
                "userhourlycostcurrency": item['userhourlycostcurrency'],
                "employeetype": item['employeetype'],
                "loginstatus": item['loginstatus'],
                "location": item['location'],
                "timezone": item['timezone'],
                "holidaycalendar": item['holidaycalendar'],
                "supervisor_processing_log": rail.result('supervisor_processing_log')
            }
        )

        wait_for_completion_process_user_28 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_process_user_28',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_user_28") }}'
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_supervisor_entries():
            supervisor_details = []
            supervisor_log_informations = get_data_from_document(
                rail.result('supervisor_processing_log'))
            for supervisor_info in supervisor_log_informations:
                super_user_info = supervisor_info['properties'].get(
                    'supervisorloginname').split('|')
                if supervisor_info['properties']:
                    supervisor_details.append({
                        "loginname": supervisor_info['properties'].get('userloginname').split('|')[0],
                        "username": supervisor_info['properties'].get('userloginname').split('|')[1],
                        "useruri": supervisor_info['properties'].get('userloginname').split('|')[-1],
                        "supervisorloginname": super_user_info[0],
                        "supervisoreffectivedate": super_user_info[1] if len(super_user_info) == 2 else None,
                        "action": supervisor_info['properties'].get('action'),
                    })
            return supervisor_details

        trigger_dag_run_genoadesign_user_import_gdi_child_add_supervisor_v1_0async_41 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_genoadesign_user_import_gdi_child_add_supervisor_v1_0async_41',
            retries=0,
            items=get_supervisor_entries,
            trigger_dag_id=f'genoadesign_user_import_gdi_child_add_supervisor_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda item: {
                "loginname": item['loginname'],
                "username": item['username'],
                "supervisorloginname": item['supervisorloginname'],
                "useruri":  item['useruri'],
                "action": item['action'],
                "supervisoreffectivedate": item['supervisoreffectivedate']
            }
        )

        wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_add_supervisor_v1_0async_41 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_add_supervisor_v1_0async_41',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_genoadesign_user_import_gdi_child_add_supervisor_v1_0async_41") }}'
        )

        genoadi_user_import_logs_search_entries_44 = rail.PythonOperator(
            task_id='genoadi_user_import_logs_search_entries_44',
            python_callable=lambda:  "true"
        )

        if_genoadi_user_import_logs_search_entries_44_entries_greater_than_0_45 = rail.IfOperator(
            task_id='if_genoadi_user_import_logs_search_entries_44_entries_greater_than_0_45',
            test='''{{ result('genoadi_user_import_logs_search_entries_44') | is_truthy}}''',
            yes_task="log_merge_46",
            no_task="rename_move_oldreference_fileto_archive_52",
        )

        def do_format_logs():
            context = get_master_log_artifact_name(rail.get_current_context())
            user_import_log = rail.load_all_records(context)
            unique_users = list(
                set(map(lambda item: item['properties'].get(
                    "username|loginname", ''), user_import_log))
            )

            def get_log_details(user_logs):
                return "|".join(list(filter(bool, (set(map(lambda x: x['properties']['details'], user_logs))))))

            def get_status_details(user_logs):
                return ";".join(list(filter(bool, (set(map(lambda x: x['properties']['status'], user_logs))))))

            logs = []
            # pylint: disable= cell-var-from-loop
            for employee_id in unique_users:
                if employee_id:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'username|loginname', '') == employee_id, user_import_log)
                    )

                    if len(user_logs) > 0:
                        first = user_logs[0]
                        logs.append(
                            {
                                "User Name": employee_id.split('|')[0],
                                "Login Name": first['properties']['username|loginname'].split('|')[-1],
                                "Status": get_status_details(user_logs),
                                "Details": get_log_details(user_logs),
                                "Jobid": first['ecid']
                            }
                        )
                else:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'Login Name', '') == '' or x['properties'].get(
                            'Login Name', '') is None, user_import_log)
                    )
                    for user in user_logs:
                        user_login = user['properties']['username|loginname'].split(
                            '|')
                        logs.append(
                            {
                                "User Name": user_login[0],
                                "Login Name": user_login[-1],
                                "Status": user['properties']['status'],
                                "Details": user['properties']['details'],
                                "Jobid": user['properties']['childjobid']
                            }
                        )

            return logs

        log_merge_46 = rail.PythonOperator(
            task_id='log_merge_46',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: do_format_logs()
        )

        create_csv_lines_46 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_46',
            source="{{ result('log_merge_46') | to_json }}",
            header=['User Name',
                    'Login Name',
                    'Status',
                    'Details',
                    'JobID'],
            row=[
                '{{ item | attr_or_default("User Name", "") }}',
                '{{ item | attr_or_default("Login Name", "") }}',
                '{{ item | attr_or_default("Status", "")}}',
                '{{ item | attr_or_default("Details", "") }}',
                '{{ item | attr_or_default("Jobid", "") }}'],
        )

        log_checkifthereareerrors_47 = rail.FilterLogEntriesOperator(
            task_id='log_checkifthereareerrors_47',
            properties={'status': 'Error'},
        )

        log_subjectline_48 = rail.PythonOperator(
            task_id='log_subjectline_48',
            python_callable=lambda:  "completed with errors" if rail.result(
                'log_checkifthereareerrors_47') and rail.result('log_checkifthereareerrors_47', 'length') > 0 else "completed successfully"
        )

        log_body_49 = rail.PythonOperator(
            task_id='log_body_49',
            python_callable=lambda:  '''<br />For any queries, please contact our support team at https://support.deltek.com <br /> <br />Regards, <br />Deltek Inc.''' if rail.result(
                'log_checkifthereareerrors_47') and rail.result('log_checkifthereareerrors_47', 'length') > 0 else '''<p> <br />For any queries, please contact our support team at https://support.deltek.com <br /> <br />Regards, <br />Deltek Inc. </p>'''
        )

        upload_uploadtologs_50 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadtologs_50',
            content='''{{ result('create_csv_lines_46') }}''',
            remote_filepath=config.log_filepath +
            '''/Logs_{{ result('get_time_for_file') }}_{{ result('new_file_sensor')| file_name }}''',
        )

        send_mail_send_emailforcompletion_51 = rail.EmailOperator(
            task_id='send_mail_send_emailforcompletion_51',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | User import - {{ result('log_subjectline_48') }} {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import job is {{ result('log_subjectline_48') }} based on the file - {{ result('new_file_sensor')|file_name }}. The logs have been placed in sftp (log file path: /logs/Logs_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}) for reference.</p>
            {%- set has_errors = result("log_checkifthereareerrors_47", key="length") > 0 -%}
            {%- if has_errors -%}
            For any queries, please contact our support team at https://support.deltek.com <br /> <br />Regards, <br />Deltek Inc.
            {%- else -%}
            <p> <br />For any queries, please contact our support team at https://support.deltek.com <br /> <br />Regards, <br />Deltek Inc. </p>
            {%- endif -%}''',
            params=None,
        )

        rename_move_oldreference_fileto_archive_52 = rail.SFTPMoveFileOperator(
            task_id='rename_move_oldreference_fileto_archive_52',
            existing_filename=config.referance_filepath +
            '''/{{ result('get_referance_file_name_21') }}''',
            new_filename=config.archive_filepath +
            '''/Old_reference_{{ result('get_time_for_file') }}_{{ result('get_referance_file_name_21') }}''',
        )

        upload_move_oldreference_fileto_archive_53 = rail.SFTPUploadFileOperator(
            task_id='upload_move_oldreference_fileto_archive_53',
            content='''{{ result('load_csv_create_list_from_csv_17') }}''',
            remote_filepath=config.referance_filepath +
            '''/{{ result('get_time_for_file') }}_{{ result('new_file_sensor')| file_name }}''',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> if_name_downcase_not_ends_with_csv_5
        if_name_downcase_not_ends_with_csv_5 >> rail.Label(
            'Yes') >> send_mail_send_emailfor_incorrect_fileformat_6 >> rename_move_input_fileto_archive_7 >> finish
        if_name_downcase_not_ends_with_csv_5 >> rail.Label(
            'No') >> if_name_downcase_ends_with_csv_9
        if_name_downcase_ends_with_csv_9 >> rail.Label(
            'Yes') >> download_11 >> parse_csv_12 >> rename_move_input_fileto_archive_12 >> load_csv_create_list_from_csv_17 >> \
            create_collection_create_list_from_csv_17 >> if_parse_csv_12_lines_less_than_1_13
        if_parse_csv_12_lines_less_than_1_13 >> rail.Label(
            'Yes') >> send_mail_send_emailfor_blankfile_filewithnorecords_14 >> finish
        if_parse_csv_12_lines_less_than_1_13 >> rail.Label(
            'No') >> if_parse_csv_12_lines_greater_than_0_15
        if_parse_csv_12_lines_greater_than_0_15 >> rail.Label(
            'Yes') >> dir_18 >> if_first_name_blank_19
        if_first_name_blank_19 >> rail.Label('Yes') >> finish
        if_first_name_blank_19 >> rail.Label('No') >> get_referance_file_name_21 >> download_21 >> parse_referance_csv >> load_csv_create_list_from_csv_22 >> \
            create_collection_create_list_from_csv_22 >> query_list_un_changed_profiles_23 >> \
            genoadi_user_import_unchanged_records_24 >> query_list_new_changed_profiles_27 >> supervisor_processing_log >> \
            process_user_28 >> wait_for_completion_process_user_28 >> \
            trigger_dag_run_genoadesign_user_import_gdi_child_add_supervisor_v1_0async_41 >> \
            wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_add_supervisor_v1_0async_41 >> \
            genoadi_user_import_logs_search_entries_44 >> if_genoadi_user_import_logs_search_entries_44_entries_greater_than_0_45
        if_genoadi_user_import_logs_search_entries_44_entries_greater_than_0_45 >> rail.Label(
            'Yes') >> log_merge_46 >> create_csv_lines_46 >> log_checkifthereareerrors_47 >> log_subjectline_48 >> log_body_49 >> upload_uploadtologs_50 >> \
            send_mail_send_emailforcompletion_51 >> rename_move_oldreference_fileto_archive_52
        if_genoadi_user_import_logs_search_entries_44_entries_greater_than_0_45 >> rail.Label(
            'No') >> rename_move_oldreference_fileto_archive_52 >> upload_move_oldreference_fileto_archive_53 >> finish
        if_parse_csv_12_lines_greater_than_0_15 >> rail.Label(
            'No') >> finish
        if_name_downcase_ends_with_csv_9 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
