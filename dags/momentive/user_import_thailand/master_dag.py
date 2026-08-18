import json
from datetime import timedelta
from pendulum import now, datetime as dt
from momentive.user_import_thailand.utils import request_payload
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_master_dag_id,
        description=f'Momentive user import Thailand - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2026, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

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
            task_id='if_first_employee_id_blank_1_8',
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
                'Worker_subType', 'Cost_center', 'Worker_cc_change_date', 'Year_of_service', 'Paygroup'],
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
            item['Legal entity'] if item['Legal entity'] else '',  # Worker_subType is sourced from Legal Entity, matching the Workato recipe
            item['Cost center'] if item['Cost center'] else '',
            item['Workers CC change eff date'] if item['Workers CC change eff date'] else '',
            item['Years of service'] if item['Years of service'] else '',
            item['Pay group'] if item['Pay group'] else '',
            ]
        )

        if_record_count_less_than_1_15 = rail.IfOperator(
            task_id='if_record_count_less_than_1_15',
            # Recipe [10-11] counts the record set. Count get_write_csv_task_source
            # (runs on BOTH the trial/SFTP and Workday branches); parse_user_sync_csv
            # only runs on the trial branch, so it would falsely report 0 on the
            # live Workday path.
            test=lambda: bool(
                int(len(rail.load_all_records(rail.result('get_write_csv_task_source')))) < 1),
            yes_task="send_mail_no_change_records",
            no_task="create_collection_create_list_from_csv",
        )

        send_mail_no_change_records = rail.EmailOperator(
            task_id='send_mail_no_change_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key() }} - Thailand| User import completed- No change records found - {{ current_time() }} ''',
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
                # CSV header is 'CF_LRV_Job_Exempt_Eff_Date'; CreateCollectionOperator
                # maps by header NAME, so the key must equal the header. Downstream
                # reads the field as 'Exemption_Eff_Date'.
                'CF_LRV_Job_Exempt_Eff_Date': 'Exemption_Eff_Date',
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

        momentive_user_import_logs_skipped_entries = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_skipped_entries',
            log="{{result('create_log_momentive_user_import_log')}}",
            items="{{result('query_list_usershereloginnameisblank_20')}}",
            severity='na',
            message='Skipped',
            properties=lambda item: {
                'jobid': rail.render_template("{{ dag_run_ecid() }}"),
                "userid": item['User_ID'],
                "username": item['First_Name'] + " " + item['Last_Name'],
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
            yes_task="getall_enabled_departments_28",
            no_task="send_mail_no_change_records",
        )

        getall_enabled_departments_28 = rail.RepliconServiceOperator(
            task_id='getall_enabled_departments_28',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response: [{
                "departmentgroupname": item["cells"][0]["textValue"],
                "departmentgroupuri": item["cells"][0]["uri"],
                "fullpath": " / ".join([cell["textValue"] for cell in item["cells"][1]["cellCollection"]]) if item["cells"][1].get("cellCollection") else item["cells"][1].get("textValue", "")
            } for item in response['rows']]
        )

        # Recipe [31]-[75]: the per-user search + add/update/disable decision tree used to
        # run sequentially inside a master ForEach. It now lives in process_each_user_dag
        # and is fanned out here, one DAG run per user, `parallel_count` at a time. The
        # task group waits for every triggered run before the pipeline continues.
        process_each_user_parallel_dagrun = rail.trigger_parallel_dagrun(
            task_id='process_each_user_parallel_dagrun',
            items="{{ result('query_list_usershereloginnameispresent_22') }}",
            trigger_dag_id=config.momentive_thailand_user_sync_process_each_user_dag_id,
            parallel_count=config.process_each_user_trigger_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: request_payload.process_each_user_payload(item)
        )

        momentive_supervisor_assignment_search_entries_77 = rail.FilterLogEntriesOperator(
            task_id='momentive_supervisor_assignment_search_entries_77',
            log="{{result('create_log_momentive_supervisor_assignment')}}",
            properties={
                'parentjobid': "{{dag_run_ecid()}}",
            }
        )

        # Resolve the 'Supervisor - Edit' permission-set URI once and pass it to every
        # supervisor-assignment child (recipe step 2/3 resolves it per run).
        get_supervisor_permission_set = rail.RepliconServiceOperator(
            task_id='get_supervisor_permission_set',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Supervisor - Edit', 'uri', '')
        )

        trigger_dag_run_momentive_supervisor_assignment_80 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_momentive_supervisor_assignment_80',
            retries=0,
            items="{{result('momentive_supervisor_assignment_search_entries_77')}}",
            trigger_dag_id=config.momentive_thailand_user_sync_supervisor_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "loginname": "{{ item.properties.loginid }}",
                "supervisorloginname": "{{ item.properties.supervisorempid}}",
                "useruri": "{{ item.properties.useruri }}",
                "parentjobid": "{{ dag_run_ecid() }}",
                "type": "{{ item.properties.type }}",
                "childjobid": "{{ item.properties.childjobid }}",
                "sup_firstname": "{{ item.properties.sup_firstname}}",
                "sup_lastname": "{{ item.properties.sup_lastname }}",
                "sup_email": "{{ item.properties.sup_email }}",
                "sup_change_effectivedate": "{{item.properties.sup_change_effective_date}}",
                "supervisor": "{{ result('get_supervisor_permission_set') }}",
                "user_import_logs": "{{ result('create_log_momentive_user_import_log') }}"
            }
        )

        wait_for_completion_trigger_dag_run_momentive_supervisor_assignment_80 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_momentive_supervisor_assignment_80',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_momentive_supervisor_assignment_80") }}'
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
            '''/userimport_log_{{ result('log_todaysdate_2') }}.csv''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name='''thailand_userimport_log_{{ result('log_todaysdate_2') }}.csv''',
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
            subject='''{{get_company_key() }} -Thailand |  Failed while uploading User import Logs to SFTP  - {{ current_time() }} ''',
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
            >> query_list_usershereloginnameisblank_20 >> create_log_momentive_user_import_log >> create_log_momentive_supervisor_assignment \
            >> momentive_user_import_logs_skipped_entries \
            >> query_list_usershereloginnameispresent_22 >> if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23

        if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23 >> rail.Label(
            'No') >> send_mail_no_change_records >> finish

        # Each user is processed by its own process_each_user DAG run (search + add/update/
        # disable decision + child trigger + wait). trigger_parallel_dagrun fans these out
        # and waits for all of them, so by the time the supervisor stage runs every child
        # has finished and supervisor_assignment_logs is complete.
        if_query_list_usershereloginnameispresent_22_rows_greater_than_0_23 >> rail.Label("Yes") >> getall_enabled_departments_28 \
            >> process_each_user_parallel_dagrun \
            >> momentive_supervisor_assignment_search_entries_77 >> get_supervisor_permission_set \
            >> trigger_dag_run_momentive_supervisor_assignment_80 \
            >> wait_for_completion_trigger_dag_run_momentive_supervisor_assignment_80 >> search_log_entries

        search_log_entries >> compose_logs_csv >> upload_logs_to_sftp >> generate_download_link >> if_log_upload_successful
        if_log_upload_successful >> rail.Label(
            'Yes') >> check_for_error_log >> check_for_exception_log >> send_import_complete_email >> finish

        if_log_upload_successful >> rail.Label(
            'No') >> send_alert_mail_log_upload_unsuccessful >> finish

        return dag


rail.for_each_instance(create_dag)
