
from datetime import timedelta
import rail
from npsg.timeoff_import.task.generate_report_batch import report_batch
from npsg.timeoff_import.utils import python_callable_method

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'npsg_timeoff_import_npsg_time_off_import_master_v1_0_{config.instance}',
        description=f'NPSG - Time off Import_Master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        schedule_interval=timedelta(seconds=config.schedule_interval),
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
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_10',
            no_task='send_mail_3',
        )

        send_mail_3 = rail.EmailOperator(
            task_id='send_mail_3',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import skipped -{{ current_time() }} ''',
            html_content='''<p>Hello, <br/> <br/> Replicon timeoff import skipped due to incorrect file extension. Please correct the file extension to .CSV and place a new file. <br/> <br/>For any queries, please contact our support team at https://support.deltek.com <br/><br/>Regards, <br/>Deltek Inc.</p> '''
        )

        rename_archivetheinputfile_4 = rail.SFTPMoveFileOperator(
            task_id='rename_archivetheinputfile_4',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_10 = rail.SFTPDownloadFileOperator(
            task_id='download_10',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ (get_task_state("new_file_sensor") == "success") and (result("new_file_sensor") | file_ext | lower == "csv") }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_csv_create_list_from_csv_12 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_12",
            document="{{ result('download_10') }}",
        )

        create_collection_create_list_from_csv_12 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_12',
            source="{{ result('load_csv_create_list_from_csv_12') }}",
            name="inputfile",
            columns={
                'User Name': 'username',
                'Employee ID': 'employeeid',
                'TimeOff Type': 'timeofftype',
                'Start Date': 'startdate',
                'Time off Status': 'timeoffstatus',
                'Amount': 'amount',
                'Status': 'status',
                'Time off Entry ID': 'entryid'
            }
        )

        if_create_list_from_csv_12_row_count_less_than_1_13 = rail.IfOperator(
            task_id='if_create_list_from_csv_12_row_count_less_than_1_13',
            test='''{{ result('create_collection_create_list_from_csv_12', 'length') < 1 }}''',
            yes_task="send_mail_15",
            no_task="create_timeoff_import_logs"
        )

        send_mail_15 = rail.EmailOperator(
            task_id='send_mail_15',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import skipped -{{ current_time() }} ''',
            html_content='''<p>Hello, <br /> <br /> Replicon timeoff import skipped due to no data in the file. Please check and place a new file. <br /> <br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        create_timeoff_import_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_logs'
        )

        query_list_missingmandatoryvalues_ignored_18 = rail.QueryCollectionOperator(
            task_id='query_list_missingmandatoryvalues_ignored_18',
            query="""SELECT * FROM inputfile WHERE NULLIF(employeeid,'') IS NULL OR NULLIF(timeofftype,'') IS NULL OR NULLIF(startdate,'') IS NULL OR NULLIF(amount,'') IS NULL OR NULLIF(status,'') IS NULL"""
        )

        insert_to_list_19 = rail.WriteLogOperator(
            task_id='insert_to_list_19',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_list_missingmandatoryvalues_ignored_18') }}",
            message="One or more mandatory field is missing.",
            severity="Info",
            properties={
                "employeeid": "{{ item.employeeid }}",
                "timeoffstatus": "{{ item.timeoffstatus }}",
                "timeofftype": "{{ item.timeofftype }}",
                "startdate": "{{ item.startdate }}",
                "hours": "{{ item.amount }}",
                "status": "Ignored",
                "details": "One or more mandatory field is missing.",
                "timeoffaction": "{{ item.status }}"
            }
        )

        query_list_recordswithmandatoryvalues_20 = rail.QueryCollectionOperator(
            task_id='query_list_recordswithmandatoryvalues_20',
            query="""SELECT * FROM inputfile WHERE NULLIF(employeeid,'') IS NOT NULL AND NULLIF(timeofftype,'') IS NOT NULL AND NULLIF(startdate,'') IS NOT NULL AND NULLIF(amount,'') IS NOT NULL AND NULLIF(status,'') IS NOT NULL"""
        )

        if_first_user_name_present_21 = rail.IfOperator(
            task_id='if_first_user_name_present_21',
            test='''{{ result('query_list_recordswithmandatoryvalues_20', 'length') > 0 }}''',
            yes_task="get_report_details",
            no_task="create_csv_lines_70",
        )

        get_report_details, load_report_data, fail_no_report_data, fail_column_order_mismatch = report_batch(
            config)

        def get_csv_rows(item):
            userdata = rail.load_all_records(
                rail.result('load_report_data'))

            def get_loginname():
                return [user['Login Name'] for user in userdata if user.get('Employee ID') == item['employeeid']]

            def get_useruri():
                return [user['UserUri'] for user in userdata if user.get('Employee ID') == item['employeeid']]

            row_data = [
                item['username'],
                item['employeeid'],
                item['timeofftype'],
                item['startdate'],
                item['timeoffstatus'],
                item['amount'],
                item['status'],
                item['entryid'],
                get_loginname()[0] if get_loginname() else '',
                get_useruri()[0] if get_useruri() else ''
            ]
            return row_data
        create_csv_lines_mergeinputdatawithuserdata_31 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_mergeinputdatawithuserdata_31',
            source="{{ result('query_list_recordswithmandatoryvalues_20') }}",
            header=['username',
                    'employeeid',
                    'timeofftype',
                    'startdate',
                    'timeoffstatus',
                    'amount',
                    'status',
                    'entryid',
                    'loginname',
                    'useruri'],
            row=get_csv_rows
        )

        load_csv_create_list_from_csv_32 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_32",
            document="{{result('create_csv_lines_mergeinputdatawithuserdata_31') }}",
        )

        create_collection_create_list_from_csv_32 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_32',
            source="{{ result('load_csv_create_list_from_csv_32') }}",
            name="merged_input_and_user_data",
            columns={
                'username': 'username',
                'employeeid': 'employeeid',
                'timeofftype': 'timeofftype',
                'startdate': 'startdate',
                'timeoffstatus': 'timeoffstatus',
                'amount': 'amount',
                'status': 'timeoffaction',
                'entryid': 'timeoffentryid',
                'loginname': 'loginname',
                'useruri': 'useruri'
            }
        )

        query_list_invalidrecords_usernotavailable_33 = rail.QueryCollectionOperator(
            task_id='query_list_invalidrecords_usernotavailable_33',
            query="""SELECT * FROM merged_input_and_user_data WHERE NULLIF(useruri, '') IS NULL""",
        )

        insert_to_list_34 = rail.WriteLogOperator(
            task_id='insert_to_list_34',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_list_invalidrecords_usernotavailable_33') }}",
            message="User is not available or disabled in Replicon",
            severity="Info",
            properties={
                "employeeid": "{{ item.employeeid }}",
                "timeoffstatus": "{{ item.timeoffstatus }}",
                "timeofftype": "{{ item.timeofftype }}",
                "startdate": "{{ item.startdate }}",
                "hours": "{{ item.amount }}",
                "status": "Ignored",
                "details": "User is not available or disabled in Replicon",
                "timeoffaction": "{{ item.timeoffaction }}"
            }
        )

        query_list_validrecords_35 = rail.QueryCollectionOperator(
            task_id='query_list_validrecords_35',
            query="""SELECT * FROM merged_input_and_user_data WHERE NULLIF(useruri, '') IS NOT NULL""",
        )

        if_query_list_validrecords_35_rows_blank = rail.IfOperator(
            task_id='if_query_list_validrecords_35_rows_blank',
            test='''{{ result('query_list_validrecords_35', 'length') < 0  }}''',
            yes_task="create_csv_lines_38",
            no_task="get_enabled_time_off_types_48",
        )

        create_csv_lines_38 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_38',
            source="{{ result('create_timeoff_import_logs') }}",
            header=['timeoffaction',
                    'employeeid',
                    'timeofftype',
                    'startdate',
                    'hours',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{ item.properties.timeoffaction }}",
                "{{ item.properties.employeeid }}",
                "{{ item.properties.timeofftype }}",
                "{{ item.properties.startdate }}",
                "{{ item.properties.hours }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ]
        )

        def file_upload_failed(context):
            subject = '{{ get_company_key() }} | Replicon timeoff import - Uploading Logs to SFTP failed {{ current_time() }}'
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.tenant_email,
                bcc=config.alert_email,
                subject=subject,
                html_content='''<p>Hi Team,<br/> <br/> The Replicon user sync for Companykey {{ get_company_key() }}  instance, hosted on  User name Properties , created on Job created at Properties  has been completed for file "{{ result('new_file_sensor') | file_name }}", however, the log upload to sftp has failed. Attached is the log file for reference.</p>
<ul>
<li>Recipe ID: {{ params.dag_id }} </li>
<li>Job ID: {{ dag_run_ecid() }} </li>
</ul>
<p>Please find the attached logs which was to be sent to intended recipients and debug the issue related to sftp upload.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
                params={
                    'dag_id': f'npsg_timeoff_import_npsg_time_off_import_master_v1_0_{config.instance}'
                },
                files=[
                    ("{{ result('create_csv_lines_38') }}")
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_40 = rail.SFTPUploadFileOperator(
            task_id='upload_40',
            content="{{ result('create_csv_lines_38') }}",
            remote_filepath=config.log_filepath +
            '/importlogs_{{ result("new_file_sensor") | file_name }}',
            on_failure_callback=file_upload_failed
        )

        send_mail_44 = rail.EmailOperator(
            task_id='send_mail_44',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import completed successfully - {{ current_time() }}''',
            html_content='''<p>Hello, <br /> <br /> Replicon timeoff import completed successfully. Please find the log file details below for reference: <br/> <br/><ul>
<li>File name: importlogs_{{ result("new_file_sensor") | file_name }} </li>
<li>File path: {{ params.log_file_path }} </li>
</ul>
<p>For any queries, please contact our support team at https://support.deltek.com <br /><br/>Regards, <br/>Deltek Inc.</p>''',
            params={'log_file_path': config.log_filepath}
        )

        get_enabled_time_off_types_48 = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_48',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data=null
        )

        trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50',
            retries=0,
            items="{{ result('query_list_validrecords_35') }}",
            trigger_dag_id=f'npsg_timeoff_import_npsg_process_timeoff_records_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types_48'), 'name', item['timeofftype'], 'uri'),
                "useruri": item['useruri'],
                "loginname": item['loginname'],
                "timeoffentryid": item['timeoffentryid'],
                "timeoffaction": item['timeoffaction'],
                "amount": item['amount'],
                "timeoffstatus": item['timeoffstatus'],
                "startdate": item['startdate'],
                "timeofftype": item['timeofftype'],
                "employeeid":  item['employeeid'],
                "username": item['username']
            }
        )

        wait_for_completion_trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50") }}'
        )

        gather_timeoff_import_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_import_child_logs',
            dag_runs="{{ result('trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50') }}",
            dagrun_task_id='create_timeoff_import_child_logs',
            flatten=True
        )

        gather_timeoff_import_timesheetstatus_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_import_timesheetstatus_logs',
            dag_runs="{{ result('trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50') }}",
            dagrun_task_id='create_timeoff_import_timesheetstatus_logs',
            flatten=True
        )

        npsg_timeofftimeport_timesheetstatus_search_entries = rail.PythonOperator(
            task_id='npsg_timeofftimeport_timesheetstatus_search_entries',
            python_callable=python_callable_method.get_timesheetstatus_entries
        )

        if_npsg_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0 = rail.IfOperator(
            task_id='if_npsg_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0',
            test='''{{ result('npsg_timeofftimeport_timesheetstatus_search_entries') | length > 0 }}''',
            yes_task="trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052",
            no_task="format_logs"
        )

        trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052',
            retries=0,
            items=lambda: rail.result(
                'npsg_timeofftimeport_timesheetstatus_search_entries'),
            trigger_dag_id=f'npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "timesheeturi": item['timesheeturi'],
                "status": item['status']
            }
        )

        wait_for_completion_trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052") }}'
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_method.do_format_logs
        )

        create_csv_lines_56 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_56',
            source="{{ result('format_logs') | to_json }}",
            header=['timeoffaction',
                    'employeeid',
                    'timeofftype',
                    'startdate',
                    'hours',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{ item.timeoffaction }}",
                "{{ item.employeeid }}",
                "{{ item.timeofftype }}",
                "{{ item.startdate }}",
                "{{ item.hours }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}"
            ]
        )

        upload_58 = rail.SFTPUploadFileOperator(
            task_id='upload_58',
            content="{{ result('create_csv_lines_56') }}",
            remote_filepath=config.log_filepath +
            '/importlogs_{{ result("new_file_sensor") | file_name }}',
            on_failure_callback=file_upload_failed
        )

        log_checkforerrors_62 = rail.PythonOperator(
            task_id='log_checkforerrors_62',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        if_log_checkforerrors_62_present_63 = rail.IfOperator(
            task_id='if_log_checkforerrors_62_present_63',
            test='''{{ result("log_checkforerrors_62", key="length") > 0 }}''',
            yes_task="send_mail_64",
            no_task="send_mail_66",
        )

        send_mail_64 = rail.EmailOperator(
            task_id='send_mail_64',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import completed with failed records -{{ current_time() }} ''',
            html_content='''<p>Hello, <br/> <br/> Replicon timeoff import is completed with failed records. Please find the log file details below for reference: <br/> <br/><ul>
<li>File name: importlogs_{{ result("new_file_sensor") | file_name }} </li>
<li>File path: {{ params.log_file_path }} </li>
</ul>
<p>For any queries, please contact our support team at https://support.deltek.com <br/><br/>Regards, <br/>Deltek Inc.</p> ''',
            params={'log_file_path': config.log_filepath}
        )

        send_mail_66 = rail.EmailOperator(
            task_id='send_mail_66',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import completed successfully -{{ current_time() }} ''',
            html_content='''<p>Hello, <br/> <br/> Replicon timeoff import is completed successfully. Please find the log file details below for reference: <br/> <br/><ul>
<li>File name: importlogs_{{ result("new_file_sensor") | file_name }} </li>
<li>File path: {{ params.log_file_path }} </li>
</ul>
<p>For any queries, please contact our support team at https://support.deltek.com <br/><br/>Regards, <br/>Deltek Inc.</p> ''',
            params={'log_file_path': config.log_filepath}
        )

        create_csv_lines_70 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_70',
            source="{{ result('create_timeoff_import_logs') }}",
            header=['timeoffaction',
                    'employeeid',
                    'timeofftype',
                    'startdate',
                    'hours',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{ item.properties.timeoffaction }}",
                "{{ item.properties.employeeid }}",
                "{{ item.properties.timeofftype }}",
                "{{ item.properties.startdate }}",
                "{{ item.properties.hours }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ]
        )

        upload_72 = rail.SFTPUploadFileOperator(
            task_id='upload_72',
            content="{{ result('create_csv_lines_70') }}",
            remote_filepath=config.log_filepath +
            '/importlogs_{{ result("new_file_sensor") | file_name }}',
            on_failure_callback=file_upload_failed
        )

        send_mail_77 = rail.EmailOperator(
            task_id='send_mail_77',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import completed successfully -{{ current_time() }} ''',
            html_content='''<p>Hello, <br/> <br/> Replicon timeoff import is completed successfully. Please find the log file details below for reference: <br/> <br/><ul>
<li>File name: importlogs_{{ result("new_file_sensor") | file_name }} </li>
<li>File path:  {{ params.log_file_path }} </li>
</ul>
<p>For any queries, please contact our support team at https://support.deltek.com <br/><br/>Regards, <br/>Deltek Inc.</p> ''',
            params={'log_file_path': config.log_filepath}
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> is_csv >> rail.Label(
            "No") >> send_mail_3 >> rename_archivetheinputfile_4 >> finish

        is_csv >> rail.Label("Yes") >> download_10 >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file >> load_csv_create_list_from_csv_12
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun >> finish

        load_csv_create_list_from_csv_12 >> create_collection_create_list_from_csv_12 >> if_create_list_from_csv_12_row_count_less_than_1_13
        if_create_list_from_csv_12_row_count_less_than_1_13 >> rail.Label(
            'Yes') >> send_mail_15 >> finish
        if_create_list_from_csv_12_row_count_less_than_1_13 >> rail.Label(
            'No') >> create_timeoff_import_logs >> query_list_missingmandatoryvalues_ignored_18 >> insert_to_list_19 >> query_list_recordswithmandatoryvalues_20 >> if_first_user_name_present_21
        if_first_user_name_present_21 >> rail.Label(
            'Yes') >> get_report_details

        fail_no_report_data >> finish
        fail_column_order_mismatch >> finish

        load_report_data >> create_csv_lines_mergeinputdatawithuserdata_31 >> load_csv_create_list_from_csv_32 \
            >> create_collection_create_list_from_csv_32 >> query_list_invalidrecords_usernotavailable_33 >> insert_to_list_34 \
            >> query_list_validrecords_35 >> if_query_list_validrecords_35_rows_blank
        if_query_list_validrecords_35_rows_blank >> rail.Label(
            'Yes') >> create_csv_lines_38 >> upload_40 >> send_mail_44 >> finish
        if_query_list_validrecords_35_rows_blank >> rail.Label('No') >> get_enabled_time_off_types_48 \
            >> trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50 \
            >> wait_for_completion_trigger_dag_run_npsg_timeoff_import_npsg_process_timeoff_records_v1_0async_50 \
            >> gather_timeoff_import_child_logs >> gather_timeoff_import_timesheetstatus_logs \
            >> npsg_timeofftimeport_timesheetstatus_search_entries >> if_npsg_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0
        if_npsg_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0 >> rail.Label(
            'Yes') >> trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052 \
            >> wait_for_completion_trigger_dag_run_npsg_timeoff_import_npsg_timeoffimport_reopenedtimesheets_v1_052 >> format_logs
        if_npsg_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0 >> rail.Label(
            'No') >> format_logs >> create_csv_lines_56 >> upload_58 >> log_checkforerrors_62 >> if_log_checkforerrors_62_present_63
        if_log_checkforerrors_62_present_63 >> rail.Label(
            'Yes') >> send_mail_64 >> finish
        if_log_checkforerrors_62_present_63 >> rail.Label(
            'No') >> send_mail_66 >> finish
        if_first_user_name_present_21 >> rail.Label(
            'No') >> create_csv_lines_70 >> upload_72 >> send_mail_77 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
