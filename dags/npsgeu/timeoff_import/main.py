from datetime import timedelta
from npsgeu.timeoff_import.task.generate_report_batch import report_batch
from npsgeu.timeoff_import.utils import python_callable_methods
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'NPSGEU - Time off Import Master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_11',
            no_task='send_mail_4',
        )


        send_mail_4 = rail.EmailOperator(
            task_id='send_mail_4',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import skipped -{{ current_time() }} ''',
            html_content='''<p>Hello, <br/> <br/> Replicon timeoff import skipped due to incorrect file extension. Please correct the file extension to .CSV and place a new file. <br/> <br/>For any queries, please contact our support team at https://support.deltek.com <br/><br/>Regards, <br/>Deltek Inc.</p> '''
        )

        rename_archivetheinputfile_5 = rail.SFTPMoveFileOperator(
            task_id='rename_archivetheinputfile_5',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_11 = rail.SFTPDownloadFileOperator(
            task_id='download_11',
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

        load_csv_create_list_from_csv_13 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_13",
            document="{{ result('download_11') }}",
        )

        create_collection_create_list_from_csv_13 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_13',
            source="{{ result('load_csv_create_list_from_csv_13') }}",
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

        if_create_list_from_csv_12_row_count_less_than_1_16 = rail.IfOperator(
            task_id='if_create_list_from_csv_12_row_count_less_than_1_16',
            test='''{{ result('create_collection_create_list_from_csv_13', 'length') < 1 }}''',
            yes_task="send_mail_18",
            no_task="create_timeoff_import_logs"
        )

        send_mail_18 = rail.EmailOperator(
            task_id='send_mail_18',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import skipped -{{ current_time() }} ''',
            html_content='''<p>Hello, <br /> <br /> Replicon timeoff import skipped due to no data in the file. Please check and place a new file. <br /> <br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        create_timeoff_import_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_logs'
        )

        query_list_missingmandatoryvalues_ignored_21 = rail.QueryCollectionOperator(
            task_id='query_list_missingmandatoryvalues_ignored_21',
            query="""SELECT * FROM inputfile WHERE NULLIF(employeeid,'') IS NULL OR NULLIF(timeofftype,'') IS NULL OR NULLIF(startdate,'') IS NULL OR NULLIF(amount,'') IS NULL OR NULLIF(status,'') IS NULL"""
        )

        insert_to_list_22 = rail.WriteLogOperator(
            task_id='insert_to_list_22',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_list_missingmandatoryvalues_ignored_21') }}",
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

        query_list_recordswithmandatoryvalues_23 = rail.QueryCollectionOperator(
            task_id='query_list_recordswithmandatoryvalues_23',
            query="""SELECT * FROM inputfile WHERE NULLIF(employeeid,'') IS NOT NULL AND NULLIF(timeofftype,'') IS NOT NULL AND NULLIF(startdate,'') IS NOT NULL AND NULLIF(amount,'') IS NOT NULL AND NULLIF(status,'') IS NOT NULL"""
        )


        if_first_user_name_present_24 = rail.IfOperator(
            task_id='if_first_user_name_present_24',
            test='''{{ result('query_list_recordswithmandatoryvalues_23', 'length') > 0 }}''',
            yes_task="get_report_details",
            no_task="create_csv_lines_73",
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

        create_csv_lines_mergeinputdatawithuserdata_34 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_mergeinputdatawithuserdata_34',
            source="{{ result('query_list_recordswithmandatoryvalues_23') }}",
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

        create_collection_create_list_from_csv_35 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_35',
            source="{{ result('create_csv_lines_mergeinputdatawithuserdata_34') }}",
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

        query_list_invalidrecords_usernotavailable_36 = rail.QueryCollectionOperator(
            task_id='query_list_invalidrecords_usernotavailable_36',
            query="""SELECT * FROM merged_input_and_user_data WHERE NULLIF(useruri, '') IS NULL""",
        )

        insert_to_list_37 = rail.WriteLogOperator(
            task_id='insert_to_list_37',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_list_invalidrecords_usernotavailable_36') }}",
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

        query_list_validrecords_38 = rail.QueryCollectionOperator(
            task_id='query_list_validrecords_38',
            query="""SELECT * FROM merged_input_and_user_data WHERE NULLIF(useruri, '') IS NOT NULL""",
        )

        if_query_list_validrecords_38_rows_blank = rail.IfOperator(
            task_id='if_query_list_validrecords_38_rows_blank',
            test='''{{ result('query_list_validrecords_38', 'length') < 0  }}''',
            yes_task="create_csv_lines_41",
            no_task="get_enabled_time_off_types_51",
        )

        create_csv_lines_41 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_41',
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

        upload_43 = rail.SFTPUploadFileOperator(
            task_id='upload_43',
            content="{{ result('create_csv_lines_41') }}",
            remote_filepath=config.log_filepath +
            '/importlogs_{{ result("new_file_sensor") | file_name }}'
        )

        send_mail_47 = rail.EmailOperator(
            task_id='send_mail_47',
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

        get_enabled_time_off_types_51 = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_51',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data=null
        )

        trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53',
            retries=0,
            items="{{ result('query_list_validrecords_38') }}",
            trigger_dag_id=config.process_timeoff_records_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                **item,
                **{
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types_51'), 'name', item['timeofftype'], 'uri'),
                }
            }
        )

        wait_for_completion_trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53") }}'
        )

        gather_timeoff_import_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_import_child_logs',
            dag_runs="{{ result('trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53') }}",
            dagrun_task_id='create_timeoff_import_child_logs',
            flatten=True
        )

        gather_timeoff_import_timesheetstatus_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_import_timesheetstatus_logs',
            dag_runs="{{ result('trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53') }}",
            dagrun_task_id='create_timeoff_import_timesheetstatus_logs',
            flatten=True
        )

        npsgeu_timeofftimeport_timesheetstatus_search_entries = rail.PythonOperator(
            task_id='npsgeu_timeofftimeport_timesheetstatus_search_entries',
            python_callable=python_callable_methods.get_timesheetstatus_entries
        )

        if_npsgeu_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0 = rail.IfOperator(
            task_id='if_npsgeu_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0',
            test='''{{ result('npsgeu_timeofftimeport_timesheetstatus_search_entries') | length > 0 }}''',
            yes_task="trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055",
            no_task="format_logs"
        )

        trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055',
            retries=0,
            items=lambda: rail.result(
                'npsgeu_timeofftimeport_timesheetstatus_search_entries'),
            trigger_dag_id=config.reopenedtimesheets_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "timesheeturi": item['timesheeturi'],
                "status": item['status']
            }
        )

        wait_for_completion_trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055") }}'
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_methods.do_format_logs
        )

        create_csv_lines_59 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_59',
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

        upload_61 = rail.SFTPUploadFileOperator(
            task_id='upload_61',
            content="{{ result('create_csv_lines_59') }}",
            remote_filepath=config.log_filepath +
            '/importlogs_{{ result("new_file_sensor") | file_name }}'
        )

        log_checkforerrors_65 = rail.PythonOperator(
            task_id='log_checkforerrors_65',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        if_log_checkforerrors_65_present_66 = rail.IfOperator(
            task_id='if_log_checkforerrors_65_present_66',
            test='''{{ result("log_checkforerrors_65", key="length") > 0 }}''',
            yes_task="send_mail_67",
            no_task="send_mail_69",
        )

        send_mail_67 = rail.EmailOperator(
            task_id='send_mail_67',
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

        send_mail_69 = rail.EmailOperator(
            task_id='send_mail_69',
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

        create_csv_lines_73 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_73',
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

        upload_75 = rail.SFTPUploadFileOperator(
            task_id='upload_75',
            content="{{ result('create_csv_lines_73') }}",
            remote_filepath=config.log_filepath +
            '/importlogs_{{ result("new_file_sensor") | file_name }}'
        )

        send_mail_80 = rail.EmailOperator(
            task_id='send_mail_80',
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

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_csv >> rail.Label(
            "No") >> send_mail_4 >> rename_archivetheinputfile_5 >> finish

        is_csv >> rail.Label("Yes") >> download_11 >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file >> load_csv_create_list_from_csv_13
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun >> finish

        load_csv_create_list_from_csv_13 >> create_collection_create_list_from_csv_13 >> if_create_list_from_csv_12_row_count_less_than_1_16
        if_create_list_from_csv_12_row_count_less_than_1_16 >> rail.Label(
            'Yes') >> send_mail_18 >> finish
        if_create_list_from_csv_12_row_count_less_than_1_16 >> rail.Label(
            'No') >> create_timeoff_import_logs >> query_list_missingmandatoryvalues_ignored_21 >> insert_to_list_22 >> query_list_recordswithmandatoryvalues_23 >> if_first_user_name_present_24
        if_first_user_name_present_24 >> rail.Label(
            'Yes') >> get_report_details

        fail_no_report_data >> finish
        fail_column_order_mismatch >> finish

        load_report_data >> create_csv_lines_mergeinputdatawithuserdata_34 \
            >> create_collection_create_list_from_csv_35 >> query_list_invalidrecords_usernotavailable_36 >> insert_to_list_37 \
            >> query_list_validrecords_38 >> if_query_list_validrecords_38_rows_blank
        if_query_list_validrecords_38_rows_blank >> rail.Label(
            'Yes') >> create_csv_lines_41 >> upload_43 >> send_mail_47 >> finish
        if_query_list_validrecords_38_rows_blank >> rail.Label('No') >> get_enabled_time_off_types_51 \
            >> trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53 \
            >> wait_for_completion_trigger_npsgeu_timeoff_import_process_timeoff_records_child_async_53 \
            >> gather_timeoff_import_child_logs >> gather_timeoff_import_timesheetstatus_logs \
            >> npsgeu_timeofftimeport_timesheetstatus_search_entries >> if_npsgeu_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0
        if_npsgeu_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0 >> rail.Label(
            'Yes') >> trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055 \
            >> wait_for_completion_trigger_dag_npsgeu_timeoff_import_reopenedtimesheets_child_055 >> format_logs
        if_npsgeu_timeofftimeport_timesheetstatus_search_entries_entries_greater_than_0 >> rail.Label(
            'No') >> format_logs >> create_csv_lines_59 >> upload_61 >> log_checkforerrors_65 >> if_log_checkforerrors_65_present_66
        if_log_checkforerrors_65_present_66 >> rail.Label(
            'Yes') >> send_mail_67 >> finish
        if_log_checkforerrors_65_present_66 >> rail.Label(
            'No') >> send_mail_69 >> finish
        if_first_user_name_present_24 >> rail.Label(
            'No') >> create_csv_lines_73 >> upload_75 >> send_mail_80 >> finish >> log_to_sumo

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
