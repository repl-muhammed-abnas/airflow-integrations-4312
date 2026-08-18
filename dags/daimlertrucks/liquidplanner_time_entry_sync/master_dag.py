
from datetime import timedelta, datetime
import uuid
import rail
from daimlertrucks.liquidplanner_time_entry_sync.utils import python_callable_method

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_time_sync_data_master_{config.instance}',
        description=f'Live|DTNA Time data (Master) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        max_active_runs=1,
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
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Time Transfer - Error in feed  {{ current_time_in_specified_tz() }}',
            html_content='templates/email/bad_file_format.html',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='log_todayin_yyyy_mm_dd_format_96',
            no_task='delete_this_dagrun',
        )

        log_todayin_yyyy_mm_dd_format_96 = rail.PythonOperator(
            task_id='log_todayin_yyyy_mm_dd_format_96',
            python_callable=lambda: datetime.today().strftime("%Y%m%d")
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            # pylint: disable=line-too-long
            new_filename=config.archive_filepath +
            '/Processed_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        create_time_entry_import_log = rail.CreateLogOperator(
            task_id='create_time_entry_import_log'
        )

        load_time_input_data = rail.LoadCSVFileOperator(
            task_id='load_time_input_data',
            document="{{ result('download_file') }}",
        )

        create_time_input_collection = rail.CreateCollectionOperator(
            task_id='create_time_input_collection',
            source="{{ result('load_time_input_data') }}",
            name="time_input_list",
            columns={
                'ENTRYDATE': 'entrydate',
                'ID': 'userid',
                'HOURS': 'hoursworked',
                'TASKNAME': 'taskname',
                'FAVORITE': 'favorite'
            }
        )

        query_empty_records = rail.QueryCollectionOperator(
            task_id="query_empty_records",
            name='empty_records',
            query="""SELECT * FROM time_input_list WHERE NULLIF(entrydate, '') IS NULL and
                    NULLIF(userid, '') IS NULL and NULLIF(hoursworked, '') IS NULL and NULLIF(taskname, '') IS NULL"""
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_time_input_collection', 'length') != result('query_empty_records', 'length') }}",
            yes_task='process_records',
            no_task='send_blank_payload_email'
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        query_non_empty_records = rail.QueryCollectionOperator(
            task_id="query_non_empty_records",
            name='non_empty_records',
            query="""SELECT * FROM time_input_list WHERE NULLIF(entrydate, '') IS NOT NULL and
                    NULLIF(userid, '') IS NOT NULL and NULLIF(hoursworked, '') IS NOT NULL and NULLIF(taskname, '') IS NOT NULL"""
        )

        query_blank_entry_date_records = rail.QueryCollectionOperator(
            task_id="query_blank_entry_date_records",
            name='blank_date_records',
            query="""SELECT * FROM time_input_list WHERE NULLIF(entrydate, '') IS NULL and
                    NULLIF(userid, '') IS NOT NULL and NULLIF(hoursworked, '') IS NOT NULL and NULLIF(taskname, '') IS NOT NULL"""
        )

        query_blank_userid_records = rail.QueryCollectionOperator(
            task_id="query_blank_userid_records",
            name='blank_userid_records',
            query="""SELECT * FROM time_input_list WHERE NULLIF(entrydate, '') IS NOT NULL and
                    NULLIF(userid, '') IS NULL and NULLIF(hoursworked, '') IS NOT NULL and NULLIF(taskname, '') IS NOT NULL"""
        )

        query_blank_hoursworked_records = rail.QueryCollectionOperator(
            task_id="query_blank_hoursworked_records",
            name='blank_hoursworked_records',
            query="""SELECT * FROM time_input_list WHERE NULLIF(entrydate, '') IS NOT NULL and
                    NULLIF(userid, '') IS NOT NULL and NULLIF(hoursworked, '') IS NULL and NULLIF(taskname, '') IS NOT NULL"""
        )

        query_blank_taskname_records = rail.QueryCollectionOperator(
            task_id="query_blank_taskname_records",
            name='blank_taskname_records',
            query="""SELECT * FROM time_input_list WHERE NULLIF(entrydate, '') IS NOT NULL and
                    NULLIF(userid, '') IS NOT NULL and NULLIF(hoursworked, '') IS NOT NULL and NULLIF(taskname, '') IS NULL"""
        )

        log_date_field_blank = rail.WriteLogOperator(
            task_id='log_date_field_blank',
            log="{{ result('create_time_entry_import_log') }}",
            items="{{ result('query_blank_entry_date_records') }}",
            message="Entry date field is blank",
            severity="Error",
            properties={
                "user_name": "{{ item.userid }}",
                "status": "Error",
                "reason": "Entry date field is blank",
                "entrydate": "{{ item.entrydate }}",
                "taskcode": "{{ item.taskname }}",
                "hoursworked": "{{ item.hoursworked }}"
            }
        )

        log_user_id_field_blank = rail.WriteLogOperator(
            task_id='log_user_id_field_blank',
            log="{{ result('create_time_entry_import_log') }}",
            items="{{ result('query_blank_userid_records') }}",
            message="User ID field is blank",
            severity="Error",
            properties={
                "user_name": "{{ item.userid }}",
                "status": "Error",
                "reason": "User ID field is blank",
                "entrydate": "{{ item.entrydate }}",
                "taskcode": "{{ item.taskname }}",
                "hoursworked": "{{ item.hoursworked }}"
            }
        )

        log_hoursworked_field_blank = rail.WriteLogOperator(
            task_id='log_hoursworked_field_blank',
            log="{{ result('create_time_entry_import_log') }}",
            items="{{ result('query_blank_hoursworked_records') }}",
            message="Time Entry not synced since hours worked field has blank value",
            severity="Error",
            properties={
                "user_name": "{{ item.userid }}",
                "status": "Error",
                "reason": "Time Entry not synced since hours worked field has blank value",
                "entrydate": "{{ item.entrydate }}",
                "taskcode": "{{ item.taskname }}",
                "hoursworked": "{{ item.hoursworked }}"
            }
        )

        log_taskname_field_blank = rail.WriteLogOperator(
            task_id='log_taskname_field_blank',
            log="{{ result('create_time_entry_import_log') }}",
            items="{{ result('query_blank_taskname_records') }}",
            message="Taskname received is blank value",
            severity="Error",
            properties={
                "user_name": "{{ item.userid }}",
                "status": "Error",
                "reason": "Taskname received is blank value",
                "entrydate": "{{ item.entrydate }}",
                "taskcode": "{{ item.taskname }}",
                "hoursworked": "{{ item.hoursworked }}"
            }
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Time Transfer - No records in the file {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_file.html"
        )

        query_list_21 = rail.QueryCollectionOperator(
            task_id='query_list_21',
            query="""SELECT DISTINCT userid FROM non_empty_records""",
        )

        trigger_dag_run_live_dtna_time_import_for_each_user_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_dtna_time_import_for_each_user_child',
            retries=0,
            items="{{ result('query_list_21') }}",
            trigger_dag_id=f'daimlertrucks_timeimport_for_each_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "userid": item['userid']
            }
        )

        wait_for_completion_trigger_dag_run_live_dtna_time_import_for_each_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_dtna_time_import_for_each_user_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_for_each_user_child") }}'
        )

        gather_formatted_submit_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_formatted_submit_logs',
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_for_each_user_child") }}',
            dagrun_task_id='format_timesheets_to_submit_logs',
            flatten=True
        )

        if_entry_col3_present_72 = rail.IfOperator(
            task_id='if_entry_col3_present_72',
            test=lambda: bool(
                len(rail.result('gather_formatted_submit_logs')) > 0),
            yes_task="create_csv_lines_73",
            no_task="dtna_timesheets_to_submit_truncate_87",
        )

        create_csv_lines_73 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_73',
            source="{{ result('gather_formatted_submit_logs') | to_json }}",
            header=['timesheeturi', 'status'],
            row=[
                "{{ item.timesheeturi }}",
                "{{ item.status }}"
            ]
        )

        load_csv_create_list_from_csv_74 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_74",
            document="{{ result('create_csv_lines_73') }}",
        )

        create_collection_create_list_from_csv_74 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_74',
            source="{{ result('load_csv_create_list_from_csv_74') }}",
            name="timesheets_to_submit",
            columns={
                'timesheeturi': 'timesheeturi',
                'status': 'status'
            }
        )

        query_list_75 = rail.QueryCollectionOperator(
            task_id='query_list_75',
            query="""SELECT DISTINCT timesheeturi FROM timesheets_to_submit""",
        )

        foreach_query_list_75_76 = rail.ForEachOperator(
            task_id='foreach_query_list_75_76',
            items="{{ result('query_list_75') }}",
            start_task='if_foreach_32443adc_76_timesheeturi_present_78',
            end_task='foreach_query_list_75_76_end'
        )

        if_foreach_32443adc_76_timesheeturi_present_78 = rail.IfOperator(
            task_id='if_foreach_32443adc_76_timesheeturi_present_78',
            test='''{{ result('foreach_query_list_75_76').timesheeturi | sn | is_truthy }}''',
            yes_task="log_timesheet_uri_status_79",
            no_task="foreach_query_list_75_76_end",
        )

        log_timesheet_uri_status_79 = rail.PythonOperator(
            task_id='log_timesheet_uri_status_79',
            python_callable=python_callable_method.get_timesheeturi_status
        )

        if_log_timesheet_uri_status_79_not_contains_notsubmitted_80 = rail.IfOperator(
            task_id='if_log_timesheet_uri_status_79_not_contains_notsubmitted_80',
            test=lambda: rail.result(
                'log_timesheet_uri_status_79') != 'Not Submitted',
            yes_task="getmostrecentvalidations_81",
            no_task="enqueue_recalculate_script_data_84",
        )

        getmostrecentvalidations_81 = rail.RepliconServiceOperator(
            task_id='getmostrecentvalidations_81',
            endpoint="/services/TimesheetService1.svc/GetMostRecentValidationResult",
            data={
                "timesheetUri": "{{ result('foreach_query_list_75_76').timesheeturi }}"
            }
        )

        if_first_uri_blank_82 = rail.IfOperator(
            task_id='if_first_uri_blank_82',
            test='''{{ result('getmostrecentvalidations_81').validationMessages[0].uri | is_falsy }}''',
            yes_task="resubmittingtimesheet_83",
            no_task="enqueue_recalculate_script_data_84",
        )

        resubmittingtimesheet_83 = rail.RepliconServiceOperator(
            task_id='resubmittingtimesheet_83',
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda: {
                "timesheetUri": rail.result('foreach_query_list_75_76')['timesheeturi'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Resubmitting the timesheet after time entry ",
                "changeReason": null
            }
        )

        enqueue_recalculate_script_data_84 = rail.RepliconServiceOperator(
            task_id='enqueue_recalculate_script_data_84',
            endpoint="/services/TimesheetService1.svc/EnqueueRecalculateScriptData",
            data={
                "timesheet": {
                    "uri": "{{ result('foreach_query_list_75_76').timesheeturi }}",
                    "user": null,
                    "date": null
                }
            }
        )

        foreach_query_list_75_76_end = rail.EmptyOperator(
            task_id='foreach_query_list_75_76_end',
        )

        dtna_timesheets_to_submit_truncate_87 = rail.EmptyOperator(
            task_id='dtna_timesheets_to_submit_truncate_87',
        )

        gather_time_entry_import_logs_from_users = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_entry_import_logs_from_users',
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_for_each_user_child") }}',
            dagrun_task_id='gather_time_entry_import_logs_from_put_entries',
            flatten=True
        )

        gather_user_not_found_logs_from_users = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_not_found_logs_from_users',
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_for_each_user_child") }}',
            dagrun_task_id='dtna_time_entry_import_logs_add_entry_36',
            flatten=True
        )

        gather_user_disabled_logs_from_users = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_disabled_logs_from_users',
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_for_each_user_child") }}',
            dagrun_task_id='dtna_time_entry_import_logs_add_entry_40',
            flatten=True
        )

        format_time_entry_import_logs = rail.PythonOperator(
            task_id='format_time_entry_import_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_method.do_format_time_entry_import_logs
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                list(filter(lambda x: x['status'] == 'Error', rail.result('format_time_entry_import_logs'))))
        )

        get_success_logs = rail.PythonOperator(
            task_id='get_success_logs',
            python_callable=lambda: rail.set_result(
                list(filter(lambda x: x['status'] == 'Success', rail.result('format_time_entry_import_logs'))))
        )

        if_accumulate_list_items_95_list_items_greater_than_0_97 = rail.IfOperator(
            task_id='if_accumulate_list_items_95_list_items_greater_than_0_97',
            test='''{{ result('get_errored_logs') | length > 0 and result('get_success_logs') | length > 0 }}''',
            yes_task="create_csv_lines_success_file_98",
            no_task="only_error_logs",
        )

        create_csv_lines_success_file_98 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_success_file_98',
            source="{{ result('get_success_logs')  | to_json }}",
            header=['jobid',
                    'user',
                    'status',
                    'reason',
                    'entrydate',
                    'taskcode',
                    'hoursworked'],
            row=[
                "{{ item.jobid }}",
                "{{ item.user_name }}",
                "{{ item.status }}",
                "{{ item.reason }}",
                "{{ item.entrydate }}",
                "{{ item.taskcode }}",
                "{{ item.hoursworked }}"
            ]
        )

        create_csv_lines_errored_file_99 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_errored_file_99',
            source="{{ result('get_errored_logs')  | to_json }}",
            header=['jobid',
                    'user',
                    'status',
                    'reason',
                    'entrydate',
                    'taskcode',
                    'hoursworked'],
            row=[
                "{{ item.jobid }}",
                "{{ item.user_name }}",
                "{{ item.status }}",
                "{{ item.reason }}",
                "{{ item.entrydate }}",
                "{{ item.taskcode }}",
                "{{ item.hoursworked }}"
            ]
        )

        upload_upload_success_logs_for_reference_100 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_success_logs_for_reference_100',
            content='''{{ result('create_csv_lines_success_file_98') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.archive_logs_filepath +
            '/Success_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        upload_upload_error_logs_for_reference_101 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_error_logs_for_reference_101',
            content='''{{ result('create_csv_lines_errored_file_99') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.archive_logs_filepath +
            '/Error_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        upload_upload_success_logs_102 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_success_logs_102',
            content='''{{ result('create_csv_lines_success_file_98') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.successfull_records_filepath +
            '/Success_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        upload_upload_error_logs_103 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_error_logs_103',
            content='''{{ result('create_csv_lines_errored_file_99') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.rejected_records_filepath +
            '/Error_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        send_mail_completedwitherrorsemail_104 = rail.EmailOperator(
            task_id='send_mail_completedwitherrorsemail_104',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Time Transfer - Completed with Errors {{ current_time_in_specified_tz() }}''',
            # pylint: disable=line-too-long
            html_content='''<p><strong>This is an automated mail, please don't reply&nbsp;</strong><br /> <br /> Hello,<br /> <br /> The time transfer job is completed with errors based on the file - {{ result("new_file_sensor") | file_name }}. Please find below the log file names for reference.</p>
<ul>
<li>Success_Replicon_LiquidPlanner_Import_{{ result('log_todayin_yyyy_mm_dd_format_96') }}.csv</li>
<li>Error_Replicon_LiquidPlanner_Import_{{ result('log_todayin_yyyy_mm_dd_format_96') }}.csv</li>
</ul><br /> For any queries, please contact our support team at https://support.deltek.com <br /> <br /> Regards, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        only_error_logs = rail.EmptyOperator(
            task_id='only_error_logs'
        )

        if_accumulate_list_items_95_list_items_greater_than_0_105 = rail.IfOperator(
            task_id='if_accumulate_list_items_95_list_items_greater_than_0_105',
            test='''{{ result('get_errored_logs') | length > 0 and result('get_success_logs') | length < 1 }}''',
            yes_task="create_csv_lines_errored_file_106",
            no_task="only_success_logs",
        )

        create_csv_lines_errored_file_106 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_errored_file_106',
            source="{{ result('get_errored_logs')  | to_json }}",
            header=['jobid',
                    'user',
                    'status',
                    'reason',
                    'entrydate',
                    'taskcode',
                    'hoursworked'],
            row=[
                "{{ item.jobid }}",
                "{{ item.user_name }}",
                "{{ item.status }}",
                "{{ item.reason }}",
                "{{ item.entrydate }}",
                "{{ item.taskcode }}",
                "{{ item.hoursworked }}"
            ]
        )

        upload_upload_error_logs_for_reference_107 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_error_logs_for_reference_107',
            content='''{{ result('create_csv_lines_errored_file_106') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.archive_logs_filepath +
            '/Error_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        upload_upload_error_logs_108 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_error_logs_108',
            content='''{{ result('create_csv_lines_errored_file_106') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.rejected_records_filepath +
            '/Error_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        send_mail_completedwitherrorsemail_109 = rail.EmailOperator(
            task_id='send_mail_completedwitherrorsemail_109',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Time Transfer - Completed with Errors {{ current_time_in_specified_tz() }}''',
            # pylint: disable=line-too-long
            html_content='''<p><strong>This is an automated mail, please don't reply&nbsp;</strong><br /> <br /> Hello,<br /> <br /> The time transfer job is completed with errors based on the file - {{ result("new_file_sensor") | file_name }}. Please find below the log file name for reference.</p>
<ul>
<li>Error_Replicon_LiquidPlanner_Import_{{ result('log_todayin_yyyy_mm_dd_format_96') }}.csv</li>
</ul> <br /> For any queries, please contact our support team at https://support.deltek.com <br /> <br /> Regards, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        only_success_logs = rail.EmptyOperator(
            task_id='only_success_logs'
        )

        if_accumulate_list_items_93_list_items_greater_than_0_110 = rail.IfOperator(
            task_id='if_accumulate_list_items_93_list_items_greater_than_0_110',
            test='''{{ result('get_success_logs') | length > 0 and result('get_errored_logs') | length < 1 }}''',
            yes_task="create_csv_lines_success_file_111",
            no_task="finish",
        )

        create_csv_lines_success_file_111 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_success_file_111',
            source="{{ result('get_success_logs') | to_json }}",
            header=['jobid',
                    'user',
                    'status',
                    'reason',
                    'entrydate',
                    'taskcode',
                    'hoursworked'],
            row=[
                "{{ item.jobid }}",
                "{{ item.user_name }}",
                "{{ item.status }}",
                "{{ item.reason }}",
                "{{ item.entrydate }}",
                "{{ item.taskcode }}",
                "{{ item.hoursworked }}"
            ]
        )

        upload_upload_success_logs_for_reference_112 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_success_logs_for_reference_112',
            content='''{{ result('create_csv_lines_success_file_111') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.archive_logs_filepath +
            '/Success_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        upload_upload_success_logs_113 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_success_logs_113',
            content='''{{ result('create_csv_lines_success_file_111') }}''',
            # pylint: disable=line-too-long
            remote_filepath=config.successfull_records_filepath +
            '/Success_Replicon_LiquidPlanner_Import_{{ result("log_todayin_yyyy_mm_dd_format_96") }}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        send_mail_completed_successfully_114 = rail.EmailOperator(
            task_id='send_mail_completed_successfully_114',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Time Transfer - Completed Successfully {{ current_time_in_specified_tz() }}''',
            # pylint: disable=line-too-long
            html_content='''<p><strong>This is an automated mail, please don't reply&nbsp;</strong><br /> <br /> Hello,<br /> <br /> The time transfer job is completed successfully based on the file - {{ result("new_file_sensor") | file_name }}. Please find below the log file name for reference.</p>
<ul>
<li>Success_Replicon_LiquidPlanner_Import_{{ result('log_todayin_yyyy_mm_dd_format_96') }}.csv</li>
</ul>
<p>For any queries, please contact our support team at https://support.deltek.com <br /> <br /> Regards, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        dtna_time_entry_import_logs_truncate_116 = rail.EmptyOperator(
            task_id='dtna_time_entry_import_logs_truncate_116',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        new_file_sensor >> is_csv >> rail.Label(
            "No") >> send_bad_file_format_email >> finish
        is_csv >> rail.Label("Yes") >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> log_todayin_yyyy_mm_dd_format_96 >> archive_file \
            >> create_time_entry_import_log >> load_time_input_data
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun >> finish

        load_time_input_data >> create_time_input_collection >> query_empty_records \
            >> has_any_records >> rail.Label("No") >> send_blank_payload_email >> finish

        has_any_records >> rail.Label(
            "Yes") >> process_records >> query_non_empty_records >> query_blank_entry_date_records \
            >> query_blank_userid_records >> query_blank_hoursworked_records >> query_blank_taskname_records \
            >> log_date_field_blank >> log_user_id_field_blank >> log_hoursworked_field_blank >> log_taskname_field_blank \
            >> query_list_21 >> trigger_dag_run_live_dtna_time_import_for_each_user_child \
            >> wait_for_completion_trigger_dag_run_live_dtna_time_import_for_each_user_child \
            >> gather_formatted_submit_logs >> if_entry_col3_present_72
        if_entry_col3_present_72 >> rail.Label(
            'Yes') >> create_csv_lines_73 >> load_csv_create_list_from_csv_74 >> create_collection_create_list_from_csv_74 \
            >> query_list_75 >> foreach_query_list_75_76 >> if_foreach_32443adc_76_timesheeturi_present_78
        if_foreach_32443adc_76_timesheeturi_present_78 >> rail.Label(
            'Yes') >> log_timesheet_uri_status_79 >> if_log_timesheet_uri_status_79_not_contains_notsubmitted_80
        if_log_timesheet_uri_status_79_not_contains_notsubmitted_80 >> rail.Label(
            'Yes') >> getmostrecentvalidations_81 >> if_first_uri_blank_82
        if_first_uri_blank_82 >> rail.Label(
            'Yes') >> resubmittingtimesheet_83 >> enqueue_recalculate_script_data_84
        if_first_uri_blank_82 >> rail.Label(
            'No') >> enqueue_recalculate_script_data_84

        if_log_timesheet_uri_status_79_not_contains_notsubmitted_80 >> rail.Label(
            'No') >> enqueue_recalculate_script_data_84 >> foreach_query_list_75_76_end
        if_foreach_32443adc_76_timesheeturi_present_78 >> rail.Label(
            'No') >> foreach_query_list_75_76_end
        foreach_query_list_75_76 >> foreach_query_list_75_76_end >> dtna_timesheets_to_submit_truncate_87
        if_entry_col3_present_72 >> rail.Label(
            'No') >> dtna_timesheets_to_submit_truncate_87 >> gather_time_entry_import_logs_from_users \
            >> gather_user_not_found_logs_from_users >> gather_user_disabled_logs_from_users >> format_time_entry_import_logs \
            >> get_errored_logs >> get_success_logs >> if_accumulate_list_items_95_list_items_greater_than_0_97
        if_accumulate_list_items_95_list_items_greater_than_0_97 >> rail.Label(
            'Yes') >> create_csv_lines_success_file_98 >> create_csv_lines_errored_file_99 >> upload_upload_success_logs_for_reference_100 \
            >> upload_upload_error_logs_for_reference_101 >> upload_upload_success_logs_102 >> upload_upload_error_logs_103 \
            >> send_mail_completedwitherrorsemail_104 >> dtna_time_entry_import_logs_truncate_116
        if_accumulate_list_items_95_list_items_greater_than_0_97 >> rail.Label(
            'No') >> only_error_logs >> if_accumulate_list_items_95_list_items_greater_than_0_105
        if_accumulate_list_items_95_list_items_greater_than_0_105 >> rail.Label(
            'Yes') >> create_csv_lines_errored_file_106 >> upload_upload_error_logs_for_reference_107 >> upload_upload_error_logs_108 \
            >> send_mail_completedwitherrorsemail_109 >> dtna_time_entry_import_logs_truncate_116
        if_accumulate_list_items_95_list_items_greater_than_0_105 >> rail.Label(
            'No') >> only_success_logs >> if_accumulate_list_items_93_list_items_greater_than_0_110
        if_accumulate_list_items_93_list_items_greater_than_0_110 >> rail.Label(
            'Yes') >> create_csv_lines_success_file_111 >> upload_upload_success_logs_for_reference_112 >> upload_upload_success_logs_113 \
            >> send_mail_completed_successfully_114 >> dtna_time_entry_import_logs_truncate_116 >> finish
        if_accumulate_list_items_93_list_items_greater_than_0_110 >> rail.Label(
            'No') >> finish

        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
