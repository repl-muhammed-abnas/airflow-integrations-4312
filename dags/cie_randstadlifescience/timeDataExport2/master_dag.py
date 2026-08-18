# pylint: disable=line-too-long, too-many-statements trailing-whitespace
from datetime import timedelta
import pendulum
import rail
from cie_randstadlifescience.timeDataExport2.utils import upload_to_s3, download_from_s3, python_callable

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dags/cie_randstadlifescience/timeDataExportv2/config.py


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_timeDataExport_master_v2_{config.instance}',
        description=f'Time Data Export - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        currentDateTime = python_callable.get_eastern_timenow(config)
        currentDateTimeStr = currentDateTime.strftime('%b %d, %Y')

        trigger_export = rail.IfOperator(
            task_id='trigger_export',
            test=lambda: python_callable.check_trigger_time(config),
            yes_task='get_last_approval_datetime',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_last_approval_datetime = rail.PythonOperator(
            task_id="get_last_approval_datetime",
            python_callable=python_callable.get_last_approval_datetime,
            op_args=[config]
        )

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: python_callable.findItemByDisplayText(
                response, config.timedata_report_name, config.audit_report_name, config.entrydata_report_name)
        )

        get_timedata_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timedata_report_details',
            report_name=config.timedata_report_name,
        )
        generate_base_time_data_report_in_batch = rail.run_report2(
            group_id='generate_base_time_data_report_in_batch',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_timedata_report_details').get('uri'),
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalDateFilter", 'uri'),
                                "value": None,
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalDateFilter", 'uri'),
                                "value": rail.result('get_last_approval_datetime'),
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalDateFilter", 'uri'),
                                "value": currentDateTimeStr,
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        # Getting current and future excluded TS from file
        get_excluded_future_timesheets = download_from_s3.DownloadCsvOperator(
            task_id='get_excluded_future_timesheets',
            file_path=config.file_path,
            file_name=config.excludedTS_file_name,
            bucket_name=config.bucket_name,
            expires_in_seconds=7*24*60*60,
        )

        future_ts_file_has_data = rail.IfOperator(
            task_id='future_ts_file_has_data',
            test='''{{ result('get_excluded_future_timesheets') | length > 0 }}''',
            yes_task='get_approved_min_max_date',
            no_task='report_and_file_has_data',
        )

        get_approved_min_max_date = rail.PythonOperator(
            task_id='get_approved_min_max_date',
            python_callable=python_callable.getApprovedMinMaxDate,
            op_args=[config]
        )

        if_min_max_available = rail.IfOperator(
            task_id='if_min_max_available',
            test='''{{ result('get_approved_min_max_date') | is_truthy }}''',
            yes_task='empty_task',
            no_task='report_and_file_has_data',
        )
        empty_task = rail.EmptyOperator(
            task_id="empty_task"
        )

        generate_base_time_data_report_in_batch_exclude = rail.run_report2(
            group_id='generate_base_time_data_report_in_batch_exclude',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_timedata_report_details').get('uri'),
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalDateFilter", 'uri'),
                                "value": None,
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalDateFilter", 'uri'),
                                "value": rail.result('get_approved_min_max_date')["min_date"],
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_timedata_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalDateFilter", 'uri'),
                                "value": rail.result('get_approved_min_max_date')["max_date"],
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        report_and_file_has_data = rail.IfOperator(
            task_id='report_and_file_has_data',
            test='''{{ result('generate_base_time_data_report_in_batch_exclude.get_report_result', 'has_data') or result('generate_base_time_data_report_in_batch.get_report_result', 'has_data')}}''',
            yes_task='get_processed_TimesheetUris',
            no_task='finish',
        )

        # get processed TimesheetUris
        get_processed_TimesheetUris = download_from_s3.DownloadCsvOperator(
            task_id='get_processed_TimesheetUris',
            file_path=config.file_path,
            file_name=config.file_name,
            bucket_name=config.bucket_name,
            expires_in_seconds=7*24*60*60,
        )

        load_timedata_csv = rail.PythonOperator(
            task_id="load_timedata_csv",
            python_callable=python_callable.get_report_data_to_csv,
            op_args=[config]
        )

        create_timedata_collection = rail.CreateCollectionOperator(
            task_id='create_timedata_collection',
            source="{{ result('load_timedata_csv') }}",
            name="timedata",
            # todo update this map from actual csv header for key name
            columns={
                "Entry Date": "Entry Date",
                "Hours Worked": "Hours Worked",
                "Project Name": "Project Name",
                "Project Code": "Project Code",
                "Client Code": "Client Code",
                "Client Name": "Client Name",
                "Task Name": "Task Name",
                "Task Code": "Task Code",
                "TaskUri": "TaskUri",
                "TimesheetUri": "TimesheetUri",
                "UserUri": "UserUri",
                "TimesheetPeriodUri": "TimesheetPeriodUri",
                "ClientUri": "ClientUri",
                "ProjectUri": "ProjectUri",
                "Timesheet End Date": "Timesheet End Date",
                "Submitted On": "Submitted On",
                "Approval Status Code": "Approval Status Code",
                "Approval Status": "Approval Status",
                "Approval Date/Time": "Approval Date/Time",
                "User First Name": "User First Name",
                "User Last Name": "User Last Name",
                "Employee ID": "Employee ID",
                "Pay Rate": "Pay Rate",
                "Vendor ID": "Vendor ID",
                "Task Description": "Task Description",
                "Non DBC TRC Code": "Non DBC TRC Code",
                "DBC TRC Code": "DBC TRC Code",
            }
        )

        query_unique_dates = rail.QueryCollectionOperator(
            task_id='query_unique_dates',
            query="""SELECT DISTINCT Entry_Date  FROM timedata""",
        )
        query_unique_users = rail.QueryCollectionOperator(
            task_id='query_unique_users',
            query="""SELECT DISTINCT UserUri  FROM timedata""",
        )

        if_users_available = rail.IfOperator(
            task_id='if_users_available',
            test="{{ result('query_unique_users','length') > 0 }}",
            yes_task='get_max_users_per_chunk',
            no_task='finish',
        )

        get_max_users_per_chunk = rail.PythonOperator(
            task_id='get_max_users_per_chunk',
            python_callable=python_callable.get_max_users_per_chunk,
            op_args=[config]
        )
        get_user_chunck_list = rail.PythonOperator(
            task_id='get_user_chunck_list',
            python_callable=python_callable.get_user_chunck_data_artifact_list,
        )
        process_user_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_user_batch',
            items=lambda: rail.result("get_user_chunck_list"),
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_process_user_chunk_child_v2{dag_id_postfix}'.lower(
            ),
            conf=lambda item: {
                'user_list': item['user_list'],
                'data_artifact': item['data_artifact'],
                'datetime_str': currentDateTimeStr,
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_for_process_user_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_user_batch',
            dag_runs='{{ result("process_user_batch") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_child_processed_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_processed_data',
            dag_runs="{{ result('process_user_batch') }}",
            dagrun_task_id='write_semi_processed_data_to_csv',
            flatten=True,
        )

        gather_child_excluded_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_excluded_data',
            dag_runs="{{ result('process_user_batch') }}",
            dagrun_task_id='excluded_data_to_csv',
            flatten=True,
        )

        generate_final_report_data = rail.PythonOperator(
            task_id='generate_final_report_data',
            execution_timeout=timedelta(days=14),
            python_callable=python_callable.get_merged_semiprocessed_data,
        )

        generate_excluded_report_data = rail.PythonOperator(
            task_id='generate_excluded_report_data',
            execution_timeout=timedelta(days=14),
            python_callable=python_callable.get_merged_excluded_data,
        )

        final_report_has_data = rail.IfOperator(
            task_id='final_report_has_data',
            test="{{ result('generate_final_report_data').get('has_data') }}",
            yes_task='write_data_to_csv',
            no_task='remove_processed_excludedTs',
        )

        col_names = ["SOURCE", "RNA_RPL_IMP_ID", "SEQNBR", "RNA_RPT_PRD_ID", "RNA_TASK_TSH_ID", "RNA_TSH_ENTRY_ID", "RNA_RPL_EMPLID", "EMPLID", "FIRST_NAME",
                     "LAST_NAME", "PAY_END_DT", "DATE_WRK", "TL_QUANTITY", "EXPENSE_TYPE", "RNA_EXPENSE_DATE", "RNA_EXP_PAY_AMT", "SP_EXP_APPROVER", "RNA_RPL_PAY_CODE", "RNA_RPL_ACTIVITY",
                     "RNA_RPL_TASKID", "APPROVAL_STATUS", "RNA_TASK_BILLABLE", "RNA_TSH_BILLABLE", "DTTIME_ADDED", "DTTM_EXPORT", "RNA_RPL_PROJ_ID", "RNA_RPL_TASK_NAME",
                     "RNA_RPL_TASK_CODE", "RNA_RPL_UNITID", "RNA_CLIENT_CODE", "RNA_CLIENT_NAME", "RNA_RPL_NEW_TIME", "VENDOR_ID", "PAY_RATE", "RUN_DTTM", "PROCESS_STATUS",
                     "RECORD_IDENTIFIER", "DTTM_IMPORTED", "EMPLID2", "FIRST_NAME_SRCH", "LAST_NAME_SRCH", "RNA_APPROVER_DTTM"]

        write_data_to_csv = rail.PythonOperator(
            task_id='write_data_to_csv',
            execution_timeout=timedelta(days=14),
            python_callable=python_callable.get_final_data_csv,
            op_args=[col_names]
        )
        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('write_data_to_csv') }}",
            remote_filepath=config.sftp_filepath + config.export_filename +
            currentDateTime.strftime('%Y%m%d%H%M%S') + '.csv',
        )

        create_timesheet_uri_content = rail.PythonOperator(
            task_id='create_timesheet_uri_content',
            python_callable=python_callable.create_timesheet_uri_str
        )

        update_timesheet_uris_file = upload_to_s3.UploadCsvOperator(
            task_id='update_timesheet_uris_file',
            source="{{ result('create_timesheet_uri_content') }}",
            bucket_name=config.bucket_name,
            file_path=config.file_path,
            file_name=config.file_name,
        )

        remove_processed_excludedTs = rail.PythonOperator(
            task_id='remove_processed_excludedTs',
            python_callable=python_callable.remove_processed_excludedTs,
            op_args=[config]
        )

        update_excludedTS_details_file = upload_to_s3.UploadCsvOperator(
            task_id='update_excludedTS_details_file',
            source="{{ result('remove_processed_excludedTs') }}",
            bucket_name=config.bucket_name,
            file_path=config.file_path,
            file_name=config.excludedTS_file_name,
        )
        update_variable_date_and_serial_number = rail.PythonOperator(
            task_id='update_variable_date_and_serial_number',
            python_callable=python_callable.update_variable_date_and_serial_number,
            op_args=[config, currentDateTimeStr]
        )
        log_timestamp = '{{ "\n" }}{{ current_time("%d/%m/%YT%H:%M:%S") }}{{ " | INFO | " }}'

        write_replicon_logs = rail.WriteLogOperator(
            task_id='write_replicon_logs',
            message=log_timestamp+'{{ "Process started." }}\
                {%- if result("get_all_report") | is_truthy -%} \
                    '+log_timestamp+'{{ "Report - ' + config.timedata_report_name + '" }}\
                    '+log_timestamp+'{{ "Report - ' + config.timedata_report_name + ' Data processed" if result("generate_base_time_data_report_in_batch.get_report_result","has_data")  else "No Data available for Report - ' + config.timedata_report_name + '" }} \
                    {%- if result("get_all_report") | is_truthy -%} \
                        '+log_timestamp+'{{ "Report - ' + config.audit_report_name + '" }}\
                        '+log_timestamp+'{{ "Report - ' + config.audit_report_name + ' Data processed" if result("generate_final_report_data","has_data")  else "No Data available for Report - ' + config.audit_report_name + '" }} \
                        {%- if result("get_all_report") | is_truthy -%} \
                            '+log_timestamp+'{{ "Report - ' + config.entrydata_report_name + '" }}\
                            '+log_timestamp+'{{ "Report - ' + config.entrydata_report_name + ' Data processed" if result("generate_final_report_data","has_data")  else "No Data available for Report - ' + config.entrydata_report_name + '" }} \
                            '+log_timestamp+'{{ "Updating file in sftp." }} \
                            '+log_timestamp+'{{ "Updating Timesheet uris in s3." }} \
                            '+log_timestamp+'{{ "Sending task completion mailssend_task_completion_email" }} \
                            '+log_timestamp+'{{ "Process completed." }} \
                        {%- else -%} \
                            '+log_timestamp+'{{ "Task is ending as Report- ' + config.entrydata_report_name + ' data was not found." }} \
                        {%- endif -%}\
                    {%- else -%} \
                        '+log_timestamp+'{{ "Task is ending as Report- ' + config.audit_report_name + ' data was not found." }} \
                    {%- endif -%} \
                {%- else -%} \
                    '+log_timestamp+'{{ "Task is ending as Report- ' + config.timedata_report_name + ' data was not found." }} \
                {%- endif -%} \
                '
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            delimiter="|",
            header=[],
            row=['{{ item.message }}'],
        )

        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Time Data Export - Run Successfully - {{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}',
            html_content="templates/email_export_complete.html",
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Time Data Export - failed to create/upload export - {{ current_time_in_specified_tz() }}",
            html_content='templates/failure_email.html',
            params={
                'dag_id': f'CIE_timeDataExport_master_{config.instance}'.lower()
            }
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )
        trigger_export >> rail.Label(
            'No') >> delete_this_dagrun
        trigger_export >> rail.Label('Yes') >> get_last_approval_datetime
        get_last_approval_datetime >> get_all_report >> get_timedata_report_details\
            >> generate_base_time_data_report_in_batch >> get_excluded_future_timesheets >> future_ts_file_has_data

        future_ts_file_has_data >> rail.Label(
            'Yes') >> get_approved_min_max_date >> if_min_max_available
        future_ts_file_has_data >> rail.Label('No') >> report_and_file_has_data
        if_min_max_available >> rail.Label(
            'Yes') >> empty_task >> generate_base_time_data_report_in_batch_exclude >> report_and_file_has_data
        if_min_max_available >> rail.Label('No') >> report_and_file_has_data

        report_and_file_has_data >> rail.Label(
            'Yes') >> get_processed_TimesheetUris
        report_and_file_has_data >> rail.Label('No') >> finish

        get_processed_TimesheetUris >> load_timedata_csv >> create_timedata_collection >> query_unique_dates >> query_unique_users >> if_users_available
        if_users_available >> rail.Label(
            'Yes') >> get_max_users_per_chunk
        if_users_available >> rail.Label(
            'No') >> finish
        get_max_users_per_chunk >> get_user_chunck_list >> process_user_batch >> wait_for_process_user_batch >> gather_child_processed_data >> generate_final_report_data >> gather_child_excluded_data >> generate_excluded_report_data >> final_report_has_data

        final_report_has_data >> rail.Label(
            'No') >> remove_processed_excludedTs >> update_excludedTS_details_file >> update_variable_date_and_serial_number >> finish
        final_report_has_data >> rail.Label(
            'Yes') >> write_data_to_csv >> upload_csv_to_sftp >> create_timesheet_uri_content >> update_timesheet_uris_file >> remove_processed_excludedTs >> update_excludedTS_details_file >> update_variable_date_and_serial_number >> finish
        finish >> write_replicon_logs >> render_logs_csv >> send_task_completion_email >> send_task_failure_email >> final_status
    return dag


rail.for_each_instance(create_dag)
