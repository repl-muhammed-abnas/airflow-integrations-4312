
from datetime import timedelta, datetime
import pendulum
from airflow.models import Variable
import rail
from omd.singapore_timeoff_import.utils import custom_methods, response_filter, request_payload, python_callable_method

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'omdsingapore_timeoff_import_master_{config.instance}',
        description=f'Omdsingapore | Timeoff_import - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        max_active_runs=1,
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

        log_timetobeused = rail.PythonOperator(
            task_id='log_timetobeused',
            python_callable=lambda: pendulum.now(
                config.time_zone).strftime('%d%m%YT%H%M%S')
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
            subject='{{ get_company_key() }} | Timeoff import - skipped {{ current_time() }}',
            html_content='''<p>Hi Team,<br /> <br /> Timeoff import for  {{ get_company_key() }} skipped, since the file format is incorrect </p>
<p> File name : "{{ result("new_file_sensor") | file_name }}" < /p>
<p>Please send the correct input file in csv file format.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
        )

        archive_invalid_file = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
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
            new_filename=config.processing_filepath +
            "/processing_{{ result('log_timetobeused') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_time_import_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_time_import_log',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_time_import_log = rail.CreateLogOperator(
            task_id='create_time_import_log'
        )

        load_time_import_data = rail.LoadCSVFileOperator(
            task_id='load_time_import_data',
            document="{{ result('download_file') }}",
        )

        create_time_import_collection = rail.CreateCollectionOperator(
            task_id='create_time_import_collection',
            source="{{ result('load_time_import_data') }}",
            name="input_data"
        )

        create_csv_lines_inputdatawithmd5_4 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_inputdatawithmd5_4',
            source="{{ result('create_time_import_collection') }}",
            header=['Employee ID',
                    'Leave Code',
                    'Start Date',
                    'Start Day Type',
                    'Start Day Hours',
                    'End Date',
                    'End Day Type',
                    'End Day Hours',
                    'Record ID',
                    'Status',
                    'md5'],
            row=custom_methods.get_csv_rows
        )

        load_csv_create_list_from_csv_inputdata_5 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_inputdata_5",
            document="{{ result('create_csv_lines_inputdatawithmd5_4') }}",
        )

        create_collection_create_list_from_csv_inputdata_5 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_inputdata_5',
            source="{{ result('load_csv_create_list_from_csv_inputdata_5') }}",
            name="inputdata",
            columns={
                'Employee ID': 'empid',
                'Leave Code': 'leavecode',
                'Start Date': 'startdate',
                'Start Day Type': 'startdaytype',
                'Start Day Hours': 'startdayhours',
                'End Date': 'enddate',
                'End Day Type': 'enddatetype',
                'End Day Hours': 'enddatehours',
                'Record ID': 'recordid',
                'Status': 'status',
                'md5': 'md5'
            }
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_filepath + '/timeoffreference.csv',
        )

        load_csv_create_list_from_csv_inputdata_7 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_inputdata_7",
            document="{{ result('download_reference_file') }}",
        )

        create_collection_create_list_from_csv_inputdata_7 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_inputdata_7',
            source="{{ result('load_csv_create_list_from_csv_inputdata_7') }}",
            name="referencedata",
            columns={
                'Employee ID': 'empid',
                'Leave Code': 'leavecode',
                'Start Date': 'startdate',
                'Start Day Type': 'startdaytype',
                'Start Day Hours': 'startdayhours',
                'End Date': 'enddate',
                'End Day Type': 'enddatetype',
                'End Day Hours': 'enddatehours',
                'Record ID': 'recordid',
                'Status': 'status',
                'md5': 'md5'
            }
        )

        query_list_delta_records_8 = rail.QueryCollectionOperator(
            task_id='query_list_delta_records_8',
            query="""SELECT * FROM inputdata WHERE md5 NOT IN (SELECT DISTINCT md5 FROM referencedata)""",
            name='delta_records'
        )

        query_list_employee_id_present = rail.QueryCollectionOperator(
            task_id='query_list_employee_id_present',
            query="""SELECT * FROM delta_records WHERE NULLIF(empid,'') IS NOT NULL""",
            name='employee_id_records'
        )

        query_list_employee_id_not_present = rail.QueryCollectionOperator(
            task_id='query_list_employee_id_not_present',
            query="""SELECT * FROM delta_records WHERE NULLIF(empid,'') IS NULL""",
            name='no_employee_id_records'
        )

        if_query_list_delta_records_8_rows_greater_than_0_9 = rail.IfOperator(
            task_id='if_query_list_delta_records_8_rows_greater_than_0_9',
            test='''{{ result("query_list_delta_records_8", "length") > 0 }}''',
            yes_task="get_all_reports_12",
            no_task="send_mail_nodelta_57",
        )

        get_all_reports_12 = rail.RepliconServiceOperator(
            task_id='get_all_reports_12',
            endpoint="/services/ReportService1.svc/GetAllReports",
            data=None
        )

        log_getreport_userlistfor_integrationuri_13 = rail.PythonOperator(
            task_id='log_getreport_userlistfor_integrationuri_13',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_reports_12'), 'displayText', 'User list for Integration', 'uri')
        )

        generate_report_14 = rail.RepliconServiceOperator(
            task_id='generate_report_14',
            endpoint="/services/ReportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('log_getreport_userlistfor_integrationuri_13') }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document='{{ result("generate_report_14").payload }}',
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source='{{ result("load_report_data") }}',
            name='userlist',
            columns={
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'Holiday Calendar': 'holidaycalendar',
                'UserUri': 'useruri'
            }
        )

        get_data_all_timeofftypes_16 = rail.RepliconServiceOperator(
            task_id='get_data_all_timeofftypes_16',
            endpoint="/services/TimeOffTypeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:time-off-type-list-column:name",
                    "urn:replicon:time-off-type-list-column:description",
                    "urn:replicon:time-off-type-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=response_filter.get_timeofftypelist
        )

        get_all_holiday_calendars_22 = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars_22',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        log_startdaterange_26 = rail.PythonOperator(
            task_id='log_startdaterange_26',
            python_callable=lambda: int(datetime.now().strftime("%Y")) - 1
        )

        log_enddaterange_27 = rail.PythonOperator(
            task_id='log_enddaterange_27',
            python_callable=lambda: int(datetime.now().strftime("%Y")) + 1
        )

        def get_holiday_entries(response, item):
            holiday_entries = []
            if response:
                for data in response:
                    holiday_entries.append({
                        "holidaycalendarname": item['name'],
                        "holidayname": data['name'],
                        "holidaydate": str(data['date']['year']) + "-" + str(data['date']['month']) + "-" + str(data['date']['day'])
                    })
            return holiday_entries

        get_holidays_in_date_range = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_holidays_in_date_range',
            items="{{ result('get_all_holiday_calendars_22') | to_json }}",
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data={
                "holidayCalendarUri": "{{ item.uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_startdaterange_26') }}",
                        "month": 1,
                        "day": 1
                    },
                    "endDate": {
                        "year": "{{ result('log_enddaterange_27') }}",
                        "month": 12,
                        "day": 31
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            data_handler=get_holiday_entries
        )

        def get_final_holiday_entries():
            final_holiday_entries = []
            for holidycalendar in rail.result('get_holidays_in_date_range'):
                final_holiday_entries.extend(holidycalendar)
            return final_holiday_entries

        result_of_final_holiday_entries = rail.PythonOperator(
            task_id='result_of_final_holiday_entries',
            python_callable=get_final_holiday_entries
        )

        trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39',
            retries=0,
            items="{{ result('query_list_employee_id_present') }}",
            trigger_dag_id=f'omdsingapore_timeoff_import_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_conf_timeoff_import
        )

        wait_for_completion_trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39") }}'
        )

        omdsingapore_timeoffimport_logs_add_entry_41 = rail.WriteLogOperator(
            task_id='omdsingapore_timeoffimport_logs_add_entry_41',
            log='{{ result("create_time_import_log") }}',
            items="{{ result('query_list_employee_id_not_present') }}",
            message="Employee ID is not present",
            severity="Info",
            properties=lambda item: {
                "employeeid": item['empid'],
                "leavecode": item['leavecode'],
                "startdaytype": item['startdaytype'],
                "startdate": item['startdate'],
                "recordid": item['recordid'],
                "status": item['status'],
                "jobstatus": "Skipped",
                "details": "Employee ID is not present"
            }
        )

        gather_timeoff_import_logs_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_import_logs_from_child',
            dag_runs='{{ result("trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39") }}',
            dagrun_task_id='create_timeoff_import_child_log',
            flatten=True
        )

        format_timeoff_import_logs = rail.PythonOperator(
            task_id='format_timeoff_import_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_method.do_format_timeoff_import_logs
        )

        get_failed_logs = rail.PythonOperator(
            task_id='get_failed_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['jobstatus'] == 'Failed', rail.result('format_timeoff_import_logs')))), 'length')
        )

        get_success_logs = rail.PythonOperator(
            task_id='get_success_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['jobstatus'] == 'Success', rail.result('format_timeoff_import_logs')))), 'length')
        )

        create_csv_lines_44 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_44',
            source="{{ result('format_timeoff_import_logs') | to_json }}",
            header=['Employee ID',
                    'Leave Code',
                    'Start Date',
                    'Start Day Type',
                    'Record ID',
                    'Status',
                    'Jobstatus',
                    'Details'],
            row=[
                "{{ item.employeeid }}",
                "{{ item.leavecode }}",
                "{{ item.startdate }}",
                "{{ item.startdaytype }}",
                "{{ item.recordid }}",
                "{{ item.status }}",
                "{{ item.jobstatus }}",
                "{{ item.jobid }}|{{ item.details }}"
            ]

        )

        upload_48 = rail.SFTPUploadFileOperator(
            task_id='upload_48',
            content='''{{ result('create_csv_lines_44') }}''',
            remote_filepath=config.log_filepath +
            "/Log{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}",
        )

        upload_uploadingtocshare_49 = rail.GeneratePresignedDownloadUrlOperator(
            task_id='upload_uploadingtocshare_49',
            artifact_name='{{ result("create_csv_lines_44")}}',
            output_file_name="Log{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}",
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        def file_upload_failed(context):
            subject = '''{{ get_company_key() }} | Timeoff import Completed - Error while uploading logs - {{ current_time() }} '''
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.tenant_email,
                bcc=config.alert_email,
                subject=subject,
                html_content='''<p>Hi Team,<br /><br />Timeoff Import for {{ get_company_key() }}, hosted on {{ get_company_key() }}, created on {{ current_time() }} has been completed, however, the log upload to sftp has failed. Attached is the log file for reference.</p>
<ul>
<li>Recipe ID: {{ dag_run.dag_id}} </li>
<li>Job ID: {{ ecid() }} </li>
</ul>
<p>Please find the attached logs which was to be sent to intended recipients and debug the issue related to sftp upload.<br /><br />Regards,<br />Deltek Inc</p> ''',
                files=[
                    ('{{ result("create_csv_lines_44") }}')
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        send_mail_51 = rail.SFTPUploadFileOperator(
            task_id='send_mail_51',
            content='{{ result("create_csv_lines_44") }}',
            remote_filepath=config.log_filepath +
            "/Log{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}",
            on_failure_callback=file_upload_failed
        )

        if_log_checkifthereisfailedjobs_46_present_52 = rail.IfOperator(
            task_id='if_log_checkifthereisfailedjobs_46_present_52',
            test='''{{ result('get_failed_logs','length') > 0 }}''',
            yes_task="send_mail_with_cshare_53",
            no_task="send_mail_with_cshare_55",
        )

        send_mail_with_cshare_53 = rail.EmailOperator(
            task_id='send_mail_with_cshare_53',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} | Timeoff import Completed with failures {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br/> <br/>Hello, <br/> <br/> The Timeoff Import is completed with failures based on the file - '{{ result("new_file_sensor") | file_name }}'. Please find the link below to download the logs.
 <br/> <br/><a href="{{ result('upload_uploadingtocshare_49') }}">Download Logs</a><br/> <br/><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
<br/>
<p>For any queries, please contact our support team at https://support.deltek.com <br/><br/>Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_with_cshare_55 = rail.EmailOperator(
            task_id='send_mail_with_cshare_55',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Timeoff import Completed Successfully - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br/> <br/>Hello, <br/> <br/> The Timeoff Import is completed successfully based on the file - '{{ result("new_file_sensor") | file_name }}'. Please find the download link below.
 <br/> <br/><a href="{{ result('upload_uploadingtocshare_49') }}">Download Logs</a><br/> <br/><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
<p>For any queries, please contact our support team at https://support.deltek.com <br/><br/>Regards, <br/>Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_nodelta_57 = rail.EmailOperator(
            task_id='send_mail_nodelta_57',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Time off import Completed file processing is skipped -  {{ current_time() }} ''',
            html_content='''<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
<p>Hi ,</p>
<p>The Timeoff Import is completed based on the file - '{{ result("new_file_sensor") | file_name }}'. There were no delta records to be processed.</p>
<p>For any queries, please contact our support team at https://support.deltek.com</p>
<p>Thanks, <br/>Deltek Inc.</p> ''',
            params=None,
        )

        upload_uploadreference_58 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadreference_58',
            content='''{{ result('create_csv_lines_inputdatawithmd5_4') }}''',
            remote_filepath=config.reference_filepath + '/timeoffreference.csv',
        )

        rename_59 = rail.SFTPMoveFileOperator(
            task_id='rename_59',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}",
            existing_filename=config.processing_filepath +
            "/processing_{{ result('log_timetobeused') }}_{{ result('new_file_sensor') | file_name }}"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        new_file_sensor >> log_timetobeused >> is_csv >> rail.Label(
            "No") >> send_bad_file_format_email >> archive_invalid_file >> finish

        is_csv >> rail.Label("Yes") >> download_file >> was_new_file_found >> rail.Label(
            "Yes") >> archive_file >> can_run_batch_task
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun >> finish

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_time_import_log >> load_time_import_data >> create_time_import_collection \
            >> create_csv_lines_inputdatawithmd5_4 >> load_csv_create_list_from_csv_inputdata_5 \
            >> create_collection_create_list_from_csv_inputdata_5 >> download_reference_file >> load_csv_create_list_from_csv_inputdata_7 \
            >> create_collection_create_list_from_csv_inputdata_7 >> query_list_delta_records_8 >> query_list_employee_id_present \
            >> query_list_employee_id_not_present >> if_query_list_delta_records_8_rows_greater_than_0_9
        if_query_list_delta_records_8_rows_greater_than_0_9 >> rail.Label(
            'Yes') >> get_all_reports_12 >> log_getreport_userlistfor_integrationuri_13 >> generate_report_14 \
            >> load_report_data >> create_report_collection >> get_data_all_timeofftypes_16 >> get_all_holiday_calendars_22 \
            >> log_startdaterange_26 >> log_enddaterange_27 >> get_holidays_in_date_range >> result_of_final_holiday_entries\
            >> trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39 \
            >> wait_for_completion_trigger_dag_run_live_omdsingapore_timeoff_import_childasync_39 >> omdsingapore_timeoffimport_logs_add_entry_41 \
            >> gather_timeoff_import_logs_from_child >> format_timeoff_import_logs >> get_failed_logs >> get_success_logs \
            >> create_csv_lines_44 >> upload_48 >> upload_uploadingtocshare_49 \
            >> send_mail_51 >> if_log_checkifthereisfailedjobs_46_present_52
        if_log_checkifthereisfailedjobs_46_present_52 >> rail.Label(
            'Yes') >> send_mail_with_cshare_53 >> upload_uploadreference_58
        if_log_checkifthereisfailedjobs_46_present_52 >> rail.Label(
            'No') >> send_mail_with_cshare_55 >> upload_uploadreference_58
        if_query_list_delta_records_8_rows_greater_than_0_9 >> rail.Label(
            'No') >> send_mail_nodelta_57 >> upload_uploadreference_58 >> rename_59 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
