from datetime import timedelta, datetime
from hashlib import sha256
import rail
from rail.lib.ecid import get_dagrun_ecid
from bearingpoint.sap_h4s4_timeoff_booking_import_v1.utils import custom_methods

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Bearingpoint Timeoff Booking Import Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon TimeOff Booking Import - File processing is skipped on {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/bad_file_format.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath +
            '/archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath +
            '/{{ result("new_file_sensor") | file_name }}',
        )

        load_timeoff_data = rail.LoadCSVFileOperator(
            task_id='load_timeoff_data',
            document="{{ result('download_file') }}",
            delimiter=','
        )

        write_timeoff_import_csv = rail.WriteCSVFileOperator(
            task_id="write_timeoff_import_csv",
            source='{{result("load_timeoff_data")}}',
            header=["employee_id", "startdate", "enddate",
                    "timeofftype", "hours", "booking_id"],
            row=lambda item: [
                item["EMPLOYEE_ID"],
                datetime.strptime(item['LEAVE_START_DT'], '%Y/%m/%d').strftime("%Y-%m-%d") if item['LEAVE_START_DT'] else None,
                datetime.strptime(item['LEAVE_END_DT'], '%Y/%m/%d').strftime("%Y-%m-%d") if item['LEAVE_END_DT'] else None,
                item["TIME_OFF_TYPE"],
                item["TIME_OFF_HOURS"],
                sha256("".join([item["EMPLOYEE_ID"], item["LEAVE_START_DT"],
                                item["LEAVE_END_DT"], item["TIME_OFF_TYPE"], item["TIME_OFF_HOURS"]]).encode()).hexdigest()
            ]
        )

        calculate_processing_window = rail.PythonOperator(
            task_id='calculate_processing_window',
            python_callable=lambda: custom_methods.calculate_three_month_window(
                custom_methods.extract_date_from_filename(
                    rail.result('new_file_sensor')
                )
            )
        )

        store_window_dates = rail.PythonOperator(
            task_id='store_window_dates',
            python_callable=lambda: {
                'window_start': rail.result('calculate_processing_window')['window_start'].strftime('%Y-%m-%d'),
                'window_end': rail.result('calculate_processing_window')['window_end'].strftime('%Y-%m-%d')
            }
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        create_rawdata_collection = rail.CreateCollectionOperator(
            task_id='create_rawdata_collection',
            source="{{ result('write_timeoff_import_csv') }}",
            name='rawdata'
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_rawdata_collection', 'length') > 0 }}",
            yes_task='list_reference_file',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon TimeOff Booking Import - no records in payload - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        list_reference_file = rail.SFTPListFilesOperator(
            task_id='list_reference_file',
            paths=[config.sftp_reference_filepath],
        )

        has_reference_file_present = rail.IfOperator(
            task_id='has_reference_file_present',
            test="{{ result('list_reference_file') | is_truthy }}",
            yes_task='get_reference_filename',
            no_task='fail_dag_reference_file_not_found'
        )

        fail_dag_reference_file_not_found = rail.FailOperator(
            task_id = 'fail_dag_reference_file_not_found',
            message= 'No Reference File Found'
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: rail.result('list_reference_file')[
                config.sftp_reference_filepath][0]['name']
            if rail.result('list_reference_file') else None
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.sftp_reference_filepath +
            "/{{ result('get_reference_filename')}}"
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
            query="""SELECT * FROM rawdata WHERE booking_id NOT IN (SELECT DISTINCT booking_id FROM referencefile)""",
            name='deltarecords'
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
            subject='''{{ get_company_key()}} | Replicon TimeOff Booking Import - No change in values - {{ current_time_in_specified_tz() }} ''',
            html_content='templates/emails/no_change_mail.html',
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM rawdata WHERE booking_id IN (SELECT DISTINCT booking_id FROM referencefile)""",
        )

        log_no_change_in_timeoff_record = rail.WriteLogOperator(
            task_id='log_no_change_in_timeoff_record',
            log='{{ result("create_log") }}',
            items='{{ result("query_unchanged_records") }}',
            message="No change in timeoff record",
            severity="Skipped",
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'startdate': item['startdate'],
                'enddate': item['enddate'],
                'timeofftype': item['timeofftype'],
                'hours': item['hours'],
                'action': 'Validation',
                'status': "Skipped",
                "details": "No change in timeoff record",
            }
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM deltarecords WHERE
                    NULLIF(employee_id, '') IS NULL or
                    NULLIF(startdate, '') IS NULL or
                    NULLIF(enddate, '') IS NULL or
                    NULLIF(timeofftype, '') IS NULL or
                    NULLIF(hours, '') IS NULL or CAST(hours AS FLOAT) <= 0 """
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_log')}}",
            message='Required fields are Missing',
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'startdate': item['startdate'],
                'enddate': item['enddate'],
                'timeofftype': item['timeofftype'],
                'hours': item['hours'],
                'action': 'Validation',
                'status': 'Exception',
                'details': custom_methods.get_mandatory_fields_exception_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='all_valid_records',
            query="""SELECT * FROM deltarecords WHERE
                NULLIF(employee_id, '') IS NOT NULL AND
                NULLIF(startdate, '') IS NOT NULL AND
                NULLIF(enddate, '') IS NOT NULL AND
                NULLIF(timeofftype, '') IS NOT NULL AND
                NULLIF(hours, '') IS NOT NULL AND CAST(hours AS FLOAT) > 0 """
        )

        query_records_outside_window = rail.QueryCollectionOperator(
            task_id='query_records_outside_window',
            query="""SELECT * FROM all_valid_records
                WHERE (startdate < :window_start AND enddate > :window_end)
                   OR (enddate < :window_start)
                   OR (startdate > :window_end)""",
            query_params={
                'window_start': '{{ result("store_window_dates").window_start }}',
                'window_end': '{{ result("store_window_dates").window_end }}'
            }
        )

        log_records_outside_window = rail.WriteLogOperator(
            task_id='log_records_outside_window',
            log='{{ result("create_log") }}',
            items='{{ result("query_records_outside_window") }}',
            message="Record skipped - outside 3-month processing window",
            severity="Skipped",
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'startdate': item['startdate'],
                'enddate': item['enddate'],
                'timeofftype': item['timeofftype'],
                'hours': item['hours'],
                'action': 'Validation',
                'status': "Skipped",
                "details": 'Timeoff Booking is outside the 3-month processing window',
            }
        )

        query_processable_records = rail.QueryCollectionOperator(
            task_id='query_processable_records',
            name='valid_records',
            query="""SELECT *,
                CASE
                    WHEN startdate < :window_start AND enddate <= :window_end AND enddate >= :window_start THEN 1
                    ELSE 0
                END as needs_auto_approval,
                :window_start as window_start,
                :window_end as window_end
                FROM all_valid_records
                WHERE (
                    (startdate >= :window_start AND startdate <= :window_end) OR
                    (enddate >= :window_start AND enddate <= :window_end)
                )
                AND NOT (startdate < :window_start AND enddate > :window_end)""",
            query_params={
                'window_start': '{{ result("store_window_dates").window_start }}',
                'window_end': '{{ result("store_window_dates").window_end }}'
            }
        )

        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test="{{ result('query_processable_records', 'length') > 0 }}",
            yes_task='get_booking_id_oef_uri',
            no_task='finish'
        )

        get_booking_id_oef_uri = rail.RepliconServiceOperator(
            task_id='get_booking_id_oef_uri',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={
                'bindingContextUri': "urn:replicon:object-type:time-off"
            },
            data_handler=lambda response: {
                'booking_id': rail.find_first_by_attr_and_get_attr(response, 'name', 'Booking Id', 'uri')
            }
        )


        query_distinct_employees = rail.QueryCollectionOperator(
            task_id='query_distinct_employees',
            query='''SELECT DISTINCT employee_id FROM valid_records'''
        )

        process_delete_timeoff = rail.TriggerDagRunOperator(
            task_id='process_delete_timeoff',
            trigger_dag_id=config.process_delete_timeoff_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'booking_id_oef_uri': rail.result('get_booking_id_oef_uri')['booking_id'],
                'log': rail.result('create_log'),
                'window_start': rail.result('calculate_processing_window')['window_start'].strftime('%Y-%m-%d'),
                'window_end': rail.result('calculate_processing_window')['window_end'].strftime('%Y-%m-%d')
            }
        )

        process_distinct_employees = rail.trigger_parallel_dagrun(
            task_id='process_distinct_employees',
            items="{{ result('query_distinct_employees') }}",
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.process_each_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'employee_id': item['employee_id'],
                'booking_id_oef_uri': rail.result('get_booking_id_oef_uri')['booking_id'],
                'log': rail.result('create_log'),
                'window_start': rail.result('calculate_processing_window')['window_start'].strftime('%Y-%m-%d'),
                'window_end': rail.result('calculate_processing_window')['window_end'].strftime('%Y-%m-%d')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('format_logs') | to_json }}",
            header=["employeeid", "startdate", "enddate", "timeofftype",
                    "hours", "action", "status", "details", "jobid"],
            row=[
                '{{ item.employee_id }}',
                '{{ item.startdate }}',
                '{{ item.enddate }}',
                '{{ item.timeofftype }}',
                '{{ item.hours }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id='get_log_file_name',
            python_callable=lambda dag_run: f'{rail.get_company_key()}_time_off_import_logs_{ get_dagrun_ecid(dag_run).replace(":", "-")}' + "_" + rail.render_template(
                "{{result('new_file_sensor') | file_name}}")
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("get_log_file_name") }}',
        )

        archive_old_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_old_reference_file',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_{{ result('get_reference_filename')}}",
            existing_filename=config.sftp_reference_filepath +
            "/{{ result('get_reference_filename')}}",
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('write_timeoff_import_csv') }}''',
            remote_filepath=config.sftp_reference_filepath +
            "/Reference_{{ result('new_file_sensor') | file_name }}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon TimeOff Booking Import is " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath,
            },
            html_content='templates/emails/import_complete.html',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{ result('new_file_sensor') | file_name }}",
                "no_of_records_in_payload":  "{{ result('create_rawdata_collection','length') }}",
                "no_of_valid_records_in_payload":  "{{ result('query_valid_records','length') }}",
                "log_file_name": '{{ result("get_log_file_name") }}'
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="{{ get_error_message() }}"
        )

        new_file_sensor >> is_csv >> rail.Label(
            "Yes") >> download_file >> was_new_file_found >> rail.Label(
                "No") >> delete_this_dagrun

        was_new_file_found >> rail.Label(
            "Yes") >> archive_file

        is_csv >> rail.Label(
            "No") >> send_bad_file_format_email

        download_file >> load_timeoff_data >> write_timeoff_import_csv >> calculate_processing_window >> store_window_dates >> create_log

        create_log >> create_rawdata_collection >> has_collection_data

        has_collection_data >> rail.Label(
            "No") >> send_blank_payload_email

        has_collection_data >> rail.Label(
            "Yes") >> list_reference_file >> has_reference_file_present
        
        has_reference_file_present >> rail.Label(
            "Yes") >> get_reference_filename
        
        has_reference_file_present >> rail.Label(
            "No") >> fail_dag_reference_file_not_found
        
        get_reference_filename >> download_reference_file >> \
            parse_reference_file >> create_referencefile_collection >> query_delta_records >> if_no_delta_records

        if_no_delta_records >> rail.Label(
            "Yes") >> send_mail_no_changes_found

        if_no_delta_records >> rail.Label(
            "No") >> query_unchanged_records >> log_no_change_in_timeoff_record >> query_invalid_records

        query_invalid_records >> log_invalid_records >> query_valid_records

        query_valid_records >> query_records_outside_window >> log_records_outside_window >> query_processable_records >> has_valid_records

        has_valid_records >> rail.Label(
            "No") >> finish

        has_valid_records >> rail.Label(
            "Yes") >> get_booking_id_oef_uri >> query_distinct_employees >> process_delete_timeoff >> process_distinct_employees >>\
            finish >> format_logs >> render_logs_csv >> get_log_file_name >> upload_logs_to_sftp >>\
            archive_old_reference_file >> upload_new_reference_file >> send_import_complete_email >> log_to_sumo >> \
            can_fail_dag >> rail.Label("Yes") >> fail_dag

    return dag


rail.for_each_instance(create_child_dag)
