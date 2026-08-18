from datetime import timedelta
from airflow.models import Variable
import rail
from galaxyusopcoinc.timeoff_hours_booking_import.utils import request_payload, response_filter, custom_methods


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Vialto Timeoff Booking Import Automation',
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
            test='{{ result("new_file_sensor") | file_ext | lower == "pgp" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon TimeOff Booking import - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=Variable.get(config.can_decrypt_file,
                              default_var='false').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            # yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            # trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        dummy_load_data = rail.PythonOperator(
            task_id="dummy_load_data",
            python_callable=lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file, default_var='false').lower() == 'true'
            else rail.result('download_file'),
            show_return_value_in_logs=False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}",
            delimiter='|'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'EmployeeId': 'employee_id',
                'Legal Full Name': 'legal_full_name',
                'Work Email Address': 'work_email',
                "BookingReferenceID": "booking_id",
                'WD Event ID': 'wd_event_id',
                'PlanRefID': 'plan_ref_id',
                'Time off Plan Name': 'timeoff_plan_name',
                'RequestType': 'request_type',
                'TimeOffDate': 'timeoff_date',
                'UnitHours': 'hours'
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='query_invalid_records',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon TimeOff Booking import - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(employee_id, '') IS NULL or
                    NULLIF(booking_id, '') IS NULL or NULLIF(plan_ref_id, '') IS NULL
                    or NULLIF(request_type, '') IS NULL or NULLIF(timeoff_date, '') IS NULL or
                    NULLIF(hours, '') IS NULL or hours == '0' """
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log='{{ result("create_log") }}',
            items='{{result("query_invalid_records")}}',
            message='Required fields are Missing',
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'timeoff_type': item['plan_ref_id'],
                'entry_date': item['timeoff_date'],
                'status': 'Exception',
                'hours': item['hours'],
                'wd_event_id': item['wd_event_id'],
                'details': custom_methods.get_mandatory_fields_exception_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM inputdatacollection WHERE
                NULLIF(employee_id, '') IS NOT NULL AND
                NULLIF(booking_id, '') IS NOT NULL AND
                NULLIF(plan_ref_id, '') IS NOT NULL AND
                NULLIF(request_type, '') IS NOT NULL AND
                NULLIF(timeoff_date, '') IS NOT NULL AND
                NULLIF(hours, '') IS NOT NULL
                """
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task="query_sum_of_hours_from_raw_data",
            no_task="format_logs"
        )

        query_sum_of_hours_from_raw_data = rail.QueryCollectionOperator(
            task_id='query_sum_of_hours_from_raw_data',
            name='uniquedata',
            query='''SELECT employee_id, booking_id, timeoff_date, plan_ref_id, request_type, wd_event_id,
                SUM(CAST(hours AS FLOAT)) as hours FROM validrecords GROUP BY employee_id, booking_id, timeoff_date'''
        )

        query_distinct_users = rail.QueryCollectionOperator(
            task_id='query_distinct_users',
            name='distinctusers',
            query='''SELECT DISTINCT employee_id FROM uniquedata'''
        )

        get_booking_id_oef_value = rail.RepliconServiceOperator(
            task_id='get_booking_id_oef_value',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data=request_payload.get_booking_id_oef_value_payload,
            data_handler=response_filter.get_booking_id_oef_value
        )

        get_all_time_off_types_uris = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_uris',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_time_off_type_uris
        )

        get_timeoff_type_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_type_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=lambda: {
                "timeOffTypeUris": rail.result('get_all_time_off_types_uris')
            },
            data_handler=response_filter.get_filtered_timeoff_details
        )

        process_distinct_timeoff = rail.trigger_parallel_dagrun(
            task_id='process_distinct_timeoff',
            items="{{ result('query_distinct_users') }}",
            parallel_count=config.parallel_dagrun_count,
            trigger_dag_id=config.process_each_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'employee_id': item['employee_id'],
                'timeoff_type_details': rail.result('get_timeoff_type_details'),
                'booking_id_oef_value': rail.result('get_booking_id_oef_value')['booking_id_oef_value'],
                'log': rail.result("create_log")
            }
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result("format_logs"),
            header=[
                'EmployeeID',
                'Time off Desc',
                'Entry Date',
                'Status',
                'Reason',
                'Hours',
                'WD Event ID',
                'JobID'],
            row=[
                '{{ item.employee_id }}',
                '{{ item.timeoff_type }}',
                '{{ item.entry_date }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.hours }}',
                '{{ item.wd_event_id }}',
                '{{ item.jobid }}'
            ],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() + " | Replicon Time Off Hours Booking Import - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}''',
            html_content="templates/emails/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
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
            'Yes') >> download_file >> was_new_file_found

        is_csv >> rail.Label(
            'No') >> send_bad_file_format_email

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        download_file >> archive_file >> create_log >> can_decrypt_file >> rail.Label(
            'Yes') >> decrypt_file

        can_decrypt_file >> rail.Label(
            'No') >> dummy_load_data

        decrypt_file >> dummy_load_data >> load_data >> create_input_data_collection >> has_input_data

        has_input_data >> rail.Label(
            'No') >> send_blank_payload_email

        has_input_data >> rail.Label(
            'Yes') >> query_invalid_records

        query_invalid_records >> has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> query_valid_records

        has_invalid_records >> rail.Label(
            'No') >> query_valid_records

        query_valid_records >> has_valid_records >> rail.Label(
            'No') >> format_logs

        has_valid_records >> rail.Label(
            'Yes') >> query_sum_of_hours_from_raw_data >> query_distinct_users >> get_booking_id_oef_value >> \
            get_all_time_off_types_uris >> get_timeoff_type_details >> process_distinct_timeoff >> format_logs

        format_logs >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email >> can_fail_dag

        can_fail_dag >> rail.Label(
            "Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
