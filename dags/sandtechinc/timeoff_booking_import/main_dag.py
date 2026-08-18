from datetime import timedelta
import pendulum
import itertools
import rail
import chardet
from rail.lib.artifact import existing_artifact
from sandtechinc.timeoff_booking_import.utils import request_payload
from sandtechinc.timeoff_booking_import.utils import python_callable
from airflow.models import Variable

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.main_dagid,
        description=f'Sand Tech Inc Time Off Booking Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        start_date=pendulum.datetime(2024, 1, 1, tz=config.time_zone),
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor_to_process = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_to_process',
            path=config.input_filepath_master,
            soft_fail_timeout=timedelta(minutes=10)
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=python_callable.get_logging_details
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor_to_process") | file_ext | lower == "csv" }}',
            yes_task='download_sftp_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Off Booking Import - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )        

        archive_file_incorrect_file_format = rail.SFTPMoveFileOperator(
            task_id='archive_file_incorrect_file_format',
            existing_filename='{{ result("new_file_sensor_to_process") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor_to_process') | file_name }}"
        )

        download_sftp_file = rail.SFTPDownloadFileOperator(
            task_id='download_sftp_file',
            remote_filepath="{{ result('new_file_sensor_to_process') }}"
        )

        def find_file_encoding_callable(task_id):
            feed_file = rail.result(task_id)
            with existing_artifact(feed_file) as ff:
                return chardet.detect_all(ff.file.read())

        find_file_encoding = rail.PythonOperator(
            task_id = "find_file_encoding",
            python_callable=find_file_encoding_callable,
            op_args=[download_sftp_file.task_id]
        )

        filter_downloaded_csv = rail.PythonOperator(
            task_id='filter_downloaded_csv',
            python_callable=python_callable.filter_downloaded_csv_file
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor_to_process") == "success" }}',
            yes_task='archive_input_file',
            no_task='delete_this_dagrun',
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor_to_process") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor_to_process') | file_name }}"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )
        
        parse_timeoff_booking_csv = rail.LoadCSVFileOperator(
            task_id="parse_timeoff_booking_csv",
            document='{{result("filter_downloaded_csv")}}',
            delimiter=",",
            encoding="{{ result('find_file_encoding')[0].encoding}}"
        )

        timeoff_booking_import_log = rail.CreateLogOperator(
            task_id="timeoff_booking_import_log"
        )

        write_timeoff_booking_import_csv = rail.WriteCSVFileOperator(
            task_id="write_timeoff_booking_import_csv",
            source='{{result("parse_timeoff_booking_csv")}}',
            header=['Email', 'Original request ID', 'Policy type', 'Start date', 'End date', 
                   'Duration', 'Unit', 'Change type', 'Status', 'Updated on', 'App', 'Approvers', 'md5'],
            row=request_payload.timeoff_booking_import_csv_data
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('write_timeoff_booking_import_csv') }}",
            name="sourcetimeoffdata",
            columns={
                'Email': 'email',
                'Original request ID': 'requestid',
                'Policy type': 'timeofftypename',
                'Start date': 'startdate',
                'End date': 'enddate',
                'Duration': 'duration',
                'Unit': 'unit',
                'Change type': 'changetype',
                'Status': 'status',
                'Updated on': 'updatedon',
                'App': 'app',
                'Approvers': 'approvers',
                'md5': 'md5'
            }
        )

        get_collection_data = rail.PythonOperator(
            task_id='get_collection_data',
            python_callable=lambda: rail.load_all_records(rail.result('parse_timeoff_booking_csv'))
        )

        if_records_present = rail.IfOperator(
            task_id="if_records_present",
            test="{{result('create_collection_from_csv', 'length') > 0}}",
            yes_task="is_use_reference_file_allowed",
            no_task="send_no_data_email"
        )

        is_use_reference_file_allowed = rail.IfOperator(
            task_id="is_use_reference_file_allowed",
            test=lambda: Variable.get(
                config.can_use_reference_file, default_var='true').lower() == 'true',
            yes_task="download_reference_file",
            no_task="query_latest_updated_records"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_filepath + config.ref_file_name
        )

        load_reference_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_csv",
            delimiter=",",
            document="{{ result('download_reference_file') }}",
            headers=["email", "requestid", "timeofftypename", "startdate", "enddate", 
                    "duration", "unit", "changetype", "status", "updatedon", "app", "approvers", "md5"]
        )

        create_ref_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_ref_collection_from_csv',
            source="{{ result('load_reference_csv') }}",
            name="timeoffreferencedata"
        )

        query_for_changed_records = rail.QueryCollectionOperator(
            task_id="query_for_changed_records",
            query="""SELECT * FROM sourcetimeoffdata WHERE md5 NOT IN (SELECT DISTINCT md5 FROM timeoffreferencedata)""",
            name="changed_records"
        )

        query_for_unchanged_records = rail.QueryCollectionOperator(
            task_id="query_for_unchanged_records",
            query="""SELECT * FROM sourcetimeoffdata WHERE md5 IN (SELECT DISTINCT md5 FROM timeoffreferencedata)""",
            name="unchanged_records"
        )

        is_unchanged_records_present = rail.IfOperator(
            task_id="is_unchanged_records_present",
            test="{{ result('query_for_unchanged_records', 'length') > 0 }}",
            yes_task="log_unchanged_records",
            no_task="is_changed_records_present"
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id='log_unchanged_records',
            log="{{ result('timeoff_booking_import_log') }}",
            items='{{result("query_for_unchanged_records")}}',
            message='No change in record',
            severity='Skipped',
            properties=request_payload.get_unchanged_record
        )

        is_changed_records_present = rail.IfOperator(
            task_id="is_changed_records_present",
            test="{{ result('query_for_changed_records', 'length') > 0 }}",
            yes_task="query_latest_updated_records",
            no_task="load_master_log"
        )

        query_latest_updated_records = rail.QueryCollectionOperator(
            task_id="query_latest_updated_records",
            name='latestupdatedrecords',
            query="""SELECT *
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY requestid ORDER BY updatedon DESC) AS rn
                        FROM changed_records
                    ) latest_records
                    WHERE rn = 1;
                    """
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query="""SELECT * FROM latestupdatedrecords 
                    WHERE NULLIF(email, '') IS NULL 
                    OR NULLIF(requestid, '') IS NULL
                    OR NULLIF(timeofftypename, '') IS NULL
                    OR NULLIF(startdate, '') IS NULL
                    OR NULLIF(enddate, '') IS NULL
                    OR NULLIF(duration, '') IS NULL
                    OR CAST(duration AS NUMERIC) <= 0
                    OR NULLIF(changetype, '') IS NULL
                    OR NULLIF(status, '') IS NULL
                    """
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('timeoff_booking_import_log') }}",
            items='{{result("query_invalid_records")}}',
            message=lambda item: "; ".join(request_payload.validate_timeoff_booking_data(item)),
            severity='Exception',
            properties=request_payload.get_invalid_record
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM latestupdatedrecords
                    WHERE NULLIF(email, '') IS NOT NULL
                    AND NULLIF(requestid, '') IS NOT NULL
                    AND NULLIF(timeofftypename, '') IS NOT NULL
                    AND NULLIF(startdate, '') IS NOT NULL
                    AND NULLIF(enddate, '') IS NOT NULL
                    AND NULLIF(duration, '') IS NOT NULL
                    AND CAST(duration AS NUMERIC) > 0
                    AND NULLIF(changetype, '') IS NOT NULL
                    AND NULLIF(status, '') IS NOT NULL;
                    """
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='get_booking_id_oef_value',
            no_task="load_master_log"
        )

        get_booking_id_oef_value = rail.RepliconServiceOperator(
            task_id='get_booking_id_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={
                "bindingContextUri": "urn:replicon:object-type:time-off"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name', "Booking_ID", 'uri').split(':')[-1],
        )

        process_timeoff_bookings = rail.trigger_parallel_dagrun(
            task_id="process_timeoff_bookings",
            items='{{ result("query_valid_records") }}',
            trigger_dag_id=config.process_timeoff_booking_child_dagid,
            parallel_count=config.max_active_process_run_count,
            conf=lambda item:{
                "booking_data": item,
                "booking_id_oef_value": rail.result('get_booking_id_oef_value')
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_process_each_timeoff_dag_ids =rail.PythonOperator(
            task_id= 'get_process_each_timeoff_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_timeoff_bookings_{x+1}') if rail.result(
                    f'process_timeoff_bookings_{x+1}') else []), range(config.max_active_process_run_count))))),
            show_return_value_in_logs= False
        )

        wait_for_process_each_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff',
            dag_runs='{{ result("get_process_each_timeoff_dag_ids") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_timeoff_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_logs',
            dag_runs='{{ result("get_process_each_timeoff_dag_ids") }}',
            dagrun_task_id='timeoff_booking_child_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ result('timeoff_booking_import_log') | load_all_records | to_json }}"
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            existing_filename=config.reference_filepath + config.ref_file_name,
            new_filename=config.archive_reference_filepath + "{{ dag_run_ecid() | replace(':', '-')}}_" + config.ref_file_name
        )

        upload_reference_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reference_csv_to_sftp',
            content="{{ result('write_timeoff_booking_import_csv') }}",
            remote_filepath=config.reference_filepath + config.ref_file_name
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.do_format_logs
        )

        write_timeoff_booking_import_log_file = rail.WriteCSVFileOperator(
            task_id='write_timeoff_booking_import_log_file',
            source="{{ result('format_logs').final_logs }}",
            header=['Request ID', 'Email', 'Status', 'Details', 'ecid'],
            row=[
                '{{ item.requestid }}',
                '{{ item.email }}',
                '{{ item.status }}',
                '{{ item.details}}',
                '{{ item.ecid }}']
        )

        check_csv_has_data = rail.IfOperator(
            task_id="check_csv_has_data",
            test=lambda: len(rail.load_all_records(rail.result('write_timeoff_booking_import_log_file'))) > 0,
            yes_task="get_email_log_details",
            no_task="fail_the_dag"
        )

        fail_the_dag = rail.FailOperator(
            task_id="fail_the_dag",
            message='No log found'
        )

        get_email_log_details = rail.PythonOperator(
            task_id='get_email_log_details',
            python_callable=python_callable.get_email_log_details
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('write_timeoff_booking_import_log_file') }}",
            output_file_name="{{ result('logging_details').log_filename }}",
            expires_in_seconds=7*24*60*60
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Off Booking Import - No Records to Import - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/send_no_data_to_import.html"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs').get_record_summary.failed == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Time Off Booking Import - " }} \
                {%- if result("format_logs").get_record_summary.failed > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if (result("format_logs").get_record_summary.exception > 0) or (result("format_logs").get_record_summary.skipped > 0) -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%}' \
                + ' - ' + '{{ current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda: {
                'records': rail.result('create_collection_from_csv', 'length'),
                'invalid_record_count': rail.result('query_invalid_records', 'length'),
                'valid_record_count': rail.result('query_valid_records', 'length'),
                'filename': rail.result("new_file_sensor_to_process").split('/')[-1] if rail.result("new_file_sensor_to_process") else ''
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

        new_file_sensor_to_process >> logging_details >> is_csv
        
        is_csv >> rail.Label('Yes') >> download_sftp_file >> was_new_file_found
        is_csv >> rail.Label('No') >> send_bad_file_format_email >> archive_file_incorrect_file_format >> log_to_sumo

        was_new_file_found >> rail.Label('No') >> delete_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_input_file

        download_sftp_file >> find_file_encoding >> filter_downloaded_csv >> parse_timeoff_booking_csv

        parse_timeoff_booking_csv >> timeoff_booking_import_log >> write_timeoff_booking_import_csv >> create_collection_from_csv >> get_collection_data >> if_records_present

        if_records_present >> rail.Label('Yes') >> is_use_reference_file_allowed
        if_records_present >> rail.Label('No') >> send_no_data_email >> log_to_sumo

        is_use_reference_file_allowed >> rail.Label('Yes') >> download_reference_file >> load_reference_csv >> \
            create_ref_collection_from_csv >> query_for_changed_records >> query_for_unchanged_records >> is_unchanged_records_present
        is_use_reference_file_allowed >> rail.Label('No') >> query_latest_updated_records

        is_unchanged_records_present >> rail.Label('Yes') >> log_unchanged_records >> is_changed_records_present
        is_unchanged_records_present >> rail.Label('No') >> is_changed_records_present

        is_changed_records_present >> rail.Label('Yes') >> query_latest_updated_records
        is_changed_records_present >> rail.Label('No') >> load_master_log

        query_latest_updated_records >> query_invalid_records >> has_invalid_records

        has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> query_valid_records
        has_invalid_records >> rail.Label('No') >> query_valid_records

        query_valid_records >> has_valid_records
        
        has_valid_records >> rail.Label('Yes') >> get_booking_id_oef_value >> process_timeoff_bookings
        has_valid_records >> rail.Label('No') >> load_master_log

        process_timeoff_bookings >> get_process_each_timeoff_dag_ids >> wait_for_process_each_timeoff >> gather_timeoff_logs >> load_master_log

        load_master_log >> archive_reference_file >> upload_reference_csv_to_sftp >> format_logs >> write_timeoff_booking_import_log_file >> check_csv_has_data
        
        check_csv_has_data >> rail.Label('Yes') >> get_email_log_details >> generate_download_link >> send_import_complete_email >> log_to_sumo
        check_csv_has_data >> rail.Label('No') >> fail_the_dag

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)