from datetime import timedelta
from airflow.models import Variable
import rail
from darkmattertechnologiesllc.timeoff_import.utils import request_payload, python_callable, response_filter
from darkmattertechnologiesllc.timeoff_import.tasks.send_logs import get_send_logs


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master,
        description='Dark Matter Timeoff Import Automation',
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
            subject='{{ get_company_key() }} | Replicon TimeOff import - Incorrect Format - {{ current_time_in_specified_tz() }}',
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
            task_id ="can_decrypt_file",
            test=Variable.get(config.can_decrypt_file, default_var='false').lower() == 'true',
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
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file, default_var='false').lower()== 'true' \
                else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}"
        )

        create_csv_with_md5 = rail.DataAdaptorOperator(
            task_id='create_csv_with_md5',
            source="{{ result('load_data') }}",
            columns=[
                    'Employee ID',
                    'Worker',
                    'Continuous Service Date',
                    "Worker's Manager",
                    'Location',
                    'Cost Center',
                    'Action Event',
                    'Time off Type',
                    'Reference ID',
                    'Request or Correction',
                    'Entered On',
                    'Approval Date',
                    'Time Off Date',
                    'Units Approved',
                    'Unit of Time request',
                    'MD5'
                ],
            data=python_callable.get_formated_timeoff_row
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('create_csv_with_md5') }}",
            name="inputdatacollection",
            columns={
                'Employee ID': 'employee_id',
                'Worker': 'worker',
                'Continuous Service Date': 'continuous_service_date',
                "Worker's Manager": "worker's_manager",
                'Location': 'location',
                'Cost Center': 'cost_center',
                'Action Event': 'action_event',
                'Time off Type': 'time_off_type',
                'Reference ID': 'reference_id',
                'Request or Correction': 'request_or_correction',
                'Entered On': 'entered_on',
                'Approval Date': 'approval_date',
                'Time Off Date': 'time_off_date',
                'Units Approved': 'units_approved',
                'Unit of Time request': 'unit_of_time_request',
                'MD5': 'md5'
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task=['query_valid_records', 'query_invalid_records'],
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon TimeOff import - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(employee_id, '') IS NULL or
                    NULLIF(time_off_type, '') IS NULL or NULLIF(time_off_date, '') IS NULL
                    or NULLIF(units_approved, '') IS NULL or NULLIF(unit_of_time_request, '') IS NULL or
                    unit_of_time_request != 'Hours'"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="finish"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log='{{ result("create_log") }}',
            items='{{result("query_invalid_records")}}',
            message='Required fields are Missing',
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'unique_id': item['md5'],
                'time_off_date': item['time_off_date'],
                'status': 'Exception',
                'details': 'Required fields are Missing or Unit of Time request is not in "Hours"'
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM inputdatacollection WHERE 
                NULLIF(employee_id, '') IS NOT NULL AND 
                NULLIF(time_off_type, '') IS NOT NULL AND 
                NULLIF(time_off_date, '') IS NOT NULL AND 
                NULLIF(units_approved, '') IS NOT NULL AND 
                NULLIF(unit_of_time_request, '') IS NOT NULL AND 
                unit_of_time_request = 'Hours'
                """
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task="query_sum_of_units_and_distinct_md5",
            no_task="finish"
        )

        query_sum_of_units_and_distinct_md5 = rail.QueryCollectionOperator(
            task_id='query_sum_of_units_and_distinct_md5',
            name='unique_md5',
            query='''SELECT employee_id, time_off_type, time_off_date, SUM(CAST(units_approved AS FLOAT)) as total_units, \
                unit_of_time_request, md5 as unique_id FROM validrecords GROUP BY md5'''
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

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=request_payload.get_timeoff_details_payload,
            data_handler=response_filter.get_filtered_timeoff_details
        )

        process_distinct_timeoff = rail.trigger_parallel_dagrun(
            task_id='process_distinct_timeoff',
            items="{{ result('query_sum_of_units_and_distinct_md5') }}",
            parallel_count=config.parallel_dagrun_count_process_distict_projects,
            trigger_dag_id=config.process_timeoff_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_conf
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success" and
                rail.get_current_context()['dag_run'].get_task_instance(
                download_file.task_id).current_state().lower() == "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
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
        is_csv >> rail.Label('No') >> send_bad_file_format_email
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> create_log >> can_decrypt_file >> rail.Label('Yes') >> decrypt_file
        can_decrypt_file >> rail.Label('No') >> dummy_load_data
        decrypt_file >> dummy_load_data >> load_data
        load_data >> create_csv_with_md5 >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label(
            'Yes') >> [query_valid_records, query_invalid_records]
        query_valid_records >> has_valid_records >> rail.Label(
            'No') >> finish
        query_invalid_records >> has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> finish
        has_invalid_records >> rail.Label(
            'No') >> finish
        has_valid_records >> rail.Label(
            'Yes') >> query_sum_of_units_and_distinct_md5 >> get_booking_id_oef_value >> get_all_time_off_types_uris >> get_timeoff_details
        get_timeoff_details >> process_distinct_timeoff >> finish
        finish >> send_logs_enter
        send_logs_end >> can_log_to_sumo >> log_to_sumo >> can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
