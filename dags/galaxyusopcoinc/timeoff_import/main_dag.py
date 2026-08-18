from datetime import timedelta
from airflow.models import Variable
import rail
from galaxyusopcoinc.timeoff_import.utils import response_filter
from galaxyusopcoinc.timeoff_import.utils import request_payload
from galaxyusopcoinc.timeoff_import.tasks.send_logs import get_send_logs


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_timeoff_import_import_master_{config.instance}',
        description='Vialto Partners Timeoff Import Automation',
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
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
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

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=Variable.get(config.can_decrypt_file, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=request_payload.do_has_file_content,
            yes_task='dummy_load_data',
            no_task='send_blank_payload_email'
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
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file).lower()== 'true' else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}",
            delimiter=config.delimiter
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'EmployeeID': 'employeeid',
                'ReferenceID': 'referenceid',
                'TimeOffEntryID': 'timeoffentryid',
                'TimeOffStartDate': 'timeoffstartdate',
                'TimeOffEndDate': 'timeoffenddate',
                'TimeOffDescription': 'timeoffdescription',
                'Flag': 'flag',
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

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(employeeid, '') IS NOT NULL and
                    NULLIF(referenceid, '') IS NOT NULL and NULLIF(timeoffentryid, '') IS NOT NULL
                    and NULLIF(timeoffstartdate, '') IS NOT NULL and NULLIF(timeoffenddate, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task="query_distinct_employees",
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(employeeid, '') IS NULL or
                    NULLIF(referenceid, '') IS NULL or NULLIF(timeoffentryid, '') IS NULL
                    or NULLIF(timeoffstartdate, '') IS NULL or NULLIF(timeoffenddate, '') IS NULL"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="no_invalid_records_present"
        )

        no_invalid_records_present = rail.EmptyOperator(
            task_id='no_invalid_records_present'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            message='Required fields are Missing',
            severity='Exception',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'referenceid': item['referenceid'],
                'timeoffentryid': item['timeoffentryid'],
                'status': 'Exception',
            }
        )

        query_distinct_employees = rail.QueryCollectionOperator(
            task_id='query_distinct_employees',
            query='''SELECT DISTINCT employeeid FROM validrecords'''
        )

        get_hidden_oef_value = rail.RepliconServiceOperator(
            task_id='get_hidden_oef_value',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data=request_payload.get_hidden_oef_value_payload,
            response_filter=response_filter.get_hidden_oef_value
        )

        get_all_time_off_types_uris = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_uris',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=response_filter.get_time_off_type_uris
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=request_payload.get_timeoff_details_payload,
            response_filter=response_filter.get_filtered_timeoff_details
        )

        process_distinct_employees = rail.TriggerDagRunForEachItemOperator(
            task_id='process_distinct_employees',
            retries=0,
            items="{{ result('query_distinct_employees') }}",
            trigger_dag_id=f'vialtopartners_timeoff_import_child_process_employees_{config.instance}',
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            conf=request_payload.get_conf
        )

        wait_for_process_distinct_employees = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_distinct_employees',
            dag_runs='{{ result("process_distinct_employees") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        send_logs_enter, _ = get_send_logs(config)

        new_file_sensor >> is_csv >> rail.Label(
            'Yes') >> download_file >> was_new_file_found
        is_csv >> rail.Label('No') >> send_bad_file_format_email
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> archive_file >> can_decrypt_file >> rail.Label('Yes') >> decrypt_file
        can_decrypt_file >> rail.Label('No') >> dummy_load_data
        decrypt_file >> has_file_content >> rail.Label(
            'Yes') >> dummy_load_data >> load_data
        has_file_content >> rail.Label('No') >> send_blank_payload_email
        load_data >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label(
            'Yes') >> [query_valid_records, query_invalid_records]
        query_valid_records >> has_valid_records >> rail.Label(
            'No') >> no_valid_records_present >> send_logs_enter
        query_invalid_records >> has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> send_logs_enter
        has_invalid_records >> rail.Label(
            'No') >> no_invalid_records_present >> send_logs_enter
        has_valid_records >> rail.Label(
            'Yes') >> query_distinct_employees >> get_hidden_oef_value >> get_all_time_off_types_uris >> get_timeoff_details
        get_timeoff_details >> process_distinct_employees >> wait_for_process_distinct_employees >> send_logs_enter

    return dag


rail.for_each_instance(create_main_dag)
