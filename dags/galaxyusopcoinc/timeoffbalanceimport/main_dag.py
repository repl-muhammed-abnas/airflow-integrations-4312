from datetime import timedelta, datetime
import itertools
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split

from galaxyusopcoinc.timeoffbalanceimport.utils import response_filter, request_payload

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_timeoffbalance_import_master_{config.instance}',
        description='Vialto Partners Timeoff Balance Import Automation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
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
            subject='{{ get_company_key() }} | Replicon TimeOff balance import - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=request_payload.do_has_file_content,
            yes_task='load_decrypted_data',
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

        load_decrypted_data = rail.LoadCSVFileOperator(
            task_id='load_decrypted_data',
            document="{{ result('decrypt_file') }}",
            delimiter=config.delimiter
        )

        create_master_log = rail.CreateLogOperator(
            task_id='create_master_log'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_decrypted_data') }}",
            name="inputdatacollection",
            columns={
                'BatchID': 'batchid',
                'EmployeeID': 'employeeid',
                'AbsencePlan': 'absenceplan',
                'ReferenceID': 'referenceid',
                'EffectiveDate': 'effectivedate',
                'Balance': 'balance',
                'Units': 'units',
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_md5',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon TimeOff balance import - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{result('create_input_data_collection')}}",
            columns=['batchid', 'employeeid', 'absenceplan',
                     'referenceid', 'effectivedate', 'balance', 'units', 'md5'],
            data=request_payload.get_create_md5_data
        )

        input_data_with_md5 = rail.CreateCollectionOperator(
            task_id="input_data_with_md5",
            name="input_data",
            source="{{result('create_md5')}}"
        )

        get_reference_file = rail.SFTPDownloadFileOperator(
            task_id="get_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_file,
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('get_reference_file')}}",
        )

        create_reference_data_collection = rail.CreateCollectionOperator(
            task_id="create_reference_data_collection",
            name="reference_data",
            source="{{result('parse_reference_file')}}"
        )

        get_delta_records = rail.QueryCollectionOperator(
            task_id="get_delta_records",
            query="""SELECT * FROM input_data WHERE md5 NOT IN (SELECT DISTINCT MD5 FROM reference_data)"""
        )

        has_any_changed_records = rail.IfOperator(
            task_id="has_any_changed_records",
            test="{{result('get_delta_records', 'length') > 0}}",
            yes_task="get_all_scripts",
            no_task="no_changed_records"
        )

        no_changed_records = rail.EmptyOperator(
            task_id='no_changed_records'
        )

        get_unchanged_records = rail.QueryCollectionOperator(
            task_id="get_unchanged_records",
            query="""SELECT * FROM input_data WHERE md5 IN (SELECT DISTINCT MD5 FROM reference_data)"""
        )

        has_any_unchanged_records = rail.IfOperator(
            task_id="has_any_unchanged_records",
            test="{{result('get_unchanged_records', 'length') > 0}}",
            yes_task="log_unchanged_records",
            no_task="dummy_process_log_generation"
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id="log_unchanged_records",
            log = '{{result("create_master_log")}}',
            items="{{result('get_unchanged_records')}}",
            message="No change to effective date and balance",
            severity="Skipped",
            properties=lambda item: {
                "batchid": item['batchid'],
                "employeeid": item['employeeid'],
                "referenceid": item['referenceid'],
                'status': "Skipped",
            }
        )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
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

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query="""SELECT * FROM get_delta_records WHERE NULLIF(employeeid, '') IS NOT NULL and
                    NULLIF(referenceid, '') IS NOT NULL and NULLIF(effectivedate, '') IS NOT NULL and NULLIF(balance, '') IS NOT NULL"""
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
            query="""SELECT * FROM get_delta_records WHERE NULLIF(employeeid, '') IS NULL or NULLIF(referenceid, '') IS NULL
                     or NULLIF(effectivedate, '') IS NULL or NULLIF(balance, '') IS NULL"""
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
            log = '{{result("create_master_log")}}',
            items='{{result("query_invalid_records")}}',
            message='Required fields are Missing',
            severity='Exception',
            properties=lambda item: {
                'batchid': item['batchid'],
                'employeeid': item['employeeid'],
                'referenceid': item['referenceid'],
                'status': 'Exception',
            }
        )

        query_distinct_employees = rail.QueryCollectionOperator(
            task_id="query_distinct_employees",
            query="""SELECT DISTINCT employeeid FROM query_valid_records """
        )

        process_distinct_employee = rail.trigger_parallel_dagrun(
            task_id='process_distinct_employee',
            items="{{ result('query_distinct_employees') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_employees,
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            trigger_dag_id=f'vialtopartners_timeoffbalance_import_process_employee_child_{config.instance}',
            conf=request_payload.get_conf
        )

        get_process_employee_task_ids =rail.PythonOperator(
            task_id= 'get_process_employee_task_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_distinct_employee_{x+1}'), range(config.trigger_parallel_dagrun_count_process_employees))))),
            show_return_value_in_logs= False
        )

        gather_employee_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_employee_logs',
            dag_runs='{{ result("get_process_employee_task_ids") }}',
            dagrun_task_id='create_employee_log',
            execution_timeout=timedelta(
                hours=config.gather_employee_logs_timeout_hours),
            flatten=True
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source=lambda: rail.result('input_data_with_md5'),
            header=['BatchID', 'EmployeeID', 'AbsencePlan',
                    'ReferenceID', 'EffectiveDate', 'Balance', 'Units', 'MD5'],
            row=[
                '{{item.batchid}}',
                '{{item.employeeid}}',
                '{{item.absenceplan}}',
                '{{item.referenceid}}',
                '{{item.effectivedate}}',
                '{{item.balance}}',
                '{{item.units}}',
                '{{item.md5}}',
            ]
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            new_filename=config.archive_filepath +
            "/INT016_Timeoff_Balance_reference_file_" +
            (datetime.now()).strftime("%Y%m%d%H%M")+".csv",
            existing_filename=config.reference_file
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_new_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            content="{{result('create_reference_file')}}",
            remote_filepath=config.reference_file
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.child_wait_execution_timeout),
            trigger_dag_id=f'vialtopartners_timeoffbalance_import_process_log_generation_{config.instance}',
            conf=lambda dag_run:{
                'employeelogs': rail.result('gather_employee_logs'),
                'masterlogs': rail.result('create_master_log'),
                # pylint: disable=line-too-long
                'log_filename': f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0] }.csv'
            }
        )

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
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> archive_file >> decrypt_file >> has_file_content >> rail.Label(
            'Yes') >> load_decrypted_data
        has_file_content >> rail.Label('No') >> send_blank_payload_email
        load_decrypted_data >> create_master_log >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label(
            'Yes') >> create_md5 >> input_data_with_md5 >> get_reference_file >> parse_reference_file >> create_reference_data_collection
        create_reference_data_collection >> [
            get_delta_records, get_unchanged_records]
        get_delta_records >> has_any_changed_records >> rail.Label(
            "Yes") >> get_all_scripts
        has_any_changed_records >> rail.Label(
            "No") >> no_changed_records >> dummy_process_log_generation
        get_unchanged_records >> has_any_unchanged_records >> rail.Label(
            "Yes") >> log_unchanged_records >> dummy_process_log_generation
        has_any_unchanged_records >> rail.Label(
            "No") >> dummy_process_log_generation
        get_all_scripts >> get_all_time_off_types_uris >> get_timeoff_details
        get_timeoff_details >> [query_valid_records, query_invalid_records]
        query_invalid_records >> has_invalid_records
        query_valid_records >> has_valid_records >> rail.Label(
            'Yes') >> query_distinct_employees
        query_distinct_employees >> process_distinct_employee >> get_process_employee_task_ids >> gather_employee_logs
        gather_employee_logs >> create_reference_file
        create_reference_file >> archive_reference_file >> upload_new_reference_file
        has_valid_records >> rail.Label(
            'No') >> no_valid_records_present >> create_reference_file
        has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> create_reference_file
        has_invalid_records >> rail.Label(
            'No') >> no_invalid_records_present >> create_reference_file

        upload_new_reference_file >> dummy_process_log_generation >> process_log_generation >> can_log_to_sumo >> log_to_sumo
        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
