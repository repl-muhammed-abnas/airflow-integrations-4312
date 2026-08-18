from datetime import timedelta
import rail
from dxctechnology.psa_organizational_unit.utils import request_payload
from dxctechnology.psa_organizational_unit.utils import response_filter
from dxctechnology.psa_organizational_unit.utils import python_callable_method
from dxctechnology.psa_organizational_unit.tasks.send_logs import get_send_logs


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.dxctechnology_psa_organizational_unit_master,
        description='DXC_PSA_ORGANIZATIONAL_UNIT Master',
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

        is_csv_pgp = rail.IfOperator(
            task_id='is_csv_pgp',
            test='{{ result("new_file_sensor") | lower | ends_with(".csv.pgp") }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | PSA Organizational Unit Import - Incorrect file format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html",
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

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            retries=0,
            source="{{ result('download_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        process_decrypted_file = rail.EmptyOperator(
            task_id='process_decrypted_file'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        load_decrypted_csv = rail.LoadCSVFileOperator(
            task_id='load_decrypted_csv',
            document="{{ result('decrypt_file') }}"
        )

        create_org_unit_collection = rail.CreateCollectionOperator(
            task_id='create_org_unit_collection',
            source='{{ result("load_decrypted_csv") }}',
            name='org_unit_input_data'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("create_org_unit_collection", "length") > 0 }}',
            yes_task='process_records',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | PSA Organizational Unit Import - No records to process - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html",
        )

        process_records = rail.EmptyOperator(
            task_id='process_records'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            name='invalidrecords',
            query="""SELECT * FROM org_unit_input_data WHERE NULLIF(organization_unit_cd, '') IS NULL"""
        )

        has_invalid_records = rail.IfOperator(
            task_id='has_invalid_records',
            test='{{ result("query_invalid_records", "length") > 0 }}',
            yes_task="log_invalid_records",
            no_task="no_invalid_records",
        )

        no_invalid_records = rail.EmptyOperator(
            task_id='no_invalid_records'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log='{{ result("create_log") }}',
            items='{{result("query_invalid_records")}}',
            message='Organization ID is Blank in feed file',
            severity='Skipped',
            properties={
                'organization_unit_cd': '{{ item.organization_unit_cd }}',
                'status': 'Skipped',
                'details': 'Organization ID is Blank in feed file',
                'ecid': '{{ dag_run_ecid() }}'
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            name='validpsaorgunits',
            query="""SELECT * FROM org_unit_input_data WHERE NULLIF(organization_unit_cd, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test='{{ result("query_valid_records", "length") > 0 }}',
            yes_task="process_valid_records",
            no_task="no_valid_records",
        )

        no_valid_records = rail.EmptyOperator(
            task_id='no_valid_records'
        )

        process_valid_records = rail.EmptyOperator(
            task_id='process_valid_records'
        )

        get_all_org_units = rail.RepliconServiceOperator(
            task_id="get_all_org_units",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_all_org_units_payload,
            data_handler=response_filter.get_all_org_units
        )

        org_unit_collection = rail.CreateCollectionOperator(
            task_id='org_unit_collection',
            name='allorgunits',
            source="{{ result('get_all_org_units') | to_json }}",
        )

        psa_parent_org_unit_uri = rail.PythonOperator(
            task_id='psa_parent_org_unit_uri',
            python_callable=python_callable_method.psa_parent_org_unit_uri
        )

        disabled_psa_org_unit = rail.QueryCollectionOperator(
            task_id='disabled_psa_org_unit',
            name='disabledpsaorgunit',
            query="""SELECT * FROM validpsaorgunits WHERE organization_unit_cd IN
                (Select name FROM allorgunits WHERE status IS FALSE)"""
        )

        has_disabled_psa_org_unit = rail.IfOperator(
            task_id='has_disabled_psa_org_unit',
            test='{{ result("disabled_psa_org_unit", "length") > 0 }}',
            yes_task="log_disabled_psa_org_unit",
            no_task="no_disabled_psa_org_unit",
        )

        no_disabled_psa_org_unit = rail.EmptyOperator(
            task_id='no_disabled_psa_org_unit'
        )

        log_disabled_psa_org_unit = rail.WriteLogOperator(
            task_id='log_disabled_psa_org_unit',
            log='{{ result("create_log") }}',
            items='{{result("disabled_psa_org_unit")}}',
            message='Organization ID is present in Disabled State',
            severity='Exception',
            properties={
                'organization_unit_cd': '{{ item.organization_unit_cd }}',
                'status': 'Exception',
                'details': 'Organization ID is present in Disabled State',
                'ecid': '{{ dag_run_ecid() }}'
            }
        )

        enabled_psa_org_unit = rail.QueryCollectionOperator(
            task_id='enabled_psa_org_unit',
            name='enabledpsaorgunits',
            query="""SELECT * FROM validpsaorgunits WHERE organization_unit_cd NOT IN
                (Select name FROM allorgunits WHERE status IS FALSE)"""
        )

        flag_enabled_org_units = rail.QueryCollectionOperator(
            task_id='flag_enabled_org_units',
            name='flag_enabled_org_units',
            query="SELECT organization_unit_cd FROM enabledpsaorgunits WHERE LOWER(psa_enabled_flg) = 'true'"
        )

        flag_disabled_org_units = rail.QueryCollectionOperator(
            task_id='flag_disabled_org_units',
            name='flag_disabled_org_units',
            query="SELECT organization_unit_cd FROM enabledpsaorgunits WHERE LOWER(psa_enabled_flg) = 'false'"
        )

        has_flag_disabled_record = rail.IfOperator(
            task_id='has_flag_disabled_record',
            test='{{ result("flag_disabled_org_units", "length") > 0 }}',
            yes_task='log_psa_false',
            no_task='no_flag_disabled_psa_org_unit',
        )

        log_psa_false = rail.WriteLogOperator(
            task_id='log_psa_false',
            log='{{ result("create_log") }}',
            items='{{ result("flag_disabled_org_units") }}',
            message='PSA Flag set to false for Organization Unit in input file',
            severity='Skipped',
            properties= {
                'organization_unit_cd': '{{ item.organization_unit_cd }}',
                'status': 'Skipped',
                'details': 'PSA Flag set to false for Organization Unit in input file',
                'ecid': '{{ dag_run_ecid() }}'
            }
        )

        no_flag_disabled_psa_org_unit = rail.EmptyOperator(
            task_id='no_flag_disabled_psa_org_unit'
        )

        has_valid_psa_org_unit = rail.IfOperator(
            task_id='has_valid_psa_org_unit',
            test='{{ result("flag_enabled_org_units", "length") > 0 }}',
            yes_task="dummy_organizational_units",
            no_task="no_valid_psa_org_unit",
        )

        no_valid_psa_org_unit = rail.EmptyOperator(
            task_id='no_valid_psa_org_unit'
        )

        dummy_organizational_units = rail.EmptyOperator(
            task_id='dummy_organizational_units'
        )

        process_psa_org_unit = rail.TriggerDagRunForEachItemOperator(
            task_id='process_psa_org_unit',
            items="{{ result('flag_enabled_org_units') }}",
            thread_pool_size=config.process_psa_org_unit_thread_pool_size,
            trigger_dag_id=config.dxctechnology_psa_process_organizational_units_child,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=request_payload.process_psa_org_unit_conf
        )

        wait_for_process_psa_org_unit = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_psa_org_unit',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_psa_org_unit") }}'
        )

        gather_process_psa_org_units_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_psa_org_units_logs',
            dag_runs='{{ result("process_psa_org_unit") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        format_log_records = rail.CreateCollectionOperator(
            task_id='format_log_records',
            source=python_callable_method.do_format_logs,
            columns=["organization_unit_cd", "status", "details", "ecid"],
            name='timeoff_bookings_records'
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
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}'
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

        new_file_sensor >> is_csv_pgp >> rail.Label("Yes") >> download_file
        is_csv_pgp >> rail.Label("No") >> send_bad_file_format_email
        download_file >> decrypt_file >> process_decrypted_file >> create_log \
            >> load_decrypted_csv >> create_org_unit_collection >> has_data
        has_data >> rail.Label("No") >> send_blank_payload_email
        has_data >> rail.Label('Yes') >> process_records

        process_records >> [query_valid_records, query_invalid_records]

        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        query_invalid_records >> has_invalid_records >> rail.Label(
            'No') >> no_invalid_records >> finish
        has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> finish
        query_valid_records >> has_valid_records >> rail.Label(
            'No') >> no_valid_records >> finish
        has_valid_records >> rail.Label("Yes") >> process_valid_records \
            >> get_all_org_units >> org_unit_collection >> psa_parent_org_unit_uri
        psa_parent_org_unit_uri >> [disabled_psa_org_unit, enabled_psa_org_unit]

        enabled_psa_org_unit >> [flag_disabled_org_units, flag_enabled_org_units]

        flag_disabled_org_units >> has_flag_disabled_record

        has_flag_disabled_record >> rail.Label("Yes") >> log_psa_false >> finish
        has_flag_disabled_record >> rail.Label("No") >> no_flag_disabled_psa_org_unit >> finish

        disabled_psa_org_unit >> has_disabled_psa_org_unit >> rail.Label(
            'Yes') >> log_disabled_psa_org_unit >> finish
        has_disabled_psa_org_unit >> rail.Label(
            'No') >> no_disabled_psa_org_unit >> finish

        flag_enabled_org_units >> has_valid_psa_org_unit

        has_valid_psa_org_unit >> rail.Label(
            'No') >> no_valid_psa_org_unit >> finish
        has_valid_psa_org_unit >> rail.Label(
            'Yes') >> dummy_organizational_units >> process_psa_org_unit >> wait_for_process_psa_org_unit >> finish

        finish >> gather_process_psa_org_units_logs \
            >> format_log_records >> send_logs_enter

        send_logs_end >> can_log_to_sumo >> log_to_sumo >> can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
