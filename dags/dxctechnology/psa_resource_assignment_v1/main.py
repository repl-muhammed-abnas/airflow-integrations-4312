from datetime import timedelta
import rail
from dxctechnology.psa_resource_assignment_v1.utils import request_payload
from dxctechnology.psa_resource_assignment_v1.utils import python_callable_method
from dxctechnology.psa_resource_assignment_v1.utils import custom_methods
from airflow.models import Variable


null = None

# pylint: disable=too-many-statements


def create_attribute_1_master_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_psa_resource_assignment_master_{config.dag_id_postfix}_v1',
        description=f'DXC PSA Resource Assignment Master V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='load_data_from_file'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        load_data_from_file = rail.PythonOperator(
            task_id="load_data_from_file",
            python_callable=lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file_var_name,
                            default_var='true').lower()=='true' else rail.result('download_file'),
            show_return_value_in_logs=False
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

        load_psa_resource_data = rail.LoadCSVFileOperator(
            task_id='load_psa_resource_data',
            document="{{ result('load_data_from_file') }}",
            delimiter="|"
        )

        create_psa_resource_data_collection = rail.CreateCollectionOperator(
            task_id='create_psa_resource_data_collection',
            source="{{ result('load_psa_resource_data') }}",
            name="psaresourcedata",
            columns={
                'PERN': 'PERN',
                'WBS': 'WBS',
                'StartDate': 'StartDate',
                'EndDate': 'EndDate',
            }
        )

        has_psa_resource_data = rail.IfOperator(
            task_id='has_psa_resource_data',
            test="{{ result('create_psa_resource_data_collection','length') > 0 }}",
            yes_task='query_invalid_records',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon PSA Resource Assignment - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/bad_file_format.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            name='invalidrecords',
            query="""SELECT * FROM psaresourcedata
                    WHERE NULLIF(PERN, '') IS NULL OR NULLIF(WBS, '') IS NULL OR NULLIF(StartDate, '') IS NULL
                    OR NULLIF(EndDate, '') IS NULL"""
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
            items='{{result("query_invalid_records")}}',
            message='Gsap PSA Resource Assignment Sync Skipped - All Mandatory Feilds are not present for this record',
            severity='Skipped',
            properties=lambda item: {
                'wbs': item['WBS'],
                'empid': item['PERN'],
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync Skipped - All Mandatory Feilds are not present for this record'
            }
        )

        query_all_valid_records = rail.QueryCollectionOperator(
            task_id='query_all_valid_records',
            query="""SELECT * FROM psaresourcedata
                    WHERE NULLIF(PERN, '') IS NOT NULL AND NULLIF(WBS, '') IS NOT NULL AND NULLIF(StartDate, '') IS NOT NULL
                    AND NULLIF(EndDate, '') IS NOT NULL"""
        )

        has_psa_resource_to_be_created = rail.IfOperator(
            task_id='has_psa_resource_to_be_created',
            test='{{ result("query_all_valid_records", "length") > 0 }}',
            yes_task="get_c1_leanstaffing_import_base_report_details",
            no_task="generate_output_log"
        )

        get_c1_leanstaffing_import_base_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_c1_leanstaffing_import_base_report_details',
            report_name=config.extract_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='c1_leanstaffing_import_base_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_c1_leanstaffing_import_base_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('c1_leanstaffing_import_base_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data',
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="Report \"**C1 Lean staffing Import base report\" execution failed",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('c1_leanstaffing_import_base_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_active_user = rail.PythonOperator(
            task_id="get_active_user",
            python_callable=python_callable_method.active_user,
            op_args=['load_report_data']
        )

        dummy_process_each_wbs_attribute = rail.EmptyOperator(
            task_id='dummy_process_each_wbs_attribute'
        )

        process_each_wbs_attribute = rail.trigger_parallel_dagrun(
            task_id='process_each_wbs_attribute',
            items='{{ result("query_all_valid_records") }}',
            parallel_count=config.parallel_dagrun_count_each_wbs_attribute,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_psa_resource_process_wbs_{config.dag_id_postfix}_v1',
            conf=request_payload.get_process_each_wbs
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        get_exception_logs = rail.FilterLogEntriesOperator(
            task_id='get_exception_logs',
            properties={'status': 'Exception'}
        )

        get_success_logs = rail.FilterLogEntriesOperator(
            task_id='get_success_logs',
            properties={'status': 'Success'}
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ get_master_log() | load_all_records | to_json }}"
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=custom_methods.get_filtered_logs,
            header=[
                'PERN__C',
                'WBS__C',
                'Transaction_Type__C',
                'Status__C',
                'Status_Description__C',
                'Replicon_Job__ID'],
            row=[
                '{{ item | attr_or_default("empid", "") }}',
                '{{ item | attr_or_default("wbs", "") }}',
                '{{ item | attr_or_default("action", "") }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.ecid }}']
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("render_logs_csv") }}',
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon GSAP PSA Resource Assignment sync -  " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath,
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

        new_file_sensor >> download_file

        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label(
                "Yes") >> archive_file
        was_new_file_found >> rail.Label(
            "No") >> delete_this_dagrun

        download_file >> can_decrypt_file
        can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> load_data_from_file
        can_decrypt_file >> rail.Label("No") >> load_data_from_file >> load_psa_resource_data >> create_psa_resource_data_collection >> has_psa_resource_data
        has_psa_resource_data >> rail.Label("No") >> send_blank_payload_email
        has_psa_resource_data >> rail.Label(
            "Yes") >> query_invalid_records >> has_invalid_records >> rail.Label("Yes") >> log_invalid_records >> query_all_valid_records
        has_invalid_records >> rail.Label(
            "No") >> no_invalid_records >> query_all_valid_records
        query_all_valid_records >> has_psa_resource_to_be_created
        has_psa_resource_to_be_created >> rail.Label(
            "No") >> generate_output_log
        has_psa_resource_to_be_created >> rail.Label(
            "Yes") >> get_c1_leanstaffing_import_base_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "YES") >> load_report_data >> get_active_user >> dummy_process_each_wbs_attribute >> process_each_wbs_attribute >> generate_output_log
        generate_output_log >> get_errored_logs >> get_exception_logs >> get_success_logs >> load_master_log >> render_logs_csv \
            >> upload_log_to_sftp >> send_import_complete_email >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo
        report_has_data >> rail.Label('NO') >> fail_no_report_data

    return dag


rail.for_each_instance(create_attribute_1_master_dag)
