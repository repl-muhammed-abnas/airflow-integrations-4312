from datetime import timedelta
import itertools
import rail
from dxctechnology.psa_resource_assignment_v2.utils import request_payload
from dxctechnology.psa_resource_assignment_v2.utils import python_callable_method
from dxctechnology.psa_resource_assignment_v2.utils import custom_methods
from airflow.models import Variable


null = None

# pylint: disable=too-many-statements


def create_attribute_1_master_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'DXC PSA Resource Assignment Bulk Master V2.0 {config.dag_id_postfix}',
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
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'false',
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
                            default_var='true').lower()=='false' else rail.result('download_file'),
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

        create_master_log = rail.CreateLogOperator(
            task_id='create_master_log'
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
            log='{{ result("create_master_log") }}',
            message='Gsap PSA Resource Assignment Sync Skipped - All Mandatory Fields are not present for this record',
            severity='Skipped',
            properties=lambda item: {
                'wbs': item['WBS'],
                'empid': item['PERN'],
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync Skipped - All Mandatory Fields are not present for this record'
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
            no_task="format_log_records"
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

        # NEW: Group users by WBS for bulk processing
        group_users_by_wbs = rail.PythonOperator(
            task_id='group_users_by_wbs',
            python_callable=python_callable_method.group_users_by_wbs,
            op_args=['query_all_valid_records', 'get_active_user']
        )

        create_grouped_wbs_collection = rail.CreateCollectionOperator(
            task_id='create_grouped_wbs_collection',
            source="{{ result('group_users_by_wbs') | to_json }}",
            name="grouped_wbs_data"
        )

        dummy_process_each_wbs_attribute = rail.EmptyOperator(
            task_id='dummy_process_each_wbs_attribute'
        )

        # Process each WBS with all its users in bulk
        process_each_wbs_bulk = rail.trigger_parallel_dagrun(
            task_id='process_each_wbs_bulk',
            items=lambda : rail.result("group_users_by_wbs"),
            parallel_count=config.parallel_dagrun_count_each_wbs_attribute,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_each_wbs_dagid,
            conf=request_payload.get_process_wbs_bulk
        )

        get_process_each_wbs_dag_ids =rail.PythonOperator(
            task_id= 'get_process_each_wbs_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_each_wbs_bulk_{x+1}'), range(config.parallel_dagrun_count_each_wbs_attribute))))),
            show_return_value_in_logs= False
        )

        gather_wbs_process_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_wbs_process_logs',
            dag_runs='{{ result("get_process_each_wbs_dag_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        format_log_records = rail.CreateCollectionOperator(
            task_id='format_log_records',
            source=python_callable_method.do_format_logs,
            columns=[
                'empid',
                'wbs',
                'action',
                'status',
                'details',
                'ecid'],
            name='final_log_records'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_log_records'),
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
            bcc="{%- if result('format_log_records', key='get_errored_records') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon GSAP PSA Resource Assignment sync -  " }} \
                {%- if result("format_log_records", key="get_errored_records") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_log_records", key="get_exception_records") > 0 -%} \
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
        can_decrypt_file >> rail.Label("No") >> load_data_from_file >> load_psa_resource_data >> create_master_log >> create_psa_resource_data_collection >> has_psa_resource_data
        has_psa_resource_data >> rail.Label("No") >> send_blank_payload_email
        has_psa_resource_data >> rail.Label(
            "Yes") >> query_invalid_records >> has_invalid_records >> rail.Label("Yes") >> log_invalid_records >> query_all_valid_records
        has_invalid_records >> rail.Label(
            "No") >> no_invalid_records >> query_all_valid_records
        query_all_valid_records >> has_psa_resource_to_be_created
        has_psa_resource_to_be_created >> rail.Label(
            "No") >> format_log_records
        has_psa_resource_to_be_created >> rail.Label(
            "Yes") >> get_c1_leanstaffing_import_base_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "YES") >> load_report_data >> get_active_user >> group_users_by_wbs >> create_grouped_wbs_collection >> dummy_process_each_wbs_attribute >> process_each_wbs_bulk
        process_each_wbs_bulk >> get_process_each_wbs_dag_ids >> gather_wbs_process_logs >> format_log_records
        format_log_records >> render_logs_csv \
            >> upload_log_to_sftp >> send_import_complete_email >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo
        report_has_data >> rail.Label('NO') >> fail_no_report_data

    return dag


rail.for_each_instance(create_attribute_1_master_dag)