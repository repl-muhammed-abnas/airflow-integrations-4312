from datetime import timedelta
from pendulum import datetime
from dateutil.relativedelta import relativedelta
import pendulum
from capgemini.morocco_leave_balance_export_to_sopra.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null=None

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Capgemini Morocco Leave Balances Export to SOPRA Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 5, 1),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        current_date = pendulum.now(config.time_zone)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_valid_scheduled_run'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_valid_scheduled_run',
            end_task='finish_leave_balances_report',
        )

        is_valid_scheduled_run = rail.IfOperator(
            task_id='is_valid_scheduled_run',
            test=lambda: current_date.strftime("%d/%m/%Y") in config.schedules,
            yes_task='logging_details'
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.time_zone, config.ma01_filename_prefix, config.ma02_ma03_filename_prefix]
        )

        get_leave_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_leave_balance_report_details',
            report_name=config.leave_balance_report_name
        )

        run_leaves_balance_report = rail.run_report2(
            group_id='run_leaves_balance_report',
            report_params=request_payload.get_report_parameters,
            target='artifact'
        )

        is_leave_balances_report_failed = rail.IfOperator(
            task_id='is_leave_balances_report_failed',
            test="{{ (result('run_leaves_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_leave_balances_report_generation',
            no_task='leave_balances_report_has_data'
        )

        fail_leave_balances_report_generation = rail.FailOperator(
            task_id='fail_leave_balances_report_generation',
            message="{{ (result('run_leaves_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        leave_balances_report_has_data = rail.IfOperator(
            task_id='leave_balances_report_has_data',
            test="{{ result('run_leaves_balance_report.get_report_result','has_data') }}",
            yes_task='is_balances_report_has_expected_columns',
            no_task='finish_leave_balances_report'
        )

        is_balances_report_has_expected_columns = rail.IfOperator(
            task_id='is_balances_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_leaves_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='load_leave_balances_csv',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_leave_balances_csv = rail.LoadCSVFileOperator(
            task_id='load_leave_balances_csv',
            document="{{ (result('run_leaves_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=';'
        )

        create_leave_balances_data_collection = rail.CreateCollectionOperator(
            task_id='create_leave_balances_data_collection',
            source='{{ result("load_leave_balances_csv") }}',
            columns={
                "Employee ID": "employee_id",
                "Local Employee Number": "local_employee_number",
                "Time Off Type": "timeoff_type",
                "Time Off Type Description": "timeoff_type_description",
                "Current Year Balance": "current_year_balance",
                "Leave Availed": "leaves_availed",
                "Leave Balance": "leave_balance",
                "Cost Center (Current) (Full Path)": "cost_center_fullpath"
            },
            name='leave_balance_data'
        )

        query_valid_users_leave_balance_data = rail.QueryCollectionOperator(
            task_id='query_valid_users_leave_balance_data',
            query="SELECT * FROM leave_balance_data WHERE NULLIF(employee_id, '') IS NOT NULL",
            name='valid_approved_leaves_data'
        )

        is_valid_balance_data_exists = rail.IfOperator(
            task_id='is_valid_balance_data_exists',
            test='{{ result("query_valid_users_leave_balance_data", "length") > 0 }}',
            yes_task='write_leave_balance_data_csv',
            no_task='finish_leave_balances_report'
        )

        write_leave_balance_data_csv = rail.WriteCSVFileOperator(
            task_id='write_leave_balance_data_csv',
            source="{{ result('query_valid_users_leave_balance_data') }}",
            row=lambda item: custom_methods.get_leave_balance_data_rows(item, config.timeoff_paycodes),
            header=config.export_headers,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        filtered_leave_balances = rail.CreateCollectionOperator(
            task_id='filtered_leave_balances',
            source='{{ result("write_leave_balance_data_csv") }}',
            name='leave_balances_data'
        )

        finish_leave_balances_report = rail.EmptyOperator(
            task_id='finish_leave_balances_report'
        )

        process_leave_balance_export = rail.EmptyOperator(
            task_id='process_leave_balance_export'
        )

        is_data_exists = rail.IfOperator(
            task_id='is_data_exists',
            test=lambda: rail.result("filtered_leave_balances") and rail.result("filtered_leave_balances", key="length") > 0,
            yes_task='process_ma01_ma02_ma03_data',
            no_task='process_blank_leaves_data'
        )

        process_ma01_ma02_ma03_data = rail.EmptyOperator(
            task_id='process_ma01_ma02_ma03_data'
        )

        query_ma01_costcenter_data = rail.QueryCollectionOperator(
            task_id='query_ma01_costcenter_data',
            query="SELECT * FROM leave_balances_data WHERE cost_center_fullpath LIKE :ma01_costcenter",
            query_params={
                "ma01_costcenter": f"%{config.ma01_costcenter}%"
            }
        )

        is_ma01_data_exists = rail.IfOperator(
            task_id='is_ma01_data_exists',
            test='{{ result("query_ma01_costcenter_data", "length") > 0 }}',
            yes_task='create_ma01_leave_data_xml',
            no_task='process_ma01_empty_export'
        )

        create_ma01_leave_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma01_leave_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_leave_balance.xml',
            dataset='{{ result("query_ma01_costcenter_data") }}'
        )

        upload_ma01_leave_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_ma01_leave_extract_to_s3',
            source="{{ result('create_ma01_leave_data_xml') }}",
            key_name=config.s3_upload_filepath + '/{{ result("logging_details").ma01_export_filename }}.xml',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_ma01_leave_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma01_leave_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma01_leave_data_xml') }}"
        )

        upload_ma01_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma01_leave_extract_to_sftp",
            content='{{ result("encrypt_ma01_leave_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma01_export_filename }}.xml.pgp'
        )

        send_ma01_export_complete_email = rail.EmailOperator(
            task_id="send_ma01_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon leave balances extract of active employees to SOPRA for Morocco for MA01 cost center'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': config.ma01_costcenter,
                'costcenters_placeholder': 'MA01'
            }
        )

        query_ma02_ma03_costcenter_data = rail.QueryCollectionOperator(
            task_id='query_ma02_ma03_costcenter_data',
            query="""SELECT * FROM leave_balances_data WHERE cost_center_fullpath LIKE :ma02_costcenter
                OR cost_center_fullpath LIKE :ma03_costcenter
                """,
            query_params={
                "ma02_costcenter": f"%{config.ma02_costcenter}%",
                "ma03_costcenter": f"%{config.ma03_costcenter}%"
            }
        )

        is_ma02_ma03_data_exists = rail.IfOperator(
            task_id='is_ma02_ma03_data_exists',
            test='{{ result("query_ma02_ma03_costcenter_data", "length") > 0 }}',
            yes_task='create_ma02_ma03_leave_data_xml',
            no_task='process_ma02_ma03_empty_export'
        )

        create_ma02_ma03_leave_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma02_ma03_leave_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_leave_balance.xml',
            dataset='{{ result("query_ma02_ma03_costcenter_data") }}'
        )

        upload_ma02_ma03_leave_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_ma02_ma03_leave_extract_to_s3',
            source="{{ result('create_ma02_ma03_leave_data_xml') }}",
            key_name=config.s3_upload_filepath + '/{{ result("logging_details").ma02_ma03_export_filename }}.xml',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_ma02_ma03_leave_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma02_ma03_leave_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma02_ma03_leave_data_xml') }}"
        )

        upload_ma02_ma03_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma02_ma03_leave_extract_to_sftp",
            content='{{ result("encrypt_ma02_ma03_leave_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma02_ma03_export_filename }}.xml.pgp'
        )

        send_ma02_ma03_export_complete_email = rail.EmailOperator(
            task_id="send_ma02_ma03_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon leave balances extract of active employees to SOPRA for Morocco for MA02 and MA03 cost centers'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': f'{config.ma02_costcenter} and {config.ma03_costcenter}',
                'costcenters_placeholder': 'MA02 and MA03'
            }
        )

        process_blank_leaves_data = rail.EmptyOperator(
            task_id='process_blank_leaves_data'
        )

        process_ma01_empty_export = rail.EmptyOperator(
            task_id='process_ma01_empty_export'
        )

        create_ma01_blank_leave_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma01_blank_leave_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_leave_balance.xml',
            dataset=custom_methods.get_empty_export_row
        )

        encrypt_ma01_blank_leave_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma01_blank_leave_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma01_blank_leave_data_xml') }}"
        )

        upload_ma01_blank_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma01_blank_leave_extract_to_sftp",
            content='{{ result("encrypt_ma01_blank_leave_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma01_export_filename }}.xml.pgp'
        )

        send_ma01_empty_export_email = rail.EmailOperator(
            task_id='send_ma01_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon leave balances extract of active employees to SOPRA for Morocco for MA01 cost center'
                + ' - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': config.ma01_costcenter,
                'costcenters_placeholder': 'MA01'
            }
        )

        process_ma02_ma03_empty_export = rail.EmptyOperator(
            task_id='process_ma02_ma03_empty_export'
        )

        create_ma02_ma03_blank_leave_data_xml = rail.RenderTemplateOperator(
            task_id='create_ma02_ma03_blank_leave_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_leave_balance.xml',
            dataset=custom_methods.get_empty_export_row
        )

        encrypt_ma02_ma03_blank_leave_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_ma02_ma03_blank_leave_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_ma02_ma03_blank_leave_data_xml') }}"
        )

        upload_ma02_ma03_blank_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ma02_ma03_blank_leave_extract_to_sftp",
            content='{{ result("encrypt_ma02_ma03_blank_leave_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").ma02_ma03_export_filename }}.xml.pgp'
        )

        send_ma02_ma03_empty_export_email = rail.EmailOperator(
            task_id='send_ma02_ma03_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon leave balances extract of active employees to SOPRA for Morocco for MA02 and MA03 cost centers'
                + ' - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'costcenters': f'{config.ma02_costcenter} and {config.ma03_costcenter}',
                'costcenters_placeholder': 'MA02 and MA03'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_leave_extract'
        )

        fail_leave_extract = rail.FailOperator(
            task_id='fail_leave_extract',
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish_leave_balances_report
        can_run_batch_task >> rail.Label("No") >> is_valid_scheduled_run >> rail.Label("Yes") >> logging_details >> get_leave_balance_report_details \
            >> run_leaves_balance_report >> is_leave_balances_report_failed

        is_leave_balances_report_failed >> rail.Label("Yes") >> fail_leave_balances_report_generation >> finish_leave_balances_report
        is_leave_balances_report_failed >> rail.Label("No") >> leave_balances_report_has_data

        leave_balances_report_has_data >> rail.Label("Yes") >> is_balances_report_has_expected_columns
        leave_balances_report_has_data >> rail.Label("No") >> finish_leave_balances_report

        is_balances_report_has_expected_columns >> rail.Label("Yes") >> load_leave_balances_csv
        is_balances_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> finish_leave_balances_report

        load_leave_balances_csv >> create_leave_balances_data_collection >> query_valid_users_leave_balance_data \
            >> is_valid_balance_data_exists >> rail.Label("Yes") >> write_leave_balance_data_csv >> filtered_leave_balances \
                >> finish_leave_balances_report >> process_leave_balance_export >> is_data_exists

        is_data_exists >> rail.Label("Yes") >> process_ma01_ma02_ma03_data
        process_ma01_ma02_ma03_data >> query_ma01_costcenter_data >> is_ma01_data_exists
        is_data_exists >> rail.Label("No") >> process_blank_leaves_data

        is_ma01_data_exists >> rail.Label("Yes") >> create_ma01_leave_data_xml >> upload_ma01_leave_extract_to_s3 \
            >> encrypt_ma01_leave_extract_data_xml >> upload_ma01_leave_extract_to_sftp >> send_ma01_export_complete_email >> dagrun_log_to_sumo

        is_ma01_data_exists >> rail.Label("No") >> process_ma01_empty_export >> create_ma01_blank_leave_data_xml

        process_ma01_ma02_ma03_data >> query_ma02_ma03_costcenter_data >> is_ma02_ma03_data_exists
        is_ma02_ma03_data_exists >> rail.Label("Yes") >> create_ma02_ma03_leave_data_xml >> upload_ma02_ma03_leave_extract_to_s3 \
            >> encrypt_ma02_ma03_leave_extract_data_xml >> upload_ma02_ma03_leave_extract_to_sftp \
                >> send_ma02_ma03_export_complete_email >> dagrun_log_to_sumo

        is_ma02_ma03_data_exists >> rail.Label("No") >> process_ma02_ma03_empty_export >> create_ma02_ma03_blank_leave_data_xml

        is_valid_balance_data_exists >> rail.Label("No") >> finish_leave_balances_report

        process_blank_leaves_data >> process_ma01_empty_export >> create_ma01_blank_leave_data_xml >> encrypt_ma01_blank_leave_extract_data_xml \
            >> upload_ma01_blank_leave_extract_to_sftp >> send_ma01_empty_export_email >> dagrun_log_to_sumo
        process_blank_leaves_data >> process_ma02_ma03_empty_export >> create_ma02_ma03_blank_leave_data_xml >> encrypt_ma02_ma03_blank_leave_extract_data_xml \
            >> upload_ma02_ma03_blank_leave_extract_to_sftp >> send_ma02_ma03_empty_export_email >> dagrun_log_to_sumo

        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_leave_extract

    return dag

rail.for_each_instance(create_dag)
