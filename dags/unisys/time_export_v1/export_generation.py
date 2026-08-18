"""
Unisys Fieldglass Time Export Integration - Log Generation and File Upload DAG
Consolidates logs from all child DAGs and generates final Fieldglass CSV export

This DAG handles the final processing phase:
1. Consolidate logs from multiple child DAG executions
2. Format data into Fieldglass CSV format
3. Generate downloadable file with PGP encryption
4. Upload to SFTP server for Fieldglass consumption
5. Send completion notification emails

Based on design document: Replicon to Fieldglass Integration - Technical Specification V1.1
"""
from datetime import timedelta
import rail
from unisys.time_export_v1.utils import custom_methods
from unisys.time_export_v1.utils.custom_methods import do_format_logs


def create_child_dag(config):
    """
    Create log generation DAG for final file processing and delivery

    Args:
        config: Configuration object containing SFTP, email, and file settings

    Returns:
        Configured DAG for log consolidation and file delivery
    """

    with rail.create_airflow_dag(
        dag_id=config.export_generation,
        description=f'Unisys Fieldglass Time Export - Process Log Generation {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_export_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        # Task 1: Display DAG run configuration for debugging
        # Shows log artifacts and filename passed from master DAG
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Task 2: Get log filename
        # Generates the log filename based on configuration
        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=lambda: custom_methods.get_log_file_name(config.utc_timezone, config.file_prefix_map.get(config.company_key, 'Dev').upper() )
        )

        # Task 3: Format and consolidate logs from child DAGs
        # Merges all log artifacts from parallel child DAG executions
        # Transforms log data into Fieldglass export format
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        # Task 3: Generate CSV file content
        # Creates CSV file with proper Fieldglass headers and weekly timesheet format
        # Maps data to required columns: WorkOrder_ID, Date, Rate_Category_Code, daily hours
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=config.csv_header,
            row=[
                "{{ item.WorkOrder_ID }}",        # Purchase Order ID from Replicon
                "{{ item.Date }}",                # Week start date (Saturday) in MM/DD/YYYY
                "{{ item.Rate_Category_Code }}",  # Rate type (REGULAR, OVERTIME, etc.)
                "{{ item.Sat_Hrs }}",             # Saturday hours with 2 decimal places
                "{{ item.Sun_Hrs }}",             # Sunday hours with 2 decimal places
                "{{ item.Mon_Hrs }}",             # Monday hours with 2 decimal places
                "{{ item.Tue_Hrs }}",             # Tuesday hours with 2 decimal places
                "{{ item.Wed_Hrs }}",             # Wednesday hours with 2 decimal places
                "{{ item.Thu_Hrs }}",             # Thursday hours with 2 decimal places
                "{{ item.Fri_Hrs }}",             # Friday hours with 2 decimal places
            ]
        )

        encrypt_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_time_export_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('render_logs_csv') }}"
        )

        # Task 4: Upload file to SFTP server
        # Delivers CSV file to Fieldglass SFTP server for processing
        # File will be PGP encrypted as per design requirements
        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('encrypt_time_export_data_csv') }}",
            remote_filepath=config.export_csv_filepath +
            '/'+"{{dag_run.conf.log_filename}}.pgp",
        )

        encrypt_secondary_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_secondary_time_export_data_csv',
            pgp_conn_id=config.secondary_pgp_conn_id,
            source="{{ result('render_logs_csv') }}"
        )

        upload_secondary_time_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_secondary_time_export_to_sftp',
            content='{{ result("encrypt_secondary_time_export_data_csv") }}',
            remote_filepath=config.export_csv_to_secondary_filepath +
            '/'+"{{ dag_run.conf.log_filename }}.pgp"
        )

        # Task 6: Get log file details for audit
        # Generates audit log content with export details
        get_log_file_details = rail.PythonOperator(
            task_id="get_log_file_details",
            python_callable=lambda dag_run: custom_methods.build_log_message(dag_run, config)
        )

        # Task 7: Generate log file
        # Creates the audit log file content
        generate_log_file = rail.WriteCSVFileOperator(
            task_id="generate_log_file",
            source="{{ result('get_log_file_details') }}",
            header=None,
            row=[
                '{{ item | attr_or_default("log", "") }}'
            ]
        )

        # Task 8: Upload log file to SFTP
        # Uploads the audit log file to SFTP server
        upload_log_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_file_to_sftp",
            content='{{ result("generate_log_file") }}',
            remote_filepath=f"{config.export_logs_csv_filepath}/{{{{ result('get_log_file_name') }}}}"
        )

        upload_ops_log_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_ops_log_file_to_sftp",
            content='{{ result("generate_log_file") }}',
            remote_filepath=f"{config.export_csv_to_secondary_filepath}/{{{{ result('get_log_file_name') }}}}"
        )

        get_base_report_filenames = rail.PythonOperator(
            task_id="get_base_report_filenames",
            python_callable=lambda dag_run: custom_methods.get_base_report_filenames(dag_run)
        )
        
        write_employee_pay_report_csv = rail.WriteCSVFileOperator(
            task_id="write_employee_pay_report_csv",
            source = lambda: rail.load_all_records(rail.get_dag_run_conf()['base_employee_pay_report_data']),
            header=[c.strip() for c in config.expected_employee_pay_report_columns.split(',')],
            row=[
                "{{ item.timesheet_period }}",
                "{{ item.timesheet_period_uri }}",
                "{{ item.total_hours }}",
                "{{ item.approval_date }}",
                "{{ item.approval_status }}",
                "{{ item.user_name }}",
                "{{ item.entry_date }}",
                "{{ item.pay_code_name }}",
                "{{ item.purchase_order_id }}",
                "{{ item.user_type_full_path }}",
                "{{ item.week_start_date }}",
                "{{ item.timesheet_start_date }}",
            ]
        )

        encrypt_employee_pay_report_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_employee_pay_report_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_employee_pay_report_csv') }}",
        )

        upload_employee_pay_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_employee_pay_report_to_sftp',
            content="{{ result('encrypt_employee_pay_report_csv') }}",
            remote_filepath=f"{config.export_base_report_filepath}/{{{{ result('get_base_report_filenames').base_report1 }}}}.pgp",
        )

        write_timesheet_period_report_csv = rail.WriteCSVFileOperator(
            task_id="write_timesheet_period_report_csv",
            source=lambda: rail.load_all_records(rail.get_dag_run_conf()['base_timesheet_period_report_data']),
            header=[c.strip() for c in config.expected_timesheet_period_report_columns.split(',')],
            row=[
                "{{ item.timesheet_period }}",
                "{{ item.timesheet_period_uri }}",
                "{{ item.total_hrs }}",
                "{{ item.approval_date }}",
                "{{ item.approval_status }}",
                "{{ item.user_name }}",
                "{{ item.pay_code_name }}",
                "{{ item.purchase_order_id }}",
                "{{ item.user_type_full_path }}",
                "{{ item.week_start_date }}",
                "{{ item.timesheet_start_date }}",
            ]
        )

        encrypt_timesheet_period_report_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_timesheet_period_report_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_timesheet_period_report_csv') }}",
        )

        upload_timesheet_period_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_timesheet_period_report_to_sftp',
            content="{{ result('encrypt_timesheet_period_report_csv') }}",
            remote_filepath=f"{config.export_base_report_filepath}/{{{{ result('get_base_report_filenames').base_report2 }}}}.pgp",
        )

        # Task 9: Send completion notification
        # Notifies stakeholders that time export process completed successfully
        # Includes processing summary and download link for verification
        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() + " | Time Export is completed successfully - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/success_notification.html",
            params={
                'filepath': config.export_csv_filepath,
                'secondary_filepath': config.export_csv_to_secondary_filepath
            }
        )

        get_log_file_name >> format_logs >> render_logs_csv >> encrypt_time_export_data_csv >> upload_log_to_sftp >> \
        encrypt_secondary_time_export_data_csv >> upload_secondary_time_export_to_sftp >> get_base_report_filenames >> \
        write_employee_pay_report_csv >> encrypt_employee_pay_report_csv >> upload_employee_pay_report_to_sftp >> write_timesheet_period_report_csv >> encrypt_timesheet_period_report_csv >> upload_timesheet_period_report_to_sftp >> \
        get_log_file_details >> generate_log_file >> upload_log_file_to_sftp >> upload_ops_log_file_to_sftp >> send_import_complete_email

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_child_dag)
