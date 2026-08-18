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
from unisys.time_export.utils.custom_methods import do_format_logs


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

        # Task 2: Format and consolidate logs from child DAGs
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

        # Task 5: Send completion notification
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

        # ============================================================================
        # LOG GENERATION DAG TASK DEPENDENCIES (5 Sequential Tasks)
        # ============================================================================
        # Task 2 → Task 3 → Task 4 → Task 5
        # Sequential flow: Format Logs → Generate CSV → Create Download Link → Upload SFTP → Send Email
        format_logs >> render_logs_csv >> encrypt_time_export_data_csv >> upload_log_to_sftp >> \
        encrypt_secondary_time_export_data_csv >> upload_secondary_time_export_to_sftp >> send_import_complete_email

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_child_dag)
