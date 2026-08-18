# log_generation.py

import rail


# Import utilities
from tsystems.cost_center_hierarchy_import_v1.utils.custom_methods import process_logs_and_filter_logs, get_email_details_callable

def create_log_generation_dag(config):
    """
    Creates the DAG for log generation and formatting in T-Systems Cost Center Hierarchy Import.
    This DAG collects logs from child DAGs and formats them into a CSV file.

    :param config: Configuration module with settings for the instance
    :return: The created DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.log_generation_dag_id,
        description=f'T-Systems Cost Center Hierarchy Import - Log Generation DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,  # This DAG is only triggered by the master DAG
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }

    ) as dag:

        # View incoming parameters for debugging
        view_conf = rail.ViewDagRunConfOperator(task_id="view_conf")

        # Process logs to create CSV content
        process_logs = rail.PythonOperator(
            task_id='process_logs',
            python_callable=process_logs_and_filter_logs,
            show_return_value_in_logs=False
        )

        # Generate CSV file
        generate_csv_file = rail.WriteCSVFileOperator(
            task_id='generate_csv_file',
            source=lambda: rail.result('process_logs'),
            header=['costCenterCode', 'managerID', 'action', 'status', 'processInfo', 'jobId'],
            row=lambda item: [
                item["CostCenterCode"],
                item["ManagerId"],
                item["Action"],
                item["Status"],
                item["Details"],
                item["JobId"],
            ]
        )

        get_email_details = rail.PythonOperator(
            task_id = "get_email_details",
            python_callable=lambda dag_run: get_email_details_callable(dag_run, config.timezone)
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('generate_csv_file')}}",
            output_file_name="{{ result('get_email_details').log_file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        upload_log = rail.SFTPUploadFileOperator(
            task_id='upload_log',
            content="{{ result('generate_csv_file') }}",
            remote_filepath=config.log_filepath + "{{ result('get_email_details').log_file_name }}"
        )

        # Log success message
        # Send success email
        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc="{%- if result('process_logs', 'error') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject="""{{ get_company_key() }} | Replicon Cost Center Hierarchy Import - {%- if result("process_logs", key="error") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("process_logs", key="exception") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} - {{ result('get_email_details').email_timestamp }}""",
            html_content="templates/emails/success_email.html",
        )

        # Define task dependencies
        view_conf >> process_logs >> generate_csv_file >> get_email_details >> generate_download_link >> upload_log >> send_success_email

        return dag

# Create DAGs for each instance
rail.for_each_instance(create_log_generation_dag)