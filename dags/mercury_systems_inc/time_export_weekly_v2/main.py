from datetime import timedelta
from pendulum import datetime
import pendulum
from airflow.models import Variable

import rail

from mercury_systems_inc.time_export_weekly_v2.tasks.time_data_export import time_data_export
from mercury_systems_inc.time_export_weekly_v2.tasks.user_report import user_data_export
from mercury_systems_inc.time_export_weekly_v2.tasks.update_export_status import cancel_time_export
from mercury_systems_inc.time_export_weekly_v2.utils import custom_methods
from mercury_systems_inc.time_export_weekly_v2.utils import request_payload

null = None


def create_dag(config):
    """
    Creates the weekly time export DAG for Mercury Systems.
    
    This DAG runs weekly on Mondays and exports approved time entries from the previous week
    (Saturday to Monday) to an SFTP location.

    Args:
        config: Configuration object with DAG settings

    Returns:
        dag: Configured Airflow DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Mercury Systems Inc Weekly Time Export {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        # Record start time of the process
        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        # Generate logging details for the export
        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config]
        )

 
        timeoff_report_complete = rail.EmptyOperator(task_id="timeoff_report_complete")

        run_user_report = user_data_export(config)
        # Get time download script for file format
        get_time_download_script = rail.RepliconServiceOperator(
            task_id='get_time_download_script',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.time_export_file_format, 'uri')
        )

        # Define task group ID for time data export
        group_id = 'time_data_export'

        # Create time data export task group
        time_export_batch_start, time_export_batch_end = time_data_export(
            group_id=group_id,
            get_export_name='{{ result("logging_details").time_export_filename }}'
        )

        # Check if export has data
        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("time_data_export.query_time_data_export_collection", "length") > 0 }}',
            yes_task='write_time_data_to_csv',
            no_task='update_export_name_to_no_data'
        )

        # Update export name for empty exports
        update_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id="update_export_name_to_no_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda:{
                "target": {
                    "uri": rail.result("time_data_export.get_export_uri")
                },
                "name": rail.result('logging_details')["time_export_filename_nodata"]
            }
        )

        # Send email for empty exports
        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Weekly Time Export - No records to export - {{ result("process_start_time") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'export_start_date': '{{ result("logging_details").export_start_date }}',
                'export_end_date': '{{ result("logging_details").export_end_date }}',
                'filename': '{{ result("logging_details").time_export_filename_nodata }}'
            }
        )

        # Log empty export to Sumo
        log_to_sumo_time_export_no_data = rail.SendToSumoOperator(
            task_id="log_to_sumo_time_export_no_data",
            data={
                'jobstarttime': '{{ result("process_start_time") }}',
                'jobendtime': '{{ current_time_in_specified_tz("UTC", "%Y-%m-%dT%H:%M:%S") }}',
                'exportperiod': '{{ result("logging_details").export_start_date }} - {{ result("logging_details").export_end_date }}',
                'exportfilename': '{{ result("logging_details").time_export_filename_nodata }}',
                'exportfilepath': '',
                'numberofrecords': '0'
            },
            sumo_conn_id=config.sumo_conn_id
        )

        # Write time data to CSV for non-empty exports
        write_time_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_time_data_to_csv',
            source='{{ result("time_data_export.query_time_data_export_collection") }}',
            header=["EMPLOYEE ID",	
                    "PROJECT NAME",
                    "WORK ORDER / PROJECT ID",
                    "TASK NAME",
                    "OPERATION / TASK ID",
                    "POSTING DATE",
                    "HOURS",
                    "EMPLOYEE APPROVAL",
                    "MANAGER APPROVAL",
                    "EMPLOYEE OU",
                    "EMPLOYEE CHARGE TYPE",
                    "FIRST NAME",
                    "LAST NAME",
                    "EMPLOYEE DEPARTMENT",
                    "CHARGE TYPE",
                    "TIME ENTRY ID",
                    "TIME OFF BOOKING ID"
                    ],
            row=lambda item: custom_methods.get_time_data_csv_rows(item),
            delimiter=',',
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size
        )

        # Upload CSV file to SFTP
        upload_time_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_time_export_to_sftp',
            content='{{ result("write_time_data_to_csv") }}',
            remote_filepath=config.sftp_export_file_path + '{{ result("logging_details").time_export_filename }}.csv'
        )

        # Send email for successful export
        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Weekly Time Export completed - {{ result("process_start_time") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.sftp_export_file_path
            }
        )

        # Log successful export to Sumo
        log_to_sumo_time_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_time_export",
            data={
                'jobstarttime': '{{ result("process_start_time") }}',
                'jobendtime': '{{ current_time_in_specified_tz("UTC", "%Y-%m-%dT%H:%M:%S") }}',
                'exportperiod': '{{ result("logging_details").export_start_date }} - {{ result("logging_details").export_end_date }}',
                'exportfilename': '{{ result("logging_details").time_export_filename }}.csv',
                'exportfilepath': config.sftp_export_file_path,
                'numberofrecords': "{{ result('time_data_export.query_time_data_export_collection', 'length') }}"
            },
            sumo_conn_id=config.sumo_conn_id
        )

        # Handle export failures
        mark_timedata_export_error = rail.EmptyOperator(
            task_id='mark_timedata_export_error',
            trigger_rule='one_failed'
        )

        # Get export URI in case of failure
        get_export_uri_failed = rail.RepliconServiceOperator(
            task_id='get_export_uri_failed',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data=lambda:{
                "timeDataExportBatchUri": rail.result('time_data_export.create_export')
            },
            data_handler=request_payload.retrieve_export_uri
        )

        # Cancel export in case of failure
        mark_export_status_cancel_start, mark_export_status_cancel_end = cancel_time_export()

        # Update export name for cancelled exports
        update_export_name_cancelled = rail.RepliconServiceOperator(
            task_id="update_export_name_cancelled",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('get_export_uri_failed') }}"
                },
                "name": "{{ result('logging_details').time_export_filename_cancelled }}"
            }
        )

        # Final failure operator
        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}'
        )

        # Helper function to get task state
        def get_task_state(task_id):
            return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

        # Log DAG run to Sumo
        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            extra_info=lambda: {
                'daterange': rail.result("logging_details")["export_start_date"] + ' - ' + rail.result("logging_details")["export_end_date"],
                'recordcount': rail.result('time_data_export.query_time_data_export_collection', 'length')
                    if rail.result('time_data_export.query_time_data_export_collection') and rail.result('time_data_export.query_time_data_export_collection', 'length') > 0 else 0,
                'filename': (rail.result("logging_details")["time_export_filename_cancelled"]
                    if get_task_state("update_export_name_cancelled") == "success" else (rail.result("logging_details")["time_export_filename_nodata"]
                        if rail.result('time_data_export.query_time_data_export_collection') and rail.result('time_data_export.query_time_data_export_collection', 'length') == 0
                            else rail.result("logging_details")["time_export_filename"])) if get_task_state(f'{group_id}.create_export') == "success"
                                else null,
                'is_exported': "Yes" if Variable.get(config.can_send_time_export_downstream, default_var="true").lower() == "true" else "No"
            }
        )

        # Conditional task to determine if DAG should fail
        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_time_export',
            no_task='time_export_finish'
        )

        # Final task to indicate export completion
        time_export_finish = rail.EmptyOperator(
            task_id='time_export_finish'
        )

        # Define DAG structure
        process_start_time >> logging_details >>\
        timeoff_report_complete >> run_user_report >>\
        get_time_download_script >> time_export_batch_start

        time_export_batch_end >> has_data
        has_data >> rail.Label("Yes") >> write_time_data_to_csv >>\
        upload_time_export_to_sftp \
            >> send_valid_export_complete_email >> log_to_sumo_time_export >> dagrun_log_to_sumo
        upload_time_export_to_sftp >> rail.Label("On Error") >> mark_timedata_export_error
        has_data >> rail.Label("No") >> update_export_name_to_no_data \
            >> send_empty_export_email >> log_to_sumo_time_export_no_data >> dagrun_log_to_sumo
        update_export_name_to_no_data >> rail.Label("On Error") >> mark_timedata_export_error

        mark_timedata_export_error >> get_export_uri_failed >> mark_export_status_cancel_start
        mark_export_status_cancel_end >> update_export_name_cancelled >> dagrun_log_to_sumo
        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_time_export
        should_fail_dag >> rail.Label("No") >> time_export_finish

    return dag


# Create DAG for each instance
rail.for_each_instance(create_dag)