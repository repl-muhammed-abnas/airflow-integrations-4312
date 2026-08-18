from datetime import timedelta
from pendulum import datetime
import pendulum
from unisys.time_export_to_oracle.tasks.time_data_export import time_data_export
from unisys.time_export_to_oracle.tasks.update_export_status import cancel_time_export
from unisys.time_export_to_oracle.utils import custom_methods
from unisys.time_export_to_oracle.utils import request_payload
from airflow.models import Variable
import rail

null=None

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'Unisys Time export to Oracle Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 10, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                f'unisys_oracle_can_run_batch_task_{config.instance}', default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_start_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_start_time',
            end_task='finish_time_export_batch_creation',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.timezone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.timezone, config.export_file_prefix]
        )

        get_time_download_script = rail.RepliconServiceOperator(
            task_id='get_time_download_script',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json()['d'],
                    'displayText', config.time_export_file_format, 'uri')
        )

        group_id = 'time_data_export'

        time_export_batch_start, time_export_batch_end = time_data_export(
            group_id=group_id,
            get_export_name='{{ result("logging_details").time_export_filename }}'
        )

        create_timeexport_collection = rail.CreateCollectionOperator(
            task_id='create_timeexport_collection',
            name='datatoexport',
            source='{{ result("' + group_id + '.load_export") }}'
        )

        finish_time_export_batch_creation = rail.EmptyOperator(
            task_id='finish_time_export_batch_creation'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("create_timeexport_collection", "length") > 0 }}',
            yes_task='can_send_downstream',
            no_task='update_export_name_to_no_data'
        )

        can_send_downstream = rail.IfOperator(
            task_id='can_send_downstream',
            test=lambda: Variable.get(f'unisys_oracle_time_export_can_send_downstream_{config.instance}', default_var='true').lower() == "true",
            yes_task='write_time_data_to_csv',
            no_task='dagrun_log_to_sumo'
        )

        update_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id="update_export_name_to_no_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('" + group_id + ".get_export_uri') }}"
                },
                "name": "{{result('logging_details').time_export_filename_nodata}}"
            }
        )

        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to Oracle - No records to export - {{ current_time_in_specified_tz("'+ config.timezone +'") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'time_zone': config.timezone,
            }
        )

        write_time_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_time_data_to_csv',
            source='{{ result("create_timeexport_collection") }}',
            header=config.oracle_export_header,
            row=custom_methods.get_time_data_csv_rows,
            delimiter=',',
            execution_timeout=timedelta(hours=config.write_csv_timeout_hours),
            thread_pool_size=config.thread_pool_size_csv
        )

        encrypt_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_time_export_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_time_data_to_csv') }}"
        )

        upload_time_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_time_export_to_sftp',
            content='{{ result("encrypt_time_export_data_csv") }}',
            remote_filepath=config.export_csv_filepath + '/{{ result("logging_details").time_export_filename }}.csv.pgp'
        )

        encrypt_secondary_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_secondary_time_export_data_csv',
            pgp_conn_id=config.secondary_pgp_conn_id,
            source="{{ result('write_time_data_to_csv') }}"
        )

        upload_secondary_time_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_secondary_time_export_to_sftp',
            content='{{ result("encrypt_secondary_time_export_data_csv") }}',
            remote_filepath=config.secondary_export_csv_filepath + '/{{ result("logging_details").time_export_filename }}.csv.pgp'
        )

        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to Oracle - {{ current_time_in_specified_tz("'+ config.timezone +'") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.export_csv_filepath,
                'secondary_upload_file_path': config.secondary_export_csv_filepath,
                'time_zone': config.timezone,
            }
        )

        mark_timedata_export_error = rail.EmptyOperator(
            task_id='mark_timedata_export_error',
            trigger_rule='one_failed'
        )

        get_export_uri_failed = rail.RepliconServiceOperator(
            task_id='get_export_uri_failed',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + group_id + ".create_export') }}"
            },
            data_handler=request_payload.retrieve_export_uri
        )

        mark_export_status_cancel_start, mark_export_status_cancel_end = cancel_time_export()

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

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}'
        )

        dagrun_log_to_sumo = rail.EmptyOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done'
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_time_export',
            no_task='time_export_finish'
        )

        time_export_finish = rail.EmptyOperator(
            task_id='time_export_finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish_time_export_batch_creation
        can_run_batch_task >> rail.Label('No') >> process_start_time
        process_start_time >> logging_details >> get_time_download_script >> time_export_batch_start

        time_export_batch_end >> create_timeexport_collection >> finish_time_export_batch_creation >> has_data
        has_data >> rail.Label("Yes") >> can_send_downstream
        can_send_downstream >> rail.Label("Yes") >> write_time_data_to_csv
        
        write_time_data_to_csv >> encrypt_time_export_data_csv >> upload_time_export_to_sftp >> send_valid_export_complete_email
        write_time_data_to_csv >> encrypt_secondary_time_export_data_csv >> upload_secondary_time_export_to_sftp >> send_valid_export_complete_email

        send_valid_export_complete_email >> dagrun_log_to_sumo

        can_send_downstream >> rail.Label("No") >> dagrun_log_to_sumo
        write_time_data_to_csv >> rail.Label("On Error") >> mark_timedata_export_error
        has_data >> rail.Label("No") >> update_export_name_to_no_data >> send_empty_export_email >> dagrun_log_to_sumo
        update_export_name_to_no_data >> rail.Label("On Error") >> mark_timedata_export_error

        mark_timedata_export_error >> get_export_uri_failed >> mark_export_status_cancel_start
        mark_export_status_cancel_end >> update_export_name_cancelled >> dagrun_log_to_sumo
        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_time_export
        should_fail_dag >> rail.Label("No") >> time_export_finish

    return dag

rail.for_each_instance(create_dag)
