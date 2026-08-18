from datetime import timedelta
from pendulum import datetime
from victoriashipyards.users_supervisor_details_export.utils import request_payload
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Victoriashipyards Users Supervisor Details Export Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunScheduleOperator(task_id="view_dagrun_schedule")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='logging_details',
            end_task='finish_export',
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=request_payload.get_logging_details,
            op_args=[config.time_zone]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_details_report
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_report_parameters,
            target='artifact'
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='send_empty_export_email'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='process_report_data',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        process_report_data = rail.EmptyOperator(
            task_id='process_report_data'
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        create_users_supervisor_data_collection = rail.CreateCollectionOperator(
            task_id='create_users_supervisor_data_collection',
            source='{{ result("load_csv") }}',
            columns={
                "User Name": "username",
                "Employee ID": "employeeid",
                "UserUri": "useruri",
                "User Supervisor Name (Current)": "supervisorname",
                "User Supervisor Email address": "supervisoremail",
                "SupervisorUri": "supervisoruri",
                "Employee Type (Current)": "employee_type",
                "User Status": "user_status"
            },
            name="users_supervisor_data"
        )

        query_users_supervisor_data = rail.QueryCollectionOperator(
            task_id='query_users_supervisor_data',
            query="""SELECT 
                        usd1.username,
                        usd1.employeeid,
                        usd1.useruri,
                        usd1.supervisorname,
                        usd1.supervisoremail,
                        usd1.supervisoruri,
                        usd2.employeeid AS supervisoremployeeid
                    FROM users_supervisor_data usd1
                    LEFT JOIN users_supervisor_data usd2 ON usd1.supervisoruri = usd2.useruri
                    WHERE usd1.employee_type = 'Hourly' and usd1.user_status = 'Enabled'""",
            name='final_users_supervisor_data'
        )

        write_users_supervisor_data_for_reference_csv = rail.WriteCSVFileOperator(
            task_id="write_users_supervisor_data_for_reference_csv",
            source='{{result("query_users_supervisor_data")}}',
            header=config.reference_headers,
            row=request_payload.users_supervisor_csv_data
        )

        create_sha256_collection = rail.CreateCollectionOperator(
            task_id='create_sha256_collection',
            source='{{ result("write_users_supervisor_data_for_reference_csv") }}',
            name='users_supervisor_sha256_collection'
        )

        is_use_reference_file_allowed = rail.IfOperator(
            task_id="is_use_reference_file_allowed",
            test=lambda: Variable.get(
                config.can_use_reference_file, default_var='true').lower() == 'true',
            yes_task="download_reference_file",
            no_task="query_for_changed_records"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id_internal,
            remote_filepath=config.reference_filepath + config.ref_file_name
        )

        load_reference_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_csv",
            delimiter=",",
            document="{{ result('download_reference_file') }}",
            headers=config.reference_headers
        )

        create_ref_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_ref_collection_from_csv',
            source="{{ result('load_reference_csv') }}",
            name="users_supervisor_reference_data"
        )

        query_for_changed_records = rail.QueryCollectionOperator(
            task_id="query_for_changed_records",
            query=request_payload.get_changed_records_query(config.can_use_reference_file),
            name="changed_records"
        )

        is_users_supervisor_data_exists = rail.IfOperator(
            task_id='is_users_supervisor_data_exists',
            test='{{ result("query_for_changed_records", "length") > 0 }}',
            yes_task='write_users_supervisor_data_csv',
            no_task='send_empty_export_email'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Users Supervisor Assignment Data Export - No records to export | {{ current_time_in_specified_tz() }}',
            html_content="/templates/emails/empty_export.html",
            params={
                "time_zone": config.time_zone
            }
        )

        write_users_supervisor_data_csv = rail.WriteCSVFileOperator(
            task_id='write_users_supervisor_data_csv',
            source="{{ result('query_for_changed_records') }}",
            header=config.export_headers,
            row=request_payload.get_users_supervisor_data_rows,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        encrypt_users_supervisor_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_users_supervisor_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_users_supervisor_data_csv') }}"
        )

        upload_users_supervisor_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_users_supervisor_data_to_sftp",
            content='{{ result("encrypt_users_supervisor_data_csv") }}',
            remote_filepath=config.output_filepath + '{{ result("logging_details").export_filename }}.csv.pgp'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('encrypt_users_supervisor_data_csv')}}",
            output_file_name='{{ result("logging_details").export_filename }}.csv.pgp',
            expires_in_seconds=config.log_file_link_expiry
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Users Supervisor Assignment Data Export - Completed successfully | {{ current_time_in_specified_tz() }}',
            html_content="/templates/emails/export_success.html",
            params={
                "upload_file_path": config.output_filepath,
                "time_zone": config.time_zone
            }
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            existing_filename=config.reference_filepath + config.ref_file_name,
            sftp_conn_id=config.sftp_conn_id_internal,
            new_filename=config.archive_reference_filepath + "{{ dag_run_ecid() | replace(':', '-')}}_" + config.ref_file_name
        )

        upload_reference_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reference_csv_to_sftp',
            content="{{ result('write_users_supervisor_data_for_reference_csv') }}",
            sftp_conn_id=config.sftp_conn_id_internal,
            remote_filepath=config.reference_filepath + config.ref_file_name
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish_export
        can_run_batch_task >> rail.Label("No") >> logging_details

        logging_details >> get_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> finish_export
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> send_empty_export_email

        is_report_has_expected_columns >> rail.Label("Yes") >> process_report_data >> load_csv

        load_csv >> create_users_supervisor_data_collection >> query_users_supervisor_data \
            >> write_users_supervisor_data_for_reference_csv >> create_sha256_collection >> is_use_reference_file_allowed
        
        is_use_reference_file_allowed >> rail.Label("Yes") >> download_reference_file \
            >> load_reference_csv >> create_ref_collection_from_csv >> query_for_changed_records \
                >> is_users_supervisor_data_exists
        is_use_reference_file_allowed >> rail.Label("No") >> query_for_changed_records
        
        is_users_supervisor_data_exists >> rail.Label("Yes") >> write_users_supervisor_data_csv \
            >> encrypt_users_supervisor_data_csv >> upload_users_supervisor_data_to_sftp >> generate_download_link \
                >> send_export_complete_email >> archive_reference_file >> upload_reference_csv_to_sftp >> finish_export
        is_users_supervisor_data_exists >> rail.Label("No") >> send_empty_export_email

        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> finish_export
        send_empty_export_email >> finish_export

    return dag

rail.for_each_instance(create_dag)
