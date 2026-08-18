from datetime import timedelta
from pendulum import datetime
from capgemini.leave_data_extract_global_row_v5_leave_balance.utils import custom_methods, request_payload, response_filter
from airflow.models import Variable
import rail


def create_child_dag(config):
    all_dags = []

    for batch_num in range(1, config.DEFAULT_MAX_BATCH_COUNT + 1):
        dag_id = f"{config.leave_balance_export_child_dag_id}_batch_{batch_num}"

        with rail.create_airflow_dag(
            dag_id=dag_id,
            description=f"Capgemini Leave Data Export Child {config.leave_status} Batch {batch_num} {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            start_date=datetime(2026, 3, 23),
            max_active_runs=config.child_max_active_runs,
            default_args={'sftp_conn_id': config.sftp_conn_id},
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='get_location_uris_for_country'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                start_task='get_location_uris_for_country',
                end_task='dagrun_log_to_sumo',
            )

            get_location_uris_for_country = rail.PythonOperator(
                task_id='get_location_uris_for_country',
                python_callable=response_filter.get_location_uris_for_country
            )

            get_report_details = rail.RepliconReportDetailsOperator(
                task_id='get_report_details',
                report_name=config.report_name
            )

            run_report_group_entry, run_report_group_exit = rail.run_report(
                group_id='run_report',
                report_params=lambda: request_payload.get_report_parameters(config.time_zone),
                target='artifact'
            )

            is_report_failed = rail.IfOperator(
                task_id='is_report_failed',
                test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy}}",
                yes_task='fail_report_generation',
                no_task='report_has_data'
            )

            fail_report_generation = rail.FailOperator(
                task_id='fail_report_generation',
                message="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}"
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

            send_empty_export_email = rail.EmailOperator(
                task_id='send_empty_export_email',
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject='{{ get_company_key() }} | Replicon leave data extract of '
                    + config.leave_status + ' for {{ dag_run.conf.region }} - {{ dag_run.conf.country_code }}'
                    + ' is completed - No records to export - {{ dag_run.conf.logging_details.process_start_time }}',
                html_content="/templates/emails/email_empty_export.html",
                params={'timoff_status': config.leave_status}
            )

            process_report_data = rail.EmptyOperator(task_id='process_report_data')

            get_batch_creation_time = rail.RepliconServiceOperator(
                task_id='get_batch_creation_time',
                endpoint='/services/BatchManagementService1.svc/GetStatus',
                data={"batchUri": '{{ result("run_report.create_report_run") }}'},
                data_handler=custom_methods.create_batch_creation_datetime
            )

            load_csv = rail.LoadCSVFileOperator(
                task_id='load_csv',
                document="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
                headers=config.export_columns,
                delimiter=';'
            )

            write_leave_data_csv = rail.WriteCSVFileOperator(
                task_id='write_leave_data_csv',
                source="{{ result('load_csv') }}",
                delimiter=';',
                header=config.export_columns,
                row=custom_methods.get_csv_row_data,
                execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv)
            )

            upload_leave_extract_to_s3 = rail.S3UploadFileOperator(
                task_id='upload_leave_extract_to_s3',
                source="{{ result('write_leave_data_csv') }}",
                key_name=config.s3_upload_filepath + '/{{ dag_run.conf.filename }}.csv',
                bucket_name=lambda: Variable.get(config.bucket_name),
                aws_conn_id=config.aws_conn_id
            )

            encrypt_leave_extract_data_csv = rail.PGPEncryptionOperator(
                task_id='encrypt_leave_extract_data_csv',
                pgp_conn_id=config.pgp_conn_id,
                source="{{ result('write_leave_data_csv') }}",
                sign=True
            )

            upload_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
                task_id="upload_leave_extract_to_sftp",
                content='{{ result("encrypt_leave_extract_data_csv") }}',
                remote_filepath=config.input_filepath + '/{{ dag_run.conf.filename }}.csv.pgp'
            )

            send_export_complete_email = rail.EmailOperator(
                task_id="send_export_complete_email",
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject='{{ get_company_key() }} | Replicon leave data extract of '
                    + config.leave_status + ' for {{ dag_run.conf.region }} - {{ dag_run.conf.country_code }}'
                    + ' is completed - {{ dag_run.conf.logging_details.process_start_time }}',
                html_content="/templates/emails/email_valid_export_complete.html",
                params={
                    'upload_file_path': config.input_filepath,
                    'timoff_status': config.leave_status
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

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> dagrun_log_to_sumo
            can_run_batch_task >> rail.Label("No") >> get_location_uris_for_country

            get_location_uris_for_country >> get_report_details

            get_report_details >> run_report_group_entry

            run_report_group_exit >> is_report_failed

            is_report_failed >> rail.Label("Yes") >> fail_report_generation >> dagrun_log_to_sumo
            is_report_failed >> rail.Label("No") >> report_has_data

            report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
            report_has_data >> rail.Label("No") >> send_empty_export_email >> dagrun_log_to_sumo

            is_report_has_expected_columns >> rail.Label("Yes") >> process_report_data
            is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> dagrun_log_to_sumo

            process_report_data >> get_batch_creation_time >> load_csv >> write_leave_data_csv \
                >> upload_leave_extract_to_s3 >> encrypt_leave_extract_data_csv >> upload_leave_extract_to_sftp \
                >> send_export_complete_email >> dagrun_log_to_sumo

            dagrun_log_to_sumo >> should_fail_dag
            should_fail_dag >> rail.Label("Yes") >> fail_leave_extract

        all_dags.append(dag)

    return all_dags


rail.for_each_instance(create_child_dag)
