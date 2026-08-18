from datetime import timedelta
from pendulum import datetime
from capgemini.unscheduled_hours_export_india.utils import custom_methods
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'UHR INDIA Shift Allowance - Capgemini MASTER {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=lambda: custom_methods.get_logging_detail(config)
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name
        )

        run_unscheduled_hours_report = rail.run_report2(
            group_id='run_report',
            report_params=custom_methods.get_report_parameters,
            target='artifact'
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='load_csv'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
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
            document="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            headers=config.export_columns,
            delimiter=','
        )

        write_uhr_data_csv = rail.WriteCSVFileOperator(
            task_id='write_uhr_data_csv',
            source="{{ result('load_csv') }}",
            header=config.export_columns,
            row=lambda item:custom_methods.get_formatted_data(item),
            thread_pool_size=config.thread_pool_size,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv)
        )

        encrypt_uhr_extract_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_uhr_extract_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_uhr_data_csv') }}",
            sign=True
        )

        upload_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_leave_extract_to_sftp",
            content='{{ result("encrypt_uhr_extract_data_csv") }}',
            remote_filepath=config.upload_filepath + '/{{ result("get_logging_details").filename }}.pgp'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Unscheduled Hours Export for India'
                + ' is completed - {{ result("get_logging_details")["processing_start_time"] }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_filepath': config.upload_filepath
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

        get_logging_details >> get_report_details >> run_unscheduled_hours_report >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> dagrun_log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label('Yes') >> is_report_has_expected_columns
        
        report_has_data >> rail.Label('No') >> load_csv

        is_report_has_expected_columns >> rail.Label("Yes") >> process_report_data >> load_csv

        load_csv >> write_uhr_data_csv >> encrypt_uhr_extract_data_csv >> upload_leave_extract_to_sftp
        upload_leave_extract_to_sftp >> send_export_complete_email >> dagrun_log_to_sumo
        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> dagrun_log_to_sumo

        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_leave_extract

    return dag

rail.for_each_instance(create_dag)
