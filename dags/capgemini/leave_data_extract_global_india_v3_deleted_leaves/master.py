from datetime import timedelta
from pendulum import datetime
import pendulum
from capgemini.leave_data_extract_global_india_v3_deleted_leaves.utils import custom_methods
from capgemini.leave_data_extract_global_india_v3_deleted_leaves.tasks.get_tenant_wide_logs import get_tenant_wide_logs
from airflow.models import Variable
import rail

null=None

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.current_day_leave_export_master_dag_id,
        description=f'Capgemini Leave Data Export Global Master {config.leave_status} {config.location} {config.instance} V3',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 1),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.time_zone, config.filename_prefix]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name='\
                {%- if dag_run.conf | is_truthy and dag_run.conf.adhoc_report_name | is_truthy -%} \
                    {{ dag_run.conf.adhoc_report_name }} \
                {%- else -%}'
                    + config.report_name +
                '{%- endif -%}'
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda: custom_methods.get_report_parameters(
                pendulum.now(tz=config.time_zone).strftime("%m/%d/%Y")),
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

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon leave data extract for '
                + config.leave_status + ' for ' + config.location
                + ' - No records to export - {{ result("logging_details")["process_start_time"] }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'timoff_status': config.leave_status,
                'location': config.location
            }
        )

        process_report_data = rail.EmptyOperator(
            task_id='process_report_data'
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            headers=config.export_columns,
            delimiter=';'
        )

        create_deleted_timeoffs_collection = rail.CreateCollectionOperator(
            task_id='create_deleted_timeoffs_collection',
            source='{{ result("load_csv") }}',
            name="deleted_timeoffs_data"
        )

        get_tenant_wide_logs_data = get_tenant_wide_logs(config.tenant_wide_log_list)

        get_query_to_merge_artifacts = rail.PythonOperator(
            task_id='get_query_to_merge_artifacts',
            python_callable=custom_methods.get_query_to_merge_artifacts,
            op_args=[config.tenant_wide_log_list]
        )

        merge_all_artifacts = rail.QueryCollectionOperator(
            task_id='merge_all_artifacts',
            query='{{ result("get_query_to_merge_artifacts") }}',
            name='final_tenant_wide_log_data'
        )

        query_for_leave_data = rail.QueryCollectionOperator(
            task_id='query_for_leave_data',
            query="""SELECT deleted_timeoffs_data.Leave_Request_ID, deleted_timeoffs_data.Employee_ID,
                deleted_timeoffs_data.Local_Employee_Number, deleted_timeoffs_data.Current_Time_Off_Type,
                deleted_timeoffs_data.Current_Start_Date, deleted_timeoffs_data.Current_End_Date,
                final_tenant_wide_log_data.total_working_days, deleted_timeoffs_data.Modified_On
                FROM deleted_timeoffs_data
                LEFT JOIN final_tenant_wide_log_data
                ON deleted_timeoffs_data.Leave_Request_ID == final_tenant_wide_log_data.timeoff_booking_uri"""
        )

        def write_leave_data_row(item):
            if not item:
                return []
            if config.should_add_timeoff_balance:
                return [
                    item['Leave_Request_ID'],
                    item['Employee_ID'],
                    item['Local_Employee_Number'],
                    item['Current_Time_Off_Type'],
                    item['Current_Start_Date'],
                    item['Current_End_Date'],
                    item['total_working_days'] if item['total_working_days'] else (1 if item['Current_Time_Off_Type'] == "Holiday" else null),
                    item['Modified_On']
                ]
            return [
                    item['Leave_Request_ID'],
                    item['Employee_ID'],
                    item['Local_Employee_Number'],
                    item['Current_Time_Off_Type'],
                    item['Current_Start_Date'],
                    item['Current_End_Date'],
                    item['Modified_On']
                ]

        def get_headers():
            if config.should_add_timeoff_balance:
                return ['Leave Request ID', 'Employee ID',
                        'Local Employee Number', 'Current Time Off Type', 'Current Start Date', 'Current End Date', 'Time Off Days', 'Modified On']
            return config.export_columns

        write_leave_data_csv = rail.WriteCSVFileOperator(
            task_id='write_leave_data_csv',
            source="{{ result('query_for_leave_data') }}",
            delimiter=';',
            header=get_headers,
            row= write_leave_data_row,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=10
        )

        upload_leave_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_leave_extract_to_s3',
            source="{{ result('write_leave_data_csv') }}",
            key_name=config.s3_upload_filepath + '/{{ result("logging_details").export_filename }}.csv',
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
            remote_filepath=config.input_filepath + '/{{ result("logging_details").export_filename }}.csv.pgp'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon leave data extract for '
                + config.leave_status + ' for ' + config.location
                + ' is completed - {{ result("logging_details")["process_start_time"] }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'timoff_status': config.leave_status,
                'location': config.location
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

        logging_details >> get_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> dagrun_log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> send_empty_export_email >> dagrun_log_to_sumo

        is_report_has_expected_columns >> rail.Label("Yes") >> process_report_data

        process_report_data >> load_csv >> create_deleted_timeoffs_collection >> get_tenant_wide_logs_data \
            >> get_query_to_merge_artifacts >> merge_all_artifacts >> query_for_leave_data >> write_leave_data_csv

        write_leave_data_csv >> upload_leave_extract_to_s3 >> encrypt_leave_extract_data_csv \
            >> upload_leave_extract_to_sftp >> send_export_complete_email >> dagrun_log_to_sumo

        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> dagrun_log_to_sumo

        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_leave_extract

    return dag

rail.for_each_instance(create_dag)
