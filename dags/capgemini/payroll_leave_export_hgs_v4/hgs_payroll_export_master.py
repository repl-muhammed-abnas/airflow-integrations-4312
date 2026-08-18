
from datetime import timedelta
import itertools
from pendulum import datetime
from capgemini.payroll_leave_export_hgs_v4.utils import request_payload, custom_methods
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'HGS Payroll Export - Capgemini {config.instance} V4',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 2, 20, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='logging_details',
            end_task='dagrun_log_to_sumo',
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.required_timeoffs, config.export_file_prefix, config.time_zone]
        )

        process_timeoff_reports = rail.EmptyOperator(
            task_id='process_timeoff_reports'
        )

        get_approved_timeoffs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_approved_timeoffs_report_details',
            report_name=config.approved_timeoffs_report
        )

        run_approved_timeoffs_report_entry, run_approved_timeoffs_report_exit = rail.run_report(
            group_id='run_approved_timeoffs_report',
            report_params=lambda: request_payload.get_approved_timeoffs_report_batch_payload(config.time_zone)
        )

        is_approved_timeoffs_report_failed = rail.IfOperator(
            task_id='is_approved_timeoffs_report_failed',
            test='{{result("run_approved_timeoffs_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_approved_timeoffs_report_generation',
            no_task='approved_timeoffs_report_has_data'
        )

        fail_approved_timeoffs_report_generation = rail.FailOperator(
            task_id='fail_approved_timeoffs_report_generation',
            message="{{result('run_approved_timeoffs_report.get_report_result').reportGenerationResults[0].error}}"
        )

        approved_timeoffs_report_has_data = rail.IfOperator(
            task_id='approved_timeoffs_report_has_data',
            test="{{result('run_approved_timeoffs_report.get_report_result','has_data')}}",
            yes_task='is_approved_timeoffs_report_has_expected_columns',
            no_task='get_deleted_timeoffs_report_details'
        )

        is_approved_timeoffs_report_has_expected_columns = rail.IfOperator(
            task_id='is_approved_timeoffs_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{result('run_approved_timeoffs_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_approved_timeoffs_report_columns,
            yes_task='load_approved_timeoffs_report_data',
            no_task='fail_approved_timeoffs_has_no_expected_columns',
        )

        fail_approved_timeoffs_has_no_expected_columns = rail.FailOperator(
            task_id='fail_approved_timeoffs_has_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_approved_timeoffs_report_data = rail.LoadCSVFileOperator(
            task_id='load_approved_timeoffs_report_data',
            document="{{ result('run_approved_timeoffs_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_approved_timeoffs_collection = rail.CreateCollectionOperator(
            task_id='create_approved_timeoffs_collection',
            source='{{ result("load_approved_timeoffs_report_data") }}',
            columns={
                "Leave Request ID": "leave_request_id",
                "Local Employee Number": "local_employee_number",
                "Employee ID": "employee_id",
                "Time Off Type": "timeoff_type",
                "Time Off Type Description": "timeoff_type_description",
                "Booking Start Date": "booking_start_date",
                "Booking End Date": "booking_end_date",
                "Approval Status": "approval_status",
                "Cost Center (Current) (Full Path)": "cost_center_full_path"
            },
            name="approved_timeoff_bookings_data"
        )

        query_get_valid_approved_timeoffs_collection = rail.QueryCollectionOperator(
            task_id='query_get_valid_approved_timeoffs_collection',
            query="""SELECT * FROM approved_timeoff_bookings_data WHERE NULLIF(leave_request_id, '') IS NOT NULL
                AND NULLIF(employee_id, '') IS NOT NULL""",
            name='valid_approved_timeoffs_collection'
        )

        trigger_process_approved_timeoffs = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_approved_timeoffs',
            items='{{ result("query_get_valid_approved_timeoffs_collection") }}',
            trigger_dag_id=config.process_approved_bookings_child_dag_id,
            thread_pool_size=config.parallel_runs_approved_bookings,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "timeoff_booking_details": item,
                "logging_details": rail.result("logging_details")
            }
        )

        get_deleted_timeoffs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_deleted_timeoffs_report_details',
            report_name=config.deleted_timeoffs_report
        )

        run_deleted_timeoffs_report_entry, run_deleted_timeoffs_report_exit = rail.run_report(
            group_id='run_deleted_timeoffs_report',
            report_params=lambda: request_payload.get_deleted_timeoffs_report_batch_payload(config.time_zone)
        )

        is_deleted_timeoffs_report_failed = rail.IfOperator(
            task_id='is_deleted_timeoffs_report_failed',
            test='{{result("run_deleted_timeoffs_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_deleted_timeoffs_report_generation',
            no_task='deleted_timeoffs_report_has_data'
        )

        fail_deleted_timeoffs_report_generation = rail.FailOperator(
            task_id='fail_deleted_timeoffs_report_generation',
            message="{{result('run_deleted_timeoffs_report.get_report_result').reportGenerationResults[0].error}}"
        )

        deleted_timeoffs_report_has_data = rail.IfOperator(
            task_id='deleted_timeoffs_report_has_data',
            test="{{result('run_deleted_timeoffs_report.get_report_result','has_data')}}",
            yes_task='is_deleted_timeoffs_report_has_expected_columns',
            no_task='gather_all_the_run_ids'
        )

        is_deleted_timeoffs_report_has_expected_columns = rail.IfOperator(
            task_id='is_deleted_timeoffs_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{result('run_deleted_timeoffs_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_deleted_timeoffs_report_columns,
            yes_task='load_deleted_timeoffs_report_data',
            no_task='fail_deleted_timeoffs_has_no_expected_columns',
        )

        fail_deleted_timeoffs_has_no_expected_columns = rail.FailOperator(
            task_id='fail_deleted_timeoffs_has_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_deleted_timeoffs_report_data = rail.LoadCSVFileOperator(
            task_id='load_deleted_timeoffs_report_data',
            document="{{ result('run_deleted_timeoffs_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_deleted_timeoffs_collection = rail.CreateCollectionOperator(
            task_id='create_deleted_timeoffs_collection',
            source='{{ result("load_deleted_timeoffs_report_data") }}',
            columns={
                "Leave Request ID": "leave_request_id",
                "Local Employee Number": "local_employee_number",
                "Employee ID": "employee_id",
                "Current Time Off Type": "timeoff_type",
                "Current Start Date": "booking_start_date",
                "Current End Date": "booking_end_date",
                "Action": "action",
                "Cost Center (Current) (Full Path)": "cost_center_full_path"
            },
            name="deleted_timeoff_bookings_data"
        )

        query_get_valid_deleted_timeoffs_collection = rail.QueryCollectionOperator(
            task_id='query_get_valid_deleted_timeoffs_collection',
            query="""SELECT * FROM deleted_timeoff_bookings_data WHERE NULLIF(leave_request_id, '') IS NOT NULL
                AND NULLIF(employee_id, '') IS NOT NULL""",
            name='valid_deleted_timeoffs_collection'
        )

        trigger_process_deleted_timeoffs = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_deleted_timeoffs',
            items='{{ result("query_get_valid_deleted_timeoffs_collection") }}',
            trigger_dag_id=config.process_deleted_bookings_child_dag_id,
            thread_pool_size=config.parallel_runs_deleted_bookings,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "timeoff_booking_details": item,
                "logging_details": rail.result("logging_details")
            }
        )

        def gather_all_the_run_ids_callable():
            run_ids = []
            if rail.result(trigger_process_approved_timeoffs.task_id):
                run_ids.append(rail.result(trigger_process_approved_timeoffs.task_id))
            if rail.result(trigger_process_deleted_timeoffs.task_id):
                run_ids.append(rail.result(trigger_process_deleted_timeoffs.task_id))
            return list(itertools.chain.from_iterable(run_ids))
 
        gather_all_the_run_ids = rail.PythonOperator(
            task_id="gather_all_the_run_ids",
            python_callable=gather_all_the_run_ids_callable
        )

        wait_for_process_timeoffs = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoffs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("gather_all_the_run_ids") }}'
        )

        gather_process_timeoffs_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_timeoffs_logs',
            dag_runs='{{ result("gather_all_the_run_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        format_timeoff_bookings_records = rail.CreateCollectionOperator(
            task_id='format_timeoff_bookings_records',
            source=custom_methods.do_format_logs,
            columns=["entity", "employeee_id", "ggid", "lwp_type", "lwp_start_date", "lwp_end_date", "lwp_code",
                    "modified_dated", "remarks", "company_name"],
            name='timeoff_bookings_records'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("format_timeoff_bookings_records", "length") > 0 }}',
            yes_task='write_payroll_data_to_csv',
            no_task='write_payroll_blankdata_to_csv'
        )

        write_payroll_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_payroll_data_to_csv',
            source='{{ result("format_timeoff_bookings_records") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=custom_methods.get_payroll_data_csv_rows,
            delimiter='|',
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv)
        )

        write_payroll_blankdata_to_csv = rail.WriteCSVFileOperator(
            task_id='write_payroll_blankdata_to_csv',
            source=[],
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=[],
            delimiter='|',
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv)
        )

        encrypt_payroll_blankexport_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_payroll_blankexport_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_payroll_blankdata_to_csv') }}"
        )

        upload_payroll_blankexport_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_payroll_blankexport_to_sftp',
            content='{{ result("encrypt_payroll_blankexport_data_csv") }}',
            remote_filepath=config.input_filepath +
            '/{{ result("logging_details").payroll_export_filename }}.csv.pgp'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for HGS - No records to export - {{ result("logging_details").process_start_time }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'location': config.location
            }
        )

        upload_payroll_export_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_payroll_export_to_s3',
            source='{{ result("write_payroll_data_to_csv") }}',
            key_name=config.s3_upload_filepath +
            '/{{ result("logging_details").payroll_export_filename }}.csv',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_payroll_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_payroll_export_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_payroll_data_to_csv') }}"
        )

        upload_payroll_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_payroll_export_to_sftp',
            content='{{ result("encrypt_payroll_export_data_csv") }}',
            remote_filepath=config.input_filepath +
            '/{{ result("logging_details").payroll_export_filename }}.csv.pgp'
        )

        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for HGS is completed - {{ result("logging_details").process_start_time }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            extra_info=lambda: {
                'locations': config.location,
                'timeoff_types_considered': rail.result("logging_details")["required_timeoffs_mapper"],
                'exportrowcount': len(rail.load_all_records(rail.result("compose_booking_data"))) if rail.result("compose_booking_data") else 0,
                'filename': rail.result("logging_details")["payroll_export_filename"] + '.csv.pgp'
                    if rail.result("compose_booking_data") and len(rail.load_all_records(rail.result("compose_booking_data"))) > 0 else ""
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_payroll_export'
        )

        fail_payroll_export = rail.FailOperator(
            task_id='fail_payroll_export',
            message='{{ get_error_message() }}'
        )

        batch_task >> dagrun_log_to_sumo
        batch_task >> logging_details >> process_timeoff_reports
        process_timeoff_reports >> get_approved_timeoffs_report_details >> run_approved_timeoffs_report_entry
        run_approved_timeoffs_report_exit >> is_approved_timeoffs_report_failed

        is_approved_timeoffs_report_failed >> rail.Label("Yes") >> fail_approved_timeoffs_report_generation >> dagrun_log_to_sumo
        is_approved_timeoffs_report_failed >> rail.Label("No") >> approved_timeoffs_report_has_data

        approved_timeoffs_report_has_data >> rail.Label("Yes") >> is_approved_timeoffs_report_has_expected_columns
        approved_timeoffs_report_has_data >> rail.Label("No") >> get_deleted_timeoffs_report_details

        is_approved_timeoffs_report_has_expected_columns >> rail.Label("Yes") >> load_approved_timeoffs_report_data \
            >> create_approved_timeoffs_collection >> query_get_valid_approved_timeoffs_collection \
                >> trigger_process_approved_timeoffs >> get_deleted_timeoffs_report_details
        is_approved_timeoffs_report_has_expected_columns >> rail.Label("No") >> fail_approved_timeoffs_has_no_expected_columns \
            >> dagrun_log_to_sumo

        get_deleted_timeoffs_report_details >> run_deleted_timeoffs_report_entry
        run_deleted_timeoffs_report_exit >> is_deleted_timeoffs_report_failed

        is_deleted_timeoffs_report_failed >> rail.Label("Yes") >> fail_deleted_timeoffs_report_generation >> dagrun_log_to_sumo
        is_deleted_timeoffs_report_failed >> rail.Label("No") >> deleted_timeoffs_report_has_data

        deleted_timeoffs_report_has_data >> rail.Label("Yes") >> is_deleted_timeoffs_report_has_expected_columns
        deleted_timeoffs_report_has_data >> rail.Label("No") >> gather_all_the_run_ids

        is_deleted_timeoffs_report_has_expected_columns >> rail.Label("Yes") >> load_deleted_timeoffs_report_data \
            >> create_deleted_timeoffs_collection
        is_deleted_timeoffs_report_has_expected_columns >> rail.Label("No") >> fail_deleted_timeoffs_has_no_expected_columns \
            >> dagrun_log_to_sumo

        create_deleted_timeoffs_collection >> query_get_valid_deleted_timeoffs_collection >> trigger_process_deleted_timeoffs \
            >> gather_all_the_run_ids >> wait_for_process_timeoffs >> gather_process_timeoffs_logs >> format_timeoff_bookings_records >> has_data

        has_data >> rail.Label("Yes") >> write_payroll_data_to_csv >> upload_payroll_export_to_s3 >> encrypt_payroll_export_data_csv \
            >> upload_payroll_export_to_sftp >> send_valid_export_complete_email >> dagrun_log_to_sumo >> should_fail_dag

        has_data >> rail.Label("No") >> write_payroll_blankdata_to_csv >> encrypt_payroll_blankexport_data_csv >> \
        upload_payroll_blankexport_to_sftp >> send_empty_export_email >> dagrun_log_to_sumo

        should_fail_dag >> rail.Label("Yes") >> fail_payroll_export

    return dag

rail.for_each_instance(create_dag)
