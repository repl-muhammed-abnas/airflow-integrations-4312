from datetime import timedelta
from pendulum import datetime
import pendulum
from capgemini.france_leave_data_export_to_sopra.other_leaves_export_v1.utils import custom_methods, request_payload
from capgemini.france_leave_data_export_to_sopra.other_leaves_export_v1.tasks.get_tenant_wide_logs import get_tenant_wide_logs
from airflow.models import Variable
import rail

null=None

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Capgemini France Other Leaves Data Export to SOPRA Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 5, 1),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config]
        )

        can_run_approved_leaves_batch_task = rail.IfOperator(
            task_id='can_run_approved_leaves_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='approved_leaves_batch_task',
            no_task='get_approved_report_details'
        )

        approved_leaves_batch_task = rail.BatchTaskRunOperator(
            task_id='approved_leaves_batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_approved_report_details',
            end_task='finish_approved_leaves_process',
        )

        get_approved_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_approved_report_details',
            report_name=config.approved_leaves_report_name
        )

        run_approved_leaves_report = rail.run_report2(
            group_id='run_approved_leaves_report',
            report_params=lambda dag_run: request_payload.get_report_parameters(rail.result("get_approved_report_details"),
                (pendulum.now(tz=config.time_zone) - timedelta(days=1)).strftime("%m/%d/%Y"), dag_run),
            target='artifact'
        )

        is_approved_report_failed = rail.IfOperator(
            task_id='is_approved_report_failed',
            test="{{ (result('run_approved_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_approved_report_generation',
            no_task='approved_report_has_data'
        )

        fail_approved_report_generation = rail.FailOperator(
            task_id='fail_approved_report_generation',
            message="{{ (result('run_approved_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        approved_report_has_data = rail.IfOperator(
            task_id='approved_report_has_data',
            test="{{ result('run_approved_leaves_report.get_report_result','has_data') }}",
            yes_task='is_approved_report_has_expected_columns',
            no_task='finish_approved_leaves_process'
        )

        is_approved_report_has_expected_columns = rail.IfOperator(
            task_id='is_approved_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_approved_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_approved_report_columns,
            yes_task='process_approved_report_data',
            no_task='fail_approved_no_expected_columns',
        )

        fail_approved_no_expected_columns = rail.FailOperator(
            task_id='fail_approved_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        process_approved_report_data = rail.EmptyOperator(
            task_id='process_approved_report_data'
        )

        load_approved_leaves_csv = rail.LoadCSVFileOperator(
            task_id='load_approved_leaves_csv',
            document="{{ (result('run_approved_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=';'
        )

        create_approved_leave_data_collection = rail.CreateCollectionOperator(
            task_id='create_approved_leave_data_collection',
            source='{{ result("load_approved_leaves_csv") }}',
            columns={
                "Employee ID": "employee_id",
                "Booking Start Date": "booking_start_date",
                "Booking End Date": "booking_end_date",
                "Time Off Type": "timeoff_type",
                "01 - Booking Day (Start Day)": "booking_day_startday",
                "02 - Booking Day (End Day)": "booking_day_endday",
                "Time Off Hrs": "timeoff_hours",
                "Approval Status": "status",
                "Booking Uri": "booking_uri",
                "Bookingdays": "booking_days"
            },
            name='approved_leaves_data'
        )

        query_valid_users_approved_leaves_data = rail.QueryCollectionOperator(
            task_id='query_valid_users_approved_leaves_data',
            query="SELECT * FROM approved_leaves_data WHERE NULLIF(employee_id, '') IS NOT NULL",
            name='valid_approved_leaves_data'
        )

        write_approved_leave_data_csv = rail.WriteCSVFileOperator(
            task_id='write_approved_leave_data_csv',
            source="{{ result('query_valid_users_approved_leaves_data') }}",
            row=custom_methods.get_approved_leave_data_rows,
            header=config.export_headers,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        final_approved_leaves = rail.CreateCollectionOperator(
            task_id='final_approved_leaves',
            source='{{ result("write_approved_leave_data_csv") }}',
            name='final_approved_leaves'
        )

        finish_approved_leaves_process = rail.EmptyOperator(
            task_id='finish_approved_leaves_process'
        )

        get_deleted_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_deleted_report_details',
            report_name=config.deleted_leaves_report_name
        )

        run_deleted_leaves_report = rail.run_report2(
            group_id='run_deleted_leaves_report',
            report_params=lambda dag_run: request_payload.get_report_parameters(rail.result("get_deleted_report_details"),
                (pendulum.now(tz=config.time_zone) - timedelta(days=1)).strftime("%m/%d/%Y"), dag_run),
            target='artifact'
        )

        is_deleted_report_failed = rail.IfOperator(
            task_id='is_deleted_report_failed',
            test="{{ (result('run_deleted_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_deleted_report_generation',
            no_task='deleted_report_has_data'
        )

        fail_deleted_report_generation = rail.FailOperator(
            task_id='fail_deleted_report_generation',
            message="{{ (result('run_deleted_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        deleted_report_has_data = rail.IfOperator(
            task_id='deleted_report_has_data',
            test="{{result('run_deleted_leaves_report.get_report_result','has_data')}}",
            yes_task='is_deleted_report_has_expected_columns',
            no_task='finish_deleted_leaves_process'
        )

        is_deleted_report_has_expected_columns = rail.IfOperator(
            task_id='is_deleted_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_deleted_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_deleted_report_columns,
            yes_task='process_deleted_report_data',
            no_task='fail_deleted_no_expected_columns',
        )

        fail_deleted_no_expected_columns = rail.FailOperator(
            task_id='fail_deleted_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        process_deleted_report_data = rail.EmptyOperator(
            task_id='process_deleted_report_data'
        )

        load_deleted_leaves_csv = rail.LoadCSVFileOperator(
            task_id='load_deleted_leaves_csv',
            document="{{ (result('run_deleted_leaves_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=';'
        )

        create_deleted_leave_data_collection = rail.CreateCollectionOperator(
            task_id='create_deleted_leave_data_collection',
            source='{{ result("load_deleted_leaves_csv") }}',
            columns={
                "Employee ID": "employee_id",
                "Current Start Date": "booking_start_date",
                "Current End Date": "booking_end_date",
                "Current Time Off Type": "timeoff_type",
                "Action": "status",
                "Booking Uri": "booking_uri"
            },
            name='deleted_leaves_data'
        )

        query_valid_users_deleted_leaves_data = rail.QueryCollectionOperator(
            task_id='query_valid_users_deleted_leaves_data',
            query="SELECT * FROM deleted_leaves_data WHERE NULLIF(employee_id, '') IS NOT NULL",
            name='valid_deleted_leaves_data'
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

        query_for_deleted_leave_data = rail.QueryCollectionOperator(
            task_id='query_for_deleted_leave_data',
            query="""SELECT valid_deleted_leaves_data.employee_id, valid_deleted_leaves_data.booking_start_date,
                valid_deleted_leaves_data.booking_end_date, valid_deleted_leaves_data.timeoff_type,
                valid_deleted_leaves_data.status, valid_deleted_leaves_data.booking_uri,
                final_tenant_wide_log_data.total_working_hours AS timeoff_hours
                FROM valid_deleted_leaves_data
                LEFT JOIN final_tenant_wide_log_data
                ON valid_deleted_leaves_data.booking_uri == final_tenant_wide_log_data.timeoff_booking_uri"""
        )

        write_deleted_leave_data_csv = rail.WriteCSVFileOperator(
            task_id='write_deleted_leave_data_csv',
            source="{{ result('query_for_deleted_leave_data') }}",
            row=custom_methods.get_deleted_leave_data_rows,
            header=config.export_headers,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        final_deleted_leaves = rail.CreateCollectionOperator(
            task_id='final_deleted_leaves',
            source='{{ result("write_deleted_leave_data_csv") }}',
            name='final_deleted_leaves'
        )

        finish_deleted_leaves_process = rail.EmptyOperator(
            task_id='finish_deleted_leaves_process'
        )

        process_leave_data = rail.EmptyOperator(
            task_id='process_leave_data'
        )

        can_run_all_leaves_batch_task = rail.IfOperator(
            task_id='can_run_all_leaves_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='all_leaves_batch_task',
            no_task='is_data_exists'
        )

        all_leaves_batch_task = rail.BatchTaskRunOperator(
            task_id='all_leaves_batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_data_exists',
            end_task='dagrun_log_to_sumo',
        )

        is_data_exists = rail.IfOperator(
            task_id='is_data_exists',
            test=lambda: rail.result("final_approved_leaves") or rail.result("final_deleted_leaves"),
            yes_task='query_to_merge_leaves_data',
            no_task='create_blank_leave_data_xml'
        )

        query_to_merge_leaves_data = rail.PythonOperator(
            task_id='query_to_merge_leaves_data',
            python_callable=custom_methods.get_query_to_merge_leaves_data
        )

        merge_all_leaves_data = rail.QueryCollectionOperator(
            task_id='merge_all_leaves_data',
            query='{{ result("query_to_merge_leaves_data") }}'
        )

        create_leave_data_xml = rail.RenderTemplateOperator(
            task_id='create_leave_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_other_leaves.xml',
            dataset='{{ result("merge_all_leaves_data") }}'
        )

        upload_leave_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_leave_extract_to_s3',
            source="{{ result('create_leave_data_xml') }}",
            key_name=config.s3_upload_filepath + '/{{ result("logging_details").export_filename }}.xml',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_leave_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_leave_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_leave_data_xml') }}"
        )

        upload_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_leave_extract_to_sftp",
            content='{{ result("encrypt_leave_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").export_filename }}.xml.pgp'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Other leaves data extract to SOPRA for France'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location,
                'time_zone': config.time_zone
            }
        )

        create_blank_leave_data_xml = rail.RenderTemplateOperator(
            task_id='create_blank_leave_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_other_leaves.xml',
            dataset=custom_methods.get_empty_export_row
        )

        encrypt_blank_leave_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_blank_leave_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_blank_leave_data_xml') }}"
        )

        upload_blank_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_blank_leave_extract_to_sftp",
            content='{{ result("encrypt_blank_leave_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ result("logging_details").export_filename }}.xml.pgp'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Other leaves data extract to SOPRA for France'
                + ' - No records to export - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'time_zone': config.time_zone
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

        logging_details >> can_run_approved_leaves_batch_task >> rail.Label("Yes") \
            >> approved_leaves_batch_task >> finish_approved_leaves_process
        can_run_approved_leaves_batch_task >> rail.Label("No") >> get_approved_report_details \
            >> run_approved_leaves_report >> is_approved_report_failed

        is_approved_report_failed >> rail.Label("Yes") >> fail_approved_report_generation >> finish_approved_leaves_process
        is_approved_report_failed >> rail.Label("No") >> approved_report_has_data

        approved_report_has_data >> rail.Label("Yes") >> is_approved_report_has_expected_columns
        approved_report_has_data >> rail.Label("No") >> finish_approved_leaves_process

        is_approved_report_has_expected_columns >> rail.Label("Yes") >> process_approved_report_data >> load_approved_leaves_csv

        load_approved_leaves_csv >> create_approved_leave_data_collection >> query_valid_users_approved_leaves_data \
            >> write_approved_leave_data_csv >> final_approved_leaves >> finish_approved_leaves_process >> process_leave_data

        is_approved_report_has_expected_columns >> rail.Label("No") >> fail_approved_no_expected_columns >> finish_approved_leaves_process

        logging_details >> get_deleted_report_details >> run_deleted_leaves_report >> is_deleted_report_failed

        is_deleted_report_failed >> rail.Label("Yes") >> fail_deleted_report_generation >> finish_deleted_leaves_process
        is_deleted_report_failed >> rail.Label("No") >> deleted_report_has_data

        deleted_report_has_data >> rail.Label("Yes") >> is_deleted_report_has_expected_columns
        deleted_report_has_data >> rail.Label("No") >> finish_deleted_leaves_process

        is_deleted_report_has_expected_columns >> rail.Label("Yes") >> process_deleted_report_data >> load_deleted_leaves_csv
        is_deleted_report_has_expected_columns >> rail.Label("No") >> fail_deleted_no_expected_columns >> finish_deleted_leaves_process

        load_deleted_leaves_csv >> create_deleted_leave_data_collection >> query_valid_users_deleted_leaves_data \
            >> get_tenant_wide_logs_data >> get_query_to_merge_artifacts >> merge_all_artifacts >> query_for_deleted_leave_data \
                >> write_deleted_leave_data_csv >> final_deleted_leaves >> finish_deleted_leaves_process >> process_leave_data

        process_leave_data >> can_run_all_leaves_batch_task >> rail.Label("Yes") >> all_leaves_batch_task >> dagrun_log_to_sumo
        can_run_all_leaves_batch_task >> rail.Label("No") >> is_data_exists
        is_data_exists >> rail.Label("Yes") >> query_to_merge_leaves_data >> merge_all_leaves_data \
            >> create_leave_data_xml >> upload_leave_extract_to_s3 >> encrypt_leave_extract_data_xml \
                >> upload_leave_extract_to_sftp >> send_export_complete_email >> dagrun_log_to_sumo
        is_data_exists >> rail.Label("No") >> create_blank_leave_data_xml >> encrypt_blank_leave_extract_data_xml \
            >> upload_blank_leave_extract_to_sftp >> send_empty_export_email >> dagrun_log_to_sumo

        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_leave_extract

    return dag

rail.for_each_instance(create_dag)
