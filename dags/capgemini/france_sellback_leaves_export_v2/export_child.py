from datetime import timedelta
from capgemini.france_sellback_leaves_export_v2.utils import custom_methods
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.export_child_dagid,
        description=f'Capgemini France Sell Back Leaves Export Child {config.instance} V2',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_report_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_report_data',
            end_task='dagrun_log_to_sumo',
        )

        has_report_data = rail.IfOperator(
            task_id='has_report_data',
            test='{{ dag_run.conf.has_data == "yes" }}',
            yes_task='query_sellback_leaves_data',
            no_task='send_empty_export_email'
        )

        query_sellback_leaves_data = rail.QueryCollectionOperator(
            task_id='query_sellback_leaves_data',
            query="SELECT * FROM valid_sellbacks_data WHERE timeofftype IN {{ dag_run.conf.timeoff_types }}"
        )

        is_sellback_leaves_exists = rail.IfOperator(
            task_id='is_sellback_leaves_exists',
            test='{{ result("query_sellback_leaves_data", "length") > 0 }}',
            yes_task='write_sellbacks_data_csv',
            no_task='send_empty_export_email'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Sell Back Leaves Data Export of {{ dag_run.conf.adj_type }} adjustments for France'
                + ' - No records to export - {{ current_time_in_specified_tz("' + config.time_zone +'") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                "time_zone": config.time_zone
            }
        )

        write_sellbacks_data_csv = rail.WriteCSVFileOperator(
            task_id='write_sellbacks_data_csv',
            source="{{ result('query_sellback_leaves_data') }}",
            header=None,
            row=lambda item: custom_methods.get_sellback_data_rows(item, config),
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        upload_sellback_leave_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_sellback_leave_extract_to_s3',
            source="{{ result('write_sellbacks_data_csv') }}",
            key_name=config.s3_upload_filepath + '/{{ dag_run.conf.export_filename }}.txt',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_sellback_leave_extract_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_sellback_leave_extract_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_sellbacks_data_csv') }}"
        )

        upload_sellback_leave_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_sellback_leave_extract_to_sftp",
            content='{{ result("encrypt_sellback_leave_extract_data_csv") }}',
            remote_filepath=config.input_filepath + '/{{ dag_run.conf.export_filename }}.txt.pgp'
        )

        send_sellback_export_complete_email = rail.EmailOperator(
            task_id="send_sellback_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Sell Back Leaves Data Export of {{ dag_run.conf.adj_type }} adjustments for France'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone +'") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                "upload_file_path": config.input_filepath,
                "time_zone": config.time_zone
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> has_report_data
        
        has_report_data >> rail.Label("Yes") >> query_sellback_leaves_data
        has_report_data >> rail.Label("No") >> send_empty_export_email

        query_sellback_leaves_data >> is_sellback_leaves_exists >> rail.Label("Yes") \
            >> write_sellbacks_data_csv >> upload_sellback_leave_extract_to_s3 \
                >> encrypt_sellback_leave_extract_data_csv >> upload_sellback_leave_extract_to_sftp \
                    >> send_sellback_export_complete_email >> dagrun_log_to_sumo
        is_sellback_leaves_exists >> rail.Label("No") >> send_empty_export_email >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
