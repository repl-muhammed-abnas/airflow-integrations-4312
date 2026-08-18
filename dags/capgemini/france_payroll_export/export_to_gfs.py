from datetime import timedelta
from pendulum import datetime
from capgemini.france_payroll_export.utils import custom_methods
from airflow.models import Variable
import pendulum
import rail

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.gfs_export_child_dag_id,
        description=f"Capgemini France Payroll Export to GFS Child {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 11, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        current_time = pendulum.now(config.time_zone).strftime('%Y%m%d_%H%M%S')

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_entries_on_paycodes',
            end_task='finish_payroll_export',
        )

        query_entries_on_paycodes = rail.QueryCollectionOperator(
            task_id='query_entries_on_paycodes',
            query=f"SELECT * FROM payrunpayrolldata WHERE \
                Pay_Code_Name IN {config.gfs_paycodes} OR Desired_Paycode IN {config.desired_paycodes_names}",
            name='paycode_entries'
        )

        is_entries_on_paycodes_exists = rail.IfOperator(
            task_id='is_entries_on_paycodes_exists',
            test='{{ result("query_entries_on_paycodes", "length") > 0 }}',
            yes_task='write_payroll_data_csv',
            no_task='process_empty_export'
        )

        write_payroll_data_csv = rail.WriteCSVFileOperator(
            task_id='write_payroll_data_csv',
            source="{{ result('query_entries_on_paycodes') }}",
            row=lambda item, dag_run, **context: custom_methods.get_gfs_payroll_data_rows(item, dag_run, context["index"], current_time,config),
            header=config.gfs_export_headers,
            delimiter=';',
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        upload_payroll_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_payroll_extract_to_s3',
            source="{{ result('write_payroll_data_csv') }}",
            key_name=config.s3_upload_filepath_gfs + '/{{ dag_run.conf.exportdetails.gfs_export_filename }}.csv',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_payroll_extract_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_payroll_extract_data_csv',
            pgp_conn_id=config.gfs_pgp_conn_id,
            source="{{ result('write_payroll_data_csv') }}",
            sign=True
        )

        upload_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_extract_to_sftp",
            content='{{ result("encrypt_payroll_extract_data_csv") }}',
            remote_filepath=config.input_filepath_gfs + '/{{ dag_run.conf.exportdetails.gfs_export_filename }}.csv.pgp'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll data extract to GFS for France'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath_gfs,
                'location': config.location,
                'time_zone': config.time_zone,
                'export_to': "GFS"
            }
        )

        process_empty_export = rail.EmptyOperator(
            task_id='process_empty_export'
        )

        write_blank_payroll_data_csv = rail.WriteCSVFileOperator(
            task_id='write_blank_payroll_data_csv',
            source=[],
            row=[],
            header=config.gfs_export_headers,
            delimiter=';',
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        encrypt_blank_payroll_extract_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_blank_payroll_extract_data_csv',
            pgp_conn_id=config.gfs_pgp_conn_id,
            source="{{ result('write_blank_payroll_data_csv') }}",
            sign=True
        )

        upload_blank_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_blank_payroll_extract_to_sftp",
            content='{{ result("encrypt_blank_payroll_extract_data_csv") }}',
            remote_filepath=config.input_filepath_gfs + '/{{ dag_run.conf.exportdetails.gfs_export_filename }}.csv.pgp'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll data extract to GFS for France'
                + ' - No records to export - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'time_zone': config.time_zone,
                'export_to': "GFS"
            }
        )

        finish_payroll_export = rail.EmptyOperator(
            task_id='finish_payroll_export'
        )

        batch_task >> finish_payroll_export
        batch_task >> query_entries_on_paycodes

        query_entries_on_paycodes >> is_entries_on_paycodes_exists
        is_entries_on_paycodes_exists >> rail.Label("Yes") >> write_payroll_data_csv >> upload_payroll_extract_to_s3 \
            >> encrypt_payroll_extract_data_csv >> upload_payroll_extract_to_sftp >> send_export_complete_email >> finish_payroll_export
        is_entries_on_paycodes_exists >> rail.Label("No") >> process_empty_export >> write_blank_payroll_data_csv \
            >> encrypt_blank_payroll_extract_data_csv >> upload_blank_payroll_extract_to_sftp \
                >> send_empty_export_email >> finish_payroll_export

    return dag

rail.for_each_instance(create_child_dag)
