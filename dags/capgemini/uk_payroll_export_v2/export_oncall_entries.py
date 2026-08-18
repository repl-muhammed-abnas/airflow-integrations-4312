from datetime import timedelta
from capgemini.uk_payroll_export_v2.utils import custom_methods
from airflow.models import Variable
import rail

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.oncall_export_child_dag_id,
        description=f"Capgemini UK Payroll Export On-Call Entries Child {config.instance} V2",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.export_oncall_max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_entries_on_paycodes'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_entries_on_paycodes',
            end_task='finish_payroll_export',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_entries_on_paycodes = rail.QueryCollectionOperator(
            task_id='query_entries_on_paycodes',
            query=f"SELECT * FROM payrunpayrolldata WHERE Pay_Code_Name IN {config.oncall_paycodes_list}",
            name='oncall_paycode_entries'
        )

        query_sum_of_pay_amount = rail.QueryCollectionOperator(
            task_id='query_sum_of_pay_amount',
            query="""SELECT ROUND(SUM(oncall_paycode_entries.Pay_Code_Hours), 2) AS total_amount FROM oncall_paycode_entries"""
        )

        is_entries_on_paycodes_exists = rail.IfOperator(
            task_id='is_entries_on_paycodes_exists',
            test='{{ result("query_entries_on_paycodes", "length") > 0 }}',
            yes_task='write_payroll_data_csv',
            no_task='send_empty_export_email'
        )

        write_payroll_data_csv = rail.WriteCSVFileOperator(
            task_id='write_payroll_data_csv',
            source="{{ result('query_entries_on_paycodes') }}",
            row=custom_methods.get_oncall_payroll_data_rows,
            header=config.oncall_export_headers,
            footer=lambda: [
                "Grand Total", "", "", "", "", rail.load_all_records(
                    rail.result("query_sum_of_pay_amount"))[0]["total_amount"]
            ],
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        upload_payroll_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_payroll_extract_to_s3',
            source="{{ result('write_payroll_data_csv') }}",
            key_name=config.s3_oncall_upload_filepath +
            '/{{ dag_run.conf.export_filename }}.csv',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_payroll_extract_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_payroll_extract_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_payroll_data_csv') }}"
        )

        upload_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_extract_to_sftp",
            content='{{ result("encrypt_payroll_extract_data_csv") }}',
            remote_filepath=config.oncall_input_filepath +
            '/{{ dag_run.conf.export_filename }}.csv.pgp'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon on-call payroll export for "{{ dag_run.conf.cost_center_name }}" cost center (United Kingdom)'
            + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.oncall_input_filepath,
                'location': config.location,
                'time_zone': config.time_zone,
                'entries_for': "on-call"
            }
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon on-call payroll export for "{{ dag_run.conf.cost_center_name }}" cost center (United Kingdom)'
            + ' - No records to export - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'time_zone': config.time_zone,
                'entries_for': "on-call"
            }
        )

        finish_payroll_export = rail.EmptyOperator(
            task_id='finish_payroll_export'
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> finish_payroll_export
        can_run_batch_task >> rail.Label("No") >> query_entries_on_paycodes
        query_entries_on_paycodes >> query_sum_of_pay_amount >> is_entries_on_paycodes_exists
        is_entries_on_paycodes_exists >> rail.Label("Yes") >> write_payroll_data_csv \
            >> upload_payroll_extract_to_s3 >> encrypt_payroll_extract_data_csv >> upload_payroll_extract_to_sftp \
            >> send_export_complete_email >> finish_payroll_export
        is_entries_on_paycodes_exists >> rail.Label("No") >> send_empty_export_email >> finish_payroll_export

    return dag


rail.for_each_instance(create_child_dag)
