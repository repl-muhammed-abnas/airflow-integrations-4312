from datetime import timedelta
from pendulum import datetime
from capgemini.france_payroll_export.utils import custom_methods
from airflow.models import Variable
import rail

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sopra_export_child_dag_id,
        description=f"Capgemini France Payroll Export to SOPRA Child {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 11, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_entries_on_paycodes',
            end_task='finish_payroll_export',
        )

        query_entries_on_paycodes = rail.QueryCollectionOperator(
            task_id='query_entries_on_paycodes',
            query=f"SELECT * FROM payrunpayrolldata WHERE Pay_Code_Name IN {config.sopra_paycodes}",
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
            row=custom_methods.get_sopra_payroll_data_rows,
            header=config.sopra_export_headers,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        create_payroll_data_xml = rail.RenderTemplateOperator(
            task_id='create_payroll_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_france_payroll.xml',
            dataset='{{ result("write_payroll_data_csv") }}'
        )

        upload_payroll_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_payroll_extract_to_s3',
            source="{{ result('create_payroll_data_xml') }}",
            key_name=config.s3_upload_filepath_sopra + '/{{ dag_run.conf.exportdetails.sopra_export_filename }}.xml',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_payroll_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_payroll_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_payroll_data_xml') }}"
        )

        upload_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_extract_to_sftp",
            content='{{ result("encrypt_payroll_extract_data_xml") }}',
            remote_filepath=config.input_filepath_sopra + '/{{ dag_run.conf.exportdetails.sopra_export_filename }}.xml.pgp'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll data extract to SOPRA for France'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath_sopra,
                'location': config.location,
                'time_zone': config.time_zone,
                'export_to': "SOPRA"
            }
        )

        process_empty_export = rail.EmptyOperator(
            task_id='process_empty_export'
        )

        create_blank_payroll_data_xml = rail.RenderTemplateOperator(
            task_id='create_blank_payroll_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_france_payroll.xml',
            dataset=custom_methods.get_empty_sopra_export_row
        )

        encrypt_blank_payroll_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_blank_payroll_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_blank_payroll_data_xml') }}"
        )

        upload_blank_payroll_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_blank_payroll_extract_to_sftp",
            content='{{ result("encrypt_blank_payroll_extract_data_xml") }}',
            remote_filepath=config.input_filepath_sopra + '/{{ dag_run.conf.exportdetails.sopra_export_filename }}.xml.pgp'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll data extract to SOPRA for France'
                + ' - No records to export - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'time_zone': config.time_zone,
                'export_to': "SOPRA"
            }
        )

        finish_payroll_export = rail.EmptyOperator(
            task_id='finish_payroll_export'
        )

        batch_task >> finish_payroll_export
        batch_task >> query_entries_on_paycodes

        query_entries_on_paycodes >> is_entries_on_paycodes_exists
        is_entries_on_paycodes_exists >> rail.Label("Yes") >> write_payroll_data_csv \
            >> create_payroll_data_xml >> upload_payroll_extract_to_s3 \
                >> encrypt_payroll_extract_data_xml >> upload_payroll_extract_to_sftp \
                    >> send_export_complete_email >> finish_payroll_export
        is_entries_on_paycodes_exists >> rail.Label("No") >> process_empty_export >> create_blank_payroll_data_xml \
            >> encrypt_blank_payroll_extract_data_xml >> upload_blank_payroll_extract_to_sftp \
                >> send_empty_export_email >> finish_payroll_export

    return dag

rail.for_each_instance(create_child_dag)
