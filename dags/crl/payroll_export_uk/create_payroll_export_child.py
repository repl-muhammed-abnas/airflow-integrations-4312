# CRL UK Payroll 2010 Infotype Payroll Data

from datetime import timedelta
import rail
from crl.payroll_export_uk.utils import request_payload
from crl.payroll_export_uk.utils import python_callable


def create_payroll_export_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.payroll_export_child_dag_id,
        description=f"CRL UK Payroll Export Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_batch_child,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dag_run_conf"
        )

        create_payroll_log = rail.CreateLogOperator(
            task_id='create_payroll_log'
        )

        logging_payroll_job_start_time = rail.WriteLogOperator(
            task_id="logging_payroll_job_start_time",
            log="{{ result('create_payroll_log') }}",
            message="{{ dag_run.conf.process_start_time }} - Payroll Export Process started",
            properties={
                "log": "{{ dag_run.conf.process_start_time }} - Payroll Export Process started"
            }
        )

        compose_payroll_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_payroll_csv_file',
            source="{{ dag_run.conf.collection_data }}",
            header=["RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                    "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "LGART", "STDAZ", "BEGUZ", "ENDUZ", "BETRG", "WAERS",
                    "ANZHL", "ZEINH", "VTKEN", "BWGRL", "AUFKZ", "ENDOF", "UFLD1", "UFLD2", "UFLD3", "KEYPR", "TRFGR", "TRFST", "PRAKN", "PRAKZ",
                    "OTYPE", "PLANS", "VERSL", "EXBEL", "WTART", "TDLANGU", "TDSUBLA", "TDTYPE"],
            row=lambda item: request_payload.get_compose_item_payroll_uk_data_row(item),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        logging_payroll_records_exported = rail.WriteLogOperator(
            task_id="logging_payroll_records_exported",
            log="{{ result('create_payroll_log') }}",
            message="{{ dag_run.conf.process_start_time }} - INFO admin No of payroll records exported" +
            " = {{ dag_run.conf.collection_length }}",
            properties={
                "log": "{{ dag_run.conf.process_start_time }} - INFO admin No of payroll records exported" +
                " = {{ dag_run.conf.collection_length }}",
            }
        )

        logging_payroll_file_creation = rail.WriteLogOperator(
            task_id="logging_payroll_file_creation",
            log="{{ result('create_payroll_log') }}",
            message="{{ dag_run.conf.process_start_time }} - INFO admin Payroll Export File : " +
            "{{ dag_run.conf.payroll_file_name }}.SAP",
        )

        no_of_payroll_records_size_including_header_footer = rail.PythonOperator(
            task_id="no_of_payroll_records_size_including_header_footer",
            python_callable=lambda collection_length: int(collection_length) + 2,
            op_kwargs={"collection_length": "{{ dag_run.conf.collection_length }}"}
        )

        create_payroll_document = rail.RenderTemplateOperator(
            task_id='create_payroll_document',
            target='artifact',
            template_file='schema/uk_payroll_export_data.txt',
            dataset="{{ result('compose_payroll_csv_file') }}",
        )

        upload_payroll_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_file_to_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('create_payroll_document')}}",
            remote_filepath=config.secondary_output_filepath +
            "/{{ dag_run.conf.payroll_file_name }}.SAP"
        )
        
        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_payroll_document') }}",
            pgp_conn_id=config.pgp_conn_id,
            sign=True
        )

        upload_payroll_encrypted_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_encrypted_file_to_sftp",
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.output_filepath +
            "/{{ dag_run.conf.payroll_file_name }}.SAP.pgp"
        )

        if_error_in_upload_to_sftp = rail.IfOperator(
            task_id="if_error_in_upload_to_sftp",
            test= request_payload.is_upload_data_to_sftp_failed_payroll,
            yes_task='send_payroll_email_for_sftp_failure',
            no_task='fail_export'
        )

        upload_encrypted_payroll_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_file_to_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "/{{ dag_run.conf.payroll_file_name }}.SAP.pgp"
        )

        payroll_process_end_time = rail.PythonOperator(
            task_id="payroll_process_end_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        logging_payroll_job_end_time = rail.WriteLogOperator(
            task_id="logging_payroll_job_end_time",
            log="{{ result('create_payroll_log') }}",
            message="{{ result('payroll_process_end_time').start_time }} - Payroll Export Process ended",
            properties={
                "log": "{{ result('payroll_process_end_time').start_time }} - Payroll Export Process ended"
            }
        )

        payroll_log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="payroll_log_file_data_to_csv",
            source="{{ result('create_payroll_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ]
        )

        upload_payroll_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_log_to_sftp",
            content="{{ result('payroll_log_file_data_to_csv') }}",
            remote_filepath=config.output_filepath +
            "/log_{{ dag_run.conf.payroll_file_name }}_{{ dag_run.conf.ymd_format }}{{ dag_run.conf.hms_format }}" + ".txt"
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        if_error_in_log_upload_to_sftp = rail.IfOperator(
            task_id="if_error_in_log_upload_to_sftp",
            test= request_payload.is_upload_log_to_sftp_failed_payroll,
            yes_task='send_email_for_log_upload_failure',
            no_task='fail_export_before_logs'
        )

        send_payroll_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_payroll_email_for_sftp_failure',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Payroll Export Notification - SFTP Upload Failure |' + \
            ' {{ dag_run.conf.process_start_time }} | for '+ config.location_for_mails + ' - Completed with errors',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ("{{ dag_run.conf.payroll_file_name }}.SAP.pgp", '{{result("create_payroll_document")}}')]
        )

        send_email_payroll_export_completion = rail.EmailOperator(
            task_id='send_email_payroll_export_completion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Payroll Export Notification |' + \
            ' {{ dag_run.conf.process_start_time }} | for '+ config.location_for_mails + ' - Completed successfully',
            params={
                'output_filepath': config.output_filepath
            },
            html_content="/templates/email/export_success.html"
        )

        send_email_for_log_upload_failure = rail.EmailOperator(
            task_id='send_email_for_log_upload_failure',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Payroll Export Notification - SFTP Log Upload Failure |' + \
            ' {{ dag_run.conf.process_start_time }} | for '+ config.location_for_mails + ' - Completed with errors',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_log_failure.html",
            files=[
                ("log_{{ dag_run.conf.payroll_file_name }}_{{ dag_run.conf.ymd_format }}{{ dag_run.conf.hms_format }}.txt", '{{result("payroll_log_file_data_to_csv")}}')]
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="Timeoff file export has failed"
        )

        fail_export_before_logs = rail.FailOperator(
            task_id="fail_export_before_logs",
            message="Timeoff file export has failed"
        )

        create_payroll_log >> logging_payroll_job_start_time >> compose_payroll_csv_file \
            >> logging_payroll_records_exported >> logging_payroll_file_creation >> no_of_payroll_records_size_including_header_footer \
            >> create_payroll_document >> pgp_encyrpt_item_file >> upload_payroll_file_to_secondary_sftp \
            >> upload_payroll_encrypted_file_to_sftp 

        upload_payroll_encrypted_file_to_sftp >> rail.Label("on_success") >> upload_encrypted_payroll_file_to_secondary_sftp >> payroll_process_end_time >> logging_payroll_job_end_time \
            >> payroll_log_file_data_to_csv >> upload_payroll_log_to_sftp
        
        upload_payroll_encrypted_file_to_sftp >> rail.Label("on_error") >> catch_error >> if_error_in_upload_to_sftp >> rail.Label("Yes") >> send_payroll_email_for_sftp_failure 
        if_error_in_upload_to_sftp >> rail.Label("No") >> fail_export

        upload_payroll_log_to_sftp >> rail.Label("on_success") >> send_email_payroll_export_completion
        upload_payroll_log_to_sftp >> rail.Label("on_error") >> catch_error >> if_error_in_log_upload_to_sftp >> rail.Label("Yes") >> send_email_for_log_upload_failure
        if_error_in_log_upload_to_sftp >> rail.Label("No") >> fail_export_before_logs

    return dag


rail.for_each_instance(create_payroll_export_dag)
