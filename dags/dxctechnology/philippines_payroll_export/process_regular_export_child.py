from datetime import timedelta, datetime as dt
from airflow.models import Variable
import rail
from dxctechnology.philippines_payroll_export.utils import request_payload, custom_method


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.regular_child_dag_id,
        description=f'DXCTechnology_philippines_Payroll_Export_Process Regular Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_regular_pay_codes_from_mapper = rail.PythonOperator(
            task_id='get_all_regular_pay_codes_from_mapper',
            python_callable=lambda: request_payload.get_all_required_pacodes(
                config.regular_paycodes_mapper)
        )

        query_list_in_final_regular_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_regular_payroll_collection',
            query="""SELECT * FROM finalpayrolldata WHERE Pay_Code_Code IN ({{result('get_all_regular_pay_codes_from_mapper')}})"""
        )

        has_regular_item_data = rail.IfOperator(
            task_id='has_regular_item_data',
            test="{{ result('query_list_in_final_regular_payroll_collection','length') > 0 }}",
            yes_task='create_regular_log',
            no_task='send_email_empty_export'
        )

        create_regular_log = rail.CreateLogOperator(
            task_id='create_regular_log'
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_regular_log') }}",
            message="{{ dag_run.conf.process_started }} - Process started",
            properties={
                "log": """{{ dag_run.conf.process_started }} - Process started"""
            }
        )

        logging_no_of_records_exported = rail.WriteLogOperator(
            task_id="logging_no_of_records_exported",
            log="{{ result('create_regular_log') }}",
            message="{{ current_time() }} - INFO admin Total No of records exported = {{result('query_list_in_final_regular_payroll_collection','length')}}",
            properties={
                "log": "{{ current_time() }} - INFO admin Total No of records exported = {{result('query_list_in_final_regular_payroll_collection','length')}}",
            }
        )

        compose_item_payroll_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_item_payroll_csv_file',
            source="{{ result('query_list_in_final_regular_payroll_collection') }}",
            header=["Employee ID","Employee Name","Claim Date","Description","Hours"],
            row=custom_method.get_compose_item_regular_payroll
        )

        pgp_encrypt_regular_file = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_regular_file",
            source="{{ result('compose_item_payroll_csv_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_encrypted_payroll_item_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_item_file_sftp",
            content="{{ result('pgp_encrypt_regular_file') }}",
            remote_filepath=config.output_filepath +
            '{{ dag_run.conf.regular_export_file_name }}.csv'
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_regular_log') }}",
            message="{{ current_time() }} - INFO admin Export File {{ dag_run.conf.regular_export_file_name }}",
            properties={
                "log": "{{ current_time() }} - INFO admin Export File {{ dag_run.conf.regular_export_file_name }}.csv"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_regular_log') }}",
            message="{{result('process_end_time')}} - Process ended",
            properties={
                "log": """{{result('process_end_time')}} - Process ended"""
            }
        )

        log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="log_file_data_to_csv",
            source="{{ result('create_regular_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ]
        )

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath +
            'log_{{ dag_run.conf.regular_export_file_name }}.csv'
        )

        send_email_for_regular_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_regular_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon philippines overtime and standby payroll export is completed on - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'No_of_records': '{{ result("create_valid_data_collection",length) }}'
            },
            html_content="templates/email/regular_export_success.html"
        )

        send_email_empty_export = rail.EmailOperator(
            task_id='send_email_empty_export',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon philippines overtime and standby payroll export is skipped on - {{ current_time_in_specified_tz() }}',

            html_content="templates/email/regular_export_empty_email.html"
        )

        get_all_regular_pay_codes_from_mapper >> query_list_in_final_regular_payroll_collection >> has_regular_item_data

        has_regular_item_data >> rail.Label(
            "Yes") >> create_regular_log >> logging_job_start_time >> logging_no_of_records_exported >> \
            compose_item_payroll_csv_file >> pgp_encrypt_regular_file >> upload_encrypted_payroll_item_file_sftp >>\
                logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv >> upload_log_data_to_sftp

        upload_log_data_to_sftp >> send_email_for_regular_export_copmpletion

        has_regular_item_data >> rail.Label(
            "No") >> send_email_empty_export

    return dag


rail.for_each_instance(create_dag)
