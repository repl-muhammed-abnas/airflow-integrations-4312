from datetime import timedelta, datetime as dt
from airflow.models import Variable
import rail
from dxctechnology.philippines_payroll_export.utils import request_payload, custom_method


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_child_dag_id,
        description=f'DXCTechnology_philippines_Payroll_Export_Process Timeoff Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_timeoff_pay_codes_from_mapper = rail.PythonOperator(
            task_id='get_all_timeoff_pay_codes_from_mapper',
            python_callable=lambda: request_payload.get_all_required_pacodes(
                config.timeoff_paycodes)
        )

        query_list_in_final_timeoff_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_timeoff_payroll_collection',
            query="""SELECT *,
                    MIN(Entry_Date) OVER () AS min_entry_date,
                    MAX(Entry_Date) OVER () AS max_entry_date
                FROM finalpayrolldata
                WHERE Pay_Code_Code IN ({{ result('get_all_timeoff_pay_codes_from_mapper') }})"""
        )

        has_timeoff_item_data = rail.IfOperator(
            task_id='has_timeoff_item_data',
            test="{{ result('query_list_in_final_timeoff_payroll_collection','length') > 0 }}",
            yes_task='create_timeoff_log',
            no_task='send_email_empty_export'
        )

        create_timeoff_log = rail.CreateLogOperator(
            task_id='create_timeoff_log'
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_timeoff_log') }}",
            message="{{ dag_run.conf.process_started }} - Process started",
            properties={
                "log": """{{ dag_run.conf.process_started }} - Process started"""
            }
        )

        logging_no_of_records_exported = rail.WriteLogOperator(
            task_id="logging_no_of_records_exported",
            log="{{ result('create_timeoff_log') }}",
            message="{{ current_time() }} - INFO admin Total No of records exported = {{result('query_list_in_final_timeoff_payroll_collection','length')}}",
            properties={
                "log": "{{ current_time() }} - INFO admin Total No of records exported = {{result('query_list_in_final_timeoff_payroll_collection','length')}}",
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        load_users_data_from_report = rail.run_report(
            group_id='load_users_data_from_report',
            report_params=custom_method.get_load_users_data_from_report
        )

        has_load_users_data_from_report_data = rail.IfOperator(
            task_id='has_load_users_data_from_report_data',
            test='{{ result("load_users_data_from_report.get_report_result", "has_data") }}',
            yes_task='is_error_present_in_batch',
            no_task='get_shift_hours'
        )

        is_error_present_in_batch = rail.IfOperator(
            task_id='is_error_present_in_batch',
            test='{{ result("load_users_data_from_report.get_report_result").reportGenerationResults[0].error | is_truthy }}',
            yes_task='fail_with_error_log',
            no_task='load_users_data_from_report_payload_to_csv'
        )

        fail_with_error_log = rail.FailOperator(
            task_id='fail_with_error_log',
            message='{{ result("load_users_data_from_report.get_report_result").reportGenerationResults[0].error }}'
        )

        load_users_data_from_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="load_users_data_from_report_payload_to_csv",
            document='{{ result("load_users_data_from_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        user_report_collection = rail.CreateCollectionOperator(
            task_id='user_report_collection',
            name='userreport',
            source='{{ result("load_users_data_from_report_payload_to_csv") }}',
            columns={
                'Employee ID': 'employeeid',
                'Entry Date': 'date',
                'Shift Work Hours': 'hours',
                'username': 'username'
            }
        )

        get_shift_hours = rail.PythonOperator(
            task_id='get_shift_hours',
            python_callable= custom_method.get_shift_hours_callable,
        )

        compose_final_data_csv = rail.WriteCSVFileOperator(
            task_id='compose_final_data_csv',
            source='{{ result("get_shift_hours") | to_json }}',
            header=['Employee ID','Employee Name','Worker Type','Worker Status','Cost Center','Shift Hours',
                    'Time Off Date','Unit in Hours','Time Off Type for Time Off Entry'],
            row=custom_method.get_final_compose_data,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        pgp_encrypt_timeoff_file = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_timeoff_file",
            source="{{ result('compose_final_data_csv') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_encrypted_payroll_item_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_item_file_sftp",
            content="{{ result('pgp_encrypt_timeoff_file') }}",
            remote_filepath=config.output_filepath +
            '{{ dag_run.conf.timeoff_export_file_name }}.csv'
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_timeoff_log') }}",
            message="{{ current_time() }} - INFO admin Export File_{{ dag_run.conf.timeoff_export_file_name }}",
            properties={
                "log": "{{ current_time() }} - INFO admin Export File_{{ dag_run.conf.timeoff_export_file_name }}.csv"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_timeoff_log') }}",
            message="{{result('process_end_time')}} - Process ended",
            properties={
                "log": "{{result('process_end_time')}} - Process ended"
            }
        )

        log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="log_file_data_to_csv",
            source="{{ result('create_timeoff_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ]
        )

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath +
            'log_{{ dag_run.conf.timeoff_export_file_name }}.csv'
        )

        send_email_for_timeoff_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_timeoff_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon philippines timeoff payroll export is completed on - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'No_of_records': '{{ result("query_list_in_final_timeoff_payroll_collection",length) }}'
            },
            html_content="templates/email/timeoff_export_success.html"
        )

        send_email_empty_export = rail.EmailOperator(
            task_id='send_email_empty_export',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon philippines timeoff payroll export is skipped on - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/timeoff_export_empty_email.html"
        )

        get_all_timeoff_pay_codes_from_mapper >> query_list_in_final_timeoff_payroll_collection >> has_timeoff_item_data

        has_timeoff_item_data >> rail.Label(
            "Yes") >> create_timeoff_log >> logging_job_start_time >> logging_no_of_records_exported >> get_report_details

        get_report_details >> load_users_data_from_report >> has_load_users_data_from_report_data

        has_load_users_data_from_report_data >> rail.Label(
            "Yes") >> is_error_present_in_batch

        has_load_users_data_from_report_data >> rail.Label(
            "No") >> get_shift_hours

        is_error_present_in_batch >> rail.Label(
            "Yes") >> fail_with_error_log

        is_error_present_in_batch >> rail.Label(
            "No") >> load_users_data_from_report_payload_to_csv >> \
            user_report_collection >> get_shift_hours >> compose_final_data_csv >> pgp_encrypt_timeoff_file

        pgp_encrypt_timeoff_file >> upload_encrypted_payroll_item_file_sftp >> logging_file_creation >> process_end_time >> \
            logging_job_end_time >> log_file_data_to_csv >> upload_log_data_to_sftp

        upload_log_data_to_sftp >> send_email_for_timeoff_export_copmpletion

        has_timeoff_item_data >> rail.Label(
            "No") >> send_email_empty_export

    return dag


rail.for_each_instance(create_dag)
