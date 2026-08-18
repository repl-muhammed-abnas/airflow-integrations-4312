from datetime import timedelta
from capgemini.payroll_export_hgs.utils import custom_methods
from airflow.models import Variable
import rail

null = None

# This dag called from the dag_id=capgemini_time_export_global_master_<instance>
# Payroll export HGS depends on the time export global
# We are using the same data extracted from TWB for time export global and payroll export hgs


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_payroll_export_hgs_child_{config.instance}',
        description=f'Capgemini payroll export hgs Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        locations = "All"

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config]
        )

        query_required_timeoffs_for_payroll = rail.QueryCollectionOperator(
            task_id='query_required_timeoffs_for_payroll',
            query="""SELECT * FROM datatoexport WHERE ProjectTime_Absence_Type_Name
                    IN {{ result('logging_details').required_timeoffs }}
                    AND (CAST(ProjectTime_Hours AS DECIMAL) < -4 OR CAST(ProjectTime_Hours AS DECIMAL) > 4)""",
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("query_required_timeoffs_for_payroll", "length") > 0 }}',
            yes_task='write_payroll_data_to_csv',
            no_task='send_empty_export_email'
        )

        write_payroll_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_payroll_data_to_csv',
            source='{{ result("query_required_timeoffs_for_payroll") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=custom_methods.get_time_data_csv_rows,
            delimiter=';',
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv)
        )

        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Global Payroll leave requests data extract is skipped - {{ result("logging_details").process_start_time }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'location': locations
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
            subject='{{ get_company_key() }} | The Global Payroll leave requests data extract is completed - \
                {{ " " + result("logging_details").process_start_time }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': locations
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            extra_info=lambda dag_run: {
                'locations': locations,
                'daterange': dag_run.conf["export_logging_details"]["export_start_date"] + ' - ' + dag_run.conf["export_logging_details"]["export_end_date"],
                'twbrowcount': len(rail.load_all_records(dag_run.conf["payroll_data"]))
                    if dag_run.conf["payroll_data"] and len(rail.load_all_records(dag_run.conf["payroll_data"])) > 0 else 0,
                'filename': rail.result("logging_details")["payroll_export_filename"] + '.csv.pgp'
                    if dag_run.conf["payroll_data"] and len(rail.load_all_records(dag_run.conf["payroll_data"])) > 0 else ""
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

        logging_details >> query_required_timeoffs_for_payroll >> has_data
        has_data >> rail.Label("Yes") >> write_payroll_data_to_csv >> upload_payroll_export_to_s3 >> encrypt_payroll_export_data_csv \
            >> upload_payroll_export_to_sftp >> send_valid_export_complete_email >> dagrun_log_to_sumo
        has_data >> rail.Label(
            "No") >> send_empty_export_email >> dagrun_log_to_sumo
        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_payroll_export

    return dag


rail.for_each_instance(create_dag)
