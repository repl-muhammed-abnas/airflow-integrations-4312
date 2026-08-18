import rail
from pendulum import datetime
from briarhallllc.report_to_sftp.utlis.custom_methods import logging_details


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'briarhallllc_timeentry_audit_master_{config.instance}',
        description='Briarhallllc Extract Report Timeentry Audit master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:


        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.time_zone]
        )

        get_timeentry_audit_details = rail.RepliconReportDetailsOperator(
            task_id='get_timeentry_audit_details',
            report_name=config.extract_timeentry_export,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_timeentry_audit_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("get_report_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('get_report_details.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('get_report_details.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='no_data',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload }}"
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        upload_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_report_to_sftp',
            content="{{ result('load_report_data') }}",
            remote_filepath=config.extract_report_file_path + config.extract_timeentry_export_file_name +
            '_{{ result("get_logging_details")["dag_start_time_file_name"] }}.csv',
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Time_Entry_Export - CSV_Extract_Completed_"{{ result("get_logging_details")["dag_start_time"] }}',
            html_content="templates/email/success_mail.html",
            params={
                'report_name': config.extract_timeentry_export_file_name,
                'report_file_path': config.extract_report_file_path
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'filename': config.extract_timeentry_export_file_name +
                '_{{ result("get_logging_details")["dag_start_time_file_name"] }}.csv'
            }
        )

        get_logging_details >> get_timeentry_audit_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label("Yes") >>fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> upload_report_to_sftp >> send_success_mail >> log_to_sumo
        log_to_sumo
        report_has_data >> rail.Label("No") >> no_data

    return dag


rail.for_each_instance(create_main_airflow_dag)
