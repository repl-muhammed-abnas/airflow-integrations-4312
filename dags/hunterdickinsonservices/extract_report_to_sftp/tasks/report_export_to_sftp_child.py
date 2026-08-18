import rail
from hunterdickinsonservices.extract_report_to_sftp.utils.custom_methods import logging_details

def process_report_to_sftp(config,report_name,file_name,file_path):
    with rail.TaskGroup(group_id='process_report_to_sftp', prefix_group_id=False):


        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.time_zone]
        )

        get_daily_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_daily_report_details',
            report_name=report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_daily_report_details').uri }}",
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
            no_task='send_no_data_mail',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload }}",
            delimiter='|',
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Export report for '+ file_name +' skipped  at - {{ result("get_logging_details")["dag_run_start_time"] }}',
            html_content="templates/emails/no_data.html",
            params={
                'report_name': file_name
            }
        )

        report_data_csv = rail.WriteCSVFileOperator(
            task_id='report_data_csv',
            source="{{ result('load_report_data') }}",
            delimiter='|',
        )

        upload_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_report_to_sftp',
            content="{{ result('report_data_csv') }}",
            remote_filepath=file_path + file_name + '_{{ result("get_logging_details")["dag_start_time"] }}.csv',
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Export report is completed for  '+ file_name +' at - {{ result("get_logging_details")["dag_run_start_time"] }}',
            html_content="templates/emails/success_mail.html",
            params={
                'report_name': file_name,
                'report_file_path': file_path
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'filename': file_name + '_{{ result("get_logging_details")["dag_start_time"] }}.csv'
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )


        get_logging_details >> get_daily_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label("Yes") >>fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> report_data_csv >> upload_report_to_sftp >> send_success_mail >> log_to_sumo
        log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun
        report_has_data >> rail.Label("No") >> send_no_data_mail

    return send_success_mail
