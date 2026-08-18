from pendulum import datetime, now
import rail

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcfr_user_report_export_master_{config.instance}",
        description="pwcfr user report export master",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_connid,
        start_date=datetime(2023,6,21,tz=config.cest_time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            "sftp_connid": config.sftp_connid
        }
    ) as dag:

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.user_report_name
        )

        run_user_report_entry, run_user_report_exit = rail.run_report(
            group_id="run_user_report",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": '{{ result("get_user_report_details").uri }}',
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        is_user_report_run_success = rail.IfOperator(
            task_id ="is_user_report_run_success",
            test='{{result("run_user_report.get_report_result").reportGenerationResults[0].error| is_falsy}}',
            yes_task="check_if_user_report_has_data",
            no_task="fail_dag_run"

        )

        check_if_user_report_has_data = rail.IfOperator(
            task_id="check_if_user_report_has_data",
            test='{{result("run_user_report.get_report_result").reportGenerationResults[0].payload| length > 0}}',
            yes_task="load_user_report_data_to_csv",
            no_task="log_to_sumo"
        )

        fail_dag_run = rail.FailOperator(
            task_id="fail_dag_run",
            message="get_error_message()"
        )

        load_user_report_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_user_report_data_to_csv",
            document='{{result("run_user_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        write_user_report_data_to_csv = rail.WriteCSVFileOperator(
            task_id="write_user_report_data_to_csv",
            source='{{result("load_user_report_data_to_csv")}}'
        )

        upload_user_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id = "upload_user_export_to_sftp",
            sftp_conn_id=config.sftp_connid,
            content='{{result("write_user_report_data_to_csv")}}',
            remote_filepath=config.sftp_file_export_path + config.user_export_filename + now(tz=config.cest_time_zone).strftime("%d%m%Y") + ".csv"
        )

        def is_upload_failed():
            if rail.get_current_context()['dag_run'].get_task_instance("upload_user_export_to_sftp").current_state() == "failed":
                return True
            return False

        is_user_export_upload_successful = rail.IfOperator(
            task_id = "is_user_export_upload_successful",
            test=is_upload_failed,
            yes_task="send_user_export_upload_failed_mail",
            no_task="log_to_sumo",
        )

        send_user_export_upload_failed_mail = rail.EmailOperator(
            task_id="send_user_export_upload_failed_mail",
            to=config.alerts_email,
            subject='{{get_company_key()}} | Failed uploading report extract to SFTP - {{current_time_in_specified_tz()}}',
            html_content="sftp_upload_failure_email.html",
            params={
                "report_name": config.user_report_name,
            },
            files=[
                    ("{{ result('write_user_report_data_to_csv') }}")
                ]
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()| is_truthy}}',
            yes_task="fail_dag_run"
        )
        get_user_report_details >> run_user_report_entry >> run_user_report_exit >>\
        is_user_report_run_success >> rail.Label("Yes") >> check_if_user_report_has_data
        is_user_report_run_success >> rail.Label("No") >> fail_dag_run
        check_if_user_report_has_data >> rail.Label("Yes") >> load_user_report_data_to_csv >> write_user_report_data_to_csv >> \
        upload_user_export_to_sftp >> is_user_export_upload_successful >> rail.Label("No") >> \
        send_user_export_upload_failed_mail
        check_if_user_report_has_data >> rail.Label("No") >> log_to_sumo
        is_user_export_upload_successful >> rail.Label("Yes") >> log_to_sumo >> can_fail_dag >> fail_dag_run

        return dag


rail.for_each_instance(create_main_airflow_dag)
