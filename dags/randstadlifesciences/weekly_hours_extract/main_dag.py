from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'randstadlifescience_weekly_hours_export_{config.instance}',
        description=f'randstadlifescience weekly hours export {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2023, 1, 1, tz=config.eastern_timezone),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_report_data',
            no_task='empty_no_report_data'
        )

        empty_no_report_data = rail.EmptyOperator(
            task_id="empty_no_report_data")

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('load_report_data') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.upload_filepath + '/' + config.report_name +
            '{{ current_time_in_specified_tz("America/New_York","%m%d%Y%H%M%S")  }}.csv',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'filename': config.report_name +
            '{{ current_time_in_specified_tz("America/New_York","%m%d%Y%H%M%S")  }}.csv'
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

        get_report_details >> run_report_group_entry
        run_report_group_exit >> report_has_data >> rail.Label(
            'Yes') >> load_report_data
        report_has_data >> rail.Label('No') >> empty_no_report_data
        load_report_data >> upload_log_to_sftp >> log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
