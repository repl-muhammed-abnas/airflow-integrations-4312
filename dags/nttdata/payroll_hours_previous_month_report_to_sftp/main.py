from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'nttdata_payroll_hours_previous_month_report_to_sftp_master_{config.instance}',
        description=f'nttdata_payroll_hours_previous_month_report_to_sftp_master {config.instance}',
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

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_report_data',
            no_task='finish'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        upload_reportdata_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reportdata_to_sftp',
            content="{{ result('load_report_data') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.filepath + "Payroll_Hours_Previous_Month"
            '{{ current_time_in_specified_tz("America/New_York","%Y%m%d") }}' + ".csv"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "exportfile_name": config.filepath + "Payroll_Hours_Previous_Month"
            '{{ current_time_in_specified_tz("America/New_York","%Y%m%d") }}' + ".csv"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed >> rail.Label('No') >> report_has_data >> rail.Label(
            'Yes') >> load_report_data >> upload_reportdata_to_sftp >> log_to_sumo
        is_report_failed >> rail.Label('Yes') >> fail_report_generation
        report_has_data >> rail.Label('No') >> finish

    return dag


rail.for_each_instance(create_main_dag)
