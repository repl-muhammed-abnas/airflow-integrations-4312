import rail
from pendulum import datetime
from bsi.report_to_sftp.utlis.custom_methods import logging_details


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'bsi_billing_monthly_master_{config.instance}',
        description='BSI Extract Report Billing Monthly master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_monthly,
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

        get_hourly_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_hourly_report_details',
            report_name=config.extract_billing_monthly,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_hourly_report_details').uri }}",
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
            remote_filepath='/'+ config.extract_billing_monthly_file_name +
            '{{ result("get_logging_details")["dag_start_time_weekly_monthly"] }} Monthly Extract.csv',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'filename': config.extract_billing_monthly_file_name +
                '{{ result("get_logging_details")["dag_start_time_weekly_monthly"] }} Monthly Extract.csv'
            }
        )

        get_logging_details >> get_hourly_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label("Yes") >>fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> upload_report_to_sftp >> log_to_sumo
        log_to_sumo
        report_has_data >> rail.Label("No") >> no_data

    return dag


rail.for_each_instance(create_main_airflow_dag)
