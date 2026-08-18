from datetime import timedelta
import rail
from pendulum import datetime
from ge.timesheet_recalc_portugal.utils.custom_methods import logging_details


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'ge_timesheet_recalc_portugal_master_{config.instance}',
        description='GE Timesheet Recalc Portugal Master',
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

        get_hourly_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_hourly_report_details',
            report_name=config.extract_timesheet_recalc_report,
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

        create_timesheet_data_collection = rail.CreateCollectionOperator(
            task_id='create_timesheet_data_collection',
            source="{{ result('load_report_data') }}",
            name="input_data"
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_timesheet_data_collection', 'length') > 0 }}",
            yes_task='process_records',
            no_task='no_records'
        )

        no_records = rail.EmptyOperator(
            task_id="no_records"
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        process_time_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_time_records",
            items="{{result('create_timesheet_data_collection')}}",
            batch_size=50,
            trigger_dag_id=f"ge_timesheet_data_process_each_record_child_{config.instance}",
            conf=lambda item: {
                "timesheetdetails": item
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_time_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_time_records",
            dag_runs="{{result('process_time_records')}}",
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['Username', 'Timesheetstartdate', 'Timesheetenddate',
                    'Recalcstatus', 'Details', 'Timesheeturi', 'parentecid', 'childecid'],
            row=['{{ item.properties.username }}', '{{ item.properties.timesheetstartdate }}',
                 '{{ item.properties.timesheetenddate }}', '{{ item.properties.status }}', '{{ item.message }}', '{{ item.properties.timesheeturi }}',
                 "{{dag_run_ecid()}}", '{{ item.properties.childecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.extract_timesheet_file_path + "{{dag_run_ecid()}}" +
            config.extract_timesheet_recalc_file_name + ".csv",
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Error',
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('filter_master_log', 'length') > 0 }}",
            yes_task='generate_download_link',
            no_task='log_to_sumo'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("render_logs_csv")}}',
            output_file_name=config.extract_timesheet_file_path + "{{dag_run_ecid()}}" +
            config.extract_timesheet_recalc_file_name + ".csv",
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Portugal Timesheet recalc has completed with errors - {{ current_time_in_specified_tz() }}',
            html_content="templates/mail_with_error.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        get_logging_details >> get_hourly_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> create_timesheet_data_collection >> has_any_records

        has_any_records >> rail.Label("Yes") >> process_records >> process_time_records >> wait_process_time_records >> render_logs_csv\
            >> upload_log_to_sftp >> filter_master_log >> any_records_failed >> rail.Label("Yes")\
            >> generate_download_link >> send_completion_error_mail >> log_to_sumo

        any_records_failed >> rail.Label("No") >> log_to_sumo

        has_any_records >> rail.Label("No") >> no_records

        report_has_data >> rail.Label("No") >> no_data

    return dag


rail.for_each_instance(create_main_airflow_dag)
