from datetime import timedelta
from pendulum import datetime
import rail
from necau.time_off_shift_assignment_90_days_rolling_period.utils import request_payload
from necau.time_off_shift_assignment_90_days_rolling_period.utils import python_callable_method
from necau.time_off_shift_assignment_90_days_rolling_period.utils import custom_method


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'necau_timeoff_shift_assignment_master_weekly_{config.instance}',
        description=f'NECAU - timeoff shift assignment_Master_weekly_v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_weekly,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_shift_report_name,
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
            no_task='send_no_data_email'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            name='userdata',
            source="{{ result('load_report_data') }}",
            columns={
                'User Name': 'user_name',
                'user uri': 'user_uri',
                'Auto schedule assignment - days Wk1': 'user_wk1_pattern',
                'Auto schedule assignment - days Wk2': 'user_wk2_pattern',
                'Auto schedule assignment - shift': 'user_shift_name',
                'Auto schedule assignment - start date Wk1': 'user_start_date'}
        )

        query_userdata = rail.QueryCollectionOperator(
            task_id='query_userdata',
            query='SELECT * FROM userdata'
        )

        query_user_has_data = rail.IfOperator(
            task_id="query_user_has_data",
            test="{{ result('query_userdata','length') > 0 }}",
            yes_task='get_timeoff_date_range',
            no_task='send_no_data_email'
        )

        get_timeoff_date_range = rail.PythonOperator(
            task_id='get_timeoff_date_range',
            python_callable=python_callable_method.get_timeoff_date_range
        )

        bulk_get_timeOff_summary_for_user_date_range = rail.RepliconServiceOperator(
            task_id='bulk_get_timeOff_summary_for_user_date_range',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffSummaryForUserAndDateRange",
            data=request_payload.get_timeoff_shift_summary_payload,
            data_handler=custom_method.get_timeoff_summary_info
        )

        bulk_get_user_holiday_series = rail.RepliconServiceOperator(
            task_id='bulk_get_user_holiday_series',
            endpoint="/services/HolidayCalendarService2.svc/BulkGetUserHolidaySeries",
            data=request_payload.get_holiday_series_payload,
            data_handler=custom_method.get_holiday_series_info
        )

        get_final_holiday_effective_dates = rail.PythonOperator(
            task_id='get_final_holiday_effective_dates',
            python_callable=lambda: python_callable_method.get_final_effective_dates(
                'bulk_get_user_holiday_series')
        )

        process_holiday_shifts = rail.TriggerDagRunForEachItemOperator(
            task_id='process_holiday_shifts',
            retries=0,
            items=lambda: rail.result('get_final_holiday_effective_dates'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_time_off_shift_assignment_child_{config.instance}',
            conf=request_payload.get_timeoff_holiday_info
        )

        wait_for_process_timeoff_shifts = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoff_shifts',
            dag_runs='{{ result("process_timeoff_shifts") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_final_timeoff_effective_dates = rail.PythonOperator(
            task_id='get_final_timeoff_effective_dates',
            python_callable=lambda: python_callable_method.get_final_effective_dates(
                'bulk_get_timeOff_summary_for_user_date_range')
        )

        process_timeoff_shifts = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff_shifts',
            retries=0,
            items=lambda: rail.result('get_final_timeoff_effective_dates'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_time_off_shift_assignment_child_{config.instance}',
            conf=request_payload.get_timeoff_holiday_info
        )

        wait_for_process_holiday_shifts = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_holiday_shifts',
            dag_runs='{{ result("process_holiday_shifts") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'booking_date',
                'user_name',
                'pattern',
                'status',
                'reason',
                'jobid'],
            row=[
                '{{ item.properties | attr_or_default("booking_date", "") }}',
                '{{ item.properties | attr_or_default("user_name", "") }}',
                '{{ item.properties | attr_or_default("pattern", "") }}',
                '{{ item.properties | attr_or_default("status", "")}}',
                '{{ item.properties | attr_or_default("reason", "") }}',
                '{{ item.properties | attr_or_default("jobid", "") }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Shift Assignment for Time Off 90 days rolling period run - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/email/import_complete.html",
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() + " |  Daily Shift Assignment for Time Off 90 days rolling period - No data found - " }} \
                {{ current_time() }}',
            html_content="templates/email/no_data.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'record_count ': '{{ result("query_userdata","length")}}'
            }
        )

        get_report_details >> run_report_group_entry
        run_report_group_exit >> report_has_data
        report_has_data >> rail.Label("Yes") >> \
            load_report_data >> create_user_collection >> query_userdata >> query_user_has_data
        query_user_has_data >> rail.Label('yes') >> get_timeoff_date_range >> bulk_get_timeOff_summary_for_user_date_range >> bulk_get_user_holiday_series >> \
            get_final_holiday_effective_dates >> process_holiday_shifts >> wait_for_process_holiday_shifts >> \
            get_final_timeoff_effective_dates >> process_timeoff_shifts >> wait_for_process_timeoff_shifts >> get_errored_logs >> render_logs_csv
        render_logs_csv >> generate_download_link >> send_import_complete_email >> log_to_sumo
        report_has_data >> rail.Label("No") >> \
            send_no_data_email >> log_to_sumo
        query_user_has_data >> rail.Label("No") >> \
            send_no_data_email >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
