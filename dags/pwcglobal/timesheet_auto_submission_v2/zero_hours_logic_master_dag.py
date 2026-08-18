from datetime import timedelta
from pendulum import datetime
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import/config.py


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_timesheet_auto_submission_zero_hours_v4_master_{config.instance}',
        description=f'PwCGlobal - Timesheet auto submission zero hours LOA Auto_submission_Master_v4.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 12, 15, tz=config.time_zone),
        # runs at 9PM on every day
        schedule_interval=config.zero_hours_schedule_interval,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timesheet_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
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
            no_task='finish'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_timesheet_collection = rail.CreateCollectionOperator(
            task_id='create_timesheet_collection',
            name='timesheet',
            source="{{ result('load_report_data') }}",
        )

        query_zero_hours_timesheets = rail.QueryCollectionOperator(
            task_id='query_zero_hours_timesheets',
            query='''SELECT timesheeturi
                        FROM timesheet
                        WHERE CAST(Scheduled_Hrs__In_Period_ as decimal)= 0 AND CAST(Total_Hrs__In_Period_ as decimal)= 0 AND
                        CAST(daydiff as decimal) < 1 AND (Approval_Status = "Not Submitted" OR
                        Approval_Status = "Waiting for Approval")'''
        )

        query_non_zero_timesheet = rail.QueryCollectionOperator(
            task_id='query_non_zero_timesheet',
            query='''SELECT User_Name, useruri, timesheeturi, Timesheet_Start_Date, Timesheet_End_Date
                        FROM timesheet
                        WHERE NOT (CAST(Scheduled_Hrs__In_Period_ as decimal)= 0 AND CAST(Total_Hrs__In_Period_ as decimal)= 0 AND
                        CAST(daydiff as decimal) < 1 AND (Approval_Status = "Not Submitted" OR
                        Approval_Status = "Waiting for Approval"))'''
        )

        query_timesheet_has_data = rail.IfOperator(
            task_id="query_timesheet_has_data",
            test="{{ result('query_zero_hours_timesheets','length') > 0 }}",
            yes_task='recalculate_timesheets',
            no_task='finish'
        )

        recalculate_timesheets = rail.TriggerDagRunForEachItemOperator(
            task_id='recalculate_timesheets',
            retries=0,
            items=lambda: rail.result('query_zero_hours_timesheets'),
            batch_size=50,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'pwcglobal_zero_hours_recalculate_timesheets_v4_child_dag_{config.instance}',
        )

        wait_for_recalculate_timesheet = rail.WaitForDagRunsSensor(
            task_id='wait_for_recalculate_timesheet',
            dag_runs='{{ result("recalculate_timesheets") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        run_report_recalculate_entry, run_report_recalculate_exit = rail.run_report(
            group_id='run_recalculated_report',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
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

        recalculated_report_has_data = rail.IfOperator(
            task_id="recalculated_report_has_data",
            test="{{ result('run_recalculated_report.get_report_result','has_data')}}",
            yes_task='load_recalculated_report_data',
            no_task='finish'
        )

        load_recalculated_report_data = rail.LoadCSVFileOperator(
            task_id='load_recalculated_report_data',
            document="{{ result('run_recalculated_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_recalculated_timesheet_collection = rail.CreateCollectionOperator(
            task_id='create_recalculated_timesheet_collection',
            name='recalculated_timesheet',
            source="{{ result('load_recalculated_report_data') }}",
        )

        query_recalculated_timesheet = rail.QueryCollectionOperator(
            task_id='query_recalculated_timesheet',
            query='''SELECT DISTINCT User_Name, useruri, timesheeturi, Timesheet_Start_Date, Timesheet_End_Date
                        FROM recalculated_timesheet
                        WHERE CAST(Scheduled_Hrs__In_Period_ as decimal)= 0 AND CAST(Total_Hrs__In_Period_ as decimal)= 0 AND
                        CAST(daydiff as decimal) < 1 AND (Approval_Status = "Not Submitted" OR
                        Approval_Status = "Waiting for Approval")
                        AND (Validation_Message="" OR Validation_Message IS NULL OR Validation_Message = "Null" OR
                        Validation_Message = ?)''',
            query_params=[config.validation_message]
        )

        query_recalc_timesheet_has_data = rail.IfOperator(
            task_id="query_recalc_timesheet_has_data",
            test="{{ result('query_recalculated_timesheet','length') > 0 }}",
            yes_task='process_timesheet',
            no_task='finish'
        )

        process_timesheet = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet',
            retries=0,
            items=lambda: rail.result('query_recalculated_timesheet'),
            batch_size=25,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'pwcglobal_timesheet_auto_submission_zero_hours_v4_child_{config.instance}',
        )

        wait_for_process_timesheet = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet',
            dag_runs='{{ result("process_timesheet") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        log_recalc_skipped_timesheet = rail.WriteLogOperator(
            task_id="log_recalc_skipped_timesheet",
            items='{{ result("query_non_zero_timesheet") }}',
            severity="Exception",
             properties=lambda item: {
                'timesheeturi': item['timesheeturi'],
                'User_Name': item['User_Name'],
                'timesheetperiod': item['Timesheet_Start_Date']+ " - " + item['Timesheet_End_Date'],
                'status': "Skipped",
            },
            message="Timesheet Skipped Recalculation",
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'User',
                'Timesheeturi',
                'Timesheetperiod',
                'Status',
                'Details',
                'Job ID'],
            row=lambda item: [
                 item["properties"].get("User_Name",""),
                 item["properties"].get("timesheeturi",""),
                 item["properties"].get("timesheetperiod",""),
                 item["properties"].get("status",""),
                 item["message"],
                 item["ecid"],
            ],
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size_count
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ ecid() | replace(":", "-") }}.csv',
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
            subject='{{ get_company_key() + " | Timesheet autosubmission/approval run - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/emails/import_complete_zero_hours.html",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_report_group_entry >> run_report_group_exit >> report_has_data
        report_has_data >> rail.Label("Yes") >> \
            load_report_data >> create_timesheet_collection >> query_zero_hours_timesheets
        report_has_data >> rail.Label("No") >> finish
        query_zero_hours_timesheets >> query_non_zero_timesheet >> query_timesheet_has_data
        query_timesheet_has_data >> rail.Label("Yes") >> recalculate_timesheets >> wait_for_recalculate_timesheet >> run_report_recalculate_entry
        query_timesheet_has_data >> rail.Label("No") >> finish
        run_report_recalculate_exit >> recalculated_report_has_data >> rail.Label("Yes") >> load_recalculated_report_data
        recalculated_report_has_data >> rail.Label("No") >> finish
        load_recalculated_report_data >> create_recalculated_timesheet_collection >> query_recalculated_timesheet
        query_recalculated_timesheet >> query_recalc_timesheet_has_data
        query_recalc_timesheet_has_data >> rail.Label("Yes") >> process_timesheet
        query_recalc_timesheet_has_data >> rail.Label("No") >> finish
        process_timesheet >> wait_for_process_timesheet >> log_recalc_skipped_timesheet >> get_errored_logs >> render_logs_csv
        render_logs_csv >> generate_download_link >> send_import_complete_email

    return dag


rail.for_each_instance(create_dag)
