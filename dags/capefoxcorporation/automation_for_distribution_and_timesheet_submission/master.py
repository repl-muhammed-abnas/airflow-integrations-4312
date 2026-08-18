from datetime import timedelta
from pendulum import datetime as dt
import rail
from capefoxcorporation.automation_for_distribution_and_timesheet_submission.utils import request_payload, custom_methods

null = None


def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'CapeFoxCorporation Automation For Distribution and Timesheet Submission Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2025, 6, 1, tz=config.time_zone),
        schedule_interval=config.master_dag_schedule,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_integration_run_date = rail.PythonOperator(
            task_id="get_integration_run_date",
            python_callable=lambda: custom_methods.get_run_date_datetime(
                config)
        )

        check_if_run_date_matches_with_schedule = rail.IfOperator(
            task_id='check_if_run_date_matches_with_schedule',
            test=lambda: (rail.result(
                'get_integration_run_date')['day'] == 1 or rail.result('get_integration_run_date')['day'] == 16),
            yes_task='create_main_log',
            no_task='finish'
        )

        create_main_log = rail.CreateLogOperator(
            task_id='create_main_log'
        )

        get_population_script_uri = rail.RepliconServiceOperator(
            task_id="get_population_script_uri",
            endpoint="/services/TimesheetPopulationService1.svc/GetTimesheetPopulationScriptsAvailableForAssignmentToTimesheetPolicySets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", config.time_population_script_name, "uri")
        )

        has_population_script = rail.IfOperator(
            task_id='has_population_script',
            test="{{ result('get_population_script_uri') | is_truthy }}",
            yes_task='get_report_details',
            no_task='fail_no_script'
        )

        fail_no_script = rail.FailOperator(
            task_id='fail_no_script',
            message='Timesheet auto population script is missing'
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        log_required_report_filter_uris = rail.PythonOperator(
            task_id='log_required_report_filter_uris',
            python_callable=lambda: {
                'timesheet_priod_filter_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri', null),
                'approval_status_filter': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', null)
            }
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda: request_payload.get_report_generate_batch_payload(rail.result('log_required_report_filter_uris'), rail.result(
                'get_integration_run_date'), config),
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
            task_id='report_has_data',
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='email_no_timesheets_to_process'
        )

        email_no_timesheets_to_process = rail.EmailOperator(
            task_id='email_no_timesheets_to_process',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Automation For Distribution and Timesheet Submission - No timesheets to process - '
            + '{{ result("get_integration_run_date").datetime }}',
            html_content='''templates/emails/no_timesheets_to_process.html'''
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_timehseets_csv",
            no_task="fail_no_expected_columns",
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_timehseets_csv = rail.LoadCSVFileOperator(
            task_id='load_timehseets_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        timesheets_data = rail.CreateCollectionOperator(
            task_id='timesheets_data',
            source='{{ result("load_timehseets_csv") }}',
            name='timesheetsdata'
        )

        process_timesheets_for_distribution = rail.trigger_parallel_dagrun(
            task_id='process_timesheets_for_distribution',
            items='{{ result("timesheets_data") }}',
            trigger_dag_id=config.child_process_timesheets_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_process_timesheet_for_distribution_parallel_count,
            conf=lambda item: {
                "timesheet_uri": item["TimesheetPeriodUri"],
                "employee_id": item["Employee_ID"],
                "username": item["User_Name"],
                "timesheet_period": item["Timesheet_Period"],
                "script_uri": rail.result("get_population_script_uri"),
                "main_log": rail.result('create_main_log')
            }
        )

        filter_successful_auto_populated_timesheets = rail.FilterLogEntriesOperator(
            task_id='filter_successful_auto_populated_timesheets',
            log='{{ result("create_main_log")}}',
            severity="Success",
            remove_filtered_entries=True
        )

        process_timesheet_submission_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_submission_batch',
            retries=0,
            items=lambda: rail.result(
                'filter_successful_auto_populated_timesheets'),
            batch_size=25,
            trigger_dag_id=config.child_process_timesheet_submission_batch_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_process_timesheet_submission_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_submission_batch',
            dag_runs='{{ result("process_timesheet_submission_batch") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs='{{ result("process_timesheet_submission_batch") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=['username', 'employee_id', 'status', 'details', 'ecid'],
            row=[
                '{{ item | attr_or_default("username", "") }}',
                '{{ item | attr_or_default("employee_id", "") }}',
                '{{ item | attr_or_default("status", "") }}',
                '{{ item | attr_or_default("details", "")}}',
                '{{ item.jobid }}'
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("get_integration_run_date").datetime | replace(":", "_")}}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Automation For Distribution and Timesheet Submission {{" - "}} \
                {%- if result("format_logs", key="error_record_count")  > 0 -%} \
                    Completed with errors  \
                {%- else -%} \
                    Completed successfully  \
                {%- endif -%} \
                {{ " " + result("get_integration_run_date").datetime }}',
            html_content="/templates/emails/completion_email.html"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_integration_run_date >> check_if_run_date_matches_with_schedule
        check_if_run_date_matches_with_schedule >> rail.Label('No') >> finish
        check_if_run_date_matches_with_schedule >> rail.Label(
            'Yes') >> create_main_log

        create_main_log >> get_population_script_uri >> has_population_script

        has_population_script >> rail.Label('No') >> fail_no_script

        has_population_script >> rail.Label(
            'Yes') >> get_report_details >> log_required_report_filter_uris >> run_report_entry

        run_report_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label(
            "No") >> email_no_timesheets_to_process >> finish
        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns

        is_report_has_expected_columns >> rail.Label(
            'No') >> fail_no_expected_columns

        is_report_has_expected_columns >> rail.Label(
            'Yes') >> load_timehseets_csv >> timesheets_data >> process_timesheets_for_distribution

        process_timesheets_for_distribution >> filter_successful_auto_populated_timesheets >> process_timesheet_submission_batch

        process_timesheet_submission_batch >> wait_for_process_timesheet_submission_batch >> gather_logs >> format_logs >> render_logs_csv >>\
            generate_download_link >> send_complete_email >> finish

    return dag


rail.for_each_instance(create_main_dag)
