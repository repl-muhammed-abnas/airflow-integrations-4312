from datetime import date, timedelta
import json

from wcs.time_sync_to_quickbooks.utils import custom_methods
import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timesheet_data_child_id,
        description=f"WCS Time Sync from Replicon to QuickBooks process timesheet data child - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.process_timesheet_data_child_max_active_run,
        default_args={
            "execution_timeout": timedelta(hours=1),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_conf"
        )

        search_entry_in_log = rail.FilterLogEntriesOperator(
            task_id="search_entry_in_log",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            properties={
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
            }
        )

        is_timesheet_uri_present_in_log = rail.IfOperator(
            task_id="is_timesheet_uri_present_in_log",
            test=lambda: len(rail.load_all_records(
                rail.result("search_entry_in_log"))) > 0,
            yes_task="stop_execution",
            no_task="add_entry_to_log"
        )

        stop_execution = rail.EmptyOperator(
            task_id="stop_execution"
        )

        add_entry_to_log = rail.WriteLogOperator(
            task_id="add_entry_to_log",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            message='Timesheet entry added to log',
            properties=lambda: {
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
                "status": "Queued",
                "date": (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
            }
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id="get_all_reports",
            endpoint="/services/ReportService1.svc/GetAllReports",
        )

        get_time_sync_report_uri = rail.PythonOperator(
            task_id="get_time_sync_report_uri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_all_reports"), "displayText", config.TIME_SYNC_REPORT_NAME, "uri")
        )

        if_report_not_present = rail.IfOperator(
            task_id="if_report_not_present",
            test=lambda: not rail.result("get_time_sync_report_uri"),
            yes_task="delete_entry_from_log",
            no_task="get_time_sync_report_details",
        )

        delete_entry_from_log = rail.FilterLogEntriesOperator(
            task_id="delete_entry_from_log",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            properties={
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
            },
            remove_filtered_entries=True
        )

        fail_with_report_not_found_error = rail.FailOperator(
            task_id="fail_with_report_not_found_error",
            message=f"Report with name {config.TIME_SYNC_REPORT_NAME} not found in Replicon."
        )

        get_time_sync_report_details = rail.RepliconServiceOperator(
            task_id="get_time_sync_report_details",
            endpoint="/services/ReportService1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result("get_time_sync_report_uri")
            }
        )

        get_report_filter_uri = rail.PythonOperator(
            task_id="get_report_filter_uri",
            python_callable=lambda: custom_methods.get_filter_uri(rail.result("get_time_sync_report_details"))
        )

        create_time_sync_report_filter = rail.PythonOperator(
            task_id="create_time_sync_report_filter",
            python_callable=lambda dag_run: custom_methods.create_report_filter(
                dag_run=dag_run,
                report_filter_uri=rail.result("get_report_filter_uri"),
            )
        )

        timesheet_report_entry, timesheet_report_exit = rail.run_report(
            group_id="timesheet_report_dag_run",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result("get_time_sync_report_uri"),
                        "filterValues": json.loads(rail.result("create_time_sync_report_filter")),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        if_error_in_report_batch_result = rail.IfOperator(
            task_id="if_error_in_report_batch_result",
            test="{{ result('timesheet_report_dag_run.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task="delete_entry_from_log_due_to_report_generation_error",
            no_task="has_empty_report_data"
        )

        delete_entry_from_log_due_to_report_generation_error = rail.FilterLogEntriesOperator(
            task_id="delete_entry_from_log_due_to_report_generation_error",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            properties={
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
            },
            remove_filtered_entries=True
        )

        fail_due_to_report_generation_error = rail.FailOperator(
            task_id="fail_due_to_report_generation_error",
            message="Report Execution Failed"
        )

        has_empty_report_data = rail.IfOperator(
            task_id='has_empty_report_data',
            test=lambda: rail.result("timesheet_report_dag_run.get_report_result")[
                'reportGenerationResults'][0]['payload'].startswith("No Data"),
            yes_task="delete_entry_from_log_due_to_no_data",
            no_task="load_report_csv_file",
        )

        delete_entry_from_log_due_to_no_data = rail.FilterLogEntriesOperator(
            task_id="delete_entry_from_log_due_to_no_data",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            properties={
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
            },
            remove_filtered_entries=True
        )

        stop_execution_due_to_no_data = rail.EmptyOperator(
            task_id="stop_execution_due_to_no_data"
        )

        load_report_csv_file = rail.LoadCSVFileOperator(
            task_id="load_report_csv_file",
            document="{{ result('timesheet_report_dag_run.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_csv_format_timesheet_data = rail.WriteCSVFileOperator(
            task_id='create_csv_format_timesheet_data',
            source="{{ result('load_report_csv_file') }}",
            header=[
                "timesheet_period",
                "user_name",
                "pay_code_name",
                "pay_code_code",
                "entry_date",
                "pay_code_hours",
                "user_uri",
                "timesheet_uri",
                "approval_status",
                "first_name",
                "last_name",
                "location"
            ],
            row=custom_methods.get_timesheet_data
        )

        create_collection_from_timesheet_data_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_timesheet_data_csv',
            source="{{ result('create_csv_format_timesheet_data') }}",
            name="timesheet_data",
            columns=[
                "timesheet_period",
                "user_name",
                "pay_code_name",
                "pay_code_code",
                "entry_date",
                "pay_code_hours",
                "user_uri",
                "timesheet_uri",
                "approval_status",
                "first_name",
                "last_name",
                "location"
            ]
        )

        query_specific_timesheet_approved_data = rail.QueryCollectionOperator(
            task_id="query_specific_timesheet_approved_data",
            name="specific_timesheet_approved_data",
            query="""SELECT * FROM timesheet_data
                     WHERE approval_status = 'Approved'
                       AND timesheet_uri = :timesheeturi
                       AND timesheet_period IS NOT NULL
                       AND timesheet_period != ''""",
            query_params={"timesheeturi": "{{ dag_run.conf.timesheet_uri }}"}
        )

        load_specific_timesheet_approved_data_from_collection = rail.PythonOperator(
            task_id='load_specific_timesheet_approved_data_from_collection',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_specific_timesheet_approved_data")),
        )

        if_timesheet_data_present_in_collection = rail.IfOperator(
            task_id="if_timesheet_data_present_in_collection",
            test=lambda: len(rail.result("load_specific_timesheet_approved_data_from_collection")) > 0,
            yes_task="wcs_replicon_to_qbo_time_and_timeoff_sync_log",
            no_task="delete_entry_from_log_due_to_empty_query_result"
        )

        wcs_replicon_to_qbo_time_and_timeoff_sync_log = rail.CreateLogOperator(
            task_id='wcs_replicon_to_qbo_time_and_timeoff_sync_log'
        )

        trigger_replicon_qbo_time_and_timeoff_sync_child = rail.trigger_parallel_dagrun(
            task_id="trigger_replicon_qbo_time_and_timeoff_sync_child",
            items=lambda: rail.result("load_specific_timesheet_approved_data_from_collection"),
            trigger_dag_id=config.replicon_qbo_time_and_timeoff_sync_child_id,
            conf=lambda item: {
                **item,
                "processing_log": rail.result("wcs_replicon_to_qbo_time_and_timeoff_sync_log"),
                "process_timesheet_data_child_job_id": rail.render_template("{{ dag_run_ecid() }}")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.trigger_replicon_qbo_time_and_timeoff_sync_child_parallel_count
        )

        get_all_sync_log_entries = rail.FilterLogEntriesOperator(
            task_id='get_all_sync_log_entries',
            log="{{ result('wcs_replicon_to_qbo_time_and_timeoff_sync_log') }}",
        )

        load_all_sync_log_entries = rail.PythonOperator(
            task_id='load_all_sync_log_entries',
            python_callable=lambda: rail.load_all_records(
                rail.result('get_all_sync_log_entries')
            )
        )

        if_sync_log_has_entries = rail.IfOperator(
            task_id='if_sync_log_has_entries',
            test=lambda: len(rail.result("load_all_sync_log_entries")) > 0,
            yes_task='check_error_records',
            no_task='stop_execution_no_sync_log_entries'
        )

        stop_execution_no_sync_log_entries = rail.EmptyOperator(
            task_id='stop_execution_no_sync_log_entries'
        )

        check_error_records = rail.PythonOperator(
            task_id="check_error_records",
            python_callable=lambda: any(
                entry.get("properties", {}).get("Status") == "Error"
                for entry in (rail.load_all_records(rail.result("wcs_replicon_to_qbo_time_and_timeoff_sync_log")) or [])
            ),
        )

        create_time_sync_log_csv = rail.WriteCSVFileOperator(
            task_id='create_time_sync_log_csv',
            source="{{ result('get_all_sync_log_entries') }}",
            header=[
                'User Name',
                'Timesheet Period',
                'Entry Date',
                'Pay Code Hours',
                'Synced Hours Post Calculation',
                'Pay Type',
                'Status',
                'Reason',
                'Job ID'
            ],
            row=lambda item: [
                item['properties']['username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype'].split('|')[0],
                item['properties']['username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype'].split('|')[1],
                item['properties']['username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype'].split('|')[2],
                item['properties']['username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype'].split('|')[3],
                item['properties']['username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype'].split('|')[4],
                item['properties']['username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype'].split('|')[5],
                item['properties']['Status'],
                item['properties']['details'],
                item['properties']['Jobid'] + '|' + item['properties'].get('childjobid', '')
            ]
        )

        generate_time_sync_log_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_time_sync_log_download_link',
            artifact_name="{{ result('create_time_sync_log_csv') }}",
            output_file_name='wcs_time_sync_logs_{{ dag_run_ecid() }}_{{ ts_nodash }}.csv',
            expires_in_seconds=30 * 24 * 60 * 60, # 30 days
        )

        get_configured_timezone = rail.PythonOperator(
            task_id='get_configured_timezone',
            python_callable=lambda: config.time_zone
        )

        send_time_sync_completion_email = rail.EmailOperator(
            task_id='send_time_sync_completion_email',
            to=config.tenant_email,
            bcc=(
                "{%- if result('check_error_records') | is_truthy -%}"
                + config.bcc_on_error
                + "{%- else -%}"
                + config.bcc_on_success
                + "{%- endif -%}"
            ),
            subject=(
                '{{ get_company_key() }} | WCS Time Sync - '
                "{% if result('check_error_records') | is_truthy %}"
                'Completed with Errors'
                '{% else %}'
                'Completed Successfully'
                '{% endif %}'
                " on {{ current_time_in_specified_tz('" + config.time_zone + "', fmt='%m/%d/%Y') }}"
            ),
            html_content='templates/emails/process_completion_email.html'
        )

        delete_queued_entry_from_tenant_log = rail.FilterLogEntriesOperator(
            task_id='delete_queued_entry_from_tenant_log',
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            properties={
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
            },
            remove_filtered_entries=True
        )

        write_synced_status_to_tenant_log = rail.WriteLogOperator(
            task_id='write_synced_status_to_tenant_log',
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            message='Timesheet entry synced status updated',
            properties=lambda: {
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
                "status": "Synced",
                "date": (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
            }
        )

        delete_entry_from_log_due_to_empty_query_result = rail.FilterLogEntriesOperator(
            task_id="delete_entry_from_log_due_to_empty_query_result",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            properties={
                "timesheet_uri": "{{ dag_run.conf.timesheet_uri }}",
                "timesheet_period": "{{ dag_run.conf.timesheet_period }}",
                "timesheet_owner": "{{ dag_run.conf.timesheet_owner }}",
            },
            remove_filtered_entries=True
        )

        stop_execution_due_to_empty_query_result = rail.EmptyOperator(
            task_id="stop_execution_due_to_empty_query_result"
        )

        search_entry_in_log >> is_timesheet_uri_present_in_log

        is_timesheet_uri_present_in_log >> rail.Label("Yes") >> stop_execution
        is_timesheet_uri_present_in_log >> rail.Label("No") >> add_entry_to_log >> get_all_reports >> get_time_sync_report_uri >> if_report_not_present

        if_report_not_present >> rail.Label("Yes") >> delete_entry_from_log >> fail_with_report_not_found_error
        if_report_not_present >> rail.Label("No") >> get_time_sync_report_details >> get_report_filter_uri >> create_time_sync_report_filter >> timesheet_report_entry >> timesheet_report_exit >> if_error_in_report_batch_result

        if_error_in_report_batch_result >> rail.Label("Yes") >> delete_entry_from_log_due_to_report_generation_error >> fail_due_to_report_generation_error
        if_error_in_report_batch_result >> rail.Label("No") >> has_empty_report_data

        has_empty_report_data >> rail.Label("Yes") >> delete_entry_from_log_due_to_no_data >> stop_execution_due_to_no_data
        has_empty_report_data >> rail.Label("No") >> load_report_csv_file >> create_csv_format_timesheet_data >> create_collection_from_timesheet_data_csv >> query_specific_timesheet_approved_data >> load_specific_timesheet_approved_data_from_collection >> if_timesheet_data_present_in_collection

        if_timesheet_data_present_in_collection >> rail.Label("No") >> delete_entry_from_log_due_to_empty_query_result >> stop_execution_due_to_empty_query_result
        if_timesheet_data_present_in_collection >> rail.Label("Yes") >> wcs_replicon_to_qbo_time_and_timeoff_sync_log >> trigger_replicon_qbo_time_and_timeoff_sync_child

        trigger_replicon_qbo_time_and_timeoff_sync_child >> get_all_sync_log_entries >> load_all_sync_log_entries >> if_sync_log_has_entries

        if_sync_log_has_entries >> rail.Label("No") >> stop_execution_no_sync_log_entries
        if_sync_log_has_entries >> rail.Label("Yes") >> check_error_records >> create_time_sync_log_csv >> generate_time_sync_log_download_link >> get_configured_timezone >> send_time_sync_completion_email >> delete_queued_entry_from_tenant_log >> write_synced_status_to_tenant_log

    return dag


rail.for_each_instance(create_child_dag)
