import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id = 'has_any_entries_in_log',
            test = "{{ result('query_valid_ts','length') > 0 }}",
            yes_task = ['get_logged_errors', 'get_logged_exceptions', 'get_logged_success'],
            no_task = 'log_no_entries_found',
        )

        log_no_entries_found = rail.PythonOperator(
            task_id = 'log_no_entries_found',
            python_callable= lambda: 'No entries in log as length of valid timesheets to be submitted is ' + str(rail.result('query_valid_ts','length')),
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id = 'get_logged_errors',
            severity = 'Error',
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id = 'get_logged_exceptions',
            severity = 'Exception',
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id = 'get_logged_success',
            severity = 'Success',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            # pylint: disable=line-too-long
            header=['User Name','Timesheet Period','Status','Job id','Details'],
            row=['{{ item.properties | attr_or_default("username", "") }}',
                 '{{ item.properties | attr_or_default("timesheetperiod", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}',
                 '{{item.properties | attr_or_default("parentjobid", "")}}|{{item.properties | attr_or_default("jobid", "")}}',
                 '{{item.properties | attr_or_default("details", "")}}'
                 ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id = 'send_import_complete_email',
            to = config.tenant_email,
            bcc = "{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject = '{{ get_company_key() + " | Replicon Timesheet autosubmission  - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content = "ts_autosubmission_email.html"
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [get_logged_errors, get_logged_exceptions, get_logged_success] >> render_logs_csv
        render_logs_csv >> generate_download_link >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> log_no_entries_found

        return has_any_entries_in_log, send_import_complete_email
