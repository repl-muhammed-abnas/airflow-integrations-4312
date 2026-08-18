import rail
from matlensilver.client_project_task_sync import python_callable_method


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=['get_logged_exceptions', 'get_logged_errors',
                      'get_logged_success', 'get_task_count', 'get_project_count'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            severity='Exception',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='Success',
        )

        get_project_count = rail.PythonOperator(
            task_id='get_project_count',
            python_callable=python_callable_method.get_project_count,
        )

        get_task_count = rail.PythonOperator(
            task_id='get_task_count',
            python_callable=python_callable_method.get_task_count,
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            # pylint: disable=line-too-long
            header=['Assignment ID', 'Assignment Title', 'Client ID', 'Client Name', 'Project ID', 'Project Name', 'Status', 'Details', 'Job ID', '{{ current_time("%d/%m/%YT%H:%M:%S") }}', 'Number of Rows:' + '{{- result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length")\
             + result("get_logged_success", key="length")}}', 'Function: MATLENSILVER Project Sync'],
            row=['{{ item.properties | attr_or_default("assignmentid", "") }}', '{{  item.properties | attr_or_default("assignmenttitle", "")}}',
                 '{{ item.properties | attr_or_default("clientid", "") }}', '{{  item.properties | attr_or_default("clientname", "")}}',
                   '{{ item.properties | attr_or_default("projectid", "") }}', '{{  item.properties | attr_or_default("projectname", "") }}',
                   '{{ item.properties | attr_or_default("status", "") }}', '{{ item.message }}', '{{ item.ecid }}'],
            footer=['Number of records found:' + '{{- result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length")+ result("get_logged_success", key="length")}}',
                    'Number of records processed:' + '{{- result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length")+ result("get_logged_success", key="length")}}', 'Number of Successes: {{ result("get_logged_success", "length")}}', 'Number of failures: {{ result("get_logged_errors", "length") }}', 'Number of project & WBS added:' + \
                    'Projects =  {{ result("get_project_count")["add_count"]}}, WBS =  {{ result("get_task_count")["add_count"]}}',
                    'Number of project & WBS updated:' + \
                    'Projects =  {{ result("get_project_count")["update_count"]}}, WBS =  {{ result("get_task_count")["update_count"]}}',
                    'Number of Records with Exception:'+'{{- result("get_logged_exceptions", "length")}}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project sync into Replicon - " }} \
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
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_exceptions, get_logged_errors, get_logged_success, get_project_count, get_task_count] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email
