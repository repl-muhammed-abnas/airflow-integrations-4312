import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=[
                'get_logged_errors',
                'get_logged_exceptions',
                'get_logged_success'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='failed',
        )
        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            severity='ignored',
        )
        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='success',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("create_input_collection", key="length") }}',
                'Function: C1 Purchaseorder balance'],
            row=[
                '{{ item.properties.action}}',
                '{{ item.properties.workordernumber }}',
                '{{ item.properties.personnelnumber }}',
                "{{ item.properties.companycode}}",
                "{{ item.properties.purchaseorder}}",
                '{{ item.properties.status }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}'],
            footer=[
                'Number of Records Processed Successfully: {{ result("get_logged_success", key="length")}}',
                'Number of Records with Error: {{ result("get_logged_errors", key="length") }}',
                'Number of Records with Ignored: {{ result("get_logged_exceptions", key="length") }}',
                '',
                '',
                '',
                '',
                '',
                '',
                ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + "/{{result('log_files_details')}}",
        )
        open_bracket = '{{'
        close_bracket = '}}'
        complete_email = f"""
            <p>
                <strong>This is an automated mail, please don't reply.</strong>
                <br /> <br />Hello, 
                <br /> <br />C1 CWF Purchaseorder balance data import into Replicon is completed.
                <br /> <br />The logs are places in the SFTP folder path {config.log_filepath}
                <br /> <br />File Name: {open_bracket}result('log_files_details'){close_bracket}</p>
                <p>For any queries, please contact our support team at https://support.deltek.com 
                <br /><br />Regards, 
                <br />Deltek Inc.
            </p>"""
        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | C1 CWF Purchaseorder balance sync to Replicon - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                        completed  \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content=complete_email,
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_exceptions,
            get_logged_success] >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email
