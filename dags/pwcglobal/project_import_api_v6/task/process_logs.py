from datetime import timedelta
import rail


def process_logs(config):
    with rail.TaskGroup(group_id="process_logs", prefix_group_id=False):

        get_application_log = rail.CreateLogOperator(
            task_id="get_application_log")

        write_application_log = rail.WriteLogOperator(
            task_id="write_application_log",
            log="{{ result('get_application_log') }}",
            message="{{ dag_run.conf.sender_id }} logs",
            items="{{ result('filter_sender_logs') }}",
            properties=lambda item: item
        )

        write_sftp_log_filename = rail.RenderTemplateOperator(
            task_id="write_sftp_log_filename",
            target="result",
            template="{{ dag_run.conf.sender_id }}_" + "{{ get_company_key() | lower }}_" + "projectimport_" +
            "{{ current_time('%Y%m%d%H%M%S') }}_" +
            "{{ dag_run_ecid() | replace(':', '-') }}_logs.xml"
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id="write_xml_file",
            target="artifact",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            template_file="output_template.xml",
            dataset="{{ result('filter_sender_logs') }}"
        )

        upload_xml_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_xml_to_sftp",
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.log_filepath +
            "/{{ result('write_sftp_log_filename') }}"
        )

        if config.secondary_sftp:
            upload_xml_to_secondary_sftp = rail.SFTPUploadFileOperator(
                task_id="upload_xml_to_secondary_sftp",
                sftp_conn_id=config.secondary_sftp_conn_id,
                content="{{ result('write_xml_file') }}",
                remote_filepath=config.secondary_log_filepath +
                "/{{ result('write_sftp_log_filename') }}"
            )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('write_xml_file') }}",
            output_file_name="{{ result('write_sftp_log_filename') }}",
            expires_in_seconds=7*24*60*60,
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            log="{{ result('get_application_log') }}",
            task_id="get_errored_logs",
            properties={'Level': 'Error'}
        )

        get_exception_logs = rail.FilterLogEntriesOperator(
            log="{{ result('get_application_log') }}",
            task_id="get_exception_logs",
            properties={'Level': 'Exception'}
        )

        get_warning_logs = rail.FilterLogEntriesOperator(
            log="{{ result('get_application_log') }}",
            task_id="get_warning_logs",
            properties={'Level': 'Warning'}
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs',  key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions \
                    {%- elif result("get_warning_logs", key="length") > 0 -%} \
                        completed with warnings \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content="email_import_complete.html",
        )

        get_application_log >> write_application_log >> write_sftp_log_filename >> \
            write_xml_file >> upload_xml_to_sftp

        if config.secondary_sftp:
            upload_xml_to_sftp >> upload_xml_to_secondary_sftp >> generate_download_link
        else:
            upload_xml_to_sftp >> generate_download_link

        generate_download_link >> get_errored_logs >> get_exception_logs >> \
            get_warning_logs >> send_import_complete_email

        return (get_application_log, send_import_complete_email)
