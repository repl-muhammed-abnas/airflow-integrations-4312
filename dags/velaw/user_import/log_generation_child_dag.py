from datetime import timedelta
import rail
from rail import load_all_records


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/macquariegroup/clientimport/config.py


def create_client_import_log_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'velaw_user_import_velawg3_child_loggeneration_{config.instance}',
        description=f'VelawG3 User Import Loggeneration_V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def do_format_logs(dag_run):
            def load_records(log_artifact):
                try:
                    logs = load_all_records(log_artifact)
                    return logs
                except:  # pylint: disable=bare-except
                    return []

            log_artifacts = []

            if dag_run.conf['user_import_logs']:
                log_artifacts.append(dag_run.conf['user_import_logs'])

            if dag_run.conf['user_add_logs']:
                log_artifacts.extend(dag_run.conf['user_add_logs'])

            if dag_run.conf['user_update_logs']:
                log_artifacts.extend(dag_run.conf['user_update_logs'])

            if dag_run.conf['user_disable_logs']:
                log_artifacts.extend(dag_run.conf['user_disable_logs'])

            if dag_run.conf['user_disable_different_iso_logs']:
                log_artifacts.extend(
                    dag_run.conf['user_disable_different_iso_logs'])

            if dag_run.conf['supervisor_assignment_logs']:
                log_artifacts.extend(
                    dag_run.conf['supervisor_assignment_logs'])

            log_records = []
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)

                    if each_log_records:
                        log_records.extend(each_log_records)

            return list(map(lambda x: {
                **{k: v for k, v in x['properties'].items() if k != 'email'},
                **{
                    'jobid': x['ecid']
                }}, log_records))

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs
        )

        create_user_import_log_csv = rail.WriteCSVFileOperator(
            task_id='create_user_import_log_csv',
            source=lambda: rail.result('format_logs'),
            header=['username',
                    'loginname',
                    'employeeid',
                    'importaction',
                    'status',
                    'details',
                    'jobid'],
            row=[
                '{{ item.username }}',
                '{{ item.loginname }}',
                "{{ item.employeeid }}",
                "{{ item.importaction }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}",
            ]
        )

        def file_upload_failed(context):
            # pylint: disable=line-too-long
            subject = '{{ get_company_key() }} | Client Import - Failure uploading Logs to SFTP  - {{ dag_run.conf.time }}'
            email = rail.EmailOperator(
                task_id='send_sftp_failure_payload_email',
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject=subject,
                html_content='''<p>Hi Team,<br/> <br/> The user import for {{ get_company_key() }} instance, hosted on  {{ get_company_key() }}, created on {{ current_time() }} has been completed for file "{{ dag_run.conf.filename }}", however, the log upload to sftp has failed. Attached is the log file for reference.</p>
<ul>
<li>Recipe ID:f'velaw_user_import_velawg3_user_import_v2_0_{config.instance}' </li>
<li>Job ID: {{ dag_run_ecid() }} </li>
</ul>
<p>Please find the attached logs which was to be sent to intended recipients and debug the issue related to sftp upload.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
                files=[
                    'log_{{ dag_run.conf.time }}_{{ dag_run.conf.filename }}'],

            )
            email.render_template_fields(context)
            email.execute(context)

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("create_user_import_log_csv") }}',
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run.conf.time }}_{{ dag_run.conf.filename }}',
            on_failure_callback=file_upload_failed
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("create_user_import_log_csv")}}',
            output_file_name='log_{{ dag_run.conf.time }}_{{ dag_run.conf.filename }}',
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Exception', rail.result('format_logs')))), 'length')
        )

        get_success_logs = rail.PythonOperator(
            task_id='get_success_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Success', rail.result('format_logs')))), 'length')
        )

        get_skipped_logs = rail.PythonOperator(
            task_id='get_skipped_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Skipped', rail.result('format_logs')))), 'length')
        )

        send_client_import_email = rail.EmailOperator(
            task_id='send_client_import_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User import - "  }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + dag_run.conf.time }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br/> <br/>Hello, <br/> <br/> The user import job is
            {%- set has_errors = result("get_errored_logs", key="length") > 0 -%}
                {%- if has_errors -%}
                    completed with errors
                {%- else -%}
                    {%- if result("get_exception_logs", key="length") > 0 -%}
                        completed with exceptions
                    {%- else -%}
                        completed successfully
                    {%- endif -%}
                {%- endif -%} on {{ current_time() }}. Please find the below link to download the user import logs for reference. <br/> <br /><a href="{{result('generate_download_link')}}">Download log file</a></p>
<p><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
<p> {%- if has_errors -%}
  {%- endif -%}
  For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>''',
        )

        format_logs >> create_user_import_log_csv >> upload_log_to_sftp >> generate_download_link \
            >> [get_errored_logs, get_exception_logs, get_skipped_logs, get_success_logs] >> send_client_import_email

        return dag


rail.for_each_instance(create_client_import_log_child_dag)
