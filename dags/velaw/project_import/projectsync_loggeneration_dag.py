
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_project_import_velaw_projectsync_loggeneration_{config.instance}',
        description=f'Velaw_ProjectSync_dynamicwait {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='format_logs',
            end_task='log_dagrun_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def do_format_logs():
            def load_records(log_artifact):
                try:
                    logs = rail.load_all_records(log_artifact)
                    return logs
                except:  # pylint: disable=bare-except
                    return []
            dag_run = rail.get_current_context()['dag_run']
            log_artifacts = dag_run.conf['logs']
            log_records = []
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)
                    if each_log_records:
                        log_records.extend(each_log_records)

            return list(map(lambda x: {
                **dict(x['properties'].items()),
                **{
                    'ecid': x['ecid']
                }}, log_records))
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=do_format_logs
        )

        should_process_logs = rail.IfOperator(
            task_id="should_process_logs",
            test="{{ result('format_logs') | length > 0 }}",
            yes_task="render_logs_csv",
            no_task="log_dagrun_to_sumo"
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=['Project Name',
                    'Client Name',
                    'Action',
                    'Details',
                    'JobID'],
            row=[
                "{{ item.project_name }}",
                "{{ item.client_name }}",
                "{{ item.action }}",
                "{{ item.details }}",
                "{{ item.ecid }}"
            ]
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['action'] == 'Error', rail.result('format_logs')))), 'length')
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['action'] == 'Exception', rail.result('format_logs')))), 'length')
        )

        def file_upload_failed(context):
            subject = '''{{ get_company_key() }}| Project Sync - Failed while uploading logs to SFTP - {{ current_time() }} '''
            email = rail.EmailOperator(
                task_id='send_sftp_failure_payload_email',
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject=subject,
                html_content='''<p>Hi Team,<br /> <br /> The Project Sync hosted on {{ get_company_key() }} instance has been completed at {{ current_time() }}, however the log upload to Cshare has failed. Attached is the log file for reference.</p>
<ul>
<li>DAG ID: {{ params.dag_id }} < /li>
<li>DAG Run: {{ dag_run_ecid() < /li>
</ul>
<p>Please find the attached logs which was to be sent to intended recipients and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
                files=[('{{ result("render_logs_csv") }}')],
                params={
                    'dag_id': f'velaw_project_import_velaw_projectsync_loggeneration_{config.instance}'
                }
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content='''{{ result('render_logs_csv') }}''',
            remote_filepath=config.log_filepath +
            '''/logs_{{ dag_run.conf.time }}_{{ dag_run.conf.filename }}''',
            on_failure_callback=file_upload_failed
        )

        upload_logs_to_cshare = rail.GeneratePresignedDownloadUrlOperator(
            task_id='upload_logs_to_cshare',
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name='''logs_{{ dag_run.conf.time }}_{{ dag_run.conf.filename }}''',
            expires_in_seconds=7*24*60*60,
        )

        log_subjectline_19 = rail.PythonOperator(
            task_id='log_subjectline_19',
            python_callable=lambda: "Completed with errors" if rail.result("get_errored_logs", key="length") > 0 else "Completed with exceptions" if rail.result(
                "get_exception_logs", key="length") > 0 else "Completed successfully"
        )

        log_body_20 = rail.PythonOperator(
            task_id='log_body_20',
            python_callable=lambda: "<br/>For any queries, please contact our support team at https://support.deltek.com. <br/> <br/>Regards,<br/>Deltek Inc. </p>" if rail.result(
                "get_errored_logs", key="length") > 0 else "<p>For any queries, please contact our support team at https://support.deltek.com. <br/><br/>Regards, <br/>Deltek Inc. </p >"
        )

        send_mail_with_cshare_21 = rail.EmailOperator(
            task_id='send_mail_with_cshare_21',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Project Sync - {{ result('log_subjectline_19') }} - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br/> <br />Hello, <br/> <br/> The Project Sync is {{ result('log_subjectline_19') }} on {{ current_time() }} based on the file name - {{ dag_run.conf.filename }}.</p>
 <br/>
 Please find the  link below to download the logs.
 <br/> <br/><a href={{result('upload_logs_to_cshare')}}>Download Logs</a><br/> <br/><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
 <br/> <br/>
<p>{{ result('log_body_20') }}</p> '''
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> format_logs

        format_logs >> should_process_logs

        should_process_logs >> rail.Label(
            'Yes') >> render_logs_csv >> get_errored_logs >> get_exception_logs >> upload_logs_to_sftp >> upload_logs_to_cshare \
            >> log_subjectline_19 >> log_body_20 >> send_mail_with_cshare_21 >> log_dagrun_to_sumo

        should_process_logs >> rail.Label(
            'No') >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_dag)
