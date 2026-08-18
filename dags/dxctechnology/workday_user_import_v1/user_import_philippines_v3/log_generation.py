from datetime import timedelta
from json import dumps
import rail
from airflow.models import Variable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dagid_phl,
        description='Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_log_generation_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_connection_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_philippines, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="format_logs"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="format_logs",
            end_task="send_import_complete_email",
            execution_timeout=timedelta(days=14)
        )

        def do_format_logs(dag_run):
            def get_status(user_logs):
                available_status = list(
                    map(lambda log: log['properties']['Status'], user_logs))
                if "Error" in available_status:
                    return "Error"
                if "Exception" in available_status:
                    return "Exception"
                return "Success"
            def get_action(user_logs):
                available_actions = list(
                    map(lambda log: log['properties']['Action'].lower(), user_logs))
                if 'update' in available_actions:
                    return "Update"
                if 'add' in available_actions:
                    return "Add"
                return user_logs[0]['properties']['Action']
            master_log = []

            for log in dag_run.conf['logs']:
                log_records = rail.load_all_records(log)
                if log_records:
                    master_log.extend(log_records)
            users = list(
                set(map(lambda x: x['properties']['Userid'], master_log)))
            logs = []
            # pylint: disable=cell-var-from-loop
            for employeeid in users:
                user_logs = list(
                    filter(lambda x: x['properties']['Userid'] == employeeid and x['properties']['Details'], master_log))
                if len(user_logs) > 0:
                    first = user_logs[0]
                    logs.append({
                        'employee_id': employeeid,
                        'login_name': first['properties']['Email'],
                        'status': get_status(user_logs),
                        'action': get_action(user_logs),
                        'details': ";".join(list(set(map(lambda x: x['properties']['Details'], user_logs)))),
                        'jobid': first['ecid'],
                    })
            skipped_record_count = len(list(filter(lambda log: log['status'].lower() == 'skipped', logs)))
            rail.set_result(key="new_users_record_count", val=len(list(filter(lambda log: log['action'].lower() in ['added', 'add'], logs))))
            rail.set_result(key="update_users_record_count", val=len(list(filter(lambda log: log['action'].lower() in ['updated', 'update'], logs))))
            rail.set_result(key="skipped", val=len(list(filter(lambda log: log['status'].lower() == 'skipped', logs))))
            rail.set_result(key="success", val=len(list(filter(lambda log: log['status'].lower() == 'success', logs))))
            rail.set_result(key="error", val=len(list(filter(lambda log: log['status'].lower() == 'error', logs))))
            rail.set_result(key="exception", val=len(list(filter(lambda log: log['status'].lower() == 'exception', logs))))
            rail.set_result(key="processed", val=(dag_run.conf['total_record_count'] - (skipped_record_count + 0)))

            return dumps(logs, ensure_ascii=False)


        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=None,
            row=[
                '{{ item.employee_id }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{dag_run.conf.log_filename}}',
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_file_path +
            '/'+"{{dag_run.conf.log_filename}}",
        )

        # the email template is set same as what was previously
        # once the templates are standrized it will be updated
        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Import from workday " }} \
                {%- if result("format_logs", key="error") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + dag_run.conf.email_body_subject_timestamp }}',
            html_content="templates/emails/import_complete_mail.html",
            params = {
                "log_filepath": config.log_file_path
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> send_import_complete_email
        can_run_batch_task >> rail.Label("No") >> format_logs

        format_logs >> render_logs_csv >> generate_download_link >> upload_log_to_sftp >> send_import_complete_email

    return dag


rail.for_each_instance(create_child_dag)
