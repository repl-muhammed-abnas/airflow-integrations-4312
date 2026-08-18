from datetime import timedelta
from airflow.models import Variable
import rail
from pwcglobal.project_import_file_based_v1.custom_method import do_format_logs


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_file_based_v1/config.py


def create_child_process_log_pregeneration(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_import_child_log_flat_file_based_{config.instance}_v1',
        description=f'Log Pregeneration {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_log_generation_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='format_logs',
            end_task='log_dagrun_to_sumo',
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=do_format_logs
        )

        filter_sender_logs = rail.DataAdaptorOperator(
            task_id='filter_sender_logs',
            source=lambda: rail.result('format_logs'),
            data=lambda row: dict(row.items()) if row else None,
            columns=[
                'Date',
                'FileName',
                'Status',
                'Details',
                'EntityId',
                'EntityName',
                'EntityType'
            ]
        )

        should_process_sender_logs = rail.IfOperator(
            task_id="should_process_sender_logs",
            test="{{ result('filter_sender_logs', 'length') > 0 }}",
            yes_task="write_sftp_log_filename",
            no_task="log_dagrun_to_sumo"
        )

        write_sftp_log_filename = rail.RenderTemplateOperator(
            task_id="write_sftp_log_filename",
            target="result",
            template="ORACLEPROD_" + "{{ get_company_key() | lower }}_" + "projectimport_" +
            "{{ current_time('%Y%m%d%H%M%S') }}_" +
            "{{ dag_run_ecid() | replace(':', '-') }}_logs.xml"
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id="write_xml_file",
            target="artifact",
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

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['Status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['Status'] == 'Exception', rail.result('format_logs')))), 'length')
        )

        get_warning_logs = rail.PythonOperator(
            task_id='get_warning_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['Status'] == 'Warning', rail.result('format_logs')))), 'length')
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
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

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done',
            extra_info={
                'Companykey': '{{ get_company_key() }}',
                'Recordcount': "{{ result('filter_sender_logs', 'length') }}",
                'LogFileName': "{{ result('write_sftp_log_filename') \
                    if get_task_state('write_sftp_log_filename') == 'success' | is_truthy else 'nil' }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> format_logs

        format_logs >> filter_sender_logs >> should_process_sender_logs

        should_process_sender_logs >> rail.Label(
            'Yes') >> write_sftp_log_filename

        write_sftp_log_filename >> write_xml_file >> upload_xml_to_sftp

        if config.secondary_sftp:
            upload_xml_to_sftp >> upload_xml_to_secondary_sftp >> generate_download_link
        else:
            upload_xml_to_sftp >> generate_download_link

        generate_download_link >> get_errored_logs >> get_exception_logs >> \
            get_warning_logs >> send_import_complete_email >> log_dagrun_to_sumo

        should_process_sender_logs >> rail.Label(
            'No') >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_process_log_pregeneration)
