import rail
import json
from rail.lib.ecid import get_dagrun_ecid
from bearingpoint.project_import_v1.utils import custom_method
from bearingpoint.project_import_v1.custom_http_operator.CustomSimpleHttpOperator import CustomSimpleHttpOperator

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_dag_id,
        description='Bearingpoint Process Logs Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_method.format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{result('format_logs')}}",
            header=[
                'projectcode',
                'projectname',
                'clientcode',
                'taskname',
                'parenttaskname',
                'action',
                'details',
                'status',
                'ecid',
            ],
            row=[
                "{{item.projectcode}}",
                "{{item.projectname}}",
                "{{item.clientcode}}",
                "{{item.taskname}}",
                "{{item.parenttaskname}}",
                "{{item.action}}",
                "{{item.details}}",
                "{{item.status}}",
                "{{item.ecid}}"
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda dag_run: f'{rail.get_company_key()}_Logs_Project_Import_{ get_dagrun_ecid(dag_run).replace(":", "-")}.csv'
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('create_csv_log')}}",
            output_file_name="{{ result('get_log_file_name') }}",
            expires_in_seconds=7*24*60*60
        )

        upload_logs_to_api = CustomSimpleHttpOperator(
            task_id='upload_logs_to_api',
            method='POST',
            http_conn_id=config.http_conn_id,
            auth_type=None,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=lambda : json.dumps(rail.load_json_artifact(rail.result("format_logs"))).encode("utf-8"),
            extra_options={
                'verify': False
            }
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Project import - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/emails/email_import_complete.html",
        )

        format_logs >> create_csv_log >> get_log_file_name >> generate_downloadable_link >> upload_logs_to_api >> \
            send_import_complete_email

    return dag

rail.for_each_instance(create_child_dag_wbs)
