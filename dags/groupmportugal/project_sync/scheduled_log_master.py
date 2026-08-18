from datetime import timedelta
from pendulum import datetime
import rail
from groupmportugal.project_sync.utils.python_callable import get_dagruns_to_process, format_logs_callable

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api/config.py


# pylint:disable = too-many-statements
def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'groupmportugal_project_import_master_log_scheduled_{config.instance}',
        description=f'Project import dynamicwait Loggeneration Project-Scheduled {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_runs=config.max_active_runs_master,
        start_date=datetime(2023, 1, 1),
    ) as dag:

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process,
            op_args=[config.lookup_log_timestamp_var,
                     config.lookup_log_timestamp_hours,
                     config.create_update_projects]
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_project_logs',
            no_task='delete_this_dagrun'
        )

        get_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_project_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='log_project_and_exception_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id="create_csv_log",
            source="{{result('format_logs')}}",
            header=[
                "jobid",
                "clientname",
                "projectname",
                "taskname",
                "status",
                "details"
            ],
            row=[
                "{{item.ecid}}",
                "{{item.properties.clientname}}",
                "{{item.properties.projectname}}",
                "{{item.properties.taskname}}",
                "{{item.properties.status}}",
                "{{item.properties.details}}",
            ]
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('create_csv_log')}}",
            output_file_name="{{ dag_run_ecid() }}_{{current_time_in_specified_tz(fmt='%Y%m%dT%H%M%S')}}_ProjectSynclogs.csv",
            expires_in_seconds=7*24*60*60

        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project Sync " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time("%Y%m%dT%H%M%S") }}',
            html_content="templates/emails/email_import_complete.html",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_project_logs >> format_logs >> create_csv_log >> generate_downloadable_link >> send_import_complete_email

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

        return dag

rail.for_each_instance(create_log_airflow_dag)
