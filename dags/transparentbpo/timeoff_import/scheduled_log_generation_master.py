from pendulum import datetime
from datetime import timedelta
import rail
from airflow.models import Variable
from transparentbpo.timeoff_import.utils import custom_methods


def create_process_final_logs_dag(config):
    """
    Create the log generation DAG for accumulating logs and processing together based on a schedule.
    """

    with rail.create_airflow_dag(
        dag_id=config.scheduled_log_generation_dag_id,
        description=f'Transparent BPO timeoff sync Scheduled Final Logs Generation Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 4, 1, tz=config.time_zone),
        schedule_interval=config.final_log_generation_dag_schedule_interval,
        max_active_runs=config.max_active_runs_final_logs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_log_dagruns_to_process'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='get_log_dagruns_to_process',
            end_task='batch_end',
        )

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=custom_methods.get_dagruns_to_process,
            op_args=[config.time_zone, config.master_dag_id]
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_logs',
            no_task='no_pregeneration_log_dag_runs_to_process'
        )

        no_pregeneration_log_dag_runs_to_process = rail.EmptyOperator(
            task_id='no_pregeneration_log_dag_runs_to_process',
        )

        get_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='format_logs',
            flatten=True
        )

        has_logs = rail.IfOperator(
            task_id='has_logs',
            test="{{ result('get_logs') | length > 0 }}",
            yes_task='compose_logs_collection',
            no_task='no_data_in_logs'
        )

        compose_logs_collection = rail.CreateCollectionOperator(
            task_id='compose_logs_collection',
            source=lambda: rail.result('get_logs'),
            name='logs_collection'
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('compose_logs_collection', 'length') > 0 }}",
            yes_task='render_logs_csv',
            no_task='no_data_in_logs'
        )

        no_data_in_logs = rail.EmptyOperator(
            task_id='no_data_in_logs',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('compose_logs_collection') }}",
            header=[
                "timeoff_id",
                "bamboohr_id",
                "employee_id",
                'username',
                "timeoff_type",
                "booking_date",
                "status",
                "details",
                "ecid"
            ],
            row=[
                '{{ item.timeoff_id }}',
                '{{ item.bamboohr_id }}',
                '{{ item.employee_id }}',
                '{{ item.username }}',
                '{{ item.timeoff_type }}',
                '{{ item.booking_date }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.ecid }}'
            ],
        )

        query_log_collection_to_find_error_logs = rail.QueryCollectionOperator(
            task_id='query_log_collection_to_find_error_logs',
            name="error_logs",
            query=f"""SELECT * FROM logs_collection WHERE status == 'Error' """
        )

        query_log_collection_to_find_exception_logs = rail.QueryCollectionOperator(
            task_id='query_log_collection_to_find_exception_logs',
            name="exception_logs",
            query=f"""SELECT * FROM logs_collection WHERE status == 'Exception' """
        )

        query_log_collection_to_find_success_logs = rail.QueryCollectionOperator(
            task_id='query_log_collection_to_find_success_logs',
            name="success_logs",
            query=f"""SELECT * FROM logs_collection WHERE status == 'Success' """
        )

        get_email_and_log_file_details = rail.PythonOperator(
            task_id="get_email_and_log_file_details",
            python_callable=lambda: custom_methods.get_email_details_callable(
                config.time_zone)
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_log_filepath +
            "/{{result('get_email_and_log_file_details').log_file_name}}"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('query_log_collection_to_find_error_logs', key='length') == 0 -%}\
                    "+ config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Bamboohr timeoff import to Replicon " }}\
                {%- if result("query_log_collection_to_find_error_logs", "length") > 0 -%}\
                    completed with errors\
                {%- else -%}\
                    {%- if result("query_log_collection_to_find_exception_logs", "length") > 0 -%}\
                        completed with exceptions\
                    {%- else -%}\
                        completed successfully\
                    {%- endif -%}\
                {%- endif -%}\
                {{ " | " + current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/emails/completion_email.html",
            params={
                'log_filepath': config.sftp_log_filepath,
            }
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> get_log_dagruns_to_process

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            "No") >> no_pregeneration_log_dag_runs_to_process >> batch_end
        is_log_dagruns_present >> rail.Label("Yes") >> get_logs >> has_logs

        has_logs >> rail.Label("Yes") >> compose_logs_collection >> has_any_data
        has_logs >> rail.Label("No") >> no_data_in_logs >> batch_end

        has_any_data >> rail.Label("No") >> no_data_in_logs >> batch_end
        has_any_data >> rail.Label("Yes") >> render_logs_csv

        render_logs_csv >> query_log_collection_to_find_error_logs >> query_log_collection_to_find_exception_logs \
            >> query_log_collection_to_find_success_logs >> get_email_and_log_file_details

        get_email_and_log_file_details >> upload_log_to_sftp >> send_import_complete_email >> batch_end

    return dag

rail.for_each_instance(create_process_final_logs_dag)
