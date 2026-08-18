from datetime import timedelta, datetime
import pendulum
import rail
from rosterfy.hubspot_polaris_psa_integration.utils.python_callable import get_dagruns_to_process, format_logs_callable

# pylint:disable = too-many-statements
def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'rosterfy_hubspot_polaris_psa_integration_master_log_scheduled_{config.instance}',
        description=f'rosterfy_hubspot_polaris_psa_integration_master_log_scheduled_{config.instance}',
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
                     [config.sales_master_dag,config.services_master_dag,config.renewals_master_dag,
                      config.update_deal_master_dag]]
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
            dagrun_task_id='log_project_process',
            flatten=True
        )

        get_invalid_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_invalid_project_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='log_invalid_pipeline_or_dealstage',
            flatten=True
        )

        get_errored_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_errored_project_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='catch_and_log_error',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=format_logs_callable
        )

        write_projectprocess_log_file = rail.WriteCSVFileOperator(
            task_id='write_projectprocess_log_file',
            source="{{ result('format_logs') }}",
            header=[ 'deal_name','pipeline','deal_satge', 'status','details', 'ecid'],
            row=[
                '{{ item.properties | attr_or_default("deal_name", "") }}',
                '{{ item.properties | attr_or_default("pipeline", "") }}',
                '{{ item.properties | attr_or_default("deal_satge", "") }}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.properties | attr_or_default("details", "") }}',
                '{{ item.ecid}}']
        )

        check_csv_has_data = rail.IfOperator(
            task_id = "check_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_projectprocess_log_file'))) > 0,
            yes_task = "generate_downloadlink",
            no_task = "fail_the_dag"
        )

        fail_the_dag = rail.FailOperator(
            task_id="fail_the_dag",
            message='No log found'
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('write_projectprocess_log_file')}}",
            output_file_name="project_sync_log_{{ current_time_in_specified_tz() }}.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.to_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0  -%}\
                    "+config.bcc_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Hubspot-Polaris PSA Integration - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%}' \
                + " - " + pendulum.now().format("MM-DD-YYYY-HH-mm-ss"),
            html_content="templates/emails/import_complete_mail.html",
            params={
                'today': pendulum.now().format("MM-DD-YYYY-HH-mm-ss"),
            }
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label('Yes') >> get_project_logs >> get_invalid_project_logs >> get_errored_project_logs >> \
            format_logs >> write_projectprocess_log_file >> check_csv_has_data
        
        check_csv_has_data >> rail.Label("Yes") >> generate_downloadlink >> send_import_complete_email
        check_csv_has_data >> rail.Label("No") >> fail_the_dag

        is_log_dagruns_present >> rail.Label("No") >> delete_this_dagrun

        return dag

rail.for_each_instance(create_log_airflow_dag)
