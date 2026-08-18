
from datetime import timedelta
import rail
from exponent.conflict_check_report.utils import vp_helpers


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.dag_id,
        description='Exponent - Conflict Check Report Publisher',
        company_key=config.company_key,
        replicon_conn_id=None,
        integration_type='generic',
        max_active_runs=config.max_active_runs,
        webhook_conf=rail.WebhookConf(
            basic_auth_username_var= config.basic_auth_username_exponent_inc,
            basic_auth_password_var=config.basic_auth_pass_exponent_inc),
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        submit_report_job = rail.VantagepointAPIOperator(
            task_id='submit_report_job',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/processMasterJob',
            request_method='POST',
            request_body=lambda: vp_helpers.submit_report_job(config.conflict_search_report_path, config.process_queue_id),
            pagination=False,
        )

        get_process_id = rail.PythonOperator(
            task_id='get_process_id',
            python_callable=lambda: vp_helpers.fetch_process_id(rail.result("submit_report_job")),
        )

        poll_report_completion = rail.PythonOperator(
            task_id='poll_report_completion',
            python_callable=lambda: vp_helpers.poll_report_completion(config.vantagepoint_conn_id),
            retries=config.poll_max_attempts,
            retry_delay=timedelta(seconds=config.poll_interval_seconds),
            retry_exponential_backoff=False,
        )

        download_pdf = rail.PythonOperator(
            task_id='download_pdf',
            execution_timeout=timedelta(minutes=5),
            python_callable=lambda: vp_helpers.download_pdf(
                vp_conn_id=config.vantagepoint_conn_id,
                file_id=rail.result("poll_report_completion"),
            ),
        )

        upload_pdf_to_project = rail.PythonOperator(
            task_id='upload_pdf_to_project',
            execution_timeout=timedelta(minutes=5),
            python_callable=vp_helpers.upload_pdf_to_project,
            op_kwargs={'vp_conn_id': config.vantagepoint_conn_id},
        )

        # get_cust_csr_employee_triggered = rail.VantagepointAPIOperator(
        #     task_id='get_cust_csr_employee_triggered',
        #     vp_conn_id=config.vantagepoint_conn_id,
        #     endpoint="/project/{{ dag_run.conf['webhook']['data']['WBS1'] }}/?fieldFilter=CustCSREmployeeTriggered",
        #     request_method='GET',
        #     request_body={},
        #     pagination=False,
        # )

        attach_file_to_project = rail.VantagepointAPIOperator(
            task_id='attach_file_to_project',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint="/project/{{ dag_run.conf['webhook']['data']['WBS1'] }}",
            request_method='PUT',
            request_body=lambda: vp_helpers.attach_file_to_project_payload(rail.result("upload_pdf_to_project")),
            pagination=False,
        )

        send_failure_email = rail.EmailOperator(
            task_id='send_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject=(
                f"[{config.dag_id}] Exponent Conflict Check Report — FAILED "
                "({{ dag_run.run_id }})"
            ),
            html_content=(
                "<p>The Exponent Conflict Check Report DAG failed.</p>"
                "<ul>"
                "<li><b>DAG:</b> {{ dag.dag_id }}</li>"
                "<li><b>Run ID:</b> {{ dag_run.run_id }}</li>"
                "<li><b>Logical date:</b> {{ ts }}</li>"
                "<li><b>Project (WBS1):</b> "
                "{{ dag_run.conf.get('webhook', {}).get('data', {}).get('WBS1', 'unknown') }}"
                "</li>"
                "<li><b>Failed task:</b> {{ task_instance.task_id }}</li>"
                "<li><b>Error message:</b><br/><pre>{{ get_error_message() }}</pre></li>"
                "</ul>"
                "<p>Please investigate the failure in Airflow.</p>"
                "<p><b>Dag run conf:</b> <pre>{{ dag_run.conf | tojson(indent=2) }}</pre></p>"
            ),
        )

        
        submit_report_job >> get_process_id >> poll_report_completion >> download_pdf >> upload_pdf_to_project >> attach_file_to_project >> send_failure_email
        

    return dag


rail.for_each_instance(create_dag)
