from rail import (for_each_instance, create_airflow_dag,
                  SimpleHttpOperator, EmailOperator)


def create_rp_api_health_check_dag(config):
    with create_airflow_dag(
        dag_id=f"resource_planner_api_health_check_{config.instance}",
        description="Probes the RP Backend API every 10 minutes and sends a priority alert if it is unreachable",
        schedule_interval=config.schedule_interval,
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        check_api_health = SimpleHttpOperator(
            task_id="check_api_health",
            method="GET",
            http_conn_id=config.rp_api_conn_id,
            endpoint=config.health_check_endpoint,
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        send_priority_alert = EmailOperator(
            task_id="send_priority_alert",
            to=config.email_alert_recipients,
            bcc=config.internal_logs_email,
            subject="[PRIORITY] {{ get_company_key() }} | Resource Planner API for Airflow is DOWN at '{{ current_time_in_specified_tz() }}'",
            html_content=(
                "<p><strong style='color:red;font-size:16px;'>[PRIORITY] Resource Planner API for Airflow is DOWN</strong></p>"
                "<table border='0' cellpadding='6'>"
                "<tr><td><strong>Instance:</strong></td><td>{{ get_company_key() }}</td></tr>"
                "<tr><td><strong>Detected at:</strong></td><td>{{ current_time_in_specified_tz() }}</td></tr>"
                "<tr><td><strong>Endpoint probed:</strong></td><td><code>GET /health</code></td></tr>"
                "</table>"
                "<br/>"
                "<p>The RP Backend API did not return HTTP 200 on the <code>/health</code> probe. "
                "All integration pipelines (task allocations, time-off, confirmed bookings) are likely blocked.</p>"
                "<p><strong>Action required:</strong> Check the RP-Airflow Backend API server, IIS process, and database connectivity immediately.</p>"
            ),
            trigger_rule="one_failed",
        )

        check_api_health >> send_priority_alert

    return dag


for_each_instance(create_rp_api_health_check_dag)
