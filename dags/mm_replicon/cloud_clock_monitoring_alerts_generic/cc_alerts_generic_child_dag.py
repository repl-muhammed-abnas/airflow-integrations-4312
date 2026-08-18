import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id="cloud_clock_monitoring_alerts_generic_child_dag",
        description="Replicon Cloud Cloud Monitoring Alerts Generic Child DAG",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child_dag,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_cloud_clock_email_content = rail.RenderTemplateOperator(
            task_id='get_cloud_clock_email_content',
            target='result',
            template_file='templates/email/output_template.html',
            dataset=[
                {
                    "Company": "{{dag_run.conf.Company}}",
                    "emailto": "{{dag_run.conf.emailto}}",
                    "Clock": "{{dag_run.conf.Clock}}",
                    "Last_Update": "{{dag_run.conf.Last_Update}}",
                    "Unsent_Punches": "{{dag_run.conf.Unsent_Punches}}"
                }
            ]
        )

        send_cloud_clock_data_in_mail = rail.EmailOperator(
            task_id='send_cloud_clock_data_in_mail',
            to="{{dag_run.conf.emailto}}",
            subject='Replicon Support | CloudClock Sync Failure Alert ! - {{dag_run.conf.Company}} - {{dag_run.conf.Clock}}',
            html_content='{{ result("get_cloud_clock_email_content")}}',
        )

        get_cloud_clock_email_content >> send_cloud_clock_data_in_mail

    return dag


rail.for_each_instance(create_child_dag)
