import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/cbreinc/add_projects_to_client/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'cbreinc_add_project_to_client_webhooks_{config.instance}',
        description=f'CBREInc Add Projects To Client Webhook receiver {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=f'cbreinc_webhook_add_project_to_client_{config.instance}_secret'),
        ]
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        start = rail.EmptyOperator(
            task_id="start"
        )
        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name="cbreinc_webhook_add_project_to_client",
            existing_log_mode="append",
        )

        write_webhook_to_log = rail.WriteLogOperator(
            task_id="write_webhook_to_log",
            log="{{ result('get_webhook_log') }}",
            message="{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] }}",
            properties={
                'project_uri': "{{ dag_run.conf.webhook.data.project.uri }}",
                'project_name': "{{ dag_run.conf.webhook.data.project.name }}"
            }
        )


        finish = rail.EmptyOperator(
            task_id="finish"
        )

        start >> get_webhook_log >> write_webhook_to_log >> finish
        return dag

rail.for_each_instance(create_dag)
