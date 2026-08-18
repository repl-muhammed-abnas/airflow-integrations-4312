import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_auto_population_of_optional_holidays_india_new_users_webhook_master_{config.instance}',
        description=f'Capgemini Auto Population of Optional Holidays India for New Users using Webhook Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_new_users,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        },
        webhook_conf=[
            rail.WebhookConf(hmac_secret_var=config.webhook_shared_secret)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.TriggerDagRunOperator(
            task_id='process_new_user',
            trigger_dag_id=f'capgemini_auto_population_of_optional_holidays_india_process_new_users_{config.instance}',
            conf={
                "user_uri": '{{ dag_run.conf.webhook.data.user.uri }}',
                "user_name": '{{ dag_run.conf.webhook.data.user.displayText }}'
            }
        )

    return dag


rail.for_each_instance(create_dag)
