from pendulum import datetime
import rail

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'cbrefcg_user_created_webhook_event_{config.instance}',
        description=f'cbrefcg_User_Created_Webhook event {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'cbrefcg_user_created_webhooks_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        check_user_and_acting_user = rail.IfOperator(
            task_id= 'check_user_and_acting_user',
            test= '{{ dag_run.conf.webhook.data.user.uri == dag_run.conf.webhook.data.authority.actingUser.uri }}',
            yes_task= 'delete_this_dagrun',
            no_task= 'create_log'
        )

        delete_this_dagrun= rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun',
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
            tenant_wide_name="cbre_webhook_data",
            existing_log_mode="append",
        )

        cbrefcg_usercreated_webhooks_add_entry= rail.WriteLogOperator(
            task_id='cbrefcg_usercreated_webhooks_add_entry',
            log="{{ result('create_log') }}",
            message="CBREFCG Webhook Data",
            properties={
                    "useruri": "{{ dag_run.conf.webhook.data.user.uri }}",
                    "loginname": "{{ dag_run.conf.webhook.data.user.loginName }}",
                    "eventdatetime": "{{ dag_run.conf.webhook.received_at }}",
                    "eventdate": "{{ dag_run.conf.webhook.received_at }}",
                    "eventtype": "UserCreated"
                }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        check_user_and_acting_user >> rail.Label(
            "Yes") >> delete_this_dagrun

        check_user_and_acting_user >> rail.Label(
            "No") >> create_log >> cbrefcg_usercreated_webhooks_add_entry >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
