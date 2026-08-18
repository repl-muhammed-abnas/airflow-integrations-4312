from pendulum import datetime
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'cbrefcg_project_team_members_added_webhook_event_{config.instance}',
        description=f'cbrefcg_Project_Team_Members_Added_Webhook event {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        webhook_conf=[rail.WebhookConf(
            hmac_secret_var=f'cbrefcg_project_team_members_added_webhooks_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        is_valid_webhookevent = rail.IfOperator(
            task_id = "is_valid_webhookevent",
            test = "{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] == 'ProjectTeamModified' }}",
            yes_task="check_user_and_acting_user",
            no_task= "fail_invalid_webhookevent"
        )

        fail_invalid_webhookevent = rail.FailOperator(
            task_id = "fail_invalid_webhookevent",
            message= "Received invalid webhook trigger event: '{{dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type']}}'"
        )

        check_user_and_acting_user = rail.IfOperator(
            task_id= 'check_user_and_acting_user',
            test= '{{ dag_run.conf.webhook.data.authority.actingUser.loginName }}' == config.login_name,
            yes_task= 'delete_this_dagrun',
            no_task= 'create_log'
        )

        delete_this_dagrun= rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun',
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
            tenant_wide_name="cbre_webhook_project_data",
            existing_log_mode="append",
        )

        write_webhook_to_log= rail.WriteLogOperator(
            task_id='write_webhook_to_log',
            log="{{ result('create_log') }}",
            message="CBREFCG Webhook Data",
            properties={
                    "projecturi": "{{ dag_run.conf.webhook.data.project.uri }}",
                    "Projectname": "{{ dag_run.conf.webhook.data.project.name }}",
                    "eventdatetime": "{{ dag_run.conf.webhook.received_at }}",
                    "eventdate": "{{ dag_run.conf.webhook.received_at }}",
                    "eventtype": "ProjectTeamModified"
                }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        is_valid_webhookevent >> rail.Label(
            "Yes") >> check_user_and_acting_user

        is_valid_webhookevent >> rail.Label(
            "No") >> fail_invalid_webhookevent

        check_user_and_acting_user >> rail.Label(
            "Yes") >> delete_this_dagrun

        check_user_and_acting_user >> rail.Label(
            "No") >> create_log >> write_webhook_to_log >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
