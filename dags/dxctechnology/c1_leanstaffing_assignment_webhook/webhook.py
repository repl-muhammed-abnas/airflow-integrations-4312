from datetime import timedelta
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dag_id,
        description=f'DXC C1 Leanstaffing Assignment Webhook receiver {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=config.team_rate_modified_token_variable_name,
                trigger_condition='{{ data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
            rail.WebhookConf(
                hmac_secret_var=config.team_dates_modified_token_variable_name,
                trigger_condition='{{ data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
            rail.WebhookConf(
                hmac_secret_var=config.team_modified_token_variable_name,
                trigger_condition='{{ data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
        ],
        max_active_runs=config.max_webhook_master_active_dag_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        was_triggered_by_repliconpsa = rail.IfOperator(
                task_id="was_triggered_by_repliconpsa",
                test='{{ dag_run.conf.webhook.data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") }}',
                yes_task="is_valid_webhookevent",
                no_task='was_triggered_by_replicon'
            )

        was_triggered_by_replicon = rail.IfOperator(
                task_id="was_triggered_by_replicon",
                test='{{ dag_run.conf.webhook.data.authority.actingUser.loginName | lower | starts_with("repliconint") }}',
                yes_task="delete_this_dagrun",
                no_task='is_valid_webhookevent'
            )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        is_valid_webhookevent = rail.IfOperator(
            task_id = "is_valid_webhookevent",
            test = "{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] in ['ProjectTeamModified', 'ProjectTeamMemberAssignmentDatesModified',\
                'ProjectTeamMemberBillingRateAssociationsModified']}}",
            yes_task="trigger_webhook_processing_dag",
            no_task= "fail_invalid_webhookevent"
        )

        trigger_webhook_processing_dag = rail.TriggerDagRunOperator(
            task_id="trigger_webhook_processing_dag",
            trigger_dag_id=config.webhook_processor_dag_id,
            conf=lambda dag_run: {
                **dag_run.conf
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        fail_invalid_webhookevent = rail.FailOperator(
            task_id = "fail_invalid_webhookevent",
            message= "Received invalid webhook trigger event: '{{dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type']}}'"
        )

        was_triggered_by_repliconpsa >> rail.Label("Yes") >> is_valid_webhookevent
        was_triggered_by_repliconpsa >> rail.Label("No") >> was_triggered_by_replicon
        was_triggered_by_replicon >> rail.Label("Yes") >> delete_this_dagrun
        was_triggered_by_replicon >> rail.Label("No") >> is_valid_webhookevent >> rail.Label("Yes") >> trigger_webhook_processing_dag
        is_valid_webhookevent >> rail.Label("No") >> fail_invalid_webhookevent
        
    return dag

rail.for_each_instance(create_main_dag)