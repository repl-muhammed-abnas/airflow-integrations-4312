from datetime import timedelta
from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= "Wipro Time Off Export Master (Endpoint)",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=config.wipro_timeoff_export_approved_access_token_variable,
                trigger_condition='{{ data.authority.displayText | lower | starts_with("system") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}' 
            ),
            rail.WebhookConf(
                hmac_secret_var=config.wipro_timeoff_export_rejected_access_token_variable,
                trigger_condition='{{ data.authority.displayText | lower | starts_with("system") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
            rail.WebhookConf(
                hmac_secret_var=config.wipro_timeoff_export_waiting_access_token_variable,
                trigger_condition='{{ data.authority.displayText | lower | starts_with("system") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
            rail.WebhookConf(
                hmac_secret_var=config.wipro_timeoff_export_deleted_access_token_variable,
                trigger_condition='{{ data.authority.displayText | lower | starts_with("system") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            )
        ]
    ) as dag:

        # NOTE: Further business logic should be done in a separate child dag
        # To child dag all the posted data will be passed in conf from master(current dag)
        # So, creating a new version of integration will be easier in the future

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id = 'process_timeoffdata',
            trigger_dag_id= config.child_dag_id,
            conf=lambda dag_run: {
                'data': dag_run.conf['webhook']['data'],
                'received_at': dag_run.conf['webhook']['received_at']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

    return dag

rail.for_each_instance(create_main_dag)
