from datetime import datetime as dt
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_dagid,
        description=f'Capgemini Auto Population of Optional Holidays India for New Users using Webhook Master v0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_new_users,
        default_args={
            'retries': 0
        },
        webhook_conf=[
            rail.WebhookConf(hmac_secret_var=config.webhook_shared_secret)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_trigger_dag_id(dag_run):
            received_at = dt.strptime(dag_run.conf['webhook']['received_at'], "%Y-%m-%dT%H:%M:%S.%f")
            # Rather than going back year 1970 we are going to Year 2023
            # as converting the date to seconds is a large number if 1970 year is considered.
            first_jan_2023 = dt(day = 1, month=1, year= 2023, hour=0, minute=0, second=0, microsecond=0)
            _seconds = int(round((received_at - first_jan_2023).total_seconds()))
            return f"{config.webhook_logging_child_dagid}_{(_seconds%len(config.tenant_wide_log_list))+1}_v0"

        rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_child",
            trigger_dag_id=get_trigger_dag_id,
            items=[1],
            conf=lambda dag_run: {
                **dag_run.conf
            }
        )

    return dag

rail.for_each_instance(create_dag)
