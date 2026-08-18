from datetime import datetime as dt
from pendulum import datetime
import rail

def create_webhook_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"capgemini_deleted_timeoff_booking_webhook_logging_{config.instance}",
        description="Logging Deleted TO booking in Tenant wide Log",
        schedule_interval=None,
        start_date=datetime(2023, 9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        webhook_conf=[
            rail.WebhookConf(hmac_secret_var=config.webhook_secret_var_name)
        ]
    ) as dag:

        def get_trigger_dag_id(dag_run):
            try:
                received_at = dt.strptime(dag_run.conf['webhook']['received_at'], "%Y-%m-%dT%H:%M:%S.%f")
            # to handle the date-time stamp where the milliseconds are zero
            except ValueError:
                received_at = dt.fromisoformat(dag_run.conf['webhook']['received_at'])
            # anything else will be raised
            except Exception as exception:
                raise exception
            # Rather than going back year 1970 we are going to Year 2023
            # as converting the date to seconds is a large number if 1970 year is considered.
            first_jan_2023 = dt(day = 1, month=1, year= 2023, hour=0, minute=0, second=0, microsecond=0)
            _seconds = int(round((received_at - first_jan_2023).total_seconds()))
            return f"capgemini_deleted_timeoff_booking_webhook_logging_child_{config.instance}_{(_seconds%len(config.tenant_wide_log_list))+1}"


        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_child",
            trigger_dag_id=get_trigger_dag_id,
            items=[1],
            conf=lambda dag_run: {
                **dag_run.conf
            }
        )

    return dag

rail.for_each_instance(create_webhook_dag)
