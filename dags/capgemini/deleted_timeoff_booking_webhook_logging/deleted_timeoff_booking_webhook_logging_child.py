from pendulum import datetime
from datetime import timedelta
import rail

def create_webhook_dag(config):
    dag_list = []

    for idx, log_name in enumerate(config.tenant_wide_log_list):
        with rail.create_airflow_dag(
            dag_id=f"capgemini_deleted_timeoff_booking_webhook_logging_child_{config.instance}_{idx+1}",
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

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            def get_log_properties(dag_run):
                webhook_details = dag_run.conf['webhook']['data']
                timeoff_hours = timedelta(hours=int(webhook_details['totalDuration']['calendarDayDuration']['hours']),
                    minutes=int(webhook_details['totalDuration']['calendarDayDuration']['minutes']),
                        seconds=int(webhook_details['totalDuration']['calendarDayDuration']['seconds'])).total_seconds()/3600
                return{
                    "user_login_name" : webhook_details['owner']['loginName'],
                    "user_uri": webhook_details['owner']['uri'],
                    "timeoff_type_name": webhook_details['timeOffType']['name'],
                    "timeoff_type_uri": webhook_details['timeOffType']['uri'],
                    "timeoff_booking_uri": webhook_details['timeOff']['uri'],
                    "total_working_days": webhook_details['totalDuration']['decimalWorkdays'],
                    "total_working_hours": f'{timeoff_hours:.2f}'
                }

            rail.WriteLogOperator(
                task_id = "log_deleted_timeoff",
                log= log_name,
                severity="Deleted",
                message="Timeoff booking Deleted",
                properties=get_log_properties
            )

        dag_list.append(dag)

    return dag_list
rail.for_each_instance(create_webhook_dag)
