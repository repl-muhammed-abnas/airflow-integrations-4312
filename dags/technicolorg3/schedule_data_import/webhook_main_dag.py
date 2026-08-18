
from datetime import timedelta, datetime
import rail


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_schedule_data_import_webhook_master_{config.instance}',
        description=f'Technicolor CETA Schedule Data Webhook_Master - V2.0 - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.webhook_secret),
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        trigger_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_dag',
            retries=0,
            items=lambda: [rail.get_current_context(
            )["dag_run"].conf["webhook"]["data"]],
            trigger_dag_id=f'technicolorg3_schedule_data_import_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "mill_mpc": item["mill_mpc"],
                "projectnumber": item["projectnumber"],
                "title": item["title"].strip(),
                "role": item["role"].strip(),
                "service": item["service"].strip(),
                "description": item["description"].strip(),
                "starttime": item["starttime"],
                "endtime": item["endtime"],
                "duration": item["duration"],
                "resourcename": item["resourcename"],
                "resourcescheduleserviceID": item["resourcescheduleserviceID"],
                "fd_status": item["fd_status"],
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        trigger_child_dag >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
