from datetime import timedelta
from pendulum import datetime as dt, now
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f"Grant Import v1 Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        schedule_interval=None,  # webhook-driven; no schedule
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(bearer_token_var=config.nber_bearer_token),
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        get_standard_file_name = rail.PythonOperator(
            task_id = 'get_standard_file_name',
            python_callable= lambda dag_run: f'{get_dagrun_ecid(dag_run).split(":")[0]}_{now().strftime("%Y%m%dT%H%M%S")}.csv'
        )

        process_projects = rail.TriggerDagRunOperator(
            task_id="process_projects",
            trigger_dag_id=config.process_payload_dagid,
            conf=lambda dag_run: {
                "project_data": dag_run.conf["webhook"]["data"],
                "log_filename": "log_"+ str(rail.result('get_standard_file_name')),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda dag_run: {
                'count_of_total_records': len(dag_run.conf['webhook']['data'])
            }
        )

        get_standard_file_name >> process_projects >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
       
