import datetime
from airflow.models import Variable
import rail

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api/config.py


# pylint:disable = too-many-statements
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_client_master_{config.instance}',
        description=f'Project Client data sync_Master V8 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.webhook_secret),
        start_date=datetime.datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_redirect_to_workato = rail.IfOperator(
            task_id='can_redirect_to_workato',
            test=lambda: Variable.get(
                config.can_redirect_to_workato_var_name, default_var='').lower() == 'true',
            yes_task='post_to_workato',
            no_task='should_strip_payload',
        )

        post_to_workato = rail.SimpleHttpOperator(
            task_id='post_to_workato',
            method='POST',
            http_conn_id=config.workato_api_endpoint,
            headers={
                "Content-Type": 'application/json; charset=utf-8',
                "API-TOKEN": "{{ var.value." + config.workato_api_token_var_name + " }}"
            },
            data="{{ dag_run.conf.webhook.data | to_json(ensure_ascii=True) }}",
        )

        should_strip_payload = rail.IfOperator(
            task_id='should_strip_payload',
            test=lambda: True,
            yes_task='get_striped_payload',
            no_task='trigger_process_payload_dag'
        )

        def get_striped_webhook_payload(dag_run):
            def get_striped_json_values(obj):
                if isinstance(obj, dict):
                    return {k: get_striped_json_values(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [get_striped_json_values(item) for item in obj]
                elif isinstance(obj, str):
                    return obj.strip()
                else:
                    return obj
            return get_striped_json_values(dag_run.conf)

        get_striped_payload = rail.PythonOperator(
            task_id="get_striped_payload",
            python_callable=get_striped_webhook_payload
        )

        trigger_process_payload_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_process_payload_dag",
            trigger_dag_id=config.project_import_api_process_payload_child_dag_id,
            retries = 0,
            conf=lambda: {
                **rail.result('get_striped_payload')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_redirect_to_workato >> rail.Label("Yes") >> post_to_workato
        can_redirect_to_workato >> rail.Label("No") >> should_strip_payload
        should_strip_payload >> rail.Label("Yes") >> get_striped_payload >> trigger_process_payload_dag
        should_strip_payload >> rail.Label("No") >> trigger_process_payload_dag >> log_to_sumo

        return dag

rail.for_each_instance(create_main_airflow_dag)
