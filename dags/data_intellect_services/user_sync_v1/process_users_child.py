from datetime import timedelta
import json
from data_intellect_services.user_sync_v1.utils import request_payload
import rail
from airflow.models import Variable

null = None

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"data_intellect_user_import_process_users_child_{config.instance}_v1",
        description=f"Data intellect services user sync process users child dag {config.instance} V1",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_users_child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_process_users_child_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details_from_hibob'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_details_from_hibob',
            end_task='dagrun_log_to_sumo',
        )

        get_user_details_from_hibob = rail.SimpleHttpOperator(
            task_id='get_user_details_from_hibob',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint="people/{{ dag_run.conf.user_details.id }}",
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            data=json.dumps({
                "humanReadable": "REPLACE"
            }),
            response_filter=lambda response: json.loads(response.text) if json.loads(response.text) else null
        )

        get_user_details_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_details_from_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_user_details_from_replicon,
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_create_user = rail.IfOperator(
            task_id='is_create_user',
            test='{{ dag_run.conf.user_details.action == "Create" }}',
            yes_task='trigger_create_user',
            no_task='is_update_user'
        )

        trigger_create_user = rail.TriggerDagRunOperator(
            task_id='trigger_create_user',
            trigger_dag_id=f"data_intellect_user_import_create_user_child_{config.instance}_v1",
            conf=lambda dag_run: {
                "user_details": dag_run.conf["user_details"],
                "hibob_user_details": rail.result("get_user_details_from_hibob"),
                "replicon_user_details": rail.result("get_user_details_from_replicon"),
                "log_artifact": dag_run.conf["log_artifact"]
            }
        )

        wait_for_create_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_create_user",
            dag_runs="{{result('trigger_create_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_update_user = rail.IfOperator(
            task_id='is_update_user',
            test='{{ dag_run.conf.user_details.action == "Update" }}',
            yes_task='trigger_update_user',
            no_task='dagrun_log_to_sumo'
        )

        trigger_update_user = rail.TriggerDagRunOperator(
            task_id='trigger_update_user',
            trigger_dag_id=f"data_intellect_user_import_update_user_child_{config.instance}_v1",
            conf=lambda dag_run: {
                "user_details": dag_run.conf["user_details"],
                "hibob_user_details": rail.result("get_user_details_from_hibob"),
                "replicon_user_details": rail.result("get_user_details_from_replicon"),
                "log_artifact": dag_run.conf["log_artifact"]
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_user",
            dag_runs="{{result('trigger_update_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> get_user_details_from_hibob

        get_user_details_from_hibob >> get_user_details_from_replicon >> is_create_user

        is_create_user >> rail.Label("Yes") >> trigger_create_user >> wait_for_create_user >> dagrun_log_to_sumo
        is_create_user >> rail.Label("No") >> is_update_user

        is_update_user >> rail.Label("Yes") >> trigger_update_user >> wait_for_update_user >> dagrun_log_to_sumo
        is_update_user >> rail.Label("No") >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
