from datetime import timedelta
import rail
from dxctechnology.c1_task_import import request_payload, custom_method

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_task_import/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_task_import_child_update_compass_task_{config.instance}",
        description=f"DXCTechnology C1 Task Import Update compass Task {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.child_dag_update_task_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        does_this_task_already_exist = rail.IfOperator(
            task_id="does_this_task_already_exist",
            test="{{ dag_run.conf.existing_tasks | is_truthy }}",
            yes_task='can_update_task',
            no_task='create_task',
        )

        can_update_task = rail.IfOperator(
            task_id="can_update_task",
            test=custom_method.can_update_task,
            yes_task="update_task",
            no_task=[]
        )

        update_task = rail.RepliconServiceOperator(
            task_id="update_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_update_c1_task_payload
        )

        create_task = rail.TriggerDagRunForEachItemOperator(
            task_id="create_task",
            items=[1],
            trigger_dag_id=f"dxctechnology_c1_task_import_child_create_compass_task_{config.instance}",
            conf=request_payload.get_create_c1_task_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_create_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_task',
            dag_runs='{{ result("create_task") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        does_this_task_already_exist >> rail.Label(
            "Yes") >> can_update_task
        can_update_task >> rail.Label("Yes") >> update_task
        does_this_task_already_exist >> rail.Label(
            "No") >> create_task >> wait_for_create_task

    return dag


rail.for_each_instance(create_child_dag)
