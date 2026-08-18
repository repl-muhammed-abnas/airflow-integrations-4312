import rail
from dxctechnology.c1_task_import import request_payload

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_task_import/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_task_import_child_create_compass_task_{config.instance}",
        description=f"DXCTechnology C1 Task Import create Compass Task {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.child_dag_create_task_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_task = rail.RepliconServiceOperator(
            task_id="create_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_add_c1_task_payload
        )

        has_any_users_to_assign = rail.IfOperator(
            task_id="has_any_users_to_assign",
            test="{{dag_run.conf.user_list | length > 0}}",
            yes_task="add_users_to_task",
            no_task="finish"
        )

        add_users_to_task = rail.RepliconServiceOperator(
            task_id="add_users_to_task",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda dag_run: {
                "taskUri": rail.result('create_task')['uri'],
                "resourceUris": dag_run.conf['user_list'],
                "isAssigned": "true"
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        create_task >> has_any_users_to_assign >> rail.Label("Yes") >> add_users_to_task >> finish
        has_any_users_to_assign >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_child_dag)
