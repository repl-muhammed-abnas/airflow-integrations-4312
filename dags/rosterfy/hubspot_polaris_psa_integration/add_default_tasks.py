import rail
from rosterfy.hubspot_polaris_psa_integration.utils import request_payload

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'rosterfy_hubspot_polaris_psa_integration_add_default_task_child_{config.instance}',
        description=f'rosterfy_hubspot_polaris_psa_integration_add_default_task_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='create_project_task',
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            data=request_payload.create_task
        )

    return dag

rail.for_each_instance(create_main_dag)
