import rail
from groupmportugal.project_sync.utils import request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.add_client,
        description=f"Add Client child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        if_clienturi_not_present = rail.IfOperator(
            task_id='if_clienturi_not_present',
            test=lambda dag_run: not bool(dag_run.conf['clienturi']),
            yes_task="add_client",
            no_task='catch_client_error'
        )

        add_client = rail.RepliconServiceOperator(
            task_id="add_client",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=request_payload.create_client_request
        )

        catch_client_error = rail.EmptyOperator(
            task_id='catch_client_error',
            trigger_rule='one_failed'
        )

        if_clienturi_not_present >> rail.Label("Yes") >> add_client >> catch_client_error
        if_clienturi_not_present >> rail.Label("No") >> catch_client_error

    return dag


rail.for_each_instance(create_child_dag)
