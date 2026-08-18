import rail
from crl.payroll_export_uk.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f"Create Object Child UK {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_batch_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        rail.RepliconServiceOperator(
            task_id="create_object_set",
            endpoint="/services/UserService1.svc/CreateObjectSet",
            data=request_payload.get_create_object_set
        )

    return dag


rail.for_each_instance(create_dag)
