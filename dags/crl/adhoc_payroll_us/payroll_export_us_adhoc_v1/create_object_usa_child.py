import rail
from crl.adhoc_payroll_us.payroll_export_us_adhoc_v1.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f"Create Object Child USA Adhoc {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_batch_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_object_set = rail.RepliconServiceOperator(
            task_id="create_object_set",
            endpoint="/services/UserService1.svc/CreateObjectSet",
            data=request_payload.get_create_object_set
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        create_object_set >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
