import rail
from eisner_amper.user_import.utils import response_filter, request_payload
from datetime import datetime, timedelta

# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.user_sync_cost_center_child_dag_id,
        description=f"Eisner Amper cost center user Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_cost_center_or_apply_modification = rail.RepliconServiceOperator(
            task_id='create_cost_center_or_apply_modification',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterOrApplyModification',
            data=request_payload.create_cost_center_or_apply_modification_payload
        )

        create_cost_center_or_apply_modification

    return dag


rail.for_each_instance(create_child_dag)
