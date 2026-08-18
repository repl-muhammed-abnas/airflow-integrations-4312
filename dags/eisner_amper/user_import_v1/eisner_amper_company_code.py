import rail
from eisner_amper.user_import_v1.utils import response_filter, request_payload
from datetime import datetime, timedelta

# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.user_sync_company_code_child_dag_id,
        description=f"Eisner Amper Company code user Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_company_code_or_apply_modification = rail.RepliconServiceOperator(
            task_id='create_company_code_or_apply_modification',
            endpoint='/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=request_payload.create_company_code_or_apply_modification_payload
        )

        create_company_code_or_apply_modification

    return dag


rail.for_each_instance(create_child_dag)
