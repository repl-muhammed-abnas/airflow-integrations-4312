from uuid import uuid4
import rail
from dxctechnology.psa_resource_assignment_v2.utils import python_callable_method

null = None


def create_process_date_range_child_dag(config):
    """
    Child DAG to process a single batch of date range updates.
    This DAG is triggered by both process_each_wbs.py (parent WBS) and
    process_child_wbs.py (child WBS) for each batch to avoid timeout issues.

    Expected dag_run.conf:
    - project_uri: The project URI (parent or child)
    - batch: The batch of users to process (contains 'users' list)
    """

    with rail.create_airflow_dag(
        dag_id=config.process_date_range_child_dagid,
        description=f'DXC PSA Resource - Date Range Update Child {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Call CreateProjectOrApplyModifications for this batch
        rail.RepliconServiceOperator(
            task_id='update_date_range_for_batch',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: python_callable_method.build_date_range_payload_from_conf(dag_run.conf)
        )


    return dag


rail.for_each_instance(create_process_date_range_child_dag)
