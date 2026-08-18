"""
Workday-Replicon User Import - Employee Type Hierarchy Child DAG

This module creates a child DAG that creates or updates employee type group hierarchies
in Replicon based on Workday user type data.

The DAG uses the EmployeeTypeGroupService to establish or modify employee type
hierarchies, ensuring user types from Workday exist in Replicon.

Functions:
    create_employeetype_child_dag(config): Creates the employee type hierarchy child DAG
"""
from airflow.models import Variable
import rail
from unisys.workday_user_import_v1.utils import request_payload

null = None
true = True

def create_employeetype_child_dag(config):
    """
    Create child DAG for processing employee type hierarchies.

    This DAG is triggered by the process_groups DAG to create new employee type
    group hierarchies in Replicon when new user types are detected in Workday data.

    Args:
        config: Configuration object containing:
            - process_new_usertypes (str): DAG identifier
            - company_key (str): Replicon company identifier
            - replicon_conn_id (str): Airflow connection ID
            - create_employeetypes_child_max_active_runs (int): Concurrent execution limit

    Expected dag_run.conf:
        full_path (str): Full path of the employee type to create

    Returns:
        airflow.DAG: Configured child DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.process_new_usertypes,
        description=f"Unisys Workday User Import - Process User Types",
        company_key=config.company_key,
        max_active_runs=config.create_employeetypes_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id="create_employee_type_hierarchy",
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupHierarchyOrApplyModifications",
            data=request_payload.get_employee_types_hierarchy_payload
        )

        return dag


rail.for_each_instance(create_employeetype_child_dag)
