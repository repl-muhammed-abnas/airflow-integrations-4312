"""
Process Employee Type - GuestTek Talent User Import Child DAG

Creates employee type hierarchies in Replicon.
This child DAG is triggered for each employee type that needs to be created.
"""
import rail
from guesttekinteractive.talent_user_import.utils import request_payload

null = None


def create_child_dag(config):
    """Create child DAG for creating employee type hierarchies."""
    with rail.create_airflow_dag(
        dag_id=config.process_new_usertypes,
        description='GuestTek Talent User Import - Create Employee Type',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_employeetype,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        create_employeetype_hierarchy = rail.RepliconServiceOperator(
            task_id="create_employeetype_hierarchy",
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupHierarchyOrApplyModifications",
            data=request_payload.get_employee_types_hierarchy_payload
        )
    
    return dag


rail.for_each_instance(create_child_dag)
