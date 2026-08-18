"""
Process Role - GuestTek Talent User Import Child DAG

Creates a single Project Role in Replicon.
"""
from uuid import uuid4
import rail

null = None


def create_child_dag(config):
    """Create child DAG for creating a single Project Role."""
    with rail.create_airflow_dag(
        dag_id=config.process_each_role_dag_id,
        description='GuestTek Talent User Import - Create Role',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_role,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        create_role = rail.RepliconServiceOperator(
            task_id='create_role',
            endpoint="/services/ProjectRoleService1.svc/CreateProjectRoleOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "name": dag_run.conf['role_name'],
                    "descriptionToApply": null,
                    "isArchivedToApply": False,
                    "isBillableToApply": True,
                    "billingRateScheduleToApply": null,
                    "costRateScheduleToApply": null
                },
                "projectRoleModificationOptionUri": "urn:replicon:project-role-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )
    
    return dag


rail.for_each_instance(create_child_dag)
