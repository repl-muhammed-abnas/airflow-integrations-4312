"""
Process Service Center - GuestTek Talent User Import Child DAG

Creates a single Service Center in Replicon.
"""
import rail

null = None


def create_child_dag(config):
    """Create child DAG for creating a single Service Center."""
    with rail.create_airflow_dag(
        dag_id=config.process_each_service_center_dag_id,
        description='GuestTek Talent User Import - Create Service Center',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_service_center,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        create_service_center = rail.RepliconServiceOperator(
            task_id='create_service_center',
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data=lambda dag_run: {
                "serviceCenter": null,
                "modifications": {
                    "name": dag_run.conf['service_center_name'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": True
                },
                "unitOfWorkId": f"servicecenter_{dag_run.conf['service_center_name']}"
            }
        )
    
    return dag


rail.for_each_instance(create_child_dag)
