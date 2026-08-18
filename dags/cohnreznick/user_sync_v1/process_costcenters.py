import uuid
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_costcenters,
        description='Cohnreznick User Sync - Process Costcenter',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_costcenters,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='add_cost_center',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterOrApplyModification',
            data= lambda dag_run:{
                "costCenter": null,
                "modifications": {
                    "name": dag_run.conf['costcentername'],
                    "codeToApply": {
                        "value": dag_run.conf['costcentercode'],
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
