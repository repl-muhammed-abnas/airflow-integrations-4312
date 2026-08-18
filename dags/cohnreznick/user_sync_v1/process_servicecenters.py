import uuid
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_servicecenters,
        description='Cohnreznick User Sync - Process Servicecenters',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_servicecenters,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id="create_new_division",
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data=lambda dag_run: {
                "serviceCenter": null,
                "modifications": {
                    "name": dag_run.conf['servicecentername'],
                    "codeToApply": {
                        "value": dag_run.conf['servicecentercode']
                        },
                    "isEnabled": 1
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
