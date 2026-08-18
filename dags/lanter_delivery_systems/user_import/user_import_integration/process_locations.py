import uuid
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_locations_dagid,
        description='Lanter Delivery Systems User Import - Process Locations',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_locations,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='create_locationorapply_modification',
            endpoint='/services/LocationService1.svc/CreateLocationOrApplyModification',
            data=lambda dag_run: {
                "location": null,
                "modifications": {
                    "name": dag_run.conf['locationname'],
                    "codeToApply": null,
                    "descriptionToApply":null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
