import uuid
import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.location_dag_id,
        description=f'VialtoPartners_User_Import_location add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.location_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        null = None
        rail.RepliconServiceOperator(
            task_id='create_locationorapply_modification',
            endpoint='/services/LocationService1.svc/CreateLocationOrApplyModification',
            data=lambda dag_run: {
                "location": {
                    "parent": {
                        "name": dag_run.conf['parent_location_name']
                    } if dag_run.conf['is_parent_location'] == "No" else null
                },
                "modifications": {
                    "name": dag_run.conf['location_name'],
                    "codeToApply": {
                        "value": dag_run.conf['location_name'],
                    },
                    "descriptionToApply": {
                        "value": dag_run.conf['parent_location_name']
                    },
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

    return dag


rail.for_each_instance(create_dag)
