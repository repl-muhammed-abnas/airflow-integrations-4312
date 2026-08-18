import uuid
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import_v3/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_user_import_location_child_{config.instance}_v3',
        description=f'PwCGlobal_User_Import_location(Countries)_location add V3.0 {config.instance}',
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
            data={
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.country }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()) + "{{ dag_run.conf.country }}",
            }
        )

    return dag


rail.for_each_instance(create_dag)
