import uuid
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_division_dagid,
        description='T-Systems - Process New Activity Type',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_new_divisions,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='create_departmentorapply_modification',
            endpoint='/services/DivisionService1.svc/CreateDivisionOrApplyModification',
            data=lambda dag_run: {
                "division": null,
                "modifications": {
                    "name": dag_run.conf['division_name'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
