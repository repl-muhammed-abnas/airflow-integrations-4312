import uuid
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_departments,
        description='Cohnreznick User Sync - Process Department',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_departments,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='create_departmentorapply_modification',
            endpoint='/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=lambda dag_run: {
                "departmentGroup": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": 'CohnReznick',
                    },
                },
                "modifications": {
                    "name": dag_run.conf['departmentname'],
                    "codeToApply": {
                    "value": dag_run.conf['departmentcode']
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
