import uuid
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_department_dagid,
        description='CRL User Import Ireland- Process Department',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_new_departments,
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
                        "name": 'Charles River Laboratories, Inc.',
                    },
                },
                "modifications": {
                    "name": dag_run.conf['department_name'],
                    "codeToApply": {
                    "value": dag_run.conf['department_code']
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
