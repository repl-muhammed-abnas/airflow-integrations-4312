import uuid
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_employee_types_dagid,
        description='IBIS User Import - Process Employee Types Group',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_employee_type,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='create_employeetypeorapply_modification',
            endpoint='services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification',
            data=lambda dag_run: {
                "employeeTypeGroup": null,
                "modifications": {
                    "name": dag_run.conf['employee_type'],
                    "codeToApply":null,
                    "descriptionToApply":null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
