import uuid
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.department_dag_id,
        description=f'VialtoPartners_User_Import_department add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_groups_child,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        null = None
        rail.RepliconServiceOperator(
            task_id='create_departmentorapply_modification',
            endpoint='/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=lambda: {
                "departmentGroup": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": request_payload.get_conf()['root'] if request_payload.get_conf()['Parent'] == "Yes" else null,
                        "uri": request_payload.get_conf()['JobFamilyGroup'] if request_payload.get_conf()['Parent'] == "No" else null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": request_payload.department_name(),
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

    return dag


rail.for_each_instance(create_dag)
