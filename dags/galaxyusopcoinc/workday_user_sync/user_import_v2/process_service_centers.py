from uuid import uuid4
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils.custom_methods import get_service_center_name


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.service_center_dag_id,
        description=f'VialtoPartners_User_Import_process service center add/update V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_groups_child,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_run_for_update = rail.IfOperator(
            task_id="is_run_for_update",
            test="{{ dag_run.conf.action == 'update'}}",
            yes_task="update_service_center_name",
            no_task="add_service_center"
        )

        update_service_center_name = rail.RepliconServiceOperator(
            task_id="update_service_center_name",
            endpoint="/services/ServiceCenterService1.svc/UpdateName",
            data=lambda dag_run: {
                "serviceCenterUri": dag_run.conf['uri'],
                "name": get_service_center_name(dag_run)
            }
        )

        add_service_center = rail.RepliconServiceOperator(
            task_id="add_service_center",
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data=lambda dag_run: {
                "serviceCenter": None,
                "modifications": {
                    "name": get_service_center_name(dag_run),
                    "codeToApply": {
                        "value": dag_run.conf['code']
                    },
                    "descriptionToApply": {
                        "value": dag_run.conf['description']
                    },
                    "isEnabled": 1
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        is_run_for_update >> rail.Label("Yes") >> update_service_center_name
        is_run_for_update >> rail.Label("No") >> add_service_center

    return dag


rail.for_each_instance(create_dag)
