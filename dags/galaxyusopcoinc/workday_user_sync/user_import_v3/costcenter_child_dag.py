import uuid
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils.custom_methods import get_cost_center_name


null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.costcenter_dag_id,
        description=f'VialtoPartners_User_Import_costcenter add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.costcenter_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_run_for_update = rail.IfOperator(
            task_id="is_run_for_update",
            test="{{ dag_run.conf.action == 'update'}}",
            yes_task="update_cost_center_name",
            no_task="add_cost_center"
        )

        update_cost_center_name = rail.RepliconServiceOperator(
            task_id="update_cost_center_name",
            endpoint="/services/CostCenterService1.svc/UpdateName",
            data=lambda dag_run: {
                "costCenterUri": dag_run.conf['cost_center_uri'],
                "name": get_cost_center_name(dag_run)
            }
        )

        add_cost_center = rail.RepliconServiceOperator(
            task_id='add_cost_center',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterOrApplyModification',
            data=lambda dag_run: {
                "costCenter": null,
                "modifications": {
                    "name": get_cost_center_name(dag_run),
                    "codeToApply": {
                        "value": dag_run.conf['cost_center_code'],
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        )

        is_run_for_update >> rail.Label("Yes") >> update_cost_center_name
        is_run_for_update >> rail.Label("No") >> add_cost_center
    return dag


rail.for_each_instance(create_dag)
