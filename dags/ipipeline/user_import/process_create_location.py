from ipipeline.user_import.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null = None
true = True

def create_location_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_locations_child_dag_id,
        description=f"iPipeline User Import Create Locations Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_locations_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_location_hierarchy"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_location_hierarchy",
            end_task="catch_and_log_errors"
        )


        create_location_hierarchy = rail.RepliconServiceOperator(
            task_id="create_location_hierarchy",
            endpoint="/services/LocationService1.svc/CreateLocationHierarchyOrApplyModifications",
            data=request_payload.get_locations_hierarchy_payload
        )

        location_end = rail.EmptyOperator(task_id="location_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Location creation failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Location creation failed - " + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_location_hierarchy >> location_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_location_child_dag)
