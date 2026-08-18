from ipipeline.user_import.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null = None
true = True

def create_department_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_departments_child_dag_id,
        description=f"iPipeline User Import Create Departments Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_departments_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_department_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_department_hierarchy"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_department_hierarchy",
            end_task="catch_and_log_errors"
        )

        create_department_hierarchy = rail.RepliconServiceOperator(
            task_id="create_department_hierarchy",
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupHierarchyOrApplyModifications",
            data=request_payload.get_departments_hierarchy_payload
        )

        department_end = rail.EmptyOperator(task_id="department_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Department creation failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Department creation failed - " + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_department_hierarchy >> department_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_department_child_dag)
