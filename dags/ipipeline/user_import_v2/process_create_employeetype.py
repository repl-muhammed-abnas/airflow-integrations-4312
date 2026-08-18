from ipipeline.user_import_v2.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null = None
true = True


def create_employeetype_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_employeetypes_child_dag_id,
        description=f"iPipeline User Import Create Employee Types Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_employeetypes_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_employeetype_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_employee_type_hierarchy"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_employee_type_hierarchy",
            end_task="catch_and_log_errors"
        )

        create_employee_type_hierarchy = rail.RepliconServiceOperator(
            task_id="create_employee_type_hierarchy",
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupHierarchyOrApplyModifications",
            data=request_payload.get_employee_types_hierarchy_payload
        )

        employeetype_end = rail.EmptyOperator(task_id="employeetype_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Employee Type creation failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Employee Type creation failed - " + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> create_employee_type_hierarchy >> employeetype_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_employeetype_child_dag)
