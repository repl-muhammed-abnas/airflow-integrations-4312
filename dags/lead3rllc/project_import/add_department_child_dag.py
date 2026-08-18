from datetime import timedelta
from airflow.models import Variable
import rail
from lead3rllc.project_import.utils.request_payload import create_department_group_payload


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_department_group_dag_id,
        description='LEAD3R LLC Project Import - Add Department Group Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_department_group'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_department_group',
            end_task='catch_and_log_error',
        )

        create_department_group = rail.RepliconServiceOperator(
            task_id='create_department_group',
            endpoint='/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=create_department_group_payload
        )

        success_log_entry_create_department_group = rail.WriteLogOperator(
            task_id='success_log_entry_create_department_group',
            log="{{dag_run.conf.missing_field_value_import_logs}}",
            message='na',
            severity='Success',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "action": "add_department_group",
                "status": "Success",
                "details": "Department Group : {{dag_run.conf.department_group_name}} is added successfully"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.missing_field_value_import_logs}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "action": dag_run.conf['operation'] + " : " + dag_run.conf['department_group_name'],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> create_department_group

        create_department_group >> success_log_entry_create_department_group >> catch_and_log_error

    return dag


rail.for_each_instance(create_child_dag)
