from datetime import timedelta
import rail
from airflow.models import Variable
from technicolorg3.user_import.task.process_group_task import process_groups_task_group
from technicolorg3.user_import.utils.python_callable_method import get_group_error_message


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


def create_groups_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_groups_{config.instance}',
        description=f'Technicolor Child_groups update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_groups_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_gmbh_groups_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_gmbh_groups_log',
            end_task='group_error_message',
        )

        create_gmbh_groups_log = rail.CreateLogOperator(
            task_id='create_gmbh_groups_log'
        )

        (get_replicon_cost_center, process_cost_centers_finish) = process_groups_task_group(
            'CostCenter', config.execution_timeout_days, config.instance)

        (get_replicon_department, process_department_finish) = process_groups_task_group(
            'Department', config.execution_timeout_days, config.instance)

        (get_replicon_service_center, process_service_center_finish) = process_groups_task_group(
            'ServiceCenter', config.execution_timeout_days, config.instance)

        (get_replicon_location, process_location_finish) = process_groups_task_group(
            'Location', config.execution_timeout_days, config.instance)

        (get_replicon_division, process_division_finish) = process_groups_task_group(
            'Division', config.execution_timeout_days, config.instance)

        group_error_message = rail.PythonOperator(
            task_id='group_error_message',
            python_callable=get_group_error_message,
            op_args=['gather_costcenter_error', 'gather_department_error',
                     'gather_servicecenter_error', 'gather_location_error',
                     'gather_division_error']
        )

        is_group_error_message = rail.IfOperator(
            task_id='is_group_error_message',
            test="{{ result('group_error_message') | is_truthy }}",
            yes_task='fail_group_with_error',
            no_task='finish'
        )

        fail_group_with_error = rail.FailOperator(
            task_id='fail_group_with_error',
            message="{{ result('group_error_message') }}"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> group_error_message

        can_run_batch_task >> rail.Label(
            'No') >> create_gmbh_groups_log

        create_gmbh_groups_log >> get_replicon_cost_center

        process_cost_centers_finish >> get_replicon_department

        process_department_finish >> get_replicon_service_center

        process_service_center_finish >> get_replicon_location

        process_location_finish >> get_replicon_division

        process_division_finish >> group_error_message >> is_group_error_message >> rail.Label(
            'Yes') >> fail_group_with_error

        is_group_error_message >> rail.Label(
            'No') >> finish

        return dag


rail.for_each_instance(create_groups_child_dag)
