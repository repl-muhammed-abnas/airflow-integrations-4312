from datetime import timedelta
import rail
from airflow.models import Variable

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'kla_disable_costcentre_and_department_child_{config.instance}',
        description=f'KLA Disable Cost Centre and Department Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=10,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        finish = rail.EmptyOperator(task_id='finish')

        can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='').lower() == 'true',
                yes_task='batch_task',
                no_task='if_request_type_contains_department_3'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='if_request_type_contains_department_3',
            end_task='finish',
        )

        if_request_type_contains_department_3=rail.IfOperator(
            task_id='if_request_type_contains_department_3',
            test='''{{ dag_run.conf.type | matches('department') }}''',
            yes_task="disable_dept",
            no_task="disable_cost_center",
        )

        disable_dept=rail.RepliconServiceOperator(
            task_id='disable_dept',
            endpoint="/services/DepartmentService1.svc/Disable",
            data={"departmentUri": "{{ dag_run.conf.uri }}"}
        )

        disable_cost_center=rail.RepliconServiceOperator(
            task_id='disable_cost_center',
            endpoint="/services/CostCenterService1.svc/Disable",
            data={"costCenterUri": "{{ dag_run.conf.uri }}"}
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label('No') >> if_request_type_contains_department_3
        if_request_type_contains_department_3 >> rail.Label('Yes') >> disable_dept >> finish >> catch_and_log_errors
        if_request_type_contains_department_3 >> rail.Label('No') >> disable_cost_center >> finish >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
