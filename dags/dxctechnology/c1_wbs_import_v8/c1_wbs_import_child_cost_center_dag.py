import rail
from dxctechnology.c1_wbs_import_v8.utils import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import_v8/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id_cost_center,
        description=f'DXC_C1 WBS Child_Cost Center add {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.cost_center_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        cost_center = "{{ dag_run.conf.cost_center }}"
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_cost_center = rail.RepliconServiceOperator(
            task_id='create_cost_center',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterOrApplyModification',
            data=request_payload.get_cost_center_create_param(cost_center)
        )

        finish = rail.EmptyOperator(
            task_id='finish')

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'cost_center': cost_center,
                'status': 'Error',
            },
        )

        create_cost_center >> finish >> catch_and_log_errors
    return dag


rail.for_each_instance(create_child_dag)
