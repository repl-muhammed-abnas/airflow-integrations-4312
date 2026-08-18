
from datetime import timedelta
from pimco.task_status_update.utils import request_payload
import rail
from airflow.models import Variable

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_task_status_update_child_{config.instance}',
        description=f'PIMCO Task status update child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                            config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_task_heirarchy_or_apply_modifications'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_task_heirarchy_or_apply_modifications',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_task_heirarchy_or_apply_modifications=rail.RepliconServiceOperator(
            task_id='create_task_heirarchy_or_apply_modifications',
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.get_payload_create_task_hierarchy
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_task_heirarchy_or_apply_modifications
        create_task_heirarchy_or_apply_modifications >> finish

    return dag

rail.for_each_instance(create_dag)
