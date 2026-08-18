"""
Router DAG for VP QBO Employee Sync.
Looks up the employee map (the shared mapping_sync `map_employee` S3 collection)
by QBOID and routes to either the employee_create or employee_update child DAG.
"""
from datetime import timedelta
import rail
from vp_quickbooks_integration.employee_sync.utils.python_callable_method import (
    lookup_employee_by_qboid,
    check_employee_exists_in_lookup,
    build_employee_conf,
    collect_triggered_dagrun_ids,
    capture_router_dag_error
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """
    Create router DAG for VP QBO Employee Sync.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_employee_sync_router_{config.instance}',
        description=(
            'Route QBO employee to create or update flow in Vantagepoint'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'employee_sync', 'router'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        get_employee_from_lookup = rail.PythonOperator(
            task_id='get_employee_from_lookup',
            python_callable=lookup_employee_by_qboid
        )

        is_employee_exist_in_lookup = rail.IfOperator(
            task_id='is_employee_exist_in_lookup',
            test=check_employee_exists_in_lookup,
            yes_task='trigger_employee_update',
            no_task='trigger_employee_create'
        )

        trigger_employee_create = rail.TriggerDagRunOperator(
            task_id='trigger_employee_create',
            retries=0,
            trigger_dag_id=(
                f'vp_qbo_employee_sync_create_{config.instance}'
            ),
            conf=lambda: build_employee_conf('create'),
            wait_for_completion=True,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        trigger_employee_update = rail.TriggerDagRunOperator(
            task_id='trigger_employee_update',
            retries=0,
            trigger_dag_id=(
                f'vp_qbo_employee_sync_update_{config.instance}'
            ),
            conf=lambda: build_employee_conf('update'),
            wait_for_completion=True,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        collect_triggered_dagrun_id = rail.PythonOperator(
            task_id='collect_triggered_dagrun_id',
            trigger_rule='all_done',
            python_callable=collect_triggered_dagrun_ids
        )

        gather_employee_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_employee_dag_errors',
            dag_runs="{{ result('collect_triggered_dagrun_id') }}",
            dagrun_task_id='catch_employee_dag_error',
            flatten=True
        )

        catch_router_dag_error = rail.PythonOperator(
            task_id='catch_router_dag_error',
            trigger_rule='all_done',
            python_callable=capture_router_dag_error,
            op_args=[
                '{{ dag_run.conf.Id }}',
                "{{ dag_run.conf.get('DisplayName') or '' }}",
                '{{ get_error_message() }}'
            ]
        )

        get_employee_from_lookup >> is_employee_exist_in_lookup
        (
            is_employee_exist_in_lookup >>
            rail.Label('Employee exists in lookup') >>
            trigger_employee_update
        )
        (
            is_employee_exist_in_lookup >>
            rail.Label('Employee not found in lookup') >>
            trigger_employee_create
        )

        trigger_employee_create >> collect_triggered_dagrun_id
        trigger_employee_update >> collect_triggered_dagrun_id
        (
            collect_triggered_dagrun_id >>
            gather_employee_dag_errors >>
            catch_router_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
