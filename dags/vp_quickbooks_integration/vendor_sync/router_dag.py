"""
Router DAG for VP QBO Vendor Sync.
Looks up the firm map (Variable-backed lookup; future Airflow lookup table)
by QBOID and routes to either the vendor_create or vendor_update child DAG.
"""
from datetime import timedelta
import rail
from vp_quickbooks_integration.vendor_sync.utils.python_callable_method import (
    lookup_firm_by_qboid,
    check_firm_exists_in_lookup,
    build_vendor_conf,
    collect_triggered_dagrun_ids,
    capture_router_dag_error
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """
    Create router DAG for VP QBO Vendor Sync.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_vendor_sync_router_{config.instance}',
        description=(
            'Route QBO vendor to create or update flow in Vantagepoint'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'vendor_sync', 'router'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        get_firm_from_lookup = rail.PythonOperator(
            task_id='get_firm_from_lookup',
            python_callable=lookup_firm_by_qboid
        )

        is_firm_exist_in_lookup = rail.IfOperator(
            task_id='is_firm_exist_in_lookup',
            test=check_firm_exists_in_lookup,
            yes_task='trigger_vendor_update',
            no_task='trigger_vendor_create'
        )

        trigger_vendor_create = rail.TriggerDagRunOperator(
            task_id='trigger_vendor_create',
            retries=0,
            trigger_dag_id=(
                f'vp_qbo_vendor_sync_create_{config.instance}'
            ),
            conf=lambda: build_vendor_conf('create'),
            wait_for_completion=True,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        trigger_vendor_update = rail.TriggerDagRunOperator(
            task_id='trigger_vendor_update',
            retries=0,
            trigger_dag_id=(
                f'vp_qbo_vendor_sync_update_{config.instance}'
            ),
            conf=lambda: build_vendor_conf('update'),
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

        gather_vendor_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_vendor_dag_errors',
            dag_runs="{{ result('collect_triggered_dagrun_id') }}",
            dagrun_task_id='catch_vendor_dag_error',
            flatten=True
        )

        catch_router_dag_error = rail.PythonOperator(
            task_id='catch_router_dag_error',
            trigger_rule='all_done',
            python_callable=capture_router_dag_error,
            op_args=[
                '{{ dag_run.conf.Id }}',
                (
                    "{{ dag_run.conf.get('CompanyName') or "
                    "dag_run.conf.get('DisplayName') or '' }}"
                ),
                '{{ get_error_message() }}'
            ]
        )

        get_firm_from_lookup >> is_firm_exist_in_lookup
        (
            is_firm_exist_in_lookup >>
            rail.Label('Firm exists in lookup') >>
            trigger_vendor_update
        )
        (
            is_firm_exist_in_lookup >>
            rail.Label('Firm not found in lookup') >>
            trigger_vendor_create
        )

        trigger_vendor_create >> collect_triggered_dagrun_id
        trigger_vendor_update >> collect_triggered_dagrun_id
        (
            collect_triggered_dagrun_id >>
            gather_vendor_dag_errors >>
            catch_router_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
