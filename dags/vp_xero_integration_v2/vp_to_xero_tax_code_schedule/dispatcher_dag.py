# dags/vp_xero_integration_v2/vp_to_xero_tax_code_schedule/dispatcher_dag.py
"""Dispatcher DAG for Xero -> VP Tax Code Schedule (V2 IPA GitSync architecture).

Per-tenant: on schedule, triggers the processor DAG once (passing connections
and customerId from the instance config), waits, gathers errors.

V2 changes from V1:
  - schedule_interval from config.schedule_interval (not None)
  - connections/customerId injected into processor conf from config (not dag_run.conf)
  - middleware_api_base_url via Jinja var.value.get (not parse-time Variable.get)
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_xero_integration_v2.common.python_callable_method import get_connections


def create_dag(config):
    """Per-tenant dispatcher: trigger one tax-code sync processor, gather errors."""
    connections = get_connections(config)
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_vp_to_xero_tax_code_schedule_v2_dispatcher_{config.instance}',
        description='Per-customer Xero -> VP Tax Code sync on schedule',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_xero', 'vp_to_xero_tax_code_schedule', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        def build_processor_conf(_item):
            return {
                'connections': connections,
                'customerId': config.customer_id,
            }

        trigger_tax_code_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_tax_code_sync',
            items=lambda: [{}],
            trigger_dag_id=(
                f'vp_xero_vp_to_xero_tax_code_schedule_v2_processor_{config.instance}'
            ),
            conf=build_processor_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_tax_code_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_tax_code_sync',
            dag_runs="{{ result('trigger_tax_code_sync') }}",
            allowed_states=['success', 'failed', 'upstream_failed', 'removed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_tax_code_sync_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_tax_code_sync_errors',
            dag_runs="{{ result('trigger_tax_code_sync') }}",
            dagrun_task_id='catch_tax_code_sync_error',
            flatten=True,
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: bool(
                [e for e in (rail.result('gather_tax_code_sync_errors') or []) if e]
            ),
            yes_task='fail_vp_to_xero_tax_code_schedule',
            no_task='log_sync_complete',
        )

        fail_vp_to_xero_tax_code_schedule = rail.FailOperator(
            task_id='fail_vp_to_xero_tax_code_schedule',
            message=(
                "{{ result('gather_tax_code_sync_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            ),
        )

        log_sync_complete = rail.PythonOperator(
            task_id='log_sync_complete',
            python_callable=lambda: None,
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url="{{ var.value.get('middleware_api_base_url', '') }}",
            trigger_rule='all_done',
        )

        (
            trigger_tax_code_sync >> wait_for_tax_code_sync >>
            gather_tax_code_sync_errors >> has_sync_errors
        )
        has_sync_errors >> rail.Label('Yes') >> fail_vp_to_xero_tax_code_schedule
        has_sync_errors >> rail.Label('No') >> log_sync_complete
        fail_vp_to_xero_tax_code_schedule >> post_dag_run_details
        log_sync_complete >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
