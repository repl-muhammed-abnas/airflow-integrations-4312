"""
Router DAG for VP PSA -> QBO Posted AR Invoice Sync.

Per-batch: reads CFG_Region for the tenant and triggers exactly one of the
two regional create DAGs:
  - vp_qbo_ar_invoice_sync_create_us_{instance}    (US tenants)
  - vp_qbo_ar_invoice_sync_create_ca_uk_{instance} (CA / UK / GB tenants)

Waits for the triggered create DAG to finish, gathers its errors via
catch_create_dag_error, and surfaces them to the dispatcher through
catch_router_dag_error.

Triggered by the dispatcher DAG with:
  dag_run.conf.Batch       — VP AR invoice batch number
  dag_run.conf.PostDate    — batch post date (informational)
  dag_run.conf.connections — {vantagepoint, intuit} connection ids
  dag_run.conf.customerId  — tenant id

Replaces the runtime check_region IfOperator that was previously embedded
inside the single ar_invoice_create_dag.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.posted_ar_invoice_sync.utils.python_callable_method import (  # noqa: E501
    fetch_region_config_method,
    is_ca_uk_region_method,
    resolve_triggered_runs_method,
    capture_router_dag_error,
)


def create_dag(config):
    """Per-batch router: read region, trigger regional create DAG, gather errors."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_ar_invoice_sync_router_{config.instance}',
        description=(
            'Route VP PSA AR invoice batch to the US or CA/UK create DAG '
            'based on CFG_Region Airflow Variable.'
        ),
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks',
            'ar_invoice_sync',
            'router',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            ),
        }
    ) as dag:

        # ------------------------------------------------------------------ #
        # Phase 1: Read region config
        # ------------------------------------------------------------------ #

        fetch_region_config = rail.PythonOperator(
            task_id='fetch_region_config',
            python_callable=lambda: fetch_region_config_method(config.instance)
        )

        # ------------------------------------------------------------------ #
        # Phase 2: Route to the correct regional create DAG
        # ------------------------------------------------------------------ #

        check_region = rail.IfOperator(
            task_id='check_region',
            test=is_ca_uk_region_method,
            yes_task='trigger_ca_uk_create',
            no_task='trigger_us_create'
        )

        def _pass_through_conf(item):  # noqa: E306
            ctx = rail.get_current_context()
            conf = ctx['dag_run'].conf
            return {
                'Batch': conf.get('Batch'),
                'PostDate': conf.get('PostDate'),
                'connections': conf.get('connections'),
                'customerId': conf.get('customerId'),
                'config': conf.get('config', {}),
            }

        trigger_us_create = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_us_create',
            items=lambda: [{}],
            trigger_dag_id=(
                f'vp_qbo_ar_invoice_sync_create_us_{config.instance}'
            ),
            conf=_pass_through_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        trigger_ca_uk_create = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_ca_uk_create',
            items=lambda: [{}],
            trigger_dag_id=(
                f'vp_qbo_ar_invoice_sync_create_ca_uk_{config.instance}'
            ),
            conf=_pass_through_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        # ------------------------------------------------------------------ #
        # Phase 3: Wait for the triggered create DAG and gather its errors
        # ------------------------------------------------------------------ #

        resolve_triggered_runs = rail.PythonOperator(
            task_id='resolve_triggered_runs',
            trigger_rule='none_failed_min_one_success',
            python_callable=resolve_triggered_runs_method
        )

        wait_for_create_dag_run = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_dag_run',
            dag_runs="{{ result('resolve_triggered_runs') }}",
            allowed_states=[
                'success', 'failed', 'upstream_failed', 'removed'
            ],
            failed_states=[],
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        gather_create_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_create_dag_errors',
            dag_runs="{{ result('resolve_triggered_runs') }}",
            dagrun_task_id='catch_create_dag_error',
            flatten=True
        )

        # ------------------------------------------------------------------ #
        # Error capture — always runs (trigger_rule='all_done') so the
        # dispatcher's GatherResultsFromDagRunsOperator can collect failures.
        # Returns None on a clean run.
        # ------------------------------------------------------------------ #

        catch_router_dag_error = rail.PythonOperator(
            task_id='catch_router_dag_error',
            trigger_rule='all_done',
            python_callable=capture_router_dag_error,
            op_args=[
                '{{ dag_run.conf.Batch }}',
                '{{ get_error_message() }}'
            ]
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #

        fetch_region_config >> check_region
        check_region >> rail.Label('US') >> trigger_us_create >> resolve_triggered_runs
        check_region >> rail.Label('CA/UK') >> trigger_ca_uk_create >> resolve_triggered_runs

        (
            resolve_triggered_runs >>
            wait_for_create_dag_run >>
            gather_create_dag_errors
        )

        fetch_region_config >> catch_router_dag_error
        gather_create_dag_errors >> catch_router_dag_error

        return dag


rail.for_each_instance(create_dag)
