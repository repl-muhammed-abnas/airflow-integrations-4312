"""
Validate Mappings child DAG for VP QBO Mapping Sync (Phase 5).

Runs read-only checks against the four mapping tables populated by the
upstream map_*_dag chain. Wired into dispatcher_dag.py AFTER
trigger_map_tax_code and BEFORE the gather/has_sync_errors chain, so a
hard_fail keeps the per-customer init Variable at 'false' and the next
dispatcher run retries the whole population. On hard_fail each
affected step's `mapping_table_state.Status` is set to 'Error' (Workato
`validate_mapping_tables` recipe parity).

Checks (severity model):
  - 'hard_fail' raises in summarize_mapping_validations and bubbles up
    through dispatcher's existing error path:
      * empty mapping table
      * cross-reference fields missing (e.g. map row with no QBOID, no
        Vantagepoint code, no QBOVendorID on map_employee)
  - 'warn' is logged but does NOT fail the run:
      * map_account_code has no row with col3 = 'Accounts Payable' (the
        critical-account check from Phase 5)
      * map_tax_code Rate column has rows that don't parse as a float
      * map_firm has rows where Is Vendor ∉ {'Y','N'}

Flow:
    can_run_batch_task               (Variable gate; default true)
       ├─ Yes → batch_task           (BatchTaskRunOperator wrapping the
       │                              run_all → summarize chain)
       └─ No  → run_all_mapping_validations
                  → summarize_mapping_validations (raises on hard_fail)
                  → catch_validate_mappings_dag_error (one_failed)

All 4 mapping tables are validated inside ONE
`open_mapping_collection(read_only=True)` block (P3 collapse — saves 3
redundant S3 GetObject + gunzip pairs vs the earlier per-table fan-out
shape).
"""
from datetime import timedelta
from airflow.models import Variable
import rail

from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
from vp_quickbooks_integration.mapping_sync.utils.python_callable_method import (
    capture_dag_error,
    run_all_mapping_validations,
    summarize_mapping_validations,
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Per-instance validate_mappings child DAG."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('validate_mappings', config.instance),
        description=(
            'Phase-5 validation: checks completeness + cross-reference '
            'integrity of map_firm / map_employee / map_account_code / '
            'map_tax_code. Hard-fails the dispatcher (keeping init=false) '
            'if any missing-key issue is found; logs warnings for '
            'business-rule misses (e.g. no AP Liability mapped).'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_quickbooks', 'mapping_sync', 'validate_mappings'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ---- Batch-task gate (perf opt-out) ----
        # See map_firm_dag for the rationale on the shared Variable.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                IntegrationConfig.CAN_RUN_BATCH_VARIABLE_NAME,
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='run_all_mapping_validations',
        )

        run_all = rail.PythonOperator(
            task_id='run_all_mapping_validations',
            python_callable=run_all_mapping_validations,
        )

        summarize = rail.PythonOperator(
            task_id='summarize_mapping_validations',
            python_callable=summarize_mapping_validations,
        )

        catch_validate_mappings_dag_error = rail.PythonOperator(
            task_id='catch_validate_mappings_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_dag_error,
            op_args=[
                'validate_mappings',
                "{{ dag_run.conf.get('customerId') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='run_all_mapping_validations',
            end_task='catch_validate_mappings_dag_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Batch path
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_validate_mappings_dag_error
        # Non-batch path: linear chain ending at the catch task. The
        # trigger_rule='one_failed' on catch_validate_mappings_dag_error
        # picks up either validator failures or summarize's hard_fail
        # RuntimeError.
        can_run_batch_task >> rail.Label(
            'No') >> run_all >> summarize >> catch_validate_mappings_dag_error

        return dag


rail.for_each_instance(create_dag)
