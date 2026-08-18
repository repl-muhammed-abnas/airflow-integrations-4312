"""
Validate Mappings child DAG for VP Xero Mapping Sync (Phase 5).

Read-only referential-integrity checks (doc 07-validation.md) on the three
mapping tables populated by the upstream map_*_dag chain. Wired into
dispatcher_dag.py AFTER trigger_map_tax_code and BEFORE the gather chain, so a
hard_fail keeps the per-customer init Variable at 'false' and the next dispatcher
run retries. On hard_fail each affected step's `mapping_table_state.Status` is
set to 'Error'.

Each map_* table is anti-joined against freshly-fetched Xero + VP data:
  - map_firm:               ContactID → live Xero contact; FirmID → live VP firm
  - map_chart_of_accounts:  VantagepointCode → live VP account; XeroID → live Xero account
  - map_tax_code:           VantagepointCode → live VP tax code; (RateName, ComponentName) → live ACTIVE Xero component

Validation is read-only/reporting (Q5): self-heal + archived cleanup live in the
sync engines, not here.

Flow:
    (6 source fetches: Xero contacts/accounts/tax-rates + VP firms/accounts/tax-codes)
       → can_run_batch_task  (Variable gate; default true)
            ├─ Yes → batch_task (run_all → summarize)
            └─ No  → run_all_mapping_validations → summarize_mapping_validations
       catch_validate_mappings_dag_error (one_failed)
"""
from datetime import timedelta
from airflow.models import Variable
import rail

from vp_xero_integration.mapping_sync.config import IntegrationConfig
from vp_xero_integration.mapping_sync.utils.python_callable_method import (
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
            'Phase-5 validation: referential-integrity checks of map_firm / '
            'map_chart_of_accounts / map_tax_code against live Xero + VP data. '
            'Hard-fails the dispatcher (keeping init=false) and marks the '
            "step Status='Error' on any dangling reference or empty table."
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_xero', 'mapping_sync', 'validate_mappings'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ---- Source fetches (outside the batch range — their results feed
        # run_all_mapping_validations via rail.result). ----
        xero_conn = "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
        vp_conn = "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"

        validate_fetch_xero_contacts = rail.XeroContactOperator(
            task_id='validate_fetch_xero_contacts',
            xero_conn_id=xero_conn,
            operation='search',
            include_archived=True,
            paginate=True,
        )
        validate_fetch_vp_firms = rail.VantagepointFirmOperator(
            task_id='validate_fetch_vp_firms',
            vp_conn_id=vp_conn,
            request_method='GET',
            pagination=True,
        )
        validate_fetch_xero_accounts = rail.XeroAccountOperator(
            task_id='validate_fetch_xero_accounts',
            xero_conn_id=xero_conn,
            operation='list',
        )
        validate_fetch_vp_accounts = rail.VantagepointChartOfAccountsOperator(
            task_id='validate_fetch_vp_accounts',
            vp_conn_id=vp_conn,
            request_method='GET',
            pagination=True,
        )
        validate_fetch_xero_tax_rates = rail.XeroTaxRateOperator(
            task_id='validate_fetch_xero_tax_rates',
            xero_conn_id=xero_conn,
            operation='list',
        )
        validate_fetch_vp_tax_codes = rail.VantagepointTaxCodesOperator(
            task_id='validate_fetch_vp_tax_codes',
            vp_conn_id=vp_conn,
            request_method='GET',
            pagination=True,
        )

        # ---- Batch-task gate (perf opt-out) ----
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

        # Fetches run first, then the batch gate.
        (
            validate_fetch_xero_contacts >> validate_fetch_vp_firms >>
            validate_fetch_xero_accounts >> validate_fetch_vp_accounts >>
            validate_fetch_xero_tax_rates >> validate_fetch_vp_tax_codes >>
            can_run_batch_task
        )

        # Batch path
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_validate_mappings_dag_error
        # Non-batch path
        can_run_batch_task >> rail.Label(
            'No') >> run_all >> summarize >> catch_validate_mappings_dag_error

        return dag


rail.for_each_instance(create_dag)
