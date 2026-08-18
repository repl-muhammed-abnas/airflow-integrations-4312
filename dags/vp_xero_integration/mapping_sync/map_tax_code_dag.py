"""
Map Tax Code child DAG for VP Xero Mapping Sync.

Direction: Xero TaxRates → VP Tax Codes (Xero is master). Operator-driven port
of Workato `014_501_psa_sync_tax_codes` (GL worker) with the `Map Tax Codes`
seeder folded in (Option A — see reverse-engineering docs 03 + 06).

Xero models tax as TaxRates with nested TaxComponents[]. The engine flattens
each ACTIVE rate into one row per component (fan-out — one Xero rate → several VP
tax codes), generates `X####` VP codes for net-new components, creates/updates
VP tax codes, and links COMPOUND components to their base component's VP code via
`CompoundOnTaxCode` in a deferred second pass. There is no QBO-style tax-group
step (Xero has no group construct here).

Writes into the `map_tax_code` S3 collection (UNIQUE (XeroName, XeroCode)).
Columns: XeroName, XeroCode, VantagepointCode, Rate, CompoundOnCode, Sequence,
Messages.

Flow:
    check_map_tax_code_step_complete / check_map_tax_code_populated
      → is_map_tax_code_populated
       ├─ Yes → skip_populate_map_tax_code
       └─ No  → fetch_xero_tax_rates    (XeroTaxRateOperator list)
              → fetch_vp_tax_codes      (VantagepointTaxCodesOperator GET)
              → create_xero_tax_components (CreateCollectionOperator; flattened)
              → create_vp_tax_codes     (CreateCollectionOperator)
              → create_tax_code_map     (CreateCollectionOperator)
              → query_compiled_tax_codes (QueryCollectionOperator JOIN)
              → process_xero_tax_codes  (PythonOperator: two-pass engine)
              → mark_map_tax_code_step_complete
       catch_map_tax_code_dag_error (one_failed; returns dict)
"""
from vp_xero_integration.common.tables import (
    MAP_TAX_CODE_TABLE_NAME,
    MAPPING_STEP_TAX_CODE,
)
from vp_xero_integration.mapping_sync.utils.python_callable_method import (
    is_table_populated,
    capture_dag_error,
    check_step_status,
    mark_step_status,
    sync_xero_tax_codes_to_vp,
    build_xero_tax_rates_staging,
    prepare_vp_tax_codes_staging,
    read_map_tax_code_for_staging,
    COMPILE_TAX_CODES_SQL,
)
from vp_xero_integration.mapping_sync.utils._tax_code_sync import (
    XERO_TAX_COMPONENTS_COLLECTION,
    VP_TAX_CODES_COLLECTION,
    TAX_CODE_MAP_COLLECTION,
    COMPILED_TAX_CODES_COLLECTION,
    XERO_TAX_COMPONENTS_STAGING_COLUMNS,
    VP_TAX_CODES_STAGING_COLUMNS,
    TAX_CODE_MAP_STAGING_COLUMNS,
)
from vp_xero_integration.mapping_sync.config import IntegrationConfig
import logging
from datetime import timedelta
from airflow.models import Variable
import rail

_log = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Per-instance map_tax_code child DAG."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('map_tax_code', config.instance),
        description=(
            'Sync Xero tax rates (and nested components) to VP tax codes with '
            'compound linking. Writes the map_tax_code cross-reference table.'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_xero', 'mapping_sync', 'map_tax_code'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ---- Batch-task gate (perf opt-out) ----
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                IntegrationConfig.CAN_RUN_BATCH_VARIABLE_NAME,
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='is_map_tax_code_populated',
        )

        # ---- Skip gates (layered) ----
        check_step_complete = rail.PythonOperator(
            task_id='check_map_tax_code_step_complete',
            python_callable=lambda: check_step_status(MAPPING_STEP_TAX_CODE),
        )

        check_populated = rail.PythonOperator(
            task_id='check_map_tax_code_populated',
            python_callable=lambda: is_table_populated(MAP_TAX_CODE_TABLE_NAME),
        )

        is_populated = rail.IfOperator(
            task_id='is_map_tax_code_populated',
            test=lambda: (
                rail.result('check_map_tax_code_step_complete') or
                rail.result('check_map_tax_code_populated')
            ),
            yes_task='skip_populate_map_tax_code',
            no_task='fetch_xero_tax_rates',
        )

        skip_populate = rail.PythonOperator(
            task_id='skip_populate_map_tax_code',
            python_callable=lambda: _log.info(
                'map_tax_code already populated for this customer — skipping'
            ),
        )

        # ---- Mark step Complete on successful population ----
        mark_step_complete = rail.PythonOperator(
            task_id='mark_map_tax_code_step_complete',
            python_callable=lambda: mark_step_status(
                MAPPING_STEP_TAX_CODE, 'Complete'
            ),
        )

        # ---- Source: Xero tax rates (list; engine flattens to components) ----
        fetch_xero_tax_rates = rail.XeroTaxRateOperator(
            task_id='fetch_xero_tax_rates',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='list',
        )

        # ---- List Tax codes in Vantagepoint ----
        fetch_vp_tax_codes = rail.VantagepointTaxCodesOperator(
            task_id='fetch_vp_tax_codes',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            pagination=True,
        )

        # ---- Stage collections + run the compile JOIN ----
        create_xero_tax_components = rail.CreateCollectionOperator(
            task_id='create_xero_tax_components',
            name=XERO_TAX_COMPONENTS_COLLECTION,
            source=build_xero_tax_rates_staging,
            columns=XERO_TAX_COMPONENTS_STAGING_COLUMNS,
        )

        create_vp_tax_codes = rail.CreateCollectionOperator(
            task_id='create_vp_tax_codes',
            name=VP_TAX_CODES_COLLECTION,
            source=prepare_vp_tax_codes_staging,
            columns=VP_TAX_CODES_STAGING_COLUMNS,
        )

        create_tax_code_map = rail.CreateCollectionOperator(
            task_id='create_tax_code_map',
            name=TAX_CODE_MAP_COLLECTION,
            source=read_map_tax_code_for_staging,
            columns=TAX_CODE_MAP_STAGING_COLUMNS,
        )

        query_compiled_tax_codes = rail.QueryCollectionOperator(
            task_id='query_compiled_tax_codes',
            name=COMPILED_TAX_CODES_COLLECTION,
            query=COMPILE_TAX_CODES_SQL,
        )

        # ---- foreach (two-pass create/update + compound link + write map) ----
        process_xero_tax_codes = rail.PythonOperator(
            task_id='process_xero_tax_codes',
            python_callable=sync_xero_tax_codes_to_vp,
            op_args=[config.instance],
        )

        catch_map_tax_code_dag_error = rail.PythonOperator(
            task_id='catch_map_tax_code_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_dag_error,
            op_args=[
                'map_tax_code',
                "{{ dag_run.conf.get('customerId') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_map_tax_code_populated',
            end_task='catch_map_tax_code_dag_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        [check_step_complete, check_populated] >> can_run_batch_task
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_map_tax_code_dag_error
        can_run_batch_task >> rail.Label('No') >> is_populated
        is_populated >> rail.Label(
            'Already populated') >> skip_populate >> catch_map_tax_code_dag_error
        (
            is_populated >> rail.Label('Needs population') >>
            fetch_xero_tax_rates >> fetch_vp_tax_codes >>
            create_xero_tax_components >> create_vp_tax_codes >>
            create_tax_code_map >> query_compiled_tax_codes >>
            process_xero_tax_codes >> mark_step_complete >>
            catch_map_tax_code_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
