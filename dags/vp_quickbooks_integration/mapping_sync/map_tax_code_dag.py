"""
Map Tax Code child DAG for VP QBO Mapping Sync.

Direction (per integration_vantagepoint_quickbooks/docs/mapping/PHASE_3_STEP_4):
- Unidirectional: QBO TaxCode (+ TaxRate) → VP TaxCodeEntity (QBO is master)
- A QBO TaxCode carries both a SalesTaxRateList and a PurchaseTaxRateList.
  Each list entry points at a QBO TaxRate by Id. The Workato recipe's
  `FlattenTaxRates` JS function (ported here as utils.flatten_qbo_tax_rates)
  emits one row per (TaxCode, TaxRate) pair, tagged with TaxOn=Sales or
  TaxOn=Purchase. Each emitted row is a VP TaxCodeEntity in its own right.

Regional handling: tax *groups* (codes with more than one rate per
direction, derived by rate count à la Workato step 14) are only created
in VP when CFG_Region == 'US'; the region is read from
dag_run.conf['config']['CFG_Region']. Non-group codes sync in every
region. The tenant decides what's active by which TaxCodes exist in QBO.

Reuse-by-name: before creating, sync_qbo_tax_codes_to_vp lists the
existing VP TaxCodeEntity catalog (Workato step 9) and, when a QBO rate's
name matches a VP code's Description, adopts that VP code instead of
POSTing a duplicate. New VP codes default their `Code` to the QBO RateId
(UUID only on collision).

Writes into the `map_tax_code` S3 collection. The dispatcher creates the
table up front; this DAG only populates rows. Columns:
    QBOCodeName       — TaxCode.Name
    QBORateName       — TaxRate.Name
    QBOCodeID         — TaxCode.Id
    VantagepointCode
    Rate              — effective rate %
    TaxTypeApplicable
    QBORateID         — TaxRate.Id
    IsTaxGroup        — 'Y' / 'N' (rate-count derived)
    TaxOn             — 'Sales' / 'Purchase'

Flow (operator-driven port of recipe steps 1-18; collection operators
mirror the abbviemst `time_export_child` pattern):
    check_map_tax_code_populated → is_map_tax_code_populated
       ├─ Yes → skip_populate_map_tax_code
       └─ No  → fetch_qbo_tax_codes   (QuickBooksTaxCodeOperator) [step 1]
              → fetch_qbo_tax_rates   (QuickBooksTaxRateOperator) [step 2]
              → fetch_vp_tax_codes    (VantagepointTaxCodesOperator GET) [step 10]
              → create_qbo_tax_rates  (CreateCollectionOperator)  [step 12]
              → create_vp_tax_codes   (CreateCollectionOperator)  [step 14]
              → create_tax_code_map   (CreateCollectionOperator)  [steps 11/13]
              → query_tax_group_ids   (QueryCollectionOperator)   [step 15]
              → query_compiled_tax_codes (QueryCollectionOperator,
                                       4-way LEFT JOIN)            [step 17]
              → process_qbo_tax_codes (PythonOperator wrapping
                                       sync_qbo_tax_codes_to_vp; the
                                       foreach create/adopt/update + map write) [step 18]
       catch_map_tax_code_dag_error  (one_failed; returns dict)

The two `query_list` SQL steps run as real SQL via QueryCollectionOperator
against run-local staging tables. The compile JOIN keeps Workato's full
`vtc` fan-out — a QBO rate that name-matches several existing VP tax codes
yields one map row per match (e.g. NO TAX PURCHASE → VP 6,7,8,9). The map
write is an idempotent INSERT OR REPLACE keyed on
(QBOCodeID, QBORateID, VantagepointCode), so the fan-out rows coexist and
re-runs converge. (Requires a clean VP tenant; a tenant with many
duplicate-named VP tax codes fans out proportionally.)
"""
from vp_quickbooks_integration.common.tables import (
    MAPPING_STEP_TAX_CODE,
)
from vp_quickbooks_integration.mapping_sync.utils.python_callable_method import (
    is_table_populated,
    capture_dag_error,
    check_step_status,
    mark_step_status,
    sync_qbo_tax_codes_to_vp,
    build_qbo_tax_rates_staging,
    prepare_vp_tax_codes_staging,
    read_map_tax_code_for_staging,
    TAX_GROUP_IDS_SQL,
    COMPILE_TAX_CODES_SQL,
)
from vp_quickbooks_integration.mapping_sync.utils._tax_code_sync import (
    QBO_TAX_RATES_COLLECTION,
    VP_TAX_CODES_COLLECTION,
    TAX_CODE_MAP_COLLECTION,
    TAX_GROUP_IDS_COLLECTION,
    COMPILED_TAX_CODES_COLLECTION,
    QBO_TAX_RATES_STAGING_COLUMNS,
    VP_TAX_CODES_STAGING_COLUMNS,
    TAX_CODE_MAP_STAGING_COLUMNS,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
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
            'Sync QBO tax codes (and embedded tax rates) to VP TaxCodeEntity. '
            'One VP entry per (TaxCode, TaxRate) Sales/Purchase component. '
            'Writes the map_tax_code cross-reference table.'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_quickbooks', 'mapping_sync', 'map_tax_code'],
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
            no_task='is_map_tax_code_populated',
        )

        # ---- Skip gates (layered) ----
        # Primary: mapping_table_state.Status == 'Complete' for this step.
        # Secondary: is_table_populated as defensive fallback.
        check_step_complete = rail.PythonOperator(
            task_id='check_map_tax_code_step_complete',
            python_callable=lambda: check_step_status(MAPPING_STEP_TAX_CODE),
        )

        check_populated = rail.PythonOperator(
            task_id='check_map_tax_code_populated',
            python_callable=lambda: is_table_populated('map_tax_code'),
        )

        is_populated = rail.IfOperator(
            task_id='is_map_tax_code_populated',
            test=lambda: (
                rail.result('check_map_tax_code_step_complete') or
                rail.result('check_map_tax_code_populated')
            ),
            yes_task='skip_populate_map_tax_code',
            no_task='fetch_qbo_tax_codes',
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

        fetch_qbo_tax_codes = rail.QuickBooksTaxCodeOperator(
            task_id='fetch_qbo_tax_codes',
            intuit_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('intuit', 'quickbooks_default') }}"
            ),
            query='select * from TaxCode where Active = true',
        )

        fetch_qbo_tax_rates = rail.QuickBooksTaxRateOperator(
            task_id='fetch_qbo_tax_rates',
            intuit_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('intuit', 'quickbooks_default') }}"
            ),
            query='select * from TaxRate where Active = true',
        )

        # ---- Recipe step 10: List Tax codes in Vantagepoint ----
        fetch_vp_tax_codes = rail.VantagepointTaxCodesOperator(
            task_id='fetch_vp_tax_codes',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            pagination=True,
        )

        # ---- Recipe steps 11-17: stage collections + run the query_list SQL ----
        # Mirrors the abbviemst time_export_child pattern: CreateCollectionOperator
        # builds the run-local tables; QueryCollectionOperator runs the recipe SQL.
        create_qbo_tax_rates = rail.CreateCollectionOperator(
            task_id='create_qbo_tax_rates',         # recipe step 12
            name=QBO_TAX_RATES_COLLECTION,
            source=build_qbo_tax_rates_staging,
            columns=QBO_TAX_RATES_STAGING_COLUMNS,
        )

        create_vp_tax_codes = rail.CreateCollectionOperator(
            task_id='create_vp_tax_codes',          # recipe step 14
            name=VP_TAX_CODES_COLLECTION,
            source=prepare_vp_tax_codes_staging,
            columns=VP_TAX_CODES_STAGING_COLUMNS,
        )

        create_tax_code_map = rail.CreateCollectionOperator(
            task_id='create_tax_code_map',          # recipe steps 11/13
            name=TAX_CODE_MAP_COLLECTION,
            source=read_map_tax_code_for_staging,
            columns=TAX_CODE_MAP_STAGING_COLUMNS,
        )

        query_tax_group_ids = rail.QueryCollectionOperator(
            task_id='query_tax_group_ids',          # recipe step 15
            name=TAX_GROUP_IDS_COLLECTION,
            query=TAX_GROUP_IDS_SQL,
        )

        query_compiled_tax_codes = rail.QueryCollectionOperator(
            task_id='query_compiled_tax_codes',     # recipe step 17
            name=COMPILED_TAX_CODES_COLLECTION,
            query=COMPILE_TAX_CODES_SQL,
        )

        # ---- Recipe step 18: foreach (POST/PUT VP + write map_tax_code) ----
        process_qbo_tax_codes = rail.PythonOperator(
            task_id='process_qbo_tax_codes',
            python_callable=sync_qbo_tax_codes_to_vp,
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
            fetch_qbo_tax_codes >> fetch_qbo_tax_rates >> fetch_vp_tax_codes >>
            create_qbo_tax_rates >> create_vp_tax_codes >> create_tax_code_map >>
            query_tax_group_ids >> query_compiled_tax_codes >>
            process_qbo_tax_codes >> mark_step_complete >> catch_map_tax_code_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
