"""
Map Account Code child DAG for VP QBO Mapping Sync.

Direction (per integration_vantagepoint_quickbooks/docs/mapping/PHASE_3_STEP_3):
- Unidirectional: QBO Chart of Accounts → VP Chart of Accounts (QBO is master)
- Operator-driven port of the Workato recipe `014_503_psa_sync_account_codes`:
  QBO accounts are matched to EXISTING VP accounts by `AcctNum` OR `Name`
  (recipe #17 `va` join), and the matched VP account's Account/Name/Type is
  recorded — several VP accounts sharing a name yield several map rows
  (Advertising → 400 AND 6000). A VP account is only CREATED when the QBO
  account has an `AcctNum` and no VP match; name-only accounts get a QBO-only
  row (empty VP), matching Workato. The QBO `Classification` → VP type code
  lookup uses the static `ACCOUNT_TYPE_MAP` constant (recipe #17 `atm`).

Writes into the `map_account_code` S3 collection. Columns:
    QBOCode            — empty (Workato parity)
    QBOName
    QBOType            — QBO AccountType
    VantagepointCode   — matched/created VP Account code
    VantagepointName   — VP account name
    VantagepointTypeRO — VP account's actual Type (read-only, from VP)
    QBOID              — QBO Account Id (the stable identifier)

Flow:
    check_map_account_code_populated → is_map_account_code_populated
       ├─ Yes → skip_populate_map_account_code
       └─ No  → fetch_qbo_accounts        (QuickBooksAccountOperator)       [#1]
              → fetch_vp_accounts         (VantagepointChartOfAccounts GET) [#3/#6]
              → create_qbo_accounts       (CreateCollectionOperator)        [#11]
              → create_vp_accounts        (CreateCollectionOperator)        [#16]
              → create_account_code_map   (CreateCollectionOperator)        [#13]
              → query_compiled_account_codes (QueryCollectionOperator JOIN) [#17]
              → get_system_formats        (VantagepointSystemFormats GET)   [#29]
              → process_qbo_accounts      (PythonOperator foreach)          [#18]
       catch_map_account_code_dag_error (one_failed; returns dict)

NOTE: critical-account validation (AP Liability must map to QBO AP, etc.)
is implemented in `validate_mappings_dag.py` (Phase 5) — see
`_validate_map_account_code_with_cursor` in
`utils/python_callable_method.py` for the `no_ap_liability_mapped`
warning check. This child DAG focuses on the bulk QBO→VP sync.
"""
from vp_quickbooks_integration.common.tables import (
    MAPPING_STEP_ACCOUNT,
)
from vp_quickbooks_integration.mapping_sync.utils.python_callable_method import (
    is_table_populated,
    capture_dag_error,
    check_step_status,
    mark_step_status,
    sync_qbo_accounts_to_vp,
    build_qbo_accounts_staging,
    prepare_vp_accounts_staging,
    read_account_code_map_for_staging,
    COMPILE_ACCOUNT_CODES_SQL,
)
from vp_quickbooks_integration.mapping_sync.utils._account_sync import (
    QBO_ACCOUNTS_COLLECTION,
    VP_ACCOUNTS_COLLECTION,
    ACCOUNT_CODE_MAP_COLLECTION,
    COMPILED_ACCOUNT_CODES_COLLECTION,
    QBO_ACCOUNTS_STAGING_COLUMNS,
    VP_ACCOUNTS_STAGING_COLUMNS,
    ACCOUNT_CODE_MAP_STAGING_COLUMNS,
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
    """Per-instance map_account_code child DAG."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('map_account_code', config.instance),
        description=(
            'Sync QBO chart-of-accounts to VP chart-of-accounts via the '
            'account_type_map lookup. Writes the map_account_code '
            'cross-reference table.'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_quickbooks', 'mapping_sync', 'map_account_code'],
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
            no_task='is_map_account_code_populated',
        )

        # ---- Skip gates (layered) ----
        # Primary: mapping_table_state.Status == 'Complete' for this step.
        # Secondary: is_table_populated as defensive fallback.
        check_step_complete = rail.PythonOperator(
            task_id='check_map_account_code_step_complete',
            python_callable=lambda: check_step_status(MAPPING_STEP_ACCOUNT),
        )

        check_populated = rail.PythonOperator(
            task_id='check_map_account_code_populated',
            python_callable=lambda: is_table_populated('map_account_code'),
        )

        is_populated = rail.IfOperator(
            task_id='is_map_account_code_populated',
            test=lambda: (
                rail.result('check_map_account_code_step_complete') or
                rail.result('check_map_account_code_populated')
            ),
            yes_task='skip_populate_map_account_code',
            no_task='fetch_qbo_accounts',
        )

        skip_populate = rail.PythonOperator(
            task_id='skip_populate_map_account_code',
            python_callable=lambda: _log.info(
                'map_account_code already populated for this customer — skipping'
            ),
        )

        # ---- Mark step Complete on successful population ----
        mark_step_complete = rail.PythonOperator(
            task_id='mark_map_account_code_step_complete',
            python_callable=lambda: mark_step_status(
                MAPPING_STEP_ACCOUNT, 'Complete'
            ),
        )

        fetch_qbo_accounts = rail.QuickBooksAccountOperator(
            task_id='fetch_qbo_accounts',
            intuit_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('intuit', 'quickbooks_default') }}"
            ),
            operation='search_account',
            query='select * from Account where Active = true',
        )

        # ---- Recipe #3/#6: List Chart of Accounts in Vantagepoint ----
        fetch_vp_accounts = rail.VantagepointChartOfAccountsOperator(
            task_id='fetch_vp_accounts',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            pagination=True,
        )

        # ---- Recipe #11-17: stage collections + run the compile JOIN ----
        # Mirrors the abbviemst / map_tax_code pattern: CreateCollectionOperator
        # builds the run-local tables; QueryCollectionOperator runs the recipe SQL.
        create_qbo_accounts = rail.CreateCollectionOperator(
            task_id='create_qbo_accounts',          # recipe #11
            name=QBO_ACCOUNTS_COLLECTION,
            source=build_qbo_accounts_staging,
            columns=QBO_ACCOUNTS_STAGING_COLUMNS,
        )

        create_vp_accounts = rail.CreateCollectionOperator(
            task_id='create_vp_accounts',           # recipe #16
            name=VP_ACCOUNTS_COLLECTION,
            source=prepare_vp_accounts_staging,
            columns=VP_ACCOUNTS_STAGING_COLUMNS,
        )

        create_account_code_map = rail.CreateCollectionOperator(
            task_id='create_account_code_map',      # recipe #13
            name=ACCOUNT_CODE_MAP_COLLECTION,
            source=read_account_code_map_for_staging,
            columns=ACCOUNT_CODE_MAP_STAGING_COLUMNS,
        )

        query_compiled_account_codes = rail.QueryCollectionOperator(
            task_id='query_compiled_account_codes',  # recipe #17
            name=COMPILED_ACCOUNT_CODES_COLLECTION,
            query=COMPILE_ACCOUNT_CODES_SQL,
        )

        # ---- Recipe #29: VP System Formats (account-number max length) ----
        # Read once per run; sync_qbo_accounts_to_vp derives the create-time
        # account-code length guard from this instead of a hardcoded 13.
        get_system_formats = rail.VantagepointSystemFormatsOperator(
            task_id='get_system_formats',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            # Narrow to the account-number format (as chart_of_accounts_sync
            # does) so the length probe can't pick up a non-account format.
            filters='?entity=account',
        )

        # ---- Recipe #18: foreach (match/create + write map_account_code) ----
        process_qbo_accounts = rail.PythonOperator(
            task_id='process_qbo_accounts',
            python_callable=sync_qbo_accounts_to_vp,
            op_args=[config.instance],
        )

        catch_map_account_code_dag_error = rail.PythonOperator(
            task_id='catch_map_account_code_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_dag_error,
            op_args=[
                'map_account_code',
                "{{ dag_run.conf.get('customerId') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_map_account_code_populated',
            end_task='catch_map_account_code_dag_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        [check_step_complete, check_populated] >> can_run_batch_task
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_map_account_code_dag_error
        can_run_batch_task >> rail.Label('No') >> is_populated
        is_populated >> rail.Label(
            'Already populated') >> skip_populate >> catch_map_account_code_dag_error
        (
            is_populated >> rail.Label('Needs population') >>
            fetch_qbo_accounts >> fetch_vp_accounts >>
            create_qbo_accounts >> create_vp_accounts >> create_account_code_map >>
            query_compiled_account_codes >> get_system_formats >>
            process_qbo_accounts >> mark_step_complete >> catch_map_account_code_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
