"""
Map Account Code child DAG for VP Xero Mapping Sync.

Direction: Xero Chart of Accounts → VP Chart of Accounts (Xero is master).
Operator-driven port of Workato `014_501_psa_synch_accounts` (orchestrator) +
`014_501_psa_sync_accounts` (worker), with the `Map Accounts` seeder folded in
(Option A — see reverse-engineering docs 02 + 06).

The DAG fetches Xero + VP accounts, stages three run-local collections, runs the
compile JOIN (`COMPILE_ACCOUNT_CODES_SQL`), reads VP System Formats (account
length), then the engine matches/creates/updates VP accounts and writes the
map_chart_of_accounts cross-reference (keyed XeroID). The Xero-Type → VP-type
translation reads the seeded `map_account_type` collection; unmapped types are
surfaced (not dropped). A scoped orphan-deactivation pass deactivates only
previously-Xero-sourced VP accounts (Q6 = A).

Writes into the `map_chart_of_accounts` S3 collection. Columns:
    XeroCode, XeroName, XeroType, VantagepointCode, VantagepointName,
    VantagepointType, XeroID, Messages

Flow:
    check_map_account_code_step_complete / check_map_account_code_populated
      → is_map_account_code_populated
       ├─ Yes → skip_populate_map_account_code
       └─ No  → fetch_xero_accounts        (XeroAccountOperator list)
              → fetch_vp_accounts          (VantagepointChartOfAccounts GET)
              → create_xero_accounts       (CreateCollectionOperator)
              → create_vp_accounts         (CreateCollectionOperator)
              → create_chart_of_accounts_map (CreateCollectionOperator)
              → query_compiled_account_codes (QueryCollectionOperator JOIN)
              → get_system_formats         (VantagepointSystemFormats GET)
              → process_xero_accounts      (PythonOperator foreach + orphan pass)
              → mark_map_account_code_step_complete
       catch_map_account_code_dag_error (one_failed; returns dict)
"""
from vp_xero_integration.common.tables import (
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAPPING_STEP_ACCOUNT,
)
from vp_xero_integration.mapping_sync.utils.python_callable_method import (
    is_table_populated,
    capture_dag_error,
    check_step_status,
    mark_step_status,
    sync_xero_accounts_to_vp,
    build_xero_accounts_staging,
    prepare_vp_accounts_staging,
    read_chart_of_accounts_map_for_staging,
    COMPILE_ACCOUNT_CODES_SQL,
)
from vp_xero_integration.mapping_sync.utils._account_sync import (
    XERO_ACCOUNTS_COLLECTION,
    VP_ACCOUNTS_COLLECTION,
    CHART_OF_ACCOUNTS_MAP_COLLECTION,
    COMPILED_ACCOUNT_CODES_COLLECTION,
    XERO_ACCOUNTS_STAGING_COLUMNS,
    VP_ACCOUNTS_STAGING_COLUMNS,
    CHART_OF_ACCOUNTS_MAP_STAGING_COLUMNS,
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
    """Per-instance map_account_code child DAG."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('map_account_code', config.instance),
        description=(
            'Sync Xero chart-of-accounts to VP chart-of-accounts via the seeded '
            'map_account_type lookup. Writes the map_chart_of_accounts '
            'cross-reference table.'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_xero', 'mapping_sync', 'map_account_code'],
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
            no_task='is_map_account_code_populated',
        )

        # ---- Skip gates (layered) ----
        check_step_complete = rail.PythonOperator(
            task_id='check_map_account_code_step_complete',
            python_callable=lambda: check_step_status(MAPPING_STEP_ACCOUNT),
        )

        check_populated = rail.PythonOperator(
            task_id='check_map_account_code_populated',
            python_callable=lambda: is_table_populated(
                MAP_CHART_OF_ACCOUNTS_TABLE_NAME),
        )

        is_populated = rail.IfOperator(
            task_id='is_map_account_code_populated',
            test=lambda: (
                rail.result('check_map_account_code_step_complete') or
                rail.result('check_map_account_code_populated')
            ),
            yes_task='skip_populate_map_account_code',
            no_task='fetch_xero_accounts',
        )

        skip_populate = rail.PythonOperator(
            task_id='skip_populate_map_account_code',
            python_callable=lambda: _log.info(
                'map_chart_of_accounts already populated for this customer — skipping'
            ),
        )

        # ---- Mark step Complete on successful population ----
        mark_step_complete = rail.PythonOperator(
            task_id='mark_map_account_code_step_complete',
            python_callable=lambda: mark_step_status(
                MAPPING_STEP_ACCOUNT, 'Complete'
            ),
        )

        # ---- Source: Xero chart of accounts (list all; SQL excludes BANK) ----
        fetch_xero_accounts = rail.XeroAccountOperator(
            task_id='fetch_xero_accounts',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='list',
        )

        # ---- List Chart of Accounts in Vantagepoint ----
        fetch_vp_accounts = rail.VantagepointChartOfAccountsOperator(
            task_id='fetch_vp_accounts',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            pagination=True,
        )

        # ---- Stage collections + run the compile JOIN ----
        create_xero_accounts = rail.CreateCollectionOperator(
            task_id='create_xero_accounts',
            name=XERO_ACCOUNTS_COLLECTION,
            source=build_xero_accounts_staging,
            columns=XERO_ACCOUNTS_STAGING_COLUMNS,
        )

        create_vp_accounts = rail.CreateCollectionOperator(
            task_id='create_vp_accounts',
            name=VP_ACCOUNTS_COLLECTION,
            source=prepare_vp_accounts_staging,
            columns=VP_ACCOUNTS_STAGING_COLUMNS,
        )

        create_chart_of_accounts_map = rail.CreateCollectionOperator(
            task_id='create_chart_of_accounts_map',
            name=CHART_OF_ACCOUNTS_MAP_COLLECTION,
            source=read_chart_of_accounts_map_for_staging,
            columns=CHART_OF_ACCOUNTS_MAP_STAGING_COLUMNS,
        )

        query_compiled_account_codes = rail.QueryCollectionOperator(
            task_id='query_compiled_account_codes',
            name=COMPILED_ACCOUNT_CODES_COLLECTION,
            query=COMPILE_ACCOUNT_CODES_SQL,
        )

        # ---- VP System Formats (account-number max length) ----
        get_system_formats = rail.VantagepointSystemFormatsOperator(
            task_id='get_system_formats',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            filters='?entity=account',
        )

        # ---- foreach (match/create/update + orphan pass + write map) ----
        process_xero_accounts = rail.PythonOperator(
            task_id='process_xero_accounts',
            python_callable=sync_xero_accounts_to_vp,
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
            fetch_xero_accounts >> fetch_vp_accounts >>
            create_xero_accounts >> create_vp_accounts >>
            create_chart_of_accounts_map >> query_compiled_account_codes >>
            get_system_formats >> process_xero_accounts >>
            mark_step_complete >> catch_map_account_code_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
