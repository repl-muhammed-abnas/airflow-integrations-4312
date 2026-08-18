"""
Unit Transaction Create DAG for VP -> QBO Unit Transaction Sync.

Per-record DAG. Receives {PostSeq, Period, TransType, connections,
customerId} from the dispatcher and:
  1. Re-fetches the full PSA Ledger row(s) for (Period, PostSeq).
     Unit transactions return MULTIPLE rows per composite key — each
     row is one debit or credit allocation line.
  2. Fetches the VP Project records for the WBS1 codes seen in the rows
     to resolve WBS1+WBS2+WBS3 -> ClientID (recipe step 17, line 4406).
  3. Loads the account-code lookup table and firm-map lookup table.
  4. Resolves every row: Account -> QBO account name/id (via acct map)
     and WBS1+WBS2+WBS3 -> ClientID -> FirmQBOID (via project + firm map).
  5. Guards for empty fetch, then validates mappings exist (recipe
     `CompoundError` path, L10774/L11922).
  6. Builds a single balanced QBO JournalEntry body (recipe L13174, single
     `create_journal_entry_v2` leaf) and POSTs via QuickBooksJournalEntryOperator.
  7. Logs missing-mapping or QBO API failures via `capture_create_dag_error`
     (recipe error sites L13086 and L13275).

Key recipe insight: PSA Ledger `un` rows carry WBS1/WBS2/WBS3 but NOT
ClientID. The recipe does a second VP API call (GET /project?fieldFilter=
WBSNumber,WBS1,WBS2,WBS3,Name,ClientID) to resolve ClientID, then joins to
the firm map on ClientID = FirmID. We replicate this with a
VantagepointProjectOperator task (`fetch_projects`) between the PSA fetch
and the lookup reads.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.unit_transaction_sync.utils.python_callable_method import (  # noqa: E501
    build_record_filter,
    build_project_filter,
    get_account_code_mapping_method,
    get_firm_mapping_method,
    resolve_rows_method,
    check_rows_fetched_method,
    is_account_mapping_resolved_method,
    is_firm_mapping_resolved_method,
    log_no_rows_to_post,
    log_missing_account_mapping,
    log_missing_firm_mapping,
    build_journal_entry_body,
    capture_create_dag_error,
)


def create_dag(config):
    """Per-record DAG: resolve mappings + post one balanced QBO JournalEntry."""
    with rail.create_airflow_dag(
        dag_id=(
            f'vp_qbo_unit_transaction_sync_create_{config.instance}'
        ),
        description=(
            'Resolve account-code + firm mappings and create one QBO '
            'JournalEntry per (Period, PostSeq) unit transfer.'
        ),
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks',
            'unit_transaction_sync',
            'create',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            ),
        }
    ) as dag:

        # ---- 1. Re-fetch full unit-transfer rows by composite key ------
        # pagination=True (default) ensures all rows are returned even
        # if the VP API pages at a limit below the row count for this
        # (Period, PostSeq). Typical unit transfer has <30 rows; keeping
        # pagination enabled eliminates any silent truncation risk.
        fetch_unit_transfer_rows = rail.VantagepointPsaledgerOperator(
            task_id='fetch_unit_transfer_rows',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            filters=build_record_filter,
            trans_type='un',
        )

        # ---- 2. Fetch VP projects for WBS resolution (recipe step 17) -
        fetch_projects = rail.VantagepointProjectOperator(
            task_id='fetch_projects',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            filters=build_project_filter,
            pagination=False
        )

        # ---- 3. Lookup table reads (v1: Airflow Variables) -------------
        fetch_account_code_mapping = rail.PythonOperator(
            task_id='fetch_account_code_mapping',
            python_callable=lambda: get_account_code_mapping_method(
                config.instance
            )
        )

        fetch_firm_mapping = rail.PythonOperator(
            task_id='fetch_firm_mapping',
            python_callable=lambda: get_firm_mapping_method(
                config.instance
            )
        )

        # ---- 4. Annotate every row with resolved QBO ids ---------------
        resolve_rows = rail.PythonOperator(
            task_id='resolve_rows',
            python_callable=resolve_rows_method
        )

        # ---- 5a. Guard: no rows returned -------------------------------
        # Separate from the mapping gates so an empty fetch produces a
        # clear "nothing to post" message instead of a misleading
        # "account(s) (none) not matched" error from the mapping branch.
        check_rows_fetched = rail.IfOperator(
            task_id='check_rows_fetched',
            test=check_rows_fetched_method,
            yes_task='is_account_mapping_resolved',
            no_task='log_no_rows_action'
        )

        log_no_rows_action = rail.PythonOperator(
            task_id='log_no_rows_action',
            python_callable=log_no_rows_to_post
        )

        # ---- 5b. Validation gates (recipe L10774 + L11922) -------------
        is_account_mapping_resolved = rail.IfOperator(
            task_id='is_account_mapping_resolved',
            test=is_account_mapping_resolved_method,
            yes_task='is_firm_mapping_resolved',
            no_task='log_missing_account_mapping_action'
        )

        is_firm_mapping_resolved = rail.IfOperator(
            task_id='is_firm_mapping_resolved',
            test=is_firm_mapping_resolved_method,
            yes_task='create_journal_entry',
            no_task='log_missing_firm_mapping_action'
        )

        log_missing_account_mapping_action = rail.PythonOperator(
            task_id='log_missing_account_mapping_action',
            python_callable=log_missing_account_mapping
        )

        log_missing_firm_mapping_action = rail.PythonOperator(
            task_id='log_missing_firm_mapping_action',
            python_callable=log_missing_firm_mapping
        )

        # ---- 6. QBO JournalEntry create (recipe L13174 leaf) -----------
        create_journal_entry = rail.QuickBooksJournalEntryOperator(
            task_id='create_journal_entry',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            request_body=build_journal_entry_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # ---- 7. Error capture terminal (recipe L13086 + L13275) --------
        catch_create_dag_error = rail.PythonOperator(
            task_id='catch_create_dag_error',
            trigger_rule='all_done',
            python_callable=capture_create_dag_error,
            op_args=[
                '{{ dag_run.conf.PostSeq }}',
                '{{ dag_run.conf.Period }}',
                '{{ get_error_message() }}'
            ]
        )

        # ---- Topology --------------------------------------------------
        fetch_unit_transfer_rows >> fetch_projects
        fetch_unit_transfer_rows >> [
            fetch_account_code_mapping, fetch_firm_mapping
        ]
        [
            fetch_projects,
            fetch_account_code_mapping,
            fetch_firm_mapping,
        ] >> resolve_rows >> check_rows_fetched

        check_rows_fetched >> rail.Label('No rows') >> log_no_rows_action
        (
            check_rows_fetched >>
            rail.Label('Rows present') >>
            is_account_mapping_resolved
        )

        (
            is_account_mapping_resolved >>
            rail.Label('Accounts mapped') >>
            is_firm_mapping_resolved
        )
        (
            is_account_mapping_resolved >>
            rail.Label('Account(s) missing') >>
            log_missing_account_mapping_action
        )

        (
            is_firm_mapping_resolved >>
            rail.Label('Firms mapped') >>
            create_journal_entry
        )
        (
            is_firm_mapping_resolved >>
            rail.Label('Firm(s) missing') >>
            log_missing_firm_mapping_action
        )

        # All branches converge on the catch terminal.
        create_journal_entry >> catch_create_dag_error
        log_no_rows_action >> catch_create_dag_error
        log_missing_account_mapping_action >> catch_create_dag_error
        log_missing_firm_mapping_action >> catch_create_dag_error

        return dag


rail.for_each_instance(create_dag)
