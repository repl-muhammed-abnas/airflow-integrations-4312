"""
Journal Export Create DAG for VP -> Xero Posted Unit Transaction Sync.

Per-record DAG. Receives {PostSeq, Period, TransType, connections,
customerId} from the dispatcher (Period/PostSeq as STRINGS) and:
  1. Re-fetches the full PSA Ledger row(s) for (Period, PostSeq).
     Unit transactions return MULTIPLE rows per composite key — each
     row is one debit or credit allocation line.
  2. Loads the account-code mapping (Vantagepoint code -> Xero code).
  3. Resolves every row's Xero account code.
  4. Guards for empty fetch, then validates the account mapping exists.
  5. Idempotency guard: searches Xero for an existing ManualJournal with
     this (Period, PostSeq)'s Narration before creating — ManualJournal
     has no create-time idempotency key, so without this a retried POST
     (create_manual_journal runs with retries=3) or an overlapping poll
     window could double-post the same transaction to the customer's
     Xero ledger.
  6. Builds a single balanced Xero ManualJournal body (recipe's active
     create_manual_journal block, line 4362) and POSTs via
     XeroAPIOperator, unless step 5 found it already posted.
  7. Logs missing-mapping or Xero API failures via
     `capture_create_dag_error`.

Ports `014_501_psa_vantagepoint_unit_journal_exports_to_xero.recipe.json`.
Unlike the VP -> QBO unit_transaction_sync sibling, this DAG does NOT
resolve VP `/project` or a firm map — Xero's ManualJournal JournalLine
has no Entity/contact reference, confirmed absent from the recipe's
active create_manual_journal block.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_xero_integration.posted_unit_transaction_sync.config import (
    TRANS_TYPE,
)
from vp_xero_integration.posted_unit_transaction_sync.utils.python_callable_method import (  # noqa: E501
    build_record_filter,
    get_account_code_mapping_method,
    resolve_rows_method,
    check_rows_fetched_method,
    is_account_mapping_resolved_method,
    log_no_rows_to_post,
    log_missing_account_mapping,
    is_already_posted_method,
    log_already_posted,
    build_manual_journal_body,
    capture_create_dag_error,
)


def create_dag(config):
    """Per-record DAG: resolve account mapping + post one balanced Xero ManualJournal."""
    with rail.create_airflow_dag(
        dag_id=(
            f'vp_xero_unit_transaction_sync_journal_export_create_'
            f'{config.instance}'
        ),
        description=(
            'Resolve account-code mapping and create one Xero '
            'ManualJournal per (Period, PostSeq) unit transfer.'
        ),
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_xero',
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
        # (Period, PostSeq).
        fetch_unit_transfer_rows = rail.VantagepointPsaledgerOperator(
            task_id='fetch_unit_transfer_rows',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            filters=build_record_filter,
            trans_type=TRANS_TYPE,
        )

        # ---- 2. Account-code mapping (v1: Airflow Variable) ------------
        fetch_account_code_mapping = rail.PythonOperator(
            task_id='fetch_account_code_mapping',
            python_callable=lambda: get_account_code_mapping_method(
                config.instance
            )
        )

        # ---- 3. Annotate every row with its resolved Xero code ---------
        resolve_rows = rail.PythonOperator(
            task_id='resolve_rows',
            python_callable=resolve_rows_method
        )

        # ---- 4a. Guard: no rows returned --------------------------------
        # Separate from the mapping gate so an empty fetch produces a
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

        # ---- 4b. Validation gate ----------------------------------------
        is_account_mapping_resolved = rail.IfOperator(
            task_id='is_account_mapping_resolved',
            test=is_account_mapping_resolved_method,
            yes_task='search_existing_manual_journal',
            no_task='log_missing_account_mapping_action'
        )

        log_missing_account_mapping_action = rail.PythonOperator(
            task_id='log_missing_account_mapping_action',
            python_callable=log_missing_account_mapping
        )

        # ---- 5. Idempotency guard: search before create -----------------
        # CI (test_no_import_errors) confirmed `rail.XeroManualJournalOperator`
        # is NOT available in the deployed/tested rail package
        # (AttributeError at DAG-parse time) — a manual wheel-download check
        # against requirements-2.11.txt's pinned branch build had suggested
        # otherwise, but the actual CI environment is authoritative and that
        # check was wrong. Reverted to the generic `rail.XeroAPIOperator`,
        # matching the existing `dags/xero/invoice_export/child_dag.py`
        # convention (`filters='?where=...'` with the `?` prefix, plain
        # Jinja template string — `filters` is a template_field so Airflow
        # renders the embedded {{ }} placeholders against dag_run.conf
        # right before execute()).
        search_existing_manual_journal = rail.XeroAPIOperator(
            task_id='search_existing_manual_journal',
            xero_conn_id="{{ dag_run.conf.connections.xero }}",
            endpoint='/api.xro/2.0/ManualJournals',
            request_method='GET',
            filters=(
                '?where=Narration=="UN {{ dag_run.conf.Period }} '
                '{{ dag_run.conf.PostSeq }}"'
            ),
        )

        is_already_posted = rail.IfOperator(
            task_id='is_already_posted',
            test=is_already_posted_method,
            yes_task='log_already_posted_action',
            no_task='create_manual_journal'
        )

        log_already_posted_action = rail.PythonOperator(
            task_id='log_already_posted_action',
            python_callable=log_already_posted
        )

        # ---- 6. Xero ManualJournal create (recipe line 4362 leaf) -------
        # Generic XeroAPIOperator (see note on search_existing_manual_journal
        # above for why the typed XeroManualJournalOperator was reverted).
        create_manual_journal = rail.XeroAPIOperator(
            task_id='create_manual_journal',
            xero_conn_id="{{ dag_run.conf.connections.xero }}",
            endpoint='/api.xro/2.0/ManualJournals',
            request_method='POST',
            request_body=build_manual_journal_body,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # ---- 7. Error capture terminal -----------------------------------
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

        # ---- Topology ----------------------------------------------------
        fetch_unit_transfer_rows >> fetch_account_code_mapping
        fetch_account_code_mapping >> resolve_rows >> check_rows_fetched

        check_rows_fetched >> rail.Label('No rows') >> log_no_rows_action
        (
            check_rows_fetched >>
            rail.Label('Rows present') >>
            is_account_mapping_resolved
        )

        (
            is_account_mapping_resolved >>
            rail.Label('Accounts mapped') >>
            search_existing_manual_journal >>
            is_already_posted
        )
        (
            is_account_mapping_resolved >>
            rail.Label('Account(s) missing') >>
            log_missing_account_mapping_action
        )

        is_already_posted >> rail.Label('Yes') >> log_already_posted_action
        is_already_posted >> rail.Label('No') >> create_manual_journal

        # All branches converge on the catch terminal.
        create_manual_journal >> catch_create_dag_error
        log_already_posted_action >> catch_create_dag_error
        log_no_rows_action >> catch_create_dag_error
        log_missing_account_mapping_action >> catch_create_dag_error

        return dag


rail.for_each_instance(create_dag)
