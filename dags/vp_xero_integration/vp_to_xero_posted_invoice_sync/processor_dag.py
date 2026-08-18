"""
Processor DAG for VP PSA -> Xero Posted Invoices Sync.

Per-batch: fetches all PSA Ledger 'IN' rows for the given Batch, loads the
account + tax code mappings from S3, resolves the Xero zero-rate tax code,
fetches SalesInvoice master headers, fetches VP Tax Codes for compound-tax
computation, fetches LedgerTax rows for the batch Period+PostSeq, groups rows
into per-invoice structures, and iterates every invoice via ForEachOperator:
  - Searches Xero for an existing invoice (idempotency gate)
  - Routes to ACCREC invoice or ACCRECCREDIT credit note
  - Credit notes in AUTHORISED status are allocated to the original invoice
    (via VP PSALedger lookup of the original invoice's Period+PostSeq)
After the per-invoice loop, re-fetches PSA Ledger rows by Period+PostSeq for
the revenue-recognition ManualJournal for UninvoicedRevenue/UnbilledServices.

Triggered by dispatcher_dag with:
  dag_run.conf.Batch       — VP AR invoice batch number
  dag_run.conf.PostDate    — batch post date (informational)
  dag_run.conf.connections — {vantagepoint, xero} connection ids
  dag_run.conf.customerId  — tenant id
  dag_run.conf.config      — {CFG_InvoiceStatusSubmitted: bool, ...}

Replaces Workato recipes:
  014-501 PSA Vantagepoint Invoice Exports to Xero
  014-501 PSA Vantagepoint AR Invoice detail with Tax
  014-501 PSA Post Invoice to Xero
  014-501 PSA Vantagepoint Revenue Generation posts to Xero
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration.vp_to_xero_posted_invoice_sync.utils.python_callable_method import (
    build_invoice_batch_filter,
    build_ledger_tax_filter,
    build_revenue_psa_filter,
    fetch_account_and_tax_maps_method,
    group_and_transform_invoices_method,
    check_invoice_exists_method,
    is_new_invoice_method,
    is_credit_note_method,
    build_invoice_body_method,
    build_credit_note_body_method,
    allocate_credit_note_method,
    is_revenue_configured_method,
    build_revenue_journal_body_method,
    capture_processor_error,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-batch processor: transform + post all invoices in batch to Xero."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_posted_invoice_sync_processor_{config.instance}',
        description=(
            'Post one VP PSA AR invoice batch to Xero '
            '(invoice / credit note + revenue journal)'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'posted_invoices', 'processor'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # ------------------------------------------------------------------ #
        # Phase 1: Parallel data fetches
        # Workato parity: recipe 3 steps 1-4 executed concurrently
        # ------------------------------------------------------------------ #

        # Step 2 of recipe 3: fetch all PSA Ledger rows for the batch
        fetch_invoice_batch = rail.VantagepointPsaledgerOperator(
            task_id='fetch_invoice_batch',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            filters=build_invoice_batch_filter,
            trans_type='IN',
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10),
        )

        # Step 1b of recipe 3: fetch per-invoice SalesInvoice master records.
        # Workato parity: Deltek connector also calls /DataEntry/inMaster/{Batch} and combines
        # with inControl — this gives Invoice, ClientName, WBS1-3, DueDate, CurrencyCode,
        # IsCreditMemo per invoice. group_and_transform_invoices uses this to build si_map.
        fetch_sales_invoice_master = rail.VantagepointSalesInvoiceOperator(
            task_id='fetch_sales_invoice_master',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            endpoint="{{ '/DataEntry/inMaster/' + dag_run.conf.get('Batch', '') }}",
            pagination=True,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10),
        )

        # Step 1c: fetch per-invoice line items from /DataEntry/inDetail/{Batch}.
        # Workato parity: the AR Invoice detail recipe reads inDetail — the user-visible
        # billing lines. The PSA Ledger also has GL offset entries (WIP reversals,
        # unbilled-services, etc.) that must NOT appear as Xero invoice line items.
        fetch_sales_invoice_detail = rail.VantagepointSalesInvoiceOperator(
            task_id='fetch_sales_invoice_detail',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            endpoint="{{ '/DataEntry/inDetail/' + dag_run.conf.get('Batch', '') }}",
            pagination=True,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10),
        )

        # Load map_chart_of_accounts + map_tax_code from the mapping_sync S3 collection.
        fetch_account_tax_maps = rail.PythonOperator(
            task_id='fetch_account_tax_maps',
            python_callable=fetch_account_and_tax_maps_method,
        )

        # Step 4 of recipe 3: fetch all VP Tax Code definitions for compound-tax rate computation.
        # Uses VantagepointTaxCodesOperator (base_path='/vision', endpoint='/TaxCodeEntity/').
        fetch_vp_tax_codes = rail.VantagepointTaxCodesOperator(
            task_id='fetch_vp_tax_codes',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            pagination=True,
            retries=2,
            retry_delay=timedelta(seconds=10),
        )

        # Resolve the Xero zero-rate tax code name (e.g. "No VAT", "Tax Exempt").
        # Workato parity: 014_501_psa_xero_no_tax_code callable.
        resolve_zero_rate_tax_code = rail.XeroTaxRateOperator(
            task_id='resolve_zero_rate_tax_code',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='list',
        )

        # ------------------------------------------------------------------ #
        # Phase 1.5: LedgerTax fetch (depends on fetch_invoice_batch for Period+PostSeq)
        # Workato parity: recipe 3 step 3 — fetch LedgerTax rows for the batch Period+PostSeq
        # ------------------------------------------------------------------ #

        fetch_ledger_tax_rows = rail.VantagepointLedgertaxOperator(
            task_id='fetch_ledger_tax_rows',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            trans_type='IN',
            filters=build_ledger_tax_filter,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10),
        )

        # ------------------------------------------------------------------ #
        # Phase 2: Group and transform
        # Workato parity: recipe 3 steps 9-16 — build per-invoice structures
        # with compound tax amounts, account mapping, SalesInvoice header data
        # ------------------------------------------------------------------ #

        group_and_transform_invoices = rail.PythonOperator(
            task_id='group_and_transform_invoices',
            python_callable=group_and_transform_invoices_method,
        )

        # ------------------------------------------------------------------ #
        # Phase 3: Per-invoice ForEach loop
        # ------------------------------------------------------------------ #

        for_each_invoice = rail.ForEachOperator(
            task_id='for_each_invoice',
            items=lambda: rail.result('group_and_transform_invoices'),
            start_task='check_invoice_exists',
            end_task='log_invoice_done',
        )

        # Idempotency gate: search Xero for existing invoice by InvoiceNumber.
        check_invoice_exists = rail.PythonOperator(
            task_id='check_invoice_exists',
            python_callable=check_invoice_exists_method,
        )

        is_new_invoice = rail.IfOperator(
            task_id='is_new_invoice',
            test=is_new_invoice_method,
            yes_task='is_credit_note',
            no_task='skip_existing_invoice',
        )

        skip_existing_invoice = rail.EmptyOperator(
            task_id='skip_existing_invoice',
        )

        is_credit_note = rail.IfOperator(
            task_id='is_credit_note',
            test=is_credit_note_method,
            yes_task='create_xero_credit_note',
            no_task='create_xero_invoice',
        )

        # ACCREC invoice create
        create_xero_invoice = rail.XeroInvoiceOperator(
            task_id='create_xero_invoice',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='create',
            request_body=build_invoice_body_method,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15),
        )

        # ACCRECCREDIT credit note create
        create_xero_credit_note = rail.XeroCreditNoteOperator(
            task_id='create_xero_credit_note',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='create',
            request_body=build_credit_note_body_method,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15),
        )

        # Two-step allocation: PUT /CreditNotes/{id}/Allocations.
        # Workato parity: original invoice located via VP PSALedger lookup (not
        # direct Xero search by CreditMemoRefNo) then Xero InvoiceNumber constructed
        # as '{Invoice}.{Period}.{PostSeq}'.
        # Only fires when invoice_status == AUTHORISED.
        allocate_credit_note_to_invoice = rail.PythonOperator(
            task_id='allocate_credit_note_to_invoice',
            python_callable=allocate_credit_note_method,
        )

        # Merge point — trigger_rule handles the three incoming paths:
        #   skip_existing_invoice, create_xero_invoice,
        #   allocate_credit_note_to_invoice (after credit note branch).
        log_invoice_done = rail.EmptyOperator(
            task_id='log_invoice_done',
            trigger_rule='none_failed_min_one_success',
        )

        # ------------------------------------------------------------------ #
        # Phase 4: Revenue-recognition ManualJournal (once per batch, after loop)
        # Workato parity: recipe 5 — CFGAutoPosting check + PSALedger re-fetch
        # by Period+PostSeq + ManualJournal create
        # ------------------------------------------------------------------ #

        fetch_revenue_accounts = rail.VantagepointAPIOperator(
            task_id='fetch_revenue_accounts',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            endpoint='/AccountingCompanyConfig/CFGAutoPosting',
            request_method='GET',
            pagination=False,
            retries=2,
            retry_delay=timedelta(seconds=10),
        )

        # Yes → fetch_revenue_psa_rows (Workato step 2: search PSALedger by Period+PostSeq)
        is_revenue_configured = rail.IfOperator(
            task_id='is_revenue_configured',
            test=is_revenue_configured_method,
            yes_task='fetch_revenue_psa_rows',
            no_task='log_no_revenue_accounts',
        )

        log_no_revenue_accounts = rail.PythonOperator(
            task_id='log_no_revenue_accounts',
            python_callable=lambda: logger.info(
                "Batch %s: CFGAutoPosting has no revenue accounts configured "
                "or no matching PSA lines — skipping ManualJournal",
                rail.get_current_context()['dag_run'].conf.get('Batch', ''),
            )
        )

        # Workato parity: recipe 5 step 2 — re-fetch PSALedger by Period+PostSeq
        # (not re-using the Batch-filtered fetch_invoice_batch rows) so the
        # revenue journal uses the canonical period-level data.
        fetch_revenue_psa_rows = rail.VantagepointPsaledgerOperator(
            task_id='fetch_revenue_psa_rows',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            trans_type='IN',
            filters=build_revenue_psa_filter,
            retries=2,
            retry_delay=timedelta(seconds=10),
        )

        build_revenue_journal_body = rail.PythonOperator(
            task_id='build_revenue_journal_body',
            python_callable=build_revenue_journal_body_method,
        )

        # Gate: skip create if no UninvoicedRevenue/UnbilledServices lines matched
        # (build_revenue_journal_body returns None when no matching PSA lines exist)
        is_revenue_journal_ready = rail.IfOperator(
            task_id='is_revenue_journal_ready',
            test=lambda: bool(rail.result('build_revenue_journal_body')),
            yes_task='create_revenue_journal',
            no_task='log_no_revenue_journal_lines',
        )

        log_no_revenue_journal_lines = rail.PythonOperator(
            task_id='log_no_revenue_journal_lines',
            python_callable=lambda: logger.info(
                'Batch %s: no UninvoicedRevenue/UnbilledServices PSA lines — skipping ManualJournal',
                rail.get_current_context()['dag_run'].conf.get('Batch', ''),
            )
        )

        create_revenue_journal = rail.XeroManualJournalOperator(
            task_id='create_revenue_journal',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='create',
            request_body=lambda: rail.result('build_revenue_journal_body'),
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15),
        )

        # ------------------------------------------------------------------ #
        # Error capture
        # ------------------------------------------------------------------ #
        # `catch_processor_dag_error` MUST be the SOLE leaf task so the child
        # run always ends SUCCESS — required by WaitForDagRunsSensor (a FAILED
        # child would short-circuit the gather/watermark path).
        #
        # SkipMixin constraint: never rely on a direct edge from an IfOperator
        # or ForEachOperator to keep catch alive. SkipMixin's _skip_all_except
        # expands branch_task_ids to all flat descendants of the chosen branch
        # before deciding what to skip. When inner ForEach IfOperators run
        # inside BatchTaskRunOperator (context['ti'].task = for_each_invoice),
        # SkipMixin walks for_each_invoice.downstream_task_ids and skips any
        # task NOT reachable from the chosen branch. As long as catch and
        # fetch_revenue_accounts are downstream of log_invoice_done (which sits
        # on every ForEach exit path), they are always reachable and never
        # skipped. The direct edges below ensure both tasks also receive a
        # FAILED/UPSTREAM_FAILED upstream to trigger one_failed when something
        # breaks earlier in the pipeline.

        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
            op_args=[
                '{{ dag_run.conf.Batch }}',
                '{{ get_error_message() }}',
            ],
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #

        # Phase 1: all parallel (no upstream dependencies → start together)

        # Phase 1.5: fetch_ledger_tax_rows needs Period+PostSeq from fetch_invoice_batch
        fetch_invoice_batch >> fetch_ledger_tax_rows

        # Phase 2: group_and_transform_invoices waits for ALL Phase 1 + 1.5 tasks
        [
            fetch_invoice_batch,
            fetch_sales_invoice_master,
            fetch_sales_invoice_detail,
            fetch_account_tax_maps,
            fetch_vp_tax_codes,
            resolve_zero_rate_tax_code,
            fetch_ledger_tax_rows,
        ] >> group_and_transform_invoices

        # Phase 3: ForEach per-invoice
        group_and_transform_invoices >> for_each_invoice
        for_each_invoice >> log_invoice_done  # end_task direct edge
        for_each_invoice >> check_invoice_exists >> is_new_invoice

        is_new_invoice >> rail.Label('Exists') >> skip_existing_invoice >> log_invoice_done
        is_new_invoice >> rail.Label('New') >> is_credit_note
        is_credit_note >> rail.Label('Invoice') >> create_xero_invoice >> log_invoice_done
        (
            is_credit_note >> rail.Label('Credit Note') >>
            create_xero_credit_note >> allocate_credit_note_to_invoice >> log_invoice_done
        )

        # Phase 4: revenue journal after the ForEach loop completes.
        # Route via log_invoice_done (not for_each_invoice directly) so that
        # fetch_revenue_accounts and catch_processor_dag_error become flat
        # relatives of the chosen inner ForEach branch. Airflow's SkipMixin
        # expands branch_task_ids to all descendants of the chosen branch before
        # deciding what to skip; tasks reachable from the chosen branch are kept.
        # log_invoice_done sits on every ForEach exit path, so anything
        # downstream of it is always reachable and never skipped.
        log_invoice_done >> fetch_revenue_accounts
        fetch_revenue_accounts >> is_revenue_configured
        is_revenue_configured >> rail.Label('No') >> log_no_revenue_accounts
        (
            is_revenue_configured >> rail.Label('Yes') >>
            fetch_revenue_psa_rows >> build_revenue_journal_body >> is_revenue_journal_ready
        )
        is_revenue_journal_ready >> rail.Label('Ready') >> create_revenue_journal
        is_revenue_journal_ready >> rail.Label('No lines') >> log_no_revenue_journal_lines

        # Phase 1+2 tasks → catch directly (FAILED upstream triggers one_failed).
        fetch_invoice_batch >> catch_processor_dag_error
        fetch_sales_invoice_master >> catch_processor_dag_error
        fetch_sales_invoice_detail >> catch_processor_dag_error
        fetch_account_tax_maps >> catch_processor_dag_error
        fetch_vp_tax_codes >> catch_processor_dag_error
        resolve_zero_rate_tax_code >> catch_processor_dag_error
        fetch_ledger_tax_rows >> catch_processor_dag_error
        group_and_transform_invoices >> catch_processor_dag_error
        # Phase 3: for_each_invoice → catch (FAILED when inner task fails) and
        # log_invoice_done → catch (makes catch a flat relative of every ForEach
        # inner branch, preventing SkipMixin from marking it SKIPPED).
        for_each_invoice >> catch_processor_dag_error
        log_invoice_done >> catch_processor_dag_error
        # Phase 4 non-IfOperator tasks → catch; all branch terminals connect so
        # catch remains the sole leaf regardless of which Phase 4 path is taken.
        fetch_revenue_accounts >> catch_processor_dag_error
        log_no_revenue_accounts >> catch_processor_dag_error
        fetch_revenue_psa_rows >> catch_processor_dag_error
        build_revenue_journal_body >> catch_processor_dag_error
        log_no_revenue_journal_lines >> catch_processor_dag_error
        create_revenue_journal >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
