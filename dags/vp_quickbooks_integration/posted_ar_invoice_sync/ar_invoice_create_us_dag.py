"""
US Worker DAG for VP PSA -> QBO Posted AR Invoice Sync.

Per-batch: fetches all PSA Ledger 'IN' rows for the given Batch, resolves
project clients and firm mappings, searches or creates the QBO 'Sales'
product item, then iterates every invoice in the batch via ForEachOperator:
  - Checks Airflow Variable outstanding_sales_invoices for duplicates
  - Builds the QBO invoice/credit-memo body (US tax logic)
  - POSTs to QuickBooks via QuickBooksInvoiceOperator
  - Upserts Airflow Variable outstanding_sales_invoices
After the per-invoice loop, posts a revenue-recognition JournalEntry to QBO.

US tax logic: each invoice line carries its own TaxCodeRef sourced directly
from the PSA Ledger TaxCode field.

Triggered by router_dag with:
  dag_run.conf.Batch       — VP AR invoice batch number
  dag_run.conf.PostDate    — batch post date (informational)
  dag_run.conf.connections — {vantagepoint, intuit} connection ids
  dag_run.conf.customerId  — tenant id

Replaces Workato recipe:
  014-503 PSA Vantagepoint AR Invoice Exports to QuickBooks US
  014-503 PSA Post Invoice to QuickBooks US
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.posted_ar_invoice_sync.utils.python_callable_method import (  # noqa: E501
    build_invoice_batch_filter,
    build_project_filter,
    check_sales_item_exists_method,
    build_sales_item_body,
    resolve_product_id_method,
    get_firm_mapping_method,
    group_invoices_method,
    check_invoice_exists_method,
    is_new_invoice_method,
    build_invoice_body_us_method,
    is_credit_memo_method,
    get_invoice_body_for_create,
    update_outstanding_invoice_pg_method,
    build_revenue_journal_entry_body_method,
    capture_create_dag_error,
)


def create_dag(config):
    """Per-batch US worker: resolve mappings + post QBO invoices + revenue journal entry."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_ar_invoice_sync_create_us_{config.instance}',
        description=(
            'Post VP PSA AR invoice batch to QBO using US per-line tax logic '
            'and record revenue recognition JournalEntry.'
        ),
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks',
            'ar_invoice_sync',
            'create',
            'us',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            ),
        }
    ) as dag:

        # ------------------------------------------------------------------ #
        # Phase 1: Parallel data fetches
        # ------------------------------------------------------------------ #

        fetch_invoice_batch = rail.VantagepointPsaledgerOperator(
            task_id='fetch_invoice_batch',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            filters=build_invoice_batch_filter,
            trans_type='IN',
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        fetch_project_clients = rail.VantagepointProjectOperator(
            task_id='fetch_project_clients',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            filters=build_project_filter,
            pagination=False
        )

        fetch_firm_mapping = rail.PythonOperator(
            task_id='fetch_firm_mapping',
            python_callable=lambda: get_firm_mapping_method(config.instance)
        )

        # ------------------------------------------------------------------ #
        # Phase 2: QBO Sales product/service item get-or-create
        # ------------------------------------------------------------------ #

        search_sales_item = rail.QuickBooksItemOperator(
            task_id='search_sales_item',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='search_item',
            query="select * from Item WHERE Name = 'Sales'",
            retries=2,
            retry_delay=timedelta(seconds=10)
        )

        check_sales_item_exists = rail.IfOperator(
            task_id='check_sales_item_exists',
            test=check_sales_item_exists_method,
            yes_task='resolve_product_id',
            no_task='create_sales_item'
        )

        create_sales_item = rail.QuickBooksItemOperator(
            task_id='create_sales_item',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='create_item',
            request_body=build_sales_item_body,
            retries=2,
            retry_delay=timedelta(seconds=10)
        )

        resolve_product_id = rail.PythonOperator(
            task_id='resolve_product_id',
            trigger_rule='none_failed_min_one_success',
            python_callable=resolve_product_id_method
        )

        # ------------------------------------------------------------------ #
        # Phase 3: Group PSA Ledger rows into per-invoice structures
        # ------------------------------------------------------------------ #

        group_invoices = rail.PythonOperator(
            task_id='group_invoices',
            python_callable=group_invoices_method
        )

        # ------------------------------------------------------------------ #
        # Phase 4: Per-invoice ForEach loop
        # ------------------------------------------------------------------ #

        for_each_invoice = rail.ForEachOperator(
            task_id='for_each_invoice',
            items=lambda: rail.result('group_invoices'),
            start_task='check_invoice_exists',
            end_task='log_invoice_done'
        )

        check_invoice_exists = rail.PythonOperator(
            task_id='check_invoice_exists',
            python_callable=check_invoice_exists_method
        )

        check_is_new_invoice = rail.IfOperator(
            task_id='check_is_new_invoice',
            test=is_new_invoice_method,
            yes_task='build_invoice_body',
            no_task='skip_invoice_existing'
        )

        skip_invoice_existing = rail.EmptyOperator(
            task_id='skip_invoice_existing'
        )

        build_invoice_body = rail.PythonOperator(
            task_id='build_invoice_body',
            python_callable=build_invoice_body_us_method
        )

        check_is_credit_memo = rail.IfOperator(
            task_id='check_is_credit_memo',
            test=is_credit_memo_method,
            yes_task='create_qbo_credit_memo',
            no_task='create_qbo_invoice'
        )

        create_qbo_invoice = rail.QuickBooksInvoiceOperator(
            task_id='create_qbo_invoice',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='create_invoice',
            request_body=get_invoice_body_for_create,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        create_qbo_credit_memo = rail.QuickBooksInvoiceOperator(
            task_id='create_qbo_credit_memo',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='create_credit_memo',
            request_body=get_invoice_body_for_create,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        update_outstanding_invoice_pg = rail.PythonOperator(
            task_id='update_outstanding_invoice_pg',
            trigger_rule='none_failed_min_one_success',
            python_callable=update_outstanding_invoice_pg_method
        )

        log_invoice_done = rail.EmptyOperator(
            task_id='log_invoice_done',
            trigger_rule='none_failed_min_one_success'
        )

        # ------------------------------------------------------------------ #
        # Phase 5: Revenue recognition JournalEntry
        # ------------------------------------------------------------------ #

        fetch_revenue_accounts = rail.VantagepointAPIOperator(
            task_id='fetch_revenue_accounts',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/AccountingCompanyConfig/CFGAutoPosting',
            request_method='GET',
            pagination=False,
            retries=2,
            retry_delay=timedelta(seconds=10)
        )

        build_revenue_journal_entry_body = rail.PythonOperator(
            task_id='build_revenue_journal_entry_body',
            python_callable=build_revenue_journal_entry_body_method
        )

        create_revenue_journal_entry = rail.QuickBooksJournalEntryOperator(
            task_id='create_revenue_journal_entry',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            request_body=lambda: rail.result('build_revenue_journal_entry_body'),
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        # ------------------------------------------------------------------ #
        # Error capture
        # ------------------------------------------------------------------ #

        catch_create_dag_error = rail.PythonOperator(
            task_id='catch_create_dag_error',
            trigger_rule='all_done',
            python_callable=capture_create_dag_error,
            op_args=[
                '{{ dag_run.conf.Batch }}',
                '{{ get_error_message() }}'
            ]
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #

        # Phase 1: parallel data fetches
        fetch_invoice_batch >> fetch_project_clients

        # Phase 2: Sales item resolution (waits for project clients + firm mapping)
        [fetch_project_clients, fetch_firm_mapping] >> search_sales_item
        search_sales_item >> check_sales_item_exists
        check_sales_item_exists >> rail.Label('Found') >> resolve_product_id
        check_sales_item_exists >> rail.Label('Not found') >> create_sales_item >> resolve_product_id

        # Phase 3: group invoices
        [fetch_invoice_batch, resolve_product_id] >> group_invoices

        # Phase 4: ForEach invoice loop
        group_invoices >> for_each_invoice
        for_each_invoice >> log_invoice_done
        for_each_invoice >> check_invoice_exists >> check_is_new_invoice
        check_is_new_invoice >> rail.Label('Exists') >> skip_invoice_existing >> log_invoice_done
        check_is_new_invoice >> rail.Label('New') >> build_invoice_body >> check_is_credit_memo
        (
            check_is_credit_memo >> rail.Label('Invoice') >>
            create_qbo_invoice >> update_outstanding_invoice_pg
        )
        (
            check_is_credit_memo >> rail.Label('Credit Memo') >>
            create_qbo_credit_memo >> update_outstanding_invoice_pg
        )
        update_outstanding_invoice_pg >> log_invoice_done

        # Phase 5: revenue generation after loop
        (
            for_each_invoice >> fetch_revenue_accounts >>
            build_revenue_journal_entry_body >> create_revenue_journal_entry
        )

        # Error capture receives all terminal paths
        fetch_invoice_batch >> catch_create_dag_error
        fetch_project_clients >> catch_create_dag_error
        fetch_firm_mapping >> catch_create_dag_error
        resolve_product_id >> catch_create_dag_error
        group_invoices >> catch_create_dag_error
        for_each_invoice >> catch_create_dag_error
        create_revenue_journal_entry >> catch_create_dag_error

        return dag


rail.for_each_instance(create_dag)
