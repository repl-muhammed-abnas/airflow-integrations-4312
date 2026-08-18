"""
Worker DAG for QBO -> VP Invoice Payment Sync.

Per-payment: fetches full payment detail from QBO, looks up the matching
Vantagepoint invoice lines, computes weighted payment allocation across WBS
line items, then posts a cash receipt to Vantagepoint and posts it to the ledger.

Triggered by the dispatcher DAG with:
  dag_run.conf.PaymentID   — QBO internal Payment Id
  dag_run.conf.InvoiceID   — QBO Invoice Id (from Payment.Line[0].LinkedTxn[0].TxnId)
  dag_run.conf.connections — {intuit, vantagepoint} connection ids
  dag_run.conf.customerId  — tenant id

Replaces Workato recipes:
  014_503_psa_quickbooks_invoice_payment_adds_to_vantagepoint
  014_503_psa_post_invoice_payment_to_vantagepoint
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.invoice_payment_sync.utils.python_callable_method import (
    build_invoice_filter,
    build_ar_account_filter,
    resolve_bank_code_method,
    compute_payment_lines_method,
    build_cash_receipt_body,
    build_post_transaction_body,
    fail_invoice_not_found_method,
    fail_bank_code_error_method,
    capture_payment_dag_error,
)


def create_dag(config):
    """Per-payment worker: QBO detail fetch -> VP PSA lookup -> cash receipt post."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_invoice_payment_sync_create_{config.instance}',
        description='Post QBO invoice payment as cash receipt to Vantagepoint',
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks', 'invoice_payment_sync', 'create_payment'
        ],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # ------------------------------------------------------------------ #
        # Step 1: Fetch full QBO payment detail
        # Provides: TxnDate, TotalAmt, CustomerRef, DepositToAccountRef,
        #           Line[].Amount, Line[].LinkedTxn[].TxnId
        # Replaces Workato recipe 2 step 2 (GET payment/{PaymentID})
        # ------------------------------------------------------------------ #
        fetch_qbo_payment = rail.QuickBooksPaymentOperator(
            task_id='fetch_qbo_payment',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='get_payment',
            payment_id="{{ dag_run.conf.PaymentID }}",
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # ------------------------------------------------------------------ #
        # Step 2: Fetch QBO invoice to obtain its DocNumber (= VP invoice ref)
        # The InvoiceID in conf is QBO's internal Id; the DocNumber is what VP
        # stores as its Invoice reference in PSALedger.
        # ------------------------------------------------------------------ #
        fetch_qbo_invoice = rail.QuickBooksInvoiceOperator(
            task_id='fetch_qbo_invoice',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='search_invoice',
            query="SELECT * FROM Invoice WHERE Id = '{{ dag_run.conf.InvoiceID }}'",
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # ------------------------------------------------------------------ #
        # Step 3: Fetch VP PSA Ledger invoice lines (TransType IN)
        # Filters by Invoice = QBO DocNumber.
        # Provides: WBS1/2/3, Org, TransactionAmount, TaxBasis, TaxCode,
        #           Batch, PostSeq, Period, SourceExchangeInfo, etc.
        # Replaces Workato recipe 2 steps 7-9 (lookup table + inLanding +
        # psaledger search — in Airflow we query PSALedger directly).
        # ------------------------------------------------------------------ #
        fetch_vp_invoice_lines = rail.VantagepointPsaledgerOperator(
            task_id='fetch_vp_invoice_lines',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            trans_type='IN',
            filters=build_invoice_filter,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # Guard: if no VP ledger rows found for the invoice, error cleanly
        # rather than posting a receipt with no line detail.
        check_vp_invoice_found = rail.IfOperator(
            task_id='check_vp_invoice_found',
            test=lambda: bool(rail.result('fetch_vp_invoice_lines')),
            yes_task='get_active_period',
            no_task='fail_invoice_not_found'
        )

        fail_invoice_not_found = rail.PythonOperator(
            task_id='fail_invoice_not_found',
            python_callable=fail_invoice_not_found_method
        )

        # ------------------------------------------------------------------ #
        # Steps 4-6: Configuration lookups (can run after invoice is found)
        # ------------------------------------------------------------------ #

        # Active accounting period — used as the posting Period for the receipt.
        # Replaces Workato recipe 2 step 5 + recipe 3 step 7.
        get_active_period = rail.VantagepointActiveAccountingPeriodOperator(
            task_id='get_active_period',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            pagination=False
        )

        # VP org codes — first code's length is used to derive the company
        # prefix from the invoice Org field (mirrors Workato recipe 2 step 4).
        fetch_vp_org_codes = rail.VantagepointCodetableRecordsOperator(
            task_id='fetch_vp_org_codes',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            codetable_object='CFGOrgCodes',
            request_method='GET',
            pagination=False
        )

        # ------------------------------------------------------------------ #
        # Step 7: Bank code resolution
        # Maps Payment.DepositToAccountRef.value (QBO Account ID) to the
        # Vantagepoint bank code via Airflow Variable lookup table.
        # Replaces Workato recipe 3 step 3 (Resolve Bank Code sub-recipe).
        # Variable key: psa_vp_qbo_invoice_payment_bank_code_map_{instance}
        # Schema: list[{QBO_Account_ID, Vantagepoint_Code, Company, Org, Account}]
        # ------------------------------------------------------------------ #
        resolve_bank_code = rail.PythonOperator(
            task_id='resolve_bank_code',
            python_callable=lambda: resolve_bank_code_method(config.instance)
        )

        # Stop processing if no bank code mapping found — posting without a
        # valid bank code would create a malformed cash receipt in VP.
        # Mirrors Workato recipe 3 step 4 (if bank code blank → stop).
        check_bank_code_resolved = rail.IfOperator(
            task_id='check_bank_code_resolved',
            test=lambda: bool(rail.result('resolve_bank_code')),
            yes_task='compute_payment_lines',
            no_task='fail_bank_code_error'
        )

        fail_bank_code_error = rail.PythonOperator(
            task_id='fail_bank_code_error',
            python_callable=lambda: fail_bank_code_error_method(config.instance)
        )

        # ------------------------------------------------------------------ #
        # Step 8: Weighted payment line computation
        # Distributes the payment amount proportionally across PSA ledger
        # line items by TransactionAmount weighting.
        # Replaces Workato recipe 2 step 12 (smart list SQL query).
        # ------------------------------------------------------------------ #
        compute_payment_lines = rail.PythonOperator(
            task_id='compute_payment_lines',
            python_callable=compute_payment_lines_method
        )

        # ------------------------------------------------------------------ #
        # Step 9: Fetch AR account code
        # GET vision/AccountConfiguration/CFGAutoPosting?company={Company}
        # Returns AcctsReceivable used in crDetail.Account.
        # Replaces Workato recipe 2 step 14 (Get Accounts Receivable Code).
        # ------------------------------------------------------------------ #
        fetch_ar_account = rail.VantagepointAPIOperator(
            task_id='fetch_ar_account',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/AccountConfiguration/CFGAutoPosting',
            request_method='GET',
            filters=build_ar_account_filter,
            pagination=False
        )

        # ------------------------------------------------------------------ #
        # Step 10: POST cash receipt to Vantagepoint
        # crMaster holds the batch header; crDetail holds per-WBS line items.
        # Replaces Workato recipe 3 step 9 (cash_receipt action).
        # ------------------------------------------------------------------ #
        post_cash_receipt = rail.VantagepointCashReceiptOperator(
            task_id='post_cash_receipt',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='POST',
            request_body=build_cash_receipt_body,
            pagination=False,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        # ------------------------------------------------------------------ #
        # Step 11: Post (commit) the cash receipt batch to the ledger
        # transtype=CR, batch from the posted receipt.
        # Replaces Workato recipe 3 step 10 (post_transaction_entries).
        # ------------------------------------------------------------------ #
        post_transaction_entries = rail.VantagepointPostTransactionOperator(
            task_id='post_transaction_entries',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='PUT',
            request_body=build_post_transaction_body,
            pagination=False,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        # ------------------------------------------------------------------ #
        # Error capture — always runs (trigger_rule='all_done') so the
        # dispatcher's GatherResultsFromDagRunsOperator can collect failures
        # from any branch. Returns None on clean run.
        # ------------------------------------------------------------------ #
        catch_payment_dag_error = rail.PythonOperator(
            task_id='catch_payment_dag_error',
            trigger_rule='all_done',
            python_callable=capture_payment_dag_error,
            op_args=[
                '{{ dag_run.conf.PaymentID }}',
                '{{ dag_run.conf.InvoiceID }}',
                '{{ get_error_message() }}'
            ]
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #

        # Phase 1: parallel data fetches
        [fetch_qbo_payment, fetch_qbo_invoice] >> fetch_vp_invoice_lines
        fetch_vp_invoice_lines >> check_vp_invoice_found

        check_vp_invoice_found >> rail.Label('Not found') >> fail_invoice_not_found
        fail_invoice_not_found >> catch_payment_dag_error

        # Phase 2: configuration lookups (after invoice confirmed) — chained
        # sequentially behind the 'Found' label. IfOperator only activates
        # yes_task/no_task; any other direct downstream is skipped regardless
        # of which branch fires, so fetch_vp_org_codes must be gated through
        # get_active_period rather than hanging directly off the IfOperator.
        (
            check_vp_invoice_found >> rail.Label('Found') >>
            get_active_period >> fetch_vp_org_codes
        )

        # Phase 3: bank code + weighted payment
        fetch_vp_org_codes >> resolve_bank_code
        resolve_bank_code >> check_bank_code_resolved

        check_bank_code_resolved >> rail.Label('No bank code') >> fail_bank_code_error
        fail_bank_code_error >> catch_payment_dag_error

        check_bank_code_resolved >> rail.Label('Resolved') >> compute_payment_lines
        compute_payment_lines >> fetch_ar_account

        # Phase 4: post to VP
        fetch_ar_account >> post_cash_receipt >> post_transaction_entries

        # Error capture converges all success/failure paths
        fetch_qbo_payment >> catch_payment_dag_error
        fetch_qbo_invoice >> catch_payment_dag_error
        fetch_vp_invoice_lines >> catch_payment_dag_error
        get_active_period >> catch_payment_dag_error
        fetch_vp_org_codes >> catch_payment_dag_error
        resolve_bank_code >> catch_payment_dag_error
        compute_payment_lines >> catch_payment_dag_error
        fetch_ar_account >> catch_payment_dag_error
        post_cash_receipt >> catch_payment_dag_error
        post_transaction_entries >> catch_payment_dag_error

        return dag


rail.for_each_instance(create_dag)
