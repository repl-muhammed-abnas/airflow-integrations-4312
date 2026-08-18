"""
Worker DAG for QBO -> VP Bill Payment Sync.

Per (payment, linked bill): looks the bill up in the mapping_sync collections
and posts the payment to Vantagepoint on one of two branches —

  PP (vendor payment): the bill matches an `outstanding_purchase_invoices`
    row. Fetch the AP voucher header, resolve the bank code, weight the
    payment across the voucher lines, POST a vendor payment (TransType PP),
    commit it, then write back the outstanding balances.

  EP (employee expense payment): the bill matches an
    `outstanding_employee_expenses` row instead. Only fully-paid bills are
    supported; resolve the bank code, POST an expense payment (TransType EP),
    commit it, then delete the paid rows.

Triggered by the dispatcher DAG with:
  dag_run.conf.PaymentID       — QBO BillPayment Id
  dag_run.conf.BillID          — QBO Bill Id (the linked txn)
  dag_run.conf.VendorRef       — QBO vendor id
  dag_run.conf.BankAccountRef  — QBO bank/deposit account id
  dag_run.conf.TxnDate         — payment date
  dag_run.conf.TotalAmt        — payment total
  dag_run.conf.LineAmount      — amount applied to this bill
  dag_run.conf.connections     — {intuit, vantagepoint} connection ids
  dag_run.conf.customerId      — tenant id (locates the mapping_sync collection)

Replaces Workato recipe 014_503_psa_quickbooks_bill_payment_adds_to_vantagepoint
(both the PP and EP posting branches).
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.bill_payment_sync.utils.python_callable_method import (
    lookup_outstanding_purchase_method,
    lookup_outstanding_expense_method,
    is_bill_fully_paid_method,
    resolve_bank_code_method,
    compute_pp_lines_method,
    build_vendor_payment_body,
    build_pp_post_transaction_body,
    update_outstanding_purchase_method,
    compute_ep_lines_method,
    build_expense_payment_body,
    build_ep_post_transaction_body,
    delete_outstanding_expense_method,
    fail_invoice_not_found_method,
    fail_bank_code_error_method,
    fail_partial_not_supported_method,
    capture_bill_payment_dag_error,
)


def create_dag(config):
    """Per-(payment, bill) worker: collection lookup -> PP or EP post to VP."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_bill_payment_sync_create_{config.instance}',
        description='Post QBO bill payment to Vantagepoint (vendor / expense)',
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs_child,
        schedule_interval=None,
        tags=[
            'vantagepoint_quickbooks', 'bill_payment_sync', 'create_payment'
        ],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        _vp_conn = "{{ dag_run.conf.connections.vantagepoint }}"
        _intuit_conn = "{{ dag_run.conf.connections.intuit }}"

        # ----------------------------------------------------------------- #
        # Phase 1: look the bill up in outstanding_purchase_invoices. The
        # presence of rows decides PP (vendor payment) vs EP (employee
        # expense) — mirrors Workato recipe step 5 (IF invoice found).
        # ----------------------------------------------------------------- #
        lookup_outstanding_purchase = rail.PythonOperator(
            task_id='lookup_outstanding_purchase',
            python_callable=lookup_outstanding_purchase_method
        )

        route_payment_type = rail.IfOperator(
            task_id='route_payment_type',
            test=lambda: len(
                rail.result('lookup_outstanding_purchase') or []
            ) > 0,
            yes_task='get_ap_voucher',
            no_task='lookup_outstanding_expense'
        )

        # ===================== PP (vendor payment) ======================= #

        # AP voucher header (Vendor, Invoice, InvoiceDate, PayTerms, Address)
        # by the Batch from the outstanding row. Workato recipe step 15.
        get_ap_voucher = rail.VantagepointApVoucherOperator(
            task_id='get_ap_voucher',
            vp_conn_id=_vp_conn,
            request_method='GET',
            batch="{{ result('lookup_outstanding_purchase')[0]['Batch'] }}",
            pagination=False,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        # Resolve the VP bank code from bank_code_map (insert-on-miss stub).
        # Workato Resolve Bank Code sub-recipe (step 20).
        resolve_bank_code = rail.PythonOperator(
            task_id='resolve_bank_code',
            python_callable=lambda: resolve_bank_code_method(config.instance)
        )

        check_bank_code = rail.IfOperator(
            task_id='check_bank_code',
            test=lambda: bool(rail.result('resolve_bank_code')),
            yes_task='get_active_period',
            no_task='fail_bank_code_error'
        )

        fail_bank_code_error = rail.PythonOperator(
            task_id='fail_bank_code_error',
            python_callable=lambda: fail_bank_code_error_method(config.instance)
        )

        # Active accounting period — posting Period for the payment.
        get_active_period = rail.VantagepointActiveAccountingPeriodOperator(
            task_id='get_active_period',
            vp_conn_id=_vp_conn,
            request_method='GET',
            pagination=False
        )

        # Weighted allocation across the voucher lines. Workato step 14.
        compute_pp_lines = rail.PythonOperator(
            task_id='compute_pp_lines',
            python_callable=compute_pp_lines_method
        )

        # POST vendor payment (TransType PP). Workato step 31.
        post_vendor_payment = rail.VantagepointVendorPaymentOperator(
            task_id='post_vendor_payment',
            vp_conn_id=_vp_conn,
            request_method='POST',
            request_body=build_vendor_payment_body,
            pagination=False,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        # Commit (post) the PP batch to the ledger.
        post_pp_transaction = rail.VantagepointPostTransactionOperator(
            task_id='post_pp_transaction',
            vp_conn_id=_vp_conn,
            request_method='PUT',
            request_body=build_pp_post_transaction_body,
            pagination=False,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        # Write back outstanding balances (decrement / delete). Workato 27-34.
        update_outstanding = rail.PythonOperator(
            task_id='update_outstanding_purchase',
            python_callable=update_outstanding_purchase_method
        )

        # ================= EP (employee expense payment) ================= #

        lookup_outstanding_expense = rail.PythonOperator(
            task_id='lookup_outstanding_expense',
            python_callable=lookup_outstanding_expense_method
        )

        check_expense_found = rail.IfOperator(
            task_id='check_expense_found',
            test=lambda: len(
                rail.result('lookup_outstanding_expense') or []
            ) > 0,
            yes_task='fetch_qbo_bill',
            no_task='fail_invoice_not_found'
        )

        fail_invoice_not_found = rail.PythonOperator(
            task_id='fail_invoice_not_found',
            python_callable=fail_invoice_not_found_method
        )

        # QBO Bill — needed for the fully-paid gate (Balance == 0).
        fetch_qbo_bill = rail.QuickBooksBillOperator(
            task_id='fetch_qbo_bill',
            intuit_conn_id=_intuit_conn,
            operation='get_bill',
            bill_id="{{ dag_run.conf.BillID }}",
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=10)
        )

        check_expense_fully_paid = rail.IfOperator(
            task_id='check_expense_fully_paid',
            test=is_bill_fully_paid_method,
            yes_task='resolve_bank_code_ep',
            no_task='fail_partial_not_supported'
        )

        fail_partial_not_supported = rail.PythonOperator(
            task_id='fail_partial_not_supported',
            python_callable=fail_partial_not_supported_method
        )

        resolve_bank_code_ep = rail.PythonOperator(
            task_id='resolve_bank_code_ep',
            python_callable=lambda: resolve_bank_code_method(config.instance)
        )

        check_bank_code_ep = rail.IfOperator(
            task_id='check_bank_code_ep',
            test=lambda: bool(rail.result('resolve_bank_code_ep')),
            yes_task='get_active_period_ep',
            no_task='fail_bank_code_error_ep'
        )

        fail_bank_code_error_ep = rail.PythonOperator(
            task_id='fail_bank_code_error_ep',
            python_callable=lambda: fail_bank_code_error_method(config.instance)
        )

        get_active_period_ep = rail.VantagepointActiveAccountingPeriodOperator(
            task_id='get_active_period_ep',
            vp_conn_id=_vp_conn,
            request_method='GET',
            pagination=False
        )

        compute_ep_lines = rail.PythonOperator(
            task_id='compute_ep_lines',
            python_callable=compute_ep_lines_method
        )

        # POST expense payment (TransType EP). Workato step 59.
        post_expense_payment = rail.VantagepointExpensePaymentOperator(
            task_id='post_expense_payment',
            vp_conn_id=_vp_conn,
            request_method='POST',
            request_body=build_expense_payment_body,
            pagination=False,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        post_ep_transaction = rail.VantagepointPostTransactionOperator(
            task_id='post_ep_transaction',
            vp_conn_id=_vp_conn,
            request_method='PUT',
            request_body=build_ep_post_transaction_body,
            pagination=False,
            retries=3,
            retry_exponential_backoff=True,
            retry_delay=timedelta(seconds=15)
        )

        # Delete the paid employee-expense rows. Workato step 60.
        delete_expense = rail.PythonOperator(
            task_id='delete_outstanding_expense',
            python_callable=delete_outstanding_expense_method
        )

        # ----------------------------------------------------------------- #
        # Error capture — always runs (trigger_rule='all_done') so the
        # dispatcher's GatherResultsFromDagRunsOperator can collect failures
        # from any branch. Returns None on a clean run.
        # ----------------------------------------------------------------- #
        catch_bill_payment_dag_error = rail.PythonOperator(
            task_id='catch_bill_payment_dag_error',
            trigger_rule='all_done',
            python_callable=capture_bill_payment_dag_error,
            op_args=[
                '{{ dag_run.conf.PaymentID }}',
                '{{ dag_run.conf.BillID }}',
                '{{ get_error_message() }}'
            ]
        )

        # ----------------------------------------------------------------- #
        # Task graph
        # ----------------------------------------------------------------- #
        lookup_outstanding_purchase >> route_payment_type

        # PP branch
        (
            route_payment_type >> rail.Label('Vendor payment (PP)') >>
            get_ap_voucher >> resolve_bank_code >> check_bank_code
        )
        check_bank_code >> rail.Label('No bank code') >> fail_bank_code_error
        (
            check_bank_code >> rail.Label('Resolved') >>
            get_active_period >> compute_pp_lines >> post_vendor_payment >>
            post_pp_transaction >> update_outstanding
        )

        # EP branch
        (
            route_payment_type >> rail.Label('Employee expense (EP)') >>
            lookup_outstanding_expense >> check_expense_found
        )
        check_expense_found >> rail.Label('Not found') >> fail_invoice_not_found
        (
            check_expense_found >> rail.Label('Found') >>
            fetch_qbo_bill >> check_expense_fully_paid
        )
        (
            check_expense_fully_paid >> rail.Label('Partial') >>
            fail_partial_not_supported
        )
        (
            check_expense_fully_paid >> rail.Label('Fully paid') >>
            resolve_bank_code_ep >> check_bank_code_ep
        )
        (
            check_bank_code_ep >> rail.Label('No bank code') >>
            fail_bank_code_error_ep
        )
        (
            check_bank_code_ep >> rail.Label('Resolved') >>
            get_active_period_ep >> compute_ep_lines >> post_expense_payment >>
            post_ep_transaction >> delete_expense
        )

        # Error capture converges all success / failure paths
        for _task in (
            lookup_outstanding_purchase, get_ap_voucher, resolve_bank_code,
            fail_bank_code_error, get_active_period, compute_pp_lines,
            post_vendor_payment, post_pp_transaction, update_outstanding,
            lookup_outstanding_expense, fail_invoice_not_found, fetch_qbo_bill,
            fail_partial_not_supported, resolve_bank_code_ep,
            fail_bank_code_error_ep, get_active_period_ep, compute_ep_lines,
            post_expense_payment, post_ep_transaction, delete_expense,
        ):
            _task >> catch_bill_payment_dag_error

        return dag


rail.for_each_instance(create_dag)
