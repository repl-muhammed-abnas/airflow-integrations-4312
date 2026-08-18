"""
Bill Payment Processor DAG for Xero -> VP Payment Sync.

Per-payment: processes one ACCPAYPAYMENT (Xero bill payment) into either a
Vantagepoint Vendor Payment (PP — AP Voucher path) or an Expense Payment (EP).
Mirrors the Workato callable recipe
`014_501_psa_xero_bill_payment_adds_to_vantagepoint` step-by-step.

CRITICAL: There is NO PostTransFile / VantagepointPostTransactionOperator in
this DAG. Both `post_vendor_payment` (PP) and `post_expense_payment` (EP) are
self-contained create-and-post VP calls. This is Workato-recipe parity.

AP Voucher path (PP) — Workato steps 3-39:
  lookup_outstanding_purchase        [step 4]   S3 outstanding_purchase_invoices
  is_ap_voucher_payment              [step 5]   IF rows found
  fetch_xero_invoice_for_pp          [step 6]   GET Xero invoice
  fetch_xero_payment_for_pp          [step 7]   GET Xero payment
  build_outstanding_voucher_lines    [step 9]   Attach payment amount to S3 rows
  compute_pp_weighted_lines          [step 11]  Pro-rata weighting
  fetch_vp_ap_voucher                [step 12]  GET VP AP Voucher
  is_ap_voucher_found                [step 13]  IF AP Voucher Batch found
  fetch_vp_periods_ap                [step 15]  GET VP Periods
  find_period_ap                     [steps 16-17] Find period
  is_period_found_ap                 [steps 18-20] IF period found
  resolve_bank_code_ap               [step 21]  Resolve bank code
  is_bank_resolved_ap                [steps 22-24] IF bank resolved
  lookup_firm_for_ap                 [step 26]  S3 map_firm
  fetch_vp_firm_for_ap               [step 27]  GET VP Firm
  build_pp_payload                   [steps 28-35] APPPCHECKS + FOREACH balance
  post_vendor_payment                [step 36]  POST VP Vendor Payment (NO PostTrans)
  update_outstanding_ap              [steps 37-38] S3 balance updates
  delete_fully_paid_purchase_invoices [step 39] S3 DELETE if fully paid

Expense path (EP) — Workato steps 40-87 (ELSE of step 5):
  lookup_outstanding_expense         [step 41]  S3 outstanding_employee_expenses
  is_expense_payment                 [step 42]  IF rows found
  fetch_xero_invoice_for_ep          [step 43]  GET Xero invoice
  fetch_xero_payment_for_ep          [step 44]  GET Xero payment
  compute_total_bill_payments        [steps 46-49] SUM Xero payments
  build_outstanding_expense_lines    [steps 50-51] Attach payment amount
  compute_ep_weighted_lines          [step 52]  Pro-rata weighting
  fetch_vp_employee                  [step 54]  GET VP Employee
  fetch_vp_periods_ep                [steps 55-56] GET VP Periods
  find_period_ep                     [step 57]  Find period
  is_period_found_ep                 [steps 58-60] IF period found
  resolve_bank_code_ep               [step 63]  Resolve bank; miss -> bank_error flag
  build_ep_payload                   [steps 68-75] EXCHECKS + FOREACH balance
  is_not_fully_paid_ep               [steps 76-78] IF NOT fully paid -> graceful stop
  build_grouped_ep_payments          [steps 79-80] GROUP BY
  has_compound_errors_ep             [steps 81-83] IF bank_error -> graceful stop
  post_expense_payment               [step 84]  POST VP Expense Payment (NO PostTrans)
  update_outstanding_ep              [step 84 post] S3 balance updates
  delete_fully_paid_expense_entries  [steps 85-87] S3 DELETE if fully paid
  catch_processor_dag_error          [sole leaf, trigger_rule=one_failed]
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration.common.python_callable_method import collection_rows
from vp_xero_integration.common.tables import (
    OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
    OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
    OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
)
from vp_xero_integration.xero_to_vp_payment_sync.utils.python_callable_method import (
    find_period_for_payment_method,
    fetch_full_ap_voucher_method,
    build_outstanding_voucher_lines_method,
    compute_pp_weighted_lines_method,
    resolve_bank_code_ap_method,
    lookup_firm_for_ap_method,
    build_pp_payload_method,
    build_pp_request_body_method,
    update_outstanding_ap_method,
    delete_fully_paid_purchase_invoices_method,
    compute_total_bill_payments_method,
    build_outstanding_expense_lines_method,
    compute_ep_weighted_lines_method,
    resolve_bank_code_ep_method,
    build_ep_payload_method,
    is_not_fully_paid_ep_method,
    build_grouped_ep_payments_method,
    has_compound_errors_ep_method,
    build_ep_request_body_method,
    update_outstanding_ep_method,
    delete_fully_paid_expense_entries_method,
    capture_processor_error,
)

logger = logging.getLogger(__name__)


def _unwrap_ap_voucher(result):
    if isinstance(result, list):
        return (result or [{}])[0]
    if isinstance(result, dict) and result.get('rows'):
        return (result['rows'] or [{}])[0]
    return result or {}




def create_dag(config):
    """Per-payment bill processor: ACCPAYPAYMENT -> VP Vendor Payment (PP) or Expense Payment (EP)."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_bill_payment_processor_{config.instance}',
        description=(
            'Process one Xero bill payment (ACCPAYPAYMENT) into a VP Vendor Payment (PP) '
            'or Expense Payment (EP)'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'payment_sync', 'bill_payment_processor'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        vp_conn_id = (
            "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
        )
        xero_conn_id = (
            "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
        )



        # [step 4] Search outstanding_purchase_invoices WHERE InvoiceID
        lookup_outstanding_purchase = rail.PythonOperator(
            task_id='lookup_outstanding_purchase',
            python_callable=lambda: collection_rows(
                OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
                OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
                'InvoiceID = ?',
                [rail.get_current_context()['dag_run'].conf.get('InvoiceID', '')],
                read_task_id='_read_outstanding_purchase_lookup',
            )
        )

        # [step 5] IF purchase invoice rows found -> PP path; else -> EP path
        is_ap_voucher_payment = rail.IfOperator(
            task_id='is_ap_voucher_payment',
            test=lambda: len(rail.result('lookup_outstanding_purchase') or []) > 0,
            yes_task='fetch_xero_invoice_for_pp',
            no_task='lookup_outstanding_expense',
        )

        # ================================================================== #
        #  AP VOUCHER PATH (PP) — steps 6-39
        # ================================================================== #

        # [step 6] GET Xero invoice by InvoiceID
        fetch_xero_invoice_for_pp = rail.XeroInvoiceOperator(
            task_id='fetch_xero_invoice_for_pp',
            xero_conn_id=xero_conn_id,
            operation='get_by_id',
            record_id="{{ dag_run.conf.get('InvoiceID') }}",
        )

        # [step 7] GET Xero payment by PaymentID
        fetch_xero_payment_for_pp = rail.XeroPaymentOperator(
            task_id='fetch_xero_payment_for_pp',
            xero_conn_id=xero_conn_id,
            operation='get_by_id',
            record_id="{{ dag_run.conf.get('PaymentID') }}",
        )

        # [step 9] Attach Total_Payment_Amount to each outstanding row
        build_outstanding_voucher_lines = rail.PythonOperator(
            task_id='build_outstanding_voucher_lines',
            python_callable=build_outstanding_voucher_lines_method,
        )

        # [step 11] Pro-rata weighting
        compute_pp_weighted_lines = rail.PythonOperator(
            task_id='compute_pp_weighted_lines',
            python_callable=compute_pp_weighted_lines_method,
        )

        # [step 12] GET VP AP Voucher — combines /apControl, /apMaster, /apDetail inline
        # Returns Workato-style nested dict: {Batch, Total, ..., apMaster: [{Invoice, ..., apDetail: [...]}]}
        fetch_vp_ap_voucher = rail.PythonOperator(
            task_id='fetch_vp_ap_voucher',
            python_callable=fetch_full_ap_voucher_method,
            retries=3,
            retry_exponential_backoff=True,
        )

        # [step 13] IF AP Voucher found
        is_ap_voucher_found = rail.IfOperator(
            task_id='is_ap_voucher_found',
            test=lambda: (
                lambda v: bool(v.get('Batch') or v.get('InvoiceNo'))
            )(_unwrap_ap_voucher(rail.result('fetch_vp_ap_voucher'))),
            yes_task='fetch_vp_periods_ap',
            no_task='log_ap_voucher_not_found',
        )

        log_ap_voucher_not_found = rail.PythonOperator(
            task_id='log_ap_voucher_not_found',
            python_callable=lambda: logger.warning(
                "AP Voucher not found in VP for PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [step 15] GET all VP Periods
        fetch_vp_periods_ap = rail.VantagepointAPIOperator(
            task_id='fetch_vp_periods_ap',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            endpoint='/Settings/Period',
        )

        # [steps 16-17] Find period for payment date
        find_period_ap = rail.PythonOperator(
            task_id='find_period_ap',
            python_callable=lambda: find_period_for_payment_method(
                'fetch_xero_payment_for_pp', 'fetch_vp_periods_ap'
            ),
        )

        # [steps 18-20] IF period found
        is_period_found_ap = rail.IfOperator(
            task_id='is_period_found_ap',
            test=lambda: rail.result('find_period_ap') is not None,
            yes_task='resolve_bank_code_ap',
            no_task='log_period_not_found_ap',
        )

        log_period_not_found_ap = rail.PythonOperator(
            task_id='log_period_not_found_ap',
            python_callable=lambda: logger.warning(
                "VP period not found for PP PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [step 21] Resolve bank code
        resolve_bank_code_ap = rail.PythonOperator(
            task_id='resolve_bank_code_ap',
            python_callable=resolve_bank_code_ap_method,
        )

        # [steps 22-24] IF bank resolved
        is_bank_resolved_ap = rail.IfOperator(
            task_id='is_bank_resolved_ap',
            test=lambda: bool(
                (rail.result('resolve_bank_code_ap') or {}).get('VantagepointCode')
            ),
            yes_task='lookup_firm_for_ap',
            no_task='log_bank_not_found_ap',
        )

        log_bank_not_found_ap = rail.PythonOperator(
            task_id='log_bank_not_found_ap',
            python_callable=lambda: logger.warning(
                "Bank code not resolved for PP PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [step 26] Search map_firm WHERE ContactID
        lookup_firm_for_ap = rail.PythonOperator(
            task_id='lookup_firm_for_ap',
            python_callable=lookup_firm_for_ap_method,
        )

        # [step 27] GET VP Firm by FirmID -> get Vendor field
        fetch_vp_firm_for_ap = rail.VantagepointFirmOperator(
            task_id='fetch_vp_firm_for_ap',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            client_id="{{ (result('lookup_firm_for_ap') or {}).get('FirmID', '') }}",
            pagination=False,
        )

        # [steps 28-35] FOREACH: build APPPCHECKS + track balance/FullyPaid
        build_pp_payload = rail.PythonOperator(
            task_id='build_pp_payload',
            python_callable=build_pp_payload_method,
        )

        # [step 36] POST VP Vendor Payment — NO PostTransFile
        post_vendor_payment = rail.VantagepointVendorPaymentOperator(
            task_id='post_vendor_payment',
            vp_conn_id=vp_conn_id,
            request_method='POST',
            request_body=build_pp_request_body_method,
            retries=3,
            retry_exponential_backoff=True,
        )

        # [steps 37-38] Update S3 outstanding balances
        update_outstanding_ap = rail.PythonOperator(
            task_id='update_outstanding_ap',
            python_callable=update_outstanding_ap_method,
        )

        # [step 39] DELETE fully paid rows from S3
        delete_fully_paid_purchase_invoices = rail.PythonOperator(
            task_id='delete_fully_paid_purchase_invoices',
            python_callable=delete_fully_paid_purchase_invoices_method,
        )

        # ================================================================== #
        #  EXPENSE PATH (EP) — steps 40-87
        # ================================================================== #

        # [step 41] Search outstanding_employee_expenses WHERE InvoiceID
        lookup_outstanding_expense = rail.PythonOperator(
            task_id='lookup_outstanding_expense',
            python_callable=lambda: collection_rows(
                OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
                OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
                'InvoiceID = ?',
                [rail.get_current_context()['dag_run'].conf.get('InvoiceID', '')],
                read_task_id='_read_outstanding_expense_lookup',
            )
        )

        # [step 42] IF expense rows found
        is_expense_payment = rail.IfOperator(
            task_id='is_expense_payment',
            test=lambda: len(rail.result('lookup_outstanding_expense') or []) > 0,
            yes_task='fetch_xero_invoice_for_ep',
            no_task='log_no_matching_payment',
        )

        log_no_matching_payment = rail.PythonOperator(
            task_id='log_no_matching_payment',
            python_callable=lambda: logger.info(
                "No outstanding AP or expense rows for InvoiceID=%s — Xero-native bill, "
                "graceful skip",
                rail.get_current_context()['dag_run'].conf.get('InvoiceID'),
            )
        )

        # [step 43] GET Xero invoice by InvoiceID
        fetch_xero_invoice_for_ep = rail.XeroInvoiceOperator(
            task_id='fetch_xero_invoice_for_ep',
            xero_conn_id=xero_conn_id,
            operation='get_by_id',
            record_id="{{ dag_run.conf.get('InvoiceID') }}",
        )

        # [step 44] GET Xero payment by PaymentID
        fetch_xero_payment_for_ep = rail.XeroPaymentOperator(
            task_id='fetch_xero_payment_for_ep',
            xero_conn_id=xero_conn_id,
            operation='get_by_id',
            record_id="{{ dag_run.conf.get('PaymentID') }}",
        )

        # [steps 46-49] SUM Xero bill payments; partial-payment gate deferred to is_not_fully_paid_ep
        compute_total_bill_payments = rail.PythonOperator(
            task_id='compute_total_bill_payments',
            python_callable=compute_total_bill_payments_method,
        )

        # [steps 50-51] Attach TotalPayments to each expense row
        build_outstanding_expense_lines = rail.PythonOperator(
            task_id='build_outstanding_expense_lines',
            python_callable=build_outstanding_expense_lines_method,
        )

        # [step 52] Pro-rata weighting (includes Period, Employee)
        compute_ep_weighted_lines = rail.PythonOperator(
            task_id='compute_ep_weighted_lines',
            python_callable=compute_ep_weighted_lines_method,
        )

        # [step 54] GET VP Employee
        fetch_vp_employee = rail.VantagepointEmployeeOperator(
            task_id='fetch_vp_employee',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            employee=(
                "{{ (result('lookup_outstanding_expense') or [{}])[0].get('Employee', '') }}"
            ),
        )

        # [steps 55-56] GET all VP Periods
        fetch_vp_periods_ep = rail.VantagepointAPIOperator(
            task_id='fetch_vp_periods_ep',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            endpoint='/Settings/Period',
        )

        # [step 57] Find period for payment date
        find_period_ep = rail.PythonOperator(
            task_id='find_period_ep',
            python_callable=lambda: find_period_for_payment_method(
                'fetch_xero_payment_for_ep', 'fetch_vp_periods_ep'
            ),
        )

        # [steps 58-60] IF period found
        is_period_found_ep = rail.IfOperator(
            task_id='is_period_found_ep',
            test=lambda: rail.result('find_period_ep') is not None,
            yes_task='resolve_bank_code_ep',
            no_task='log_period_not_found_ep',
        )

        log_period_not_found_ep = rail.PythonOperator(
            task_id='log_period_not_found_ep',
            python_callable=lambda: logger.warning(
                "VP period not found for EP PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [step 63] Resolve bank code; miss -> {bank_error: True}, NO IfOperator here
        resolve_bank_code_ep = rail.PythonOperator(
            task_id='resolve_bank_code_ep',
            python_callable=resolve_bank_code_ep_method,
        )

        # [steps 68-75] FOREACH: build EXCHECKS + track balance/FullyPaid
        build_ep_payload = rail.PythonOperator(
            task_id='build_ep_payload',
            python_callable=build_ep_payload_method,
        )

        # [steps 76-78] IF NOT fully paid -> graceful stop (covers partial payment)
        is_not_fully_paid_ep = rail.IfOperator(
            task_id='is_not_fully_paid_ep',
            test=is_not_fully_paid_ep_method,
            yes_task='log_partial_expense_not_supported',
            no_task='build_grouped_ep_payments',
        )

        log_partial_expense_not_supported = rail.PythonOperator(
            task_id='log_partial_expense_not_supported',
            python_callable=lambda: logger.warning(
                "Expense payment not fully paid for PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [steps 79-80] GROUP BY Period/Employee/Voucher/Org/Company/CheckNo; SUM(Amount)
        build_grouped_ep_payments = rail.PythonOperator(
            task_id='build_grouped_ep_payments',
            python_callable=build_grouped_ep_payments_method,
        )

        # [steps 81-83] IF CompoundError (bank not found) -> graceful stop
        has_compound_errors_ep = rail.IfOperator(
            task_id='has_compound_errors_ep',
            test=has_compound_errors_ep_method,
            yes_task='log_compound_errors_ep',
            no_task='post_expense_payment',
        )

        log_compound_errors_ep = rail.PythonOperator(
            task_id='log_compound_errors_ep',
            python_callable=lambda: logger.warning(
                "EP compound error (bank not resolved) for PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [step 84] POST VP Expense Payment — NO PostTransFile
        post_expense_payment = rail.VantagepointExpensePaymentOperator(
            task_id='post_expense_payment',
            vp_conn_id=vp_conn_id,
            request_method='POST',
            request_body=build_ep_request_body_method,
            retries=3,
            retry_exponential_backoff=True,
        )

        # [step 84 post] Update S3 outstanding balances
        update_outstanding_ep = rail.PythonOperator(
            task_id='update_outstanding_ep',
            python_callable=update_outstanding_ep_method,
        )

        # [steps 85-87] DELETE fully paid rows from S3
        delete_fully_paid_expense_entries = rail.PythonOperator(
            task_id='delete_fully_paid_expense_entries',
            python_callable=delete_fully_paid_expense_entries_method,
        )

        # Sole leaf — catches any upstream failure; result gathered by dispatcher
        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #
        lookup_outstanding_purchase >> is_ap_voucher_payment

        # PP path
        (
            is_ap_voucher_payment >> rail.Label('AP Voucher') >>
            fetch_xero_invoice_for_pp >>
            fetch_xero_payment_for_pp >>
            build_outstanding_voucher_lines >>
            compute_pp_weighted_lines >>
            fetch_vp_ap_voucher >>
            is_ap_voucher_found
        )

        is_ap_voucher_found >> rail.Label('Not found') >> log_ap_voucher_not_found

        (
            is_ap_voucher_found >> rail.Label('Found') >>
            fetch_vp_periods_ap >>
            find_period_ap >>
            is_period_found_ap
        )

        is_period_found_ap >> rail.Label('No period') >> log_period_not_found_ap

        (
            is_period_found_ap >> rail.Label('Found') >>
            resolve_bank_code_ap >>
            is_bank_resolved_ap
        )

        is_bank_resolved_ap >> rail.Label('No bank') >> log_bank_not_found_ap

        (
            is_bank_resolved_ap >> rail.Label('Resolved') >>
            lookup_firm_for_ap >>
            fetch_vp_firm_for_ap >>
            build_pp_payload >>
            post_vendor_payment >>
            update_outstanding_ap >>
            delete_fully_paid_purchase_invoices
        )

        # EP path
        (
            is_ap_voucher_payment >> rail.Label('No AP') >>
            lookup_outstanding_expense >>
            is_expense_payment
        )

        is_expense_payment >> rail.Label('No expense') >> log_no_matching_payment

        (
            is_expense_payment >> rail.Label('Found') >>
            fetch_xero_invoice_for_ep >>
            fetch_xero_payment_for_ep >>
            compute_total_bill_payments >>
            build_outstanding_expense_lines >>
            compute_ep_weighted_lines >>
            fetch_vp_employee >>
            fetch_vp_periods_ep >>
            find_period_ep >>
            is_period_found_ep
        )

        is_period_found_ep >> rail.Label('No period') >> log_period_not_found_ep

        (
            is_period_found_ep >> rail.Label('Found') >>
            resolve_bank_code_ep >>
            build_ep_payload >>
            is_not_fully_paid_ep
        )

        is_not_fully_paid_ep >> rail.Label('Partial') >> log_partial_expense_not_supported

        (
            is_not_fully_paid_ep >> rail.Label('Fully paid') >>
            build_grouped_ep_payments >>
            has_compound_errors_ep
        )

        has_compound_errors_ep >> rail.Label('Errors') >> log_compound_errors_ep

        (
            has_compound_errors_ep >> rail.Label('Clean') >>
            post_expense_payment >>
            update_outstanding_ep >>
            delete_fully_paid_expense_entries
        )

        # Every work task -> catch_processor_dag_error (sole leaf)
        lookup_outstanding_purchase >> catch_processor_dag_error
        # PP path tasks
        fetch_xero_invoice_for_pp >> catch_processor_dag_error
        fetch_xero_payment_for_pp >> catch_processor_dag_error
        build_outstanding_voucher_lines >> catch_processor_dag_error
        compute_pp_weighted_lines >> catch_processor_dag_error
        fetch_vp_ap_voucher >> catch_processor_dag_error
        log_ap_voucher_not_found >> catch_processor_dag_error
        fetch_vp_periods_ap >> catch_processor_dag_error
        find_period_ap >> catch_processor_dag_error
        log_period_not_found_ap >> catch_processor_dag_error
        resolve_bank_code_ap >> catch_processor_dag_error
        log_bank_not_found_ap >> catch_processor_dag_error
        lookup_firm_for_ap >> catch_processor_dag_error
        fetch_vp_firm_for_ap >> catch_processor_dag_error
        build_pp_payload >> catch_processor_dag_error
        post_vendor_payment >> catch_processor_dag_error
        update_outstanding_ap >> catch_processor_dag_error
        delete_fully_paid_purchase_invoices >> catch_processor_dag_error
        # EP path tasks
        lookup_outstanding_expense >> catch_processor_dag_error
        log_no_matching_payment >> catch_processor_dag_error
        fetch_xero_invoice_for_ep >> catch_processor_dag_error
        fetch_xero_payment_for_ep >> catch_processor_dag_error
        compute_total_bill_payments >> catch_processor_dag_error
        build_outstanding_expense_lines >> catch_processor_dag_error
        compute_ep_weighted_lines >> catch_processor_dag_error
        fetch_vp_employee >> catch_processor_dag_error
        fetch_vp_periods_ep >> catch_processor_dag_error
        find_period_ep >> catch_processor_dag_error
        log_period_not_found_ep >> catch_processor_dag_error
        resolve_bank_code_ep >> catch_processor_dag_error
        build_ep_payload >> catch_processor_dag_error
        log_partial_expense_not_supported >> catch_processor_dag_error
        build_grouped_ep_payments >> catch_processor_dag_error
        log_compound_errors_ep >> catch_processor_dag_error
        post_expense_payment >> catch_processor_dag_error
        update_outstanding_ep >> catch_processor_dag_error
        delete_fully_paid_expense_entries >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
