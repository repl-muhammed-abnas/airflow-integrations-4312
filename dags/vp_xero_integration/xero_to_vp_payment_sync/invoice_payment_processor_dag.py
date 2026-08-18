"""
Invoice Payment Processor DAG for Xero -> VP Payment Sync.

Per-payment: processes one ACCRECPAYMENT (Xero invoice payment) into a
Vantagepoint Cash Receipt (CR). Mirrors the Workato callable recipe
`014_501_psa_xero_invoice_payment_adds_to_vantagepoint` step-by-step.

Task chain (Workato step numbers annotated):
  fetch_xero_payment            [step 3]  GET Xero payment by PaymentID
  fetch_vp_invoice_lines        [step 5]  GET VP PSA Ledger TransType=in
  is_vp_invoice_found           [step 6]  IF rows found
  check_cr_duplicate            [step 10] GET VP PSA Ledger TransType=cr (dedup)
  is_cr_duplicate               [step 11] IF rows found -> graceful stop
  compute_weighted_lines        [steps 15-16] Pro-rata weight per PSA row
  fetch_org_codes               [step 17] GET VP CFGOrgCodes codetable
  build_payment_vars            [step 18] Derive Batch/Company/RefNo/Description
  fetch_ar_account              [step 19] GET VP CFGAutoPosting -> AcctsReceivable
  resolve_bank_code             [step 20] Resolve Xero bank -> VP bank code
  is_bank_resolved              [step 21] IF VantagepointCode not blank
  fetch_vp_periods              [steps 24-25] GET VP Periods
  find_payment_period           [steps 26-28] Find period for payment date
  is_period_found               [step 30] IF period found
  set_active_period             [step 29] PUT VP ActivePeriod
  post_cash_receipt             [step 33] POST VP Cash Receipt (crControl)
  post_cr_transaction           [step 34] PUT PostTransFile TransType=CR
  catch_processor_dag_error     [sole leaf, trigger_rule=one_failed]
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration.xero_to_vp_payment_sync.utils.python_callable_method import (
    build_ar_psa_filter_method,
    build_cr_dedup_filter_method,
    compute_weighted_lines_method,
    build_payment_vars_method,
    build_ar_account_filter,
    resolve_bank_code_method,
    find_payment_period_method,
    build_cr_body_method,
    build_cr_post_trans_body_method,
    capture_processor_error,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-payment invoice processor: ACCRECPAYMENT -> VP Cash Receipt."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_invoice_payment_processor_{config.instance}',
        description='Process one Xero invoice payment (ACCRECPAYMENT) into a VP Cash Receipt',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'payment_sync', 'invoice_payment_processor'],
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

        # [step 3] GET Xero payment by PaymentID
        fetch_xero_payment = rail.XeroPaymentOperator(
            task_id='fetch_xero_payment',
            xero_conn_id=xero_conn_id,
            operation='get_by_id',
            record_id="{{ dag_run.conf.get('PaymentID') }}",
        )

        # [step 5] GET VP PSA Ledger TransType=in to find invoice lines
        fetch_vp_invoice_lines = rail.VantagepointPsaledgerOperator(
            task_id='fetch_vp_invoice_lines',
            vp_conn_id=vp_conn_id,
            filters=build_ar_psa_filter_method,
            trans_type='IN',
            retries=3,
            retry_exponential_backoff=True,
        )

        # [step 6] IF PSA rows found (no rows -> log + graceful stop)
        is_vp_invoice_found = rail.IfOperator(
            task_id='is_vp_invoice_found',
            test=lambda: len(rail.result('fetch_vp_invoice_lines') or []) > 0,
            yes_task='check_cr_duplicate',
            no_task='log_no_invoice_match',
        )

        log_no_invoice_match = rail.PythonOperator(
            task_id='log_no_invoice_match',
            python_callable=lambda: logger.warning(
                "No VP PSA Ledger rows for InvoiceID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('InvoiceID'),
            )
        )

        # [step 10] Check for duplicate CR (same Batch = PaymentID stripped)
        check_cr_duplicate = rail.VantagepointPsaledgerOperator(
            task_id='check_cr_duplicate',
            vp_conn_id=vp_conn_id,
            filters=build_cr_dedup_filter_method,
            trans_type='CR',
            retries=3,
            retry_exponential_backoff=True,
        )

        # [step 11] IF CR rows found -> duplicate, graceful stop
        is_cr_duplicate = rail.IfOperator(
            task_id='is_cr_duplicate',
            test=lambda: len(rail.result('check_cr_duplicate') or []) > 0,
            yes_task='log_duplicate_cr',
            no_task='compute_weighted_lines',
        )

        log_duplicate_cr = rail.PythonOperator(
            task_id='log_duplicate_cr',
            python_callable=lambda: logger.warning(
                "Duplicate CR detected for PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [steps 15-16] Pro-rata weight PSA rows by TxnAmt
        compute_weighted_lines = rail.PythonOperator(
            task_id='compute_weighted_lines',
            python_callable=compute_weighted_lines_method,
        )

        # [step 17] GET VP CFGOrgCodes codetable (to derive Company)
        fetch_org_codes = rail.VantagepointCodetableRecordsOperator(
            task_id='fetch_org_codes',
            vp_conn_id=vp_conn_id,
            codetable_object='CFGOrgCodes',
        )

        # [step 18] Derive Batch, Company, RefNo, Description
        build_payment_vars = rail.PythonOperator(
            task_id='build_payment_vars',
            python_callable=build_payment_vars_method,
        )

        # [step 19] GET VP CFGAutoPosting (AcctsReceivable account)
        # Uses VantagepointCustomOperator (base_path='') so the /vision/ prefix
        # is honoured — VantagepointAPIOperator prepends /api/ which gives 404.
        fetch_ar_account = rail.VantagepointCustomOperator(
            task_id='fetch_ar_account',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            endpoint='/vision/AccountConfiguration/CFGAutoPosting',
            filters=build_ar_account_filter,
            pagination=False,
        )

        # [step 20] Resolve Xero bank account -> VP bank code (lazy-populate map_bank_code)
        resolve_bank_code = rail.PythonOperator(
            task_id='resolve_bank_code',
            python_callable=resolve_bank_code_method,
        )

        # [step 21] IF bank code resolved (blank -> graceful stop)
        is_bank_resolved = rail.IfOperator(
            task_id='is_bank_resolved',
            test=lambda: bool(
                (rail.result('resolve_bank_code') or {}).get('VantagepointCode')
            ),
            yes_task='fetch_vp_periods',
            no_task='log_bank_not_found',
        )

        log_bank_not_found = rail.PythonOperator(
            task_id='log_bank_not_found',
            python_callable=lambda: logger.warning(
                "Bank code not resolved for PaymentID=%s — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [steps 24-25] GET all VP Periods
        fetch_vp_periods = rail.VantagepointAPIOperator(
            task_id='fetch_vp_periods',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            endpoint='/Settings/Period',
        )

        # [steps 26-28] Find period containing payment date
        find_payment_period = rail.PythonOperator(
            task_id='find_payment_period',
            python_callable=find_payment_period_method,
        )

        # [step 30] IF period found (not found -> graceful stop)
        is_period_found = rail.IfOperator(
            task_id='is_period_found',
            test=lambda: rail.result('find_payment_period') is not None,
            yes_task='set_active_period',
            no_task='log_period_not_found',
        )

        log_period_not_found = rail.PythonOperator(
            task_id='log_period_not_found',
            python_callable=lambda: logger.warning(
                "VP period not found for PaymentID=%s payment date — graceful stop",
                rail.get_current_context()['dag_run'].conf.get('PaymentID'),
            )
        )

        # [step 29] PUT VP ActivePeriod (must precede CR POST)
        set_active_period = rail.VantagepointAPIOperator(
            task_id='set_active_period',
            vp_conn_id=vp_conn_id,
            request_method='PUT',
            endpoint="/Settings/ActivePeriod/{{ result('find_payment_period') }}",
        )

        # [step 33] POST VP Cash Receipt
        post_cash_receipt = rail.VantagepointCashReceiptOperator(
            task_id='post_cash_receipt',
            vp_conn_id=vp_conn_id,
            request_method='POST',
            request_body=build_cr_body_method,
            retries=3,
            retry_exponential_backoff=True,
        )

        # [step 34] PUT PostTransFile TransType=CR
        post_cr_transaction = rail.VantagepointPostTransactionOperator(
            task_id='post_cr_transaction',
            vp_conn_id=vp_conn_id,
            request_method='PUT',
            request_body=build_cr_post_trans_body_method,
            retries=3,
            retry_exponential_backoff=True,
        )

        # Sole leaf — catches any upstream failure; result gathered by dispatcher
        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
        )

        # ------------------------------------------------------------------ #
        # Task graph — decision spine
        # ------------------------------------------------------------------ #
        (
            fetch_xero_payment >>
            fetch_vp_invoice_lines >>
            is_vp_invoice_found
        )

        # No invoice match -> graceful stop
        is_vp_invoice_found >> rail.Label('No match') >> log_no_invoice_match

        # Invoice found -> dedup check
        (
            is_vp_invoice_found >> rail.Label('Found') >>
            check_cr_duplicate >> is_cr_duplicate
        )

        # Duplicate CR -> graceful stop
        is_cr_duplicate >> rail.Label('Duplicate') >> log_duplicate_cr

        # New payment -> process
        (
            is_cr_duplicate >> rail.Label('New') >>
            compute_weighted_lines >>
            fetch_org_codes >>
            build_payment_vars >>
            fetch_ar_account >>
            resolve_bank_code >>
            is_bank_resolved
        )

        is_bank_resolved >> rail.Label('No bank') >> log_bank_not_found

        (
            is_bank_resolved >> rail.Label('Resolved') >>
            fetch_vp_periods >>
            find_payment_period >>
            is_period_found
        )

        is_period_found >> rail.Label('No period') >> log_period_not_found

        (
            is_period_found >> rail.Label('Found') >>
            set_active_period >>
            post_cash_receipt >>
            post_cr_transaction
        )

        # Every work task must have a direct edge to catch_processor_dag_error
        # (trigger_rule='one_failed' requires a DIRECT failed upstream, not upstream_failed)
        fetch_xero_payment >> catch_processor_dag_error
        fetch_vp_invoice_lines >> catch_processor_dag_error
        log_no_invoice_match >> catch_processor_dag_error
        check_cr_duplicate >> catch_processor_dag_error
        log_duplicate_cr >> catch_processor_dag_error
        compute_weighted_lines >> catch_processor_dag_error
        fetch_org_codes >> catch_processor_dag_error
        build_payment_vars >> catch_processor_dag_error
        fetch_ar_account >> catch_processor_dag_error
        resolve_bank_code >> catch_processor_dag_error
        log_bank_not_found >> catch_processor_dag_error
        fetch_vp_periods >> catch_processor_dag_error
        find_payment_period >> catch_processor_dag_error
        log_period_not_found >> catch_processor_dag_error
        set_active_period >> catch_processor_dag_error
        post_cash_receipt >> catch_processor_dag_error
        post_cr_transaction >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
