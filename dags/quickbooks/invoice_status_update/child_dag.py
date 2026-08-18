from datetime import datetime, timedelta
import rail
from airflow.models import Variable
null = None

GET_BILL_ASSOCIATED_PAYMENTS_QUERY = """query getBillAssociatedPayments($page: Int!, $pageSize: Int!, $id: String!) {
    getBill(id: $id) {
        id
        associatedBillPayments(page: $page, pageSize: $pageSize) {
            associatedBillPayments {
                id
                displayText
                allocatedBillAmount {
                    amount
                    currency { id symbol __typename }
                    __typename
                }
                __typename
            }
            __typename
        }
        __typename
    }
}"""


def _extract_associated_payments():
    payments_response = rail.result('get_bill_associated_payments')
    bill = payments_response.get('data', {}).get(
        'getBill', {}) if payments_response else {}
    wrapper = bill.get('associatedBillPayments', {})
    return wrapper.get('associatedBillPayments', []) if isinstance(wrapper, dict) else []


def should_create_payment_test():
    dag_run_conf = rail.get_dag_run_conf()
    invoice = rail.result('get_invoice_details_from_polaris')
    bill_total = float(invoice.get('totalPaymentAmount', {}).get('amount', 0))
    total_allocated = sum(float(p.get('allocatedBillAmount', {}).get('amount', 0))
                          for p in _extract_associated_payments())
    current_balance = bill_total - total_allocated
    target_balance = float(dag_run_conf.get('Balance', 0))
    linked_txns = dag_run_conf.get('LinkedTxn', [])
    return (invoice.get('status', '').lower() != 'paid'
            and linked_txns
            and current_balance > target_balance)


def build_payment_data():
    dag_run_conf = rail.get_dag_run_conf()
    invoice = rail.result('get_invoice_details_from_polaris')
    bill_total = float(invoice.get('totalPaymentAmount', {}).get('amount', 0))
    payments = _extract_associated_payments()

    total_allocated = sum(
        float(p.get('allocatedBillAmount', {}).get('amount', 0)) for p in payments)
    current_balance = bill_total - total_allocated
    target_balance = float(dag_run_conf.get('Balance', 0))
    payment_amount = current_balance - target_balance

    doc_number = dag_run_conf.get('DocNumber', 'Unknown')
    prefix = f"PAY_{doc_number}_"
    seq = sum(1 for p in payments if p.get(
        'displayText', '').startswith(prefix)) + 1
    payment_display_id = f"{prefix}{str(seq).zfill(3)}"
    txn_id = (dag_run_conf.get('LinkedTxn', [])
              or [{}])[-1].get('TxnId', 'Unknown')

    return {
        "variables": {
            "input": {
                "id": null,
                "displayId": payment_display_id,
                "description": f"QuickBooks Payment Sync - Transaction: {txn_id} - Amount: ${payment_amount}",
                "paymentAmount": float(payment_amount),
                "associatedBills": [{
                    "id": dag_run_conf.get('PrivateNote'),
                    "allocatedBillAmount": {
                        "amount": float(payment_amount),
                        "currency": {
                            "id": rail.result('get_invoice_details_from_polaris').get('invoiceCurrency', {}).get('uri'),
                            "symbol": rail.result('get_invoice_details_from_polaris').get('invoiceCurrency', {}).get('symbol'),
                            "displayText": rail.result('get_invoice_details_from_polaris').get('invoiceCurrency', {}).get('displayText')
                        }
                    },
                    "allocatedBillPaymentAmount": {
                        "amount": float(payment_amount),
                        "currency": {
                            "id": rail.result('get_invoice_details_from_polaris').get('invoiceCurrency', {}).get('uri'),
                            "symbol": rail.result('get_invoice_details_from_polaris').get('invoiceCurrency', {}).get('symbol'),
                            "displayText": rail.result('get_invoice_details_from_polaris').get('invoiceCurrency', {}).get('displayText')
                        }
                    }
                }],
                "paymentDate": {"year": datetime.now().year, "month": datetime.now().month, "day": datetime.now().day},
                "clientUri": rail.result('get_invoice_details_from_polaris').get('client', {}).get('uri'),
                "currencyUri": rail.result('get_invoice_details_from_polaris').get('invoiceCurrency', {}).get('uri'),
            }
        },
        "query": """mutation putBillPayment($input: BillPaymentInput2!) {
            putBillPayment: putBillPayment3(input: $input) { paymentUri __typename }
        }"""
    }


def get_downstreamtasks_error(invoice_number, error_message):
    return {'error': f'Error with {invoice_number} - {error_message}'}


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_quickbooks_online_{config.region.replace('-', '_')}_invoice_status_update_child_dag_{config.instance}",
        description=f'QuickBooks Online {config.region} Invoice Status Update Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_invoice_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_invoice_data',
            end_task='catch_invoice_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_invoice_data = rail.EmptyOperator(
            task_id='process_invoice_data')

        if_polaris_permissions_present = rail.IfOperator(
            task_id='if_polaris_permissions_present',
            test=lambda dag_run: dag_run.conf.get(
                'is_polaris_permissions_present', False),
            yes_task='get_invoice_details_from_polaris',
            no_task='is_balance_not_zero'
        )

        get_invoice_details_from_polaris = rail.RepliconServiceOperator(
            task_id="get_invoice_details_from_polaris",
            endpoint="/services/InvoiceService2.svc/GetInvoiceDetails",
            data={"invoiceUri": "{{ dag_run.conf.PrivateNote }}"},
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        get_bill_associated_payments = rail.RepliconServiceOperator(
            task_id="get_bill_associated_payments",
            app="polaris",
            endpoint="graphql",
            data={
                "variables": {"page": 1, "pageSize": 50, "id": "{{ dag_run.conf.PrivateNote }}"},
                "query": GET_BILL_ASSOCIATED_PAYMENTS_QUERY
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        should_create_payment = rail.IfOperator(
            task_id='should_create_payment',
            test=should_create_payment_test,
            yes_task='update_invoice_to_paid_polaris',
            no_task='catch_invoice_error'
        )

        update_invoice_to_paid_polaris = rail.RepliconServiceOperator(
            task_id="update_invoice_to_paid_polaris",
            app="polaris",
            endpoint="graphql",
            data=build_payment_data,
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        is_balance_not_zero = rail.IfOperator(
            task_id='is_balance_not_zero',
            test="{{ dag_run.conf.Balance != 0 }}",
            yes_task='mark_as_billed_invoice',
            no_task='is_balance_zero'
        )

        mark_as_billed_invoice = rail.RepliconServiceOperator(
            task_id="mark_as_billed_invoice",
            endpoint="/services/InvoiceService2.svc/MarkAsBilled",
            data={"invoiceUri": "{{ dag_run.conf.PrivateNote }}"},
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        is_balance_zero = rail.IfOperator(
            task_id='is_balance_zero',
            test="{{ dag_run.conf.Balance == 0 }}",
            yes_task='mark_as_paid_invoice',
            no_task='catch_invoice_error'
        )

        mark_as_paid_invoice = rail.RepliconServiceOperator(
            task_id="mark_as_paid_invoice",
            endpoint="/services/InvoiceService2.svc/MarkAsPaid",
            data={"invoiceUri": "{{ dag_run.conf.PrivateNote }}"},
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        catch_invoice_error = rail.PythonOperator(
            task_id='catch_invoice_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.DocNumber }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> rail.Label(
            'on Error') >> catch_invoice_error
        can_run_batch_task >> rail.Label(
            'No') >> process_invoice_data >> if_polaris_permissions_present

        if_polaris_permissions_present >> rail.Label(
            'Yes') >> get_invoice_details_from_polaris >> get_bill_associated_payments >> should_create_payment
        should_create_payment >> rail.Label(
            'Yes') >> update_invoice_to_paid_polaris >> rail.Label('On Error') >> catch_invoice_error
        should_create_payment >> rail.Label('No') >> catch_invoice_error

        if_polaris_permissions_present >> rail.Label(
            'No') >> is_balance_not_zero
        is_balance_not_zero >> rail.Label('Yes') >> mark_as_billed_invoice >> rail.Label(
            'On Error') >> catch_invoice_error
        is_balance_not_zero >> rail.Label('No') >> is_balance_zero
        is_balance_zero >> rail.Label('Yes') >> mark_as_paid_invoice >> rail.Label(
            'On Error') >> catch_invoice_error
        is_balance_zero >> rail.Label('No') >> catch_invoice_error

    return dag


rail.for_each_instance(create_child_dag)
