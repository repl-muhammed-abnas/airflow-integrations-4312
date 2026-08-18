
from bccsstechnologyservices.create_invoices_for_each_project_code.utils import request_payload, python_callable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long, unnecessary-lambda
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_create_invoice_for_project_child{dag_id_postfix}',
        description=f'create_invoice_for_project_child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
        },
    ) as dag:

        get_client_details = rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint="/services/ClientService1.svc/GetClientDetails",
            data=request_payload.get_client_details_payload
        )
        get_todays_date = rail.PythonOperator(
            task_id='get_todays_date',
            python_callable=python_callable.get_todays_date_data
        )
        get_payment_date = rail.PythonOperator(
            task_id='get_payment_date',
            python_callable=python_callable.get_payment_date_data
        )
        get_invoice_payload = rail.PythonOperator(
            task_id='get_invoice_payload',
            python_callable=request_payload.get_create_invoice_payload
        )
        create_invoice = rail.SimpleHttpOperator(
            task_id='create_invoice',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoice3',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_payload') | to_json }}",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']


        )
        # create_invoice = rail.RepliconServiceOperator(
        #     task_id='create_invoice',
        #     endpoint="/services/billing/InvoiceService2.svc/PutInvoice3",
        #     data=request_payload.get_create_invoice_payload
        # )

        get_start_date = rail.PythonOperator(
            task_id='get_start_date',
            python_callable=python_callable.get_start_date_data
        )
        get_end_date = rail.PythonOperator(
            task_id='get_end_date',
            python_callable=python_callable.get_end_date_data
        )
        get_invoice_item_payload = rail.PythonOperator(
            task_id='get_invoice_item_payload',
            python_callable=request_payload.get_invoice_items_payload
        )
        create_invoice_items_from_billing_items_for_invoice = rail.SimpleHttpOperator(
            task_id='create_invoice_items_from_billing_items_for_invoice',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/CreateInvoiceItemsFromBillingItemsForInvoice2',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_item_payload') | to_json }}",
            extra_options={
                'verify': False
            }
        )
        check_invoice_from_biling_items = rail.IfOperator(
            task_id='check_invoice_from_biling_items',
            test='{{ get_task_state("create_invoice_items_from_billing_items_for_invoice") == "success" }}',
            yes_task="finish",
            no_task="create_invoice_items_from_billing_items_for_invoice_retries",
        )
        create_invoice_items_from_billing_items_for_invoice_retries = rail.SimpleHttpOperator(
            task_id='create_invoice_items_from_billing_items_for_invoice_retries',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/CreateInvoiceItemsFromBillingItemsForInvoice2',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_item_payload') | to_json }}",
            extra_options={
                'verify': False
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        get_client_details >> get_todays_date >> get_payment_date >> get_invoice_payload >> create_invoice >> get_start_date >> get_end_date >> get_invoice_item_payload >> create_invoice_items_from_billing_items_for_invoice >> check_invoice_from_biling_items
        check_invoice_from_biling_items >> rail.Label('Yes') >> finish
        check_invoice_from_biling_items >> rail.Label(
            'No') >> create_invoice_items_from_billing_items_for_invoice_retries >> finish

    return dag


rail.for_each_instance(create_dag)
