from datetime import timedelta
import rail
from airflow.models import Variable
from bccsstechnologyservices.update_paid_invoice.util import data_formatting
from bccsstechnologyservices.update_paid_invoice import request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"bccss_phsa_update_paid_invoice_child_{config.instance}",
        description=f"PHSA Update Paid Invoice - Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        # api_token = Variable.get(config.token_var, default_var='')
        finish = rail.EmptyOperator(task_id='finish')

        can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='').lower() == 'true',
                yes_task='batch_task',
                no_task='get_invoice_uri'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_invoice_uri',
            end_task='finish',
        )

        get_invoice_uri = rail.SimpleHttpOperator(
            task_id='get_invoice_uri',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/GetUriFromInvoiceNumber1',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="""{"invoiceNumberText": "{{ dag_run.conf["invoice_number"] }}"}""",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_details = rail.SimpleHttpOperator(
            task_id='get_invoice_details',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/GetInvoiceDetails',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="""{"invoiceUri": "{{result('get_invoice_uri') }}"}""",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        check_paid_invoice = rail.IfOperator(
            task_id='check_paid_invoice',
            test='{{ result("get_invoice_details")["invoiceStatus"].split(":")[-1] == "billed" }}',
            yes_task='process_paid_invoice_details',
            no_task='check_payment_date'
        )

        process_paid_invoice_details = rail.PythonOperator(
            task_id='process_paid_invoice_details',
            python_callable=request_payload.process_paid_invoice_details,
            op_args=['{{ result("get_invoice_details") | tojson }}',
            "{{result('get_invoice_uri') }}",
            '{{ dag_run.conf["invoice_number"] }}',
            '{{ dag_run.conf["payment_date"] }}']
        )

        put_invoice = rail.SimpleHttpOperator(
            task_id='put_invoice',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoice3',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data='{{ result("process_paid_invoice_details") }}',
            extra_options={
                'verify': False
            },
            response_filter=lambda response: data_formatting.check_put_success(response, rail.result('get_invoice_uri'))
        )

        check_success_invoice_update = rail.IfOperator(
            task_id='check_success_invoice_update',
            test='{{ result("put_invoice") | is_truthy}}',
            yes_task='markas_paid_mark_invoiceas_paid',
            no_task='finish'
        )

        markas_paid_mark_invoiceas_paid = rail.SimpleHttpOperator(
            task_id='markas_paid_mark_invoiceas_paid',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsPaid',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="""{"invoiceUri": "{{result('get_invoice_uri') }}"}""",
            extra_options={
                'verify': False
            }
        )

        sucess_log = rail.WriteLogOperator(
            task_id = "sucess_log",
            severity="Success",
            message="Invoice update | " +'{{ dag_run.conf["invoice_number"] }}',
            properties={
                'invoicenumber':'{{ dag_run.conf["invoice_number"] }}',
                'status': 'Success'
            }
        )

        check_payment_date = rail.PythonOperator(
            task_id='check_payment_date',
            python_callable=data_formatting.check_payment_date,
            op_args=['{{ dag_run.conf["payment_date"] }}']
        )

        check_valid_payment_date = rail.IfOperator(
            task_id='check_valid_payment_date',
            test='{{ result("check_payment_date") | is_truthy }}',
            yes_task='unbilled_invoice_log',
            no_task='skipped_invoice_log'
        )

        unbilled_invoice_log = rail.WriteLogOperator(
            task_id = "unbilled_invoice_log",
            severity="skipped",
            message = 'Invoice is not in "Issued" status | ' +'{{ dag_run.conf["invoice_number"] }}',
            properties={
                'invoicenumber':'{{ dag_run.conf["invoice_number"] }}',
                'status': 'Skipped'
            }
        )

        skipped_invoice_log = rail.WriteLogOperator(
            task_id = "skipped_invoice_log",
            severity="skipped",
            message = 'Invoice is not in "Issued" status and payment date is not present | ' +'{{ dag_run.conf["invoice_number"] }}',
            properties={
                'invoicenumber':'{{ dag_run.conf["invoice_number"] }}',
                'status': 'Skipped'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label('No') >> get_invoice_uri >> get_invoice_details

        get_invoice_uri >> get_invoice_details >> check_paid_invoice >> rail.Label('No') >> check_payment_date >> check_valid_payment_date
        check_valid_payment_date >> rail.Label('Yes') >> unbilled_invoice_log >> finish >> catch_and_log_errors
        check_valid_payment_date >> rail.Label('No') >> skipped_invoice_log >> finish >> catch_and_log_errors

        check_paid_invoice >> rail.Label('Yes') >> process_paid_invoice_details >> put_invoice >> check_success_invoice_update
        check_success_invoice_update >> rail.Label('No') >> finish >> catch_and_log_errors

        check_success_invoice_update >> rail.Label('Yes') >> markas_paid_mark_invoiceas_paid >> sucess_log >> finish >> catch_and_log_errors
    return dag

rail.for_each_instance(create_child_dag)
