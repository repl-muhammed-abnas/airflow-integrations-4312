from datetime import timedelta
import rail
from airflow.models import Variable
null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_invoice_status_update_paid_child_dag_{config.instance}",
        description=f'Xero Connector {config.region} Invoice Paid Status Update Child{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_status_in_replicon_equals_paid'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_status_in_replicon_equals_paid',
            end_task='catch_status_update_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        if_status_in_replicon_equals_paid = rail.IfOperator(
            task_id='if_status_in_replicon_equals_paid',
            test=lambda dag_run: dag_run.conf['invoice_status'] == 'urn:replicon:invoice2-status:paid',
            yes_task='catch_status_update_error',
            no_task='if_status_in_replicon_equals_draft'
        )

        if_status_in_replicon_equals_draft = rail.IfOperator(
            task_id='if_status_in_replicon_equals_draft',
            test=lambda dag_run: dag_run.conf['invoice_status'] == 'urn:replicon:invoice2-status:open',
            yes_task='mark_as_billed_invoice',
            no_task='mark_as_paid_invoice'
        )

        mark_as_billed_invoice = rail.RepliconServiceOperator(
            task_id="mark_as_billed_invoice",
            endpoint="/services/InvoiceService2.svc/MarkAsBilled",
            data={
                "invoiceUri": "{{ dag_run.conf.invoice_uri }}"
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        mark_as_paid_invoice = rail.RepliconServiceOperator(
            task_id="mark_as_paid_invoice",
            endpoint="/services/InvoiceService2.svc/MarkAsPaid",
            data={
                "invoiceUri": "{{ dag_run.conf.invoice_uri }}"
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        def get_downstreamtasks_error(invoice_number_name, error_message):
            return {
                'error': f'Error with {invoice_number_name} - {error_message}'
            }
        catch_status_update_error = rail.PythonOperator(
            task_id='catch_status_update_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.invoice_number }}-{{ dag_run.conf.client_name}}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_status_update_error
        can_run_batch_task >> rail.Label(
            'No') >> if_status_in_replicon_equals_paid
        if_status_in_replicon_equals_paid >> rail.Label(
            'Yes') >> catch_status_update_error
        if_status_in_replicon_equals_paid >> rail.Label(
            'No') >> if_status_in_replicon_equals_draft
        if_status_in_replicon_equals_draft >> rail.Label(
            'Yes') >> mark_as_billed_invoice >> mark_as_paid_invoice
        if_status_in_replicon_equals_draft >> rail.Label(
            'No') >> mark_as_paid_invoice >> catch_status_update_error
    return dag


rail.for_each_instance(create_main_dag)
