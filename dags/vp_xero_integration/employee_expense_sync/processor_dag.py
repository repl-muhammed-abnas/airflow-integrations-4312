"""Processor DAG for VP -> Xero Employee Expense Sync (one voucher per run)."""

# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration.employee_expense_sync import config as sync_config
from vp_xero_integration.employee_expense_sync.utils.python_callable_method import (
    build_vp_expense_lines_filter_method,
    check_already_exported_method,
    should_skip_if_exported_method,
    build_xero_bill_body_method,
    check_has_payable_lines_method,
    record_expense_result_method,
    capture_processor_error,
)

logger = logging.getLogger(__name__)

_XERO_CONN_ID = (
    "{{ dag_run.conf.get('connections', {}).get('xero', '"
    + sync_config.xero_conn_id_default
    + "') }}"
)


def create_dag(config):
    """Per-voucher processor DAG: fetch lines, build Xero ACCPAY bill, record result."""
    with rail.create_airflow_dag(
        dag_id=f'{sync_config.processor_dag_id_prefix}_{config.instance}',
        description=sync_config.processor_dag_description,
        integration_type=sync_config.integration_type,
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=sync_config.processor_dag_tags,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        check_already_exported = rail.PythonOperator(
            task_id='check_already_exported',
            python_callable=check_already_exported_method,
        )

        skip_if_exported = rail.IfOperator(
            task_id='skip_if_exported',
            test=should_skip_if_exported_method,
            yes_task='skip_already_exported',
            no_task='get_expense_lines',
        )

        skip_already_exported = rail.PythonOperator(
            task_id='skip_already_exported',
            python_callable=lambda: logger.info(
                "Expense voucher already exported to Xero — skipping."
            ),
        )

        get_expense_lines = rail.VantagepointPsaledgerOperator(
            task_id='get_expense_lines',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            trans_type=sync_config.PSA_LEDGER_TRANS_TYPE,
            request_method='GET',
            filters=build_vp_expense_lines_filter_method,
        )

        fetch_xero_tax_rates = rail.XeroTaxRateOperator(
            task_id='fetch_xero_tax_rates',
            xero_conn_id=_XERO_CONN_ID,
            operation='list',
        )

        build_xero_bill_body = rail.PythonOperator(
            task_id='build_xero_bill_body',
            python_callable=build_xero_bill_body_method,
        )

        check_has_payable_lines = rail.IfOperator(
            task_id='check_has_payable_lines',
            test=check_has_payable_lines_method,
            yes_task='create_bill_in_xero',
            no_task='skip_no_payable_lines',
        )

        skip_no_payable_lines = rail.PythonOperator(
            task_id='skip_no_payable_lines',
            python_callable=lambda: logger.info(
                "No employee-payable expense lines found — skipping Xero bill creation."
            ),
        )

        create_bill_in_xero = rail.XeroInvoiceOperator(
            task_id='create_bill_in_xero',
            xero_conn_id=_XERO_CONN_ID,
            operation='create_bill',
            request_body=lambda: rail.result('build_xero_bill_body'),
        )

        record_expense_result = rail.PythonOperator(
            task_id='record_expense_result',
            python_callable=record_expense_result_method,
        )

        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
            op_args=[
                "{{ dag_run.conf.get('Employee') or '' }}",
                "{{ dag_run.conf.get('Voucher') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        # Main flow
        check_already_exported >> skip_if_exported

        (
            skip_if_exported >> rail.Label('Already exported')
            >> skip_already_exported
        )

        (
            skip_if_exported >> rail.Label('Not yet exported')
            >> get_expense_lines
            >> fetch_xero_tax_rates
            >> build_xero_bill_body
            >> check_has_payable_lines
        )

        (
            check_has_payable_lines >> rail.Label('Has payable lines')
            >> create_bill_in_xero
            >> record_expense_result
        )

        (
            check_has_payable_lines >> rail.Label('No payable lines')
            >> skip_no_payable_lines
        )

        # Error catcher — explicit edges from every task that can fail
        check_already_exported >> catch_processor_dag_error
        get_expense_lines >> catch_processor_dag_error
        fetch_xero_tax_rates >> catch_processor_dag_error
        build_xero_bill_body >> catch_processor_dag_error
        create_bill_in_xero >> catch_processor_dag_error
        record_expense_result >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
