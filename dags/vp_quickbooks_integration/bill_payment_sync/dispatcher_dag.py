"""
Dispatcher DAG for QBO -> VP Bill Payment Sync.

Per-tenant: polls QBO for new/updated bill payments by MetaData.LastUpdatedTime,
flattens one work item per (payment, linked bill), then fans out one worker
DAG per item.
Mirrors `vp_quickbooks_integration/invoice_payment_sync/dispatcher_dag.py`.

Replaces Workato recipe 014_503_psa_poll_quickbooks_bill_payment.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_quickbooks_integration.bill_payment_sync.utils.python_callable_method import (
    prepare_payment_sync_timestamps_method,
    update_payment_last_sync_times_method,
    is_integration_enabled_method,
    extract_and_filter_bill_payments_method,
)


logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll QBO bill payments, flatten, fan out."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_bill_payment_sync_dispatcher_{config.instance}',
        description=(
            'Poll QBO for new bill payments and trigger per-payment worker DAG'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_quickbooks', 'bill_payment_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: (
                prepare_payment_sync_timestamps_method(config.instance)
            )
        )

        # Mirrors Workato CFG_DisableBillPaymentIntegration account property.
        check_disabled_flag = rail.IfOperator(
            task_id='check_disabled_flag',
            test=lambda: is_integration_enabled_method(config.instance),
            yes_task='get_recently_changed_bill_payments',
            no_task='skip_run'
        )

        skip_run = rail.EmptyOperator(task_id='skip_run')

        # Single QBO poll by MetaData.LastUpdatedTime captures new bill
        # payments. QuickBooksBillPaymentOperator wraps the /query plumbing and
        # normalises the response to {success, entity_type, data, count}.
        get_recently_changed_bill_payments = rail.QuickBooksBillPaymentOperator(
            task_id='get_recently_changed_bill_payments',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='search_bill_payment',
            query=(
                "SELECT * FROM BillPayment WHERE MetaData.LastUpdatedTime >= "
                "'{{ result('prepare_sync_timestamps')['last_sync_time'] }}'"
                " AND MetaData.LastUpdatedTime < "
                "'{{ result('prepare_sync_timestamps')['current_sync_time'] }}'"
            )
        )

        # Flatten to one item per (payment, linked bill) and drop zero-amount /
        # unlinked rows. Mirrors Workato poll recipe foreach over Line.
        extract_payments = rail.PythonOperator(
            task_id='extract_bill_payment_list',
            python_callable=extract_and_filter_bill_payments_method
        )

        check_if_records_exist = rail.IfOperator(
            task_id='check_if_records_exist',
            test=lambda: len(rail.result('extract_bill_payment_list')) > 0,
            yes_task='process_records',
            no_task='log_no_records'
        )

        log_no_records = rail.PythonOperator(
            task_id='log_no_records',
            python_callable=lambda: logger.info(
                'No new non-zero bill payments in this poll window.'
            )
        )

        # Fan out to worker DAG — one run per (payment, linked bill). Conf
        # forwards PaymentID, BillID, the QBO payment fields, connections,
        # customerId and integrationType so the worker is self-contained.
        process_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_records',
            items=lambda: rail.result('extract_bill_payment_list'),
            trigger_dag_id=(
                f'vp_qbo_bill_payment_sync_create_{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'connections': (
                    rail.get_current_context()['dag_run'].conf
                    .get('connections')
                ),
                'customerId': (
                    rail.get_current_context()['dag_run'].conf
                    .get('customerId')
                ),
                'integrationType': (
                    rail.get_current_context()['dag_run'].conf
                    .get('integrationType')
                ),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Wait for every worker regardless of outcome; errors are collected
        # below via GatherResultsFromDagRunsOperator.
        wait_for_create_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_dag_runs',
            dag_runs="{{ result('process_records') }}",
            allowed_states=['success', 'failed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_create_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_create_dag_errors',
            dag_runs="{{ result('process_records') }}",
            dagrun_task_id='catch_bill_payment_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: len(
                rail.result('gather_create_dag_errors') or []
            ) > 0,
            yes_task='fail_bill_payment_sync',
            no_task='update_last_sync_time'
        )

        # FailOperator concatenates per-payment errors into one message and
        # marks this dispatcher run as failed so middleware picks it up.
        fail_bill_payment_sync = rail.FailOperator(
            task_id='fail_bill_payment_sync',
            message=(
                "{{ result('gather_create_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        # trigger_rule='all_done': advance watermark even when fail branch
        # raises, so the next poll window doesn't re-process the same window
        # forever on partial failures.
        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=lambda: (
                update_payment_last_sync_times_method(config.instance)
            ),
            trigger_rule='all_done'
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done'
        )

        prepare_timestamps >> check_disabled_flag
        check_disabled_flag >> rail.Label('Disabled') >> skip_run
        skip_run >> update_sync_time

        (
            check_disabled_flag >> rail.Label('Enabled') >>
            get_recently_changed_bill_payments >> extract_payments >>
            check_if_records_exist
        )

        (
            check_if_records_exist >> rail.Label('No records') >>
            log_no_records >> update_sync_time
        )

        (
            check_if_records_exist >> rail.Label('Records found') >>
            process_records >> wait_for_create_dag_runs >>
            gather_create_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_bill_payment_sync >> update_sync_time
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
