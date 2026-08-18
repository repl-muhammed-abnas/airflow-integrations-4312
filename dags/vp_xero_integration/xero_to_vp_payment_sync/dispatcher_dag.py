"""
Dispatcher DAG for Xero -> VP Payment Sync.

Per-tenant: applies the polling watermark, polls Xero for payments modified
since the last run, routes each payment to the correct processor DAG by
PaymentType (ACCRECPAYMENT -> invoice processor, ACCPAYPAYMENT -> bill
processor), gathers errors, and advances the watermark.

Mirrors Workato `014_501_psa_poll_xero_payment` routing exactly: each payment
is processed through its own typed recipe. TRANSFER, PREPAYMENT and any other
PaymentTypes are silently skipped.
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_xero_integration.xero_to_vp_payment_sync.config import (
    initial_sync_time,
    watermark_variable_key_template,
)
from vp_xero_integration.xero_to_vp_payment_sync.utils.python_callable_method import (
    prepare_payment_items_method,
    build_payment_processor_conf,
    prepare_sync_timestamps_method,
    update_last_sync_time_method,
)


def create_dag(config):
    """Per-tenant dispatcher: poll Xero payments, route per PaymentType, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_payment_sync_dispatcher_{config.instance}',
        description=(
            'Poll Xero for modified payments and route each to the correct '
            'processor DAG by PaymentType'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'payment_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        def _get_processor_dag_id(item):
            """Return the processor DAG ID for this payment's PaymentType."""
            if (item or {}).get('PaymentType') == 'ACCRECPAYMENT':
                return f'vp_xero_invoice_payment_processor_{config.instance}'
            return f'vp_xero_bill_payment_processor_{config.instance}'

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps_method(
                config.instance,
                watermark_variable_key_template,
                initial_sync_time,
            )
        )

        poll_xero_payments = rail.XeroPaymentOperator(
            task_id='poll_xero_payments',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='search',
            modified_since=(
                "{{ result('prepare_sync_timestamps')['last_sync_time'][:19] + 'Z' }}"
            ),
        )

        # Filter to ACCRECPAYMENT + ACCPAYPAYMENT; silently skip other types
        prepare_payment_items = rail.PythonOperator(
            task_id='prepare_payment_items',
            python_callable=prepare_payment_items_method,
        )

        # Route each payment to the correct processor based on PaymentType
        trigger_payment_processors = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_payment_processors',
            items=lambda: rail.result('prepare_payment_items'),
            trigger_dag_id=_get_processor_dag_id,
            conf=build_payment_processor_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_payment_processors = rail.WaitForDagRunsSensor(
            task_id='wait_for_payment_processors',
            dag_runs="{{ result('trigger_payment_processors') }}",
            allowed_states=['success', 'failed', 'upstream_failed', 'removed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_processor_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_errors',
            dag_runs="{{ result('trigger_payment_processors') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True,
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: len(rail.result('gather_processor_errors') or []) > 0,
            yes_task='fail_payment_sync',
            no_task='update_last_sync_time',
        )

        fail_payment_sync = rail.FailOperator(
            task_id='fail_payment_sync',
            message=(
                "{{ result('gather_processor_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=lambda: update_last_sync_time_method(
                config.instance,
                watermark_variable_key_template,
            )
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url="{{ var.value.get('middleware_api_base_url', '') }}",
            trigger_rule='all_done',
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #
        (
            prepare_timestamps >>
            poll_xero_payments >>
            prepare_payment_items >>
            trigger_payment_processors >>
            wait_for_payment_processors >>
            gather_processor_errors >>
            has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_payment_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
