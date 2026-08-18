# dags/vp_xero_integration_v2/employee_expense_sync/dispatcher_dag.py
"""Dispatcher DAG for VP -> Xero Employee Expense Sync (V2 IPA GitSync architecture).

Per-tenant: applies the polling watermark, queries VP PSA Ledger for posted
employee expenses (TransType=ex) in the window, deduplicates into unique
(Period, PostSeq, Employee, Voucher) identities, and triggers the processor
DAG per voucher. Gathers child errors; holds the watermark on any failure.

V2 changes from V1:
  - schedule_interval from config.schedule_interval (not None)
  - vp_conn_id from get_connections(config) (not Jinja dag_run.conf)
  - connections/customerId in build_processor_dag_conf from config (not dag_run.conf)
  - middleware_api_base_url via Jinja var.value.get (not parse-time Variable.get)
  - check_disabled_flag / skip_run removed (RAIL handles disabled=True at parse time)
  - trigger_rule='all_done' on update_last_sync_time (watermark advances on partial failure)
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration_v2.employee_expense_sync.config import (
    PSA_LEDGER_TRANS_TYPE,
    watermark_variable_key_template,
)
from vp_xero_integration_v2.common.python_callable_method import (
    get_connections,
    prepare_sync_timestamps,
    update_last_sync_time,
    has_sync_errors_method,
)
from vp_xero_integration_v2.employee_expense_sync.utils.python_callable_method import (
    build_vp_expense_poll_filter_method,
    extract_expense_vouchers_method,
    check_if_vouchers_exist_method,
)

_log = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll VP PSA Ledger, fan out per voucher, gather errors, advance watermark."""
    connections = get_connections(config)
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_employee_expense_sync_v2_dispatcher_{config.instance}',
        description=(
            'Poll VP PSA Ledger for posted employee expenses (TransType=ex) '
            'and trigger per-voucher Xero ACCPAY bill create'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_xero', 'employee_expense_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps(
                config.instance,
                watermark_variable_key_template,
                config.initial_sync_time,
            )
        )

        poll_expense_ledger = rail.VantagepointPsaledgerOperator(
            task_id='poll_expense_ledger',
            vp_conn_id=connections['vantagepoint'],
            trans_type=PSA_LEDGER_TRANS_TYPE,
            request_method='GET',
            filters=build_vp_expense_poll_filter_method,
        )

        extract_expense_vouchers = rail.PythonOperator(
            task_id='extract_expense_vouchers',
            python_callable=extract_expense_vouchers_method,
        )

        check_if_vouchers_exist = rail.IfOperator(
            task_id='check_if_vouchers_exist',
            test=check_if_vouchers_exist_method,
            yes_task='process_expense_vouchers',
            no_task='log_no_vouchers',
        )

        log_no_vouchers = rail.PythonOperator(
            task_id='log_no_vouchers',
            python_callable=lambda: _log.info(
                'No new employee expense vouchers in this poll window.'
            )
        )

        def build_processor_dag_conf(item):
            return {
                'Period': item.get('Period'),
                'PostSeq': item.get('PostSeq'),
                'Employee': item.get('Employee'),
                'Voucher': item.get('Voucher'),
                'Org': item.get('Org') or '',
                'TransDate': item.get('TransDate') or '',
                'connections': connections,
                'customerId': config.customer_id,
            }

        process_expense_vouchers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_expense_vouchers',
            items=lambda: rail.result('extract_expense_vouchers'),
            trigger_dag_id=(
                f'vp_xero_employee_expense_sync_v2_processor_{config.instance}'
            ),
            conf=build_processor_dag_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_expense_vouchers') }}",
            allowed_states=['success', 'failed', 'upstream_failed', 'removed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_processor_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_dag_errors',
            dag_runs="{{ result('process_expense_vouchers') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=has_sync_errors_method,
            yes_task='fail_employee_expense_sync',
            no_task='update_last_sync_time',
        )

        fail_employee_expense_sync = rail.FailOperator(
            task_id='fail_employee_expense_sync',
            message=(
                "{{ result('gather_processor_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            trigger_rule='all_done',
            python_callable=lambda: update_last_sync_time(
                config.instance,
                watermark_variable_key_template,
            )
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url="{{ var.value.get('middleware_api_base_url', '') }}",
            trigger_rule='all_done'
        )

        prepare_timestamps >> poll_expense_ledger >> extract_expense_vouchers >> check_if_vouchers_exist

        (
            check_if_vouchers_exist >> rail.Label('No vouchers') >>
            log_no_vouchers >> update_sync_time
        )

        (
            check_if_vouchers_exist >> rail.Label('Vouchers found') >>
            process_expense_vouchers >> wait_for_processor_dag_runs >>
            gather_processor_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_employee_expense_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
