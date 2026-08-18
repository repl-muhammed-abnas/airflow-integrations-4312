"""Dispatcher DAG for VP -> Xero Employee Expense Sync."""

# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_xero_integration.common.python_callable_method import (
    prepare_sync_timestamps,
    update_last_sync_time,
    has_sync_errors_method,
)
from vp_xero_integration.employee_expense_sync import config as sync_config
from vp_xero_integration.employee_expense_sync.config import (
    initial_sync_time,
    watermark_variable_key_template,
)
from vp_xero_integration.employee_expense_sync.utils.python_callable_method import (
    build_vp_expense_poll_filter_method,
    extract_expense_vouchers_method,
    check_if_vouchers_exist_method,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll VP PSA Ledger, fan out per voucher, gather, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=f'{sync_config.dispatcher_dag_id_prefix}_{config.instance}',
        description=sync_config.dispatcher_dag_description,
        integration_type=sync_config.integration_type,
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=sync_config.dispatcher_dag_tags,
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps(
                config.instance,
                watermark_variable_key_template,
                initial_sync_time,
            )
        )

        poll_expense_ledger = rail.VantagepointPsaledgerOperator(
            task_id='poll_expense_ledger',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            trans_type=sync_config.PSA_LEDGER_TRANS_TYPE,
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
            python_callable=lambda: logger.info(
                'No new employee expense vouchers in this poll window.'
            )
        )

        def build_processor_dag_conf(item):
            ctx_conf = rail.get_current_context()['dag_run'].conf
            return {
                'Period': item.get('Period'),
                'PostSeq': item.get('PostSeq'),
                'Employee': item.get('Employee'),
                'Voucher': item.get('Voucher'),
                'Org': item.get('Org') or '',
                'TransDate': item.get('TransDate') or '',
                'connections': ctx_conf.get('connections'),
                'customerId': ctx_conf.get('customerId'),
            }

        process_expense_vouchers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_expense_vouchers',
            items=lambda: rail.result('extract_expense_vouchers'),
            trigger_dag_id=(
                f'{sync_config.processor_dag_id_prefix}_{config.instance}'
            ),
            conf=build_processor_dag_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_expense_vouchers') }}",
            allowed_states=[
                'success', 'failed', 'upstream_failed', 'removed'
            ],
            failed_states=[],
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
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
            python_callable=lambda: update_last_sync_time(
                config.instance,
                watermark_variable_key_template,
            )
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=Variable.get(
                sync_config.middleware_api_base_url_variable_key,
                default_var='',
            ),
            trigger_rule='all_done'
        )

        (
            prepare_timestamps >> poll_expense_ledger >>
            extract_expense_vouchers >> check_if_vouchers_exist
        )

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
