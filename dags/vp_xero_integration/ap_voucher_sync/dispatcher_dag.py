# dags/vp_xero_integration/ap_voucher_sync/dispatcher_dag.py
"""Dispatcher DAG for VP -> Xero AP Voucher Sync.

Per-tenant: applies the PostDate watermark, queries VP PSALedger for posted
AP vouchers (TransType=ap) in the window, groups header rows (AutoEntry="N",
TaxCode="") into unique (Period, PostSeq, Voucher) identities, and triggers
the create DAG per voucher. Gathers child errors; holds the watermark on any
failure so the same window re-polls next run.

Failure pattern: QBO-style (gather_processor_dag_errors, has_sync_errors,
FailOperator — watermark held on error; watermark advances on clean run).
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_xero_integration.ap_voucher_sync.config import (
    initial_sync_time,
    watermark_variable_key_template,
)
from vp_xero_integration.common.python_callable_method import (
    prepare_sync_timestamps,
    update_last_sync_time,
    has_sync_errors_method,
)
from vp_xero_integration.ap_voucher_sync.utils.python_callable_method import (
    build_vp_psaledger_ap_filter_method,
    extract_ap_vouchers_list_method,
    check_if_ap_vouchers_exist_method,
    is_integration_enabled_method,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: check disabled flag, poll PSALedger, fan out."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_ap_voucher_sync_dispatcher_{config.instance}',
        description=(
            'Poll VP PSALedger (TransType=ap) and trigger per-voucher '
            'Xero Bill create DAG'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_xero',
            'ap_voucher_sync',
            'dispatcher',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        },
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps(
                config.instance,
                watermark_variable_key_template,
                initial_sync_time,
            ),
        )

        check_disabled_flag = rail.IfOperator(
            task_id='check_disabled_flag',
            test=lambda: is_integration_enabled_method(config.instance),
            yes_task='get_changed_psaledger_ap_rows',
            no_task='skip_run',
        )

        skip_run = rail.PythonOperator(
            task_id='skip_run',
            python_callable=lambda: logger.info(
                'AP Voucher Xero integration is disabled '
                '(CFG_DisableApVoucherXeroIntegration_%s=true) — skipping run.',
                config.instance,
            ),
        )

        get_changed_psaledger_ap_rows = rail.VantagepointPsaledgerOperator(
            task_id='get_changed_psaledger_ap_rows',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            trans_type='ap',
            filters=build_vp_psaledger_ap_filter_method,
        )

        extract_ap_vouchers = rail.PythonOperator(
            task_id='extract_ap_vouchers_list',
            python_callable=extract_ap_vouchers_list_method,
        )

        check_if_ap_vouchers_exist = rail.IfOperator(
            task_id='check_if_ap_vouchers_exist',
            test=check_if_ap_vouchers_exist_method,
            yes_task='process_ap_vouchers',
            no_task='log_no_ap_vouchers',
        )

        log_no_ap_vouchers = rail.PythonOperator(
            task_id='log_no_ap_vouchers',
            python_callable=lambda: logger.info(
                'No newly posted VP AP vouchers in this poll window.'
            ),
        )

        def build_processor_dag_conf(item):
            ctx_conf = rail.get_current_context()['dag_run'].conf
            config_obj = ctx_conf.get('config') or {}
            return {
                'Period': item.get('Period'),
                'PostSeq': item.get('PostSeq'),
                'Voucher': item.get('Voucher') or '',
                'Batch': item.get('Batch') or '',
                'Vendor': item.get('Vendor') or '',
                'RefNo': item.get('RefNo') or '',
                'Desc1': item.get('Desc1') or '',
                'FirstTransDate': item.get('FirstTransDate') or '',
                'RowCount': item.get('RowCount') or 0,
                'CFG_VoucherStatusSubmitted': (
                    config_obj.get('CFG_VoucherStatusSubmitted')
                    or ctx_conf.get('CFG_VoucherStatusSubmitted')
                    or ''
                ),
                'connections': ctx_conf.get('connections'),
                'customerId': ctx_conf.get('customerId'),
            }

        process_ap_vouchers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_ap_vouchers',
            items=lambda: rail.result('extract_ap_vouchers_list'),
            trigger_dag_id=(
                f'vp_xero_ap_voucher_sync_ap_voucher_create_{config.instance}'
            ),
            conf=build_processor_dag_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            ),
        )

        # Allow all terminal child-dag states; failed_states=[] prevents
        # short-circuiting so we gather errors from all children before deciding.
        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_ap_vouchers') }}",
            allowed_states=[
                'success', 'failed', 'upstream_failed', 'removed'
            ],
            failed_states=[],
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            ),
        )

        # Task ID MUST be 'gather_processor_dag_errors' — has_sync_errors_method
        # from vp_xero_integration.common reads this exact XCom key.
        gather_processor_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_dag_errors',
            dag_runs="{{ result('process_ap_vouchers') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True,
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=has_sync_errors_method,
            yes_task='fail_ap_voucher_sync',
            no_task='update_last_sync_time',
        )

        fail_ap_voucher_sync = rail.FailOperator(
            task_id='fail_ap_voucher_sync',
            message=(
                "{{ result('gather_processor_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            ),
        )

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=lambda: update_last_sync_time(
                config.instance,
                watermark_variable_key_template,
            ),
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=Variable.get(
                'middleware_api_base_url', default_var=''
            ),
            trigger_rule='all_done',
        )

        # Main spine
        (
            prepare_timestamps >> check_disabled_flag
        )

        # Disabled branch
        (
            check_disabled_flag >> rail.Label('Disabled') >>
            skip_run >> post_dag_run_details
        )

        # Enabled branch: poll -> group -> check
        (
            check_disabled_flag >> rail.Label('Enabled') >>
            get_changed_psaledger_ap_rows >>
            extract_ap_vouchers >>
            check_if_ap_vouchers_exist
        )

        # No-vouchers branch: advance watermark and finish
        (
            check_if_ap_vouchers_exist >> rail.Label('No AP vouchers') >>
            log_no_ap_vouchers >> update_sync_time
        )

        # Vouchers-found branch: fan out -> gather -> gate
        (
            check_if_ap_vouchers_exist >> rail.Label('AP vouchers found') >>
            process_ap_vouchers >> wait_for_processor_dag_runs >>
            gather_processor_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No errors') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Errors') >>
            fail_ap_voucher_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
