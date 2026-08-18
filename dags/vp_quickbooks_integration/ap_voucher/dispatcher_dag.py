"""
Dispatcher DAG for VP -> QBO AP Voucher Sync (region-agnostic).

Per-tenant: applies the polling watermark, queries VP PSALedger for posted AP
vouchers (TransType=AP) modified in the window, groups the line-level rows into
unique (Period, PostSeq) voucher identities, and triggers the region-specific
processor DAG per voucher. The region (US vs CA-UK) is read from the tenant's
`CFG_Region` (delivered in the integration `config` field) and selects the
processor `trigger_dag_id` per item — there is no US/CA-UK business logic here.

Replaces the Workato poll recipe
`014_503_psa_poll_vantagepoint_posted_ap_voucher` (the CFG_Region branch) plus
the shared poll/group portion of the `..._exports_to_quickbooks_{us,ca_uk}`
dispatcher recipes.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_quickbooks_integration.ap_voucher.config import (
    initial_sync_time,
    watermark_variable_key_template,
)
from vp_quickbooks_integration.common.python_callable_method import (
    prepare_sync_timestamps,
    update_last_sync_time,
    has_sync_errors_method,
)
from vp_quickbooks_integration.ap_voucher.utils.python_callable_method import (  # noqa: E501
    build_vp_psaledger_ap_filter_method,
    extract_ap_vouchers_list_method,
    check_if_ap_vouchers_exist_method,
    resolve_region_slug_method,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll PSALedger, route per region, gather, advance."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_ap_voucher_sync_dispatcher_{config.instance}',
        description=(
            'Poll VP PSALedger (TransType=AP) and trigger per-voucher '
            'region processor (US or CA-UK)'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_quickbooks',
            'ap_voucher_sync',
            'dispatcher',
        ],
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

        get_changed_psaledger_ap_rows = rail.VantagepointPsaledgerOperator(
            task_id='get_changed_psaledger_ap_rows',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            trans_type='AP',
            filters=build_vp_psaledger_ap_filter_method,
        )

        extract_ap_vouchers = rail.PythonOperator(
            task_id='extract_ap_vouchers_list',
            python_callable=extract_ap_vouchers_list_method
        )

        check_if_ap_vouchers_exist = rail.IfOperator(
            task_id='check_if_ap_vouchers_exist',
            test=check_if_ap_vouchers_exist_method,
            yes_task='process_ap_vouchers',
            no_task='log_no_ap_vouchers'
        )

        log_no_ap_vouchers = rail.PythonOperator(
            task_id='log_no_ap_vouchers',
            python_callable=lambda: logger.info(
                'No newly modified VP posted AP vouchers in this poll window.'
            )
        )

        def route_processor_dag_id(item=None, **context):
            """Pick the region processor DAG id for this tenant's vouchers.

            Reads CFG_Region from the dag_run.conf (integration `config`
            field) and maps it to a processor slug. CA and UK both route to
            the `ca_uk` processor. The whole dispatcher run is one tenant /
            one region, so every item resolves to the same processor.
            """
            dag_run = context.get('dag_run')
            conf = (dag_run.conf if dag_run else {}) or {}
            config_obj = conf.get('config') or {}
            cfg_region = (
                config_obj.get('CFG_Region')
                or conf.get('CFG_Region')
                or ''
            )
            slug = resolve_region_slug_method(cfg_region)
            return f'vp_qbo_ap_voucher_sync_{slug}_processor_{config.instance}'

        def build_processor_dag_conf(item):
            ctx_conf = (
                rail.get_current_context()['dag_run'].conf
            )
            config_obj = ctx_conf.get('config') or {}
            return {
                'Period': item.get('Period'),
                'PostSeq': item.get('PostSeq'),
                'Voucher': item.get('Voucher') or '',
                'Batch': item.get('Batch') or '',
                'InvoiceNumber': item.get('InvoiceNumber') or '',
                'Vendor': item.get('Vendor') or '',
                'RefNo': item.get('RefNo') or '',
                'Desc1': item.get('Desc1') or '',
                'FirstTransDate': item.get('FirstTransDate') or '',
                'RowCount': item.get('RowCount') or 0,
                'CFG_Region': (
                    config_obj.get('CFG_Region')
                    or ctx_conf.get('CFG_Region')
                    or ''
                ),
                # CA-UK only: the "no VAT" QBO tax-code NAME; the processor
                # resolves it to a QBO TaxCode Id and stamps it on untaxed
                # lines (Workato NoTaxCodeID).
                'CFG_NoTaxCode': (
                    config_obj.get('CFG_NoTaxCode')
                    or ctx_conf.get('CFG_NoTaxCode')
                    or ''
                ),
                'connections': ctx_conf.get('connections'),
                'customerId': ctx_conf.get('customerId'),
            }

        process_ap_vouchers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_ap_vouchers',
            items=lambda: rail.result('extract_ap_vouchers_list'),
            trigger_dag_id=route_processor_dag_id,
            conf=build_processor_dag_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        # Cover every terminal child-dag state so the sensor never stalls on
        # `upstream_failed` / `removed`. `failed_states=[]` keeps it from
        # short-circuiting on the first failure — we want to gather errors
        # from all children.
        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_ap_vouchers') }}",
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
            dag_runs="{{ result('process_ap_vouchers') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=has_sync_errors_method,
            yes_task='fail_ap_voucher_sync',
            no_task='update_last_sync_time'
        )

        fail_ap_voucher_sync = rail.FailOperator(
            task_id='fail_ap_voucher_sync',
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
                'middleware_api_base_url', default_var=''
            ),
            trigger_rule='all_done'
        )

        (
            prepare_timestamps >> get_changed_psaledger_ap_rows >>
            extract_ap_vouchers >> check_if_ap_vouchers_exist
        )

        (
            check_if_ap_vouchers_exist >> rail.Label('No AP vouchers') >>
            log_no_ap_vouchers >> update_sync_time
        )

        (
            check_if_ap_vouchers_exist >> rail.Label('AP vouchers found') >>
            process_ap_vouchers >> wait_for_processor_dag_runs >>
            gather_processor_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_ap_voucher_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
