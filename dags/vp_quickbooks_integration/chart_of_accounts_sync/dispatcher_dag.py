"""
Dispatcher DAG for QBO -> VP Chart of Accounts Sync.

Per-tenant: applies the polling watermark, queries QuickBooks for accounts
created/updated in the window, fetches the full VP /Accounts list (slimmed to
a code/name/type index), and triggers the processor DAG per QBO account.
Replaces the Workato `new_updated_account` polling trigger on
`014_503_psa_poll_quickbooks_upserted_account_code` and the data-loading
front half of `014_503_psa_sync_account_codes`.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_quickbooks_integration.chart_of_accounts_sync.config import (
    initial_sync_time,
    watermark_variable_key_template,
)
from vp_quickbooks_integration.common.python_callable_method import (
    prepare_sync_timestamps,
    update_last_sync_time,
    has_sync_errors_method,
)
from vp_quickbooks_integration.chart_of_accounts_sync.utils.python_callable_method import (  # noqa: E501
    extract_account_list_method,
    build_vp_account_index_method,
    check_if_accounts_exist_method,
    build_processor_dag_conf,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll QBO accounts, fan out, gather, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_chart_of_accounts_sync_dispatcher_{config.instance}',
        description=(
            'Poll QuickBooks for changed accounts and trigger the per-account '
            'chart-of-accounts processor'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_quickbooks',
            'chart_of_accounts_sync',
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

        get_recently_changed_accounts = rail.QuickBooksAccountOperator(
            task_id='get_recently_changed_accounts',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            query=(
                "SELECT * FROM Account WHERE Active = true AND "
                "MetaData.LastUpdatedTime >= "
                "'{{ result('prepare_sync_timestamps')"
                "['last_sync_time'] }}'"
                " AND MetaData.LastUpdatedTime < "
                "'{{ result('prepare_sync_timestamps')"
                "['current_sync_time'] }}'"
            )
        )

        extract_accounts = rail.PythonOperator(
            task_id='extract_account_list',
            python_callable=extract_account_list_method
        )

        get_all_vp_accounts = rail.VantagepointChartOfAccountsOperator(
            task_id='get_all_vp_accounts',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
        )

        build_vp_account_index = rail.PythonOperator(
            task_id='build_vp_account_index',
            python_callable=build_vp_account_index_method
        )

        check_if_accounts_exist = rail.IfOperator(
            task_id='check_if_accounts_exist',
            test=check_if_accounts_exist_method,
            yes_task='get_all_vp_accounts',
            no_task='log_no_accounts'
        )

        log_no_accounts = rail.PythonOperator(
            task_id='log_no_accounts',
            python_callable=lambda: logger.info(
                "No recently changed QuickBooks accounts in this poll window "
                "(%s to %s)",
                rail.result('prepare_sync_timestamps')['last_sync_time'],
                rail.result('prepare_sync_timestamps')['current_sync_time'],
            )
        )

        process_accounts = rail.TriggerDagRunForEachItemOperator(
            task_id='process_accounts',
            items=lambda: rail.result('extract_account_list'),
            trigger_dag_id=(
                f'vp_qbo_chart_of_accounts_sync_processor_{config.instance}'
            ),
            conf=build_processor_dag_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        # Cover every terminal child-dag state so the sensor never stalls on
        # `upstream_failed` / `removed`. `failed_states=[]` keeps it from
        # short-circuiting on the first failure — we gather errors from all
        # children before deciding whether to advance the watermark.
        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_accounts') }}",
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
            dag_runs="{{ result('process_accounts') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=has_sync_errors_method,
            yes_task='fail_chart_of_accounts_sync',
            no_task='update_last_sync_time'
        )

        fail_chart_of_accounts_sync = rail.FailOperator(
            task_id='fail_chart_of_accounts_sync',
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
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done'
        )

        (
            prepare_timestamps >> get_recently_changed_accounts >>
            extract_accounts >> check_if_accounts_exist
        )

        (
            check_if_accounts_exist >> rail.Label('No accounts') >>
            log_no_accounts >> update_sync_time
        )

        (
            check_if_accounts_exist >> rail.Label('Accounts found') >>
            get_all_vp_accounts >> build_vp_account_index >>
            process_accounts >> wait_for_processor_dag_runs >>
            gather_processor_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_chart_of_accounts_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
