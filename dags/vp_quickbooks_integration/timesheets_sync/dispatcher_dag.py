"""
Dispatcher DAG for VP -> QBO Timesheets Sync.

Per-tenant: applies the polling watermark, queries the VP PSA Ledger
for new/updated timesheet records (TransType='ts'), filters out
'Labor Posting - Billing Transfer' rows, and triggers the router DAG
for each remaining record. Replaces the Workato
`014_503_psa_poll_vantagepoint_timesheets` recipe.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_quickbooks_integration.timesheets_sync.utils.python_callable_method import (  # noqa: E501
    prepare_sync_timestamps_method,
    update_last_sync_time_method,
    is_integration_enabled_method,
    build_psa_ledger_filter_method,
    filter_billing_transfer_records_method,
)


def create_dag(config):
    """Per-tenant dispatcher: poll, filter, fan out, gather, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=(
            f'vp_qbo_timesheets_sync_dispatcher_{config.instance}'
        ),
        description=(
            'Poll VP PSA timesheet ledger and trigger per-record router'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        # Backstop for stuck deferred sensors so a hung child fanout
        # doesn't pin this DAG's only slot forever.
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_quickbooks', 'timesheets_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps_method(
                config.instance
            )
        )

        # Workato CFG_DisableTimesheetIntegration account property.
        check_disabled_flag = rail.IfOperator(
            task_id='check_disabled_flag',
            test=lambda: is_integration_enabled_method(config.instance),
            yes_task='poll_psa_ledger',
            no_task='skip_run'
        )

        skip_run = rail.EmptyOperator(task_id='skip_run')

        poll_psa_ledger = rail.VantagepointPsaledgerOperator(
            task_id='poll_psa_ledger',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            filters=build_psa_ledger_filter_method,
            trans_type='ts'
        )

        filter_records = rail.PythonOperator(
            task_id='filter_records',
            python_callable=filter_billing_transfer_records_method
        )

        check_if_records_exist = rail.IfOperator(
            task_id='check_if_records_exist',
            test=lambda: len(rail.result('filter_records')) > 0,
            yes_task='process_records',
            no_task='log_no_records'
        )

        log_no_records = rail.PythonOperator(
            task_id='log_no_records',
            python_callable=lambda: print(
                'No new/updated PSA timesheet records in this poll window.'
            )
        )

        def build_create_dag_conf(item):
            conf = rail.get_current_context()['dag_run'].conf
            return {
                'PostSeq': item.get('PostSeq'),
                'Period': item.get('Period'),
                'TransType': item.get('TransType'),
                'customerId': conf.get('customerId'),
                'connections': conf.get('connections'),
            }

        process_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_records',
            items=lambda: rail.result('filter_records'),
            trigger_dag_id=(
                f'vp_qbo_timesheets_sync_time_activity_create_'
                f'{config.instance}'
            ),
            conf=build_create_dag_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        # Cover every terminal child-dag state so the sensor never stalls
        # on `upstream_failed` / `removed`. `failed_states=[]` keeps the
        # sensor from short-circuiting on the first failure — we want to
        # gather errors from all children.
        wait_for_create_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_dag_runs',
            dag_runs="{{ result('process_records') }}",
            allowed_states=[
                'success', 'failed', 'upstream_failed', 'removed'
            ],
            failed_states=[],
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        gather_create_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_create_dag_errors',
            dag_runs="{{ result('process_records') }}",
            dagrun_task_id='catch_create_dag_error',
            flatten=True
        )

        # Callable test (not Jinja string) — RAIL's IfOperator expects a
        # bool-returning callable; a non-empty Jinja string is always
        # truthy and would route every run down the failure branch.
        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: len(
                rail.result('gather_create_dag_errors') or []
            ) > 0,
            yes_task='fail_timesheets_sync',
            no_task='update_last_sync_time'
        )

        # FailOperator concatenates per-record errors into a single
        # message and marks the dispatcher run as failed. Middleware
        # picks this up via post_dag_run_details (trigger_rule='all_done').
        fail_timesheets_sync = rail.FailOperator(
            task_id='fail_timesheets_sync',
            message=(
                "{{ result('gather_create_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        # trigger_rule='all_done' so the watermark advances on every
        # terminal state, INCLUDING when the error branch's FailOperator
        # raises (default all_success would skip this task and the next
        # poll would re-process the same window forever). The disabled
        # path is handled INSIDE update_last_sync_time_method by checking
        # the check_disabled_flag XCom — see that method.
        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            trigger_rule='all_done',
            python_callable=lambda: update_last_sync_time_method(
                config.instance
            )
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=Variable.get(
                'middleware_api_base_url', default_var=''
            ),
            trigger_rule='all_done'
        )

        prepare_timestamps >> check_disabled_flag
        # Disabled branch bypasses update_sync_time entirely — combined
        # with trigger_rule='none_skipped' on update_sync_time this
        # guarantees the watermark cannot advance while the integration
        # is disabled.
        check_disabled_flag >> rail.Label('Disabled') >> skip_run
        skip_run >> post_dag_run_details

        check_disabled_flag >> rail.Label('Enabled') >> poll_psa_ledger
        poll_psa_ledger >> filter_records >> check_if_records_exist

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
            fail_timesheets_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
