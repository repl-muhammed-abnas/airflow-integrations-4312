"""
Dispatcher DAG for VP -> QBO Unit Transaction Sync.

Per-tenant: applies the polling watermark, queries the VP PSA Ledger
for new/updated unit transaction records (TransType='un'), and triggers
the per-record `unit_transaction_create` child DAG for each row.
Replaces the Workato `014_503_psa_poll_vantagepoint_posted_unit_transaction`
trigger recipe.

Single-leaf topology: each PSA Ledger row produces exactly one QBO
JournalEntry, so the dispatcher fans out directly to the create child
(no router). Mirrors timesheets_sync/dispatcher_dag.py with TransType
swapped and no billing-transfer filter (unit transactions have no
equivalent Desc1 marker to strip).
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_quickbooks_integration.unit_transaction_sync.utils.python_callable_method import (  # noqa: E501
    prepare_sync_timestamps_method,
    update_last_sync_time_method,
    is_integration_enabled_method,
    build_psa_ledger_filter_method,
    cap_records_for_audit,
)


def create_dag(config):
    """Per-tenant dispatcher: poll, fan out, gather errors, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=(
            f'vp_qbo_unit_transaction_sync_dispatcher_{config.instance}'
        ),
        description=(
            'Poll VP PSA Ledger for posted unit transactions '
            '(TransType=un) and trigger per-record QBO journal entry create'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        # Backstop for stuck deferred sensors so a hung child fanout
        # doesn't pin this DAG's only slot forever.
        dagrun_timeout=timedelta(hours=2),
        tags=[
            'vantagepoint_quickbooks',
            'unit_transaction_sync',
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
            python_callable=lambda: prepare_sync_timestamps_method(
                config.instance
            )
        )

        # Mirrors Workato CFG_DisableUnitTransactionIntegration account
        # property. Variable: CFG_DisableUnitTransactionIntegration_{instance}.
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
            trans_type='un'
        )

        # TEMP audit cap (mirrors timesheets_sync's filter slice) so dev
        # runs don't flood the QBO sandbox. Remove the cap before
        # production rollout.
        cap_records = rail.PythonOperator(
            task_id='cap_records_for_audit',
            python_callable=lambda: cap_records_for_audit(config.environment)
        )

        check_if_records_exist = rail.IfOperator(
            task_id='check_if_records_exist',
            test=lambda: len(rail.result('cap_records_for_audit') or []) > 0,
            yes_task='process_records',
            no_task='log_no_records'
        )

        log_no_records = rail.PythonOperator(
            task_id='log_no_records',
            python_callable=lambda: print(
                'No new/updated PSA unit-transaction records in this poll '
                'window.'
            )
        )

        def build_create_dag_conf(item):
            connections = (
                rail.get_current_context()['dag_run'].conf
                .get('connections')
            )
            return {
                'PostSeq': item.get('PostSeq'),
                'Period': item.get('Period'),
                'TransType': item.get('TransType'),
                'connections': connections,
                'customerId': (
                    rail.get_current_context()['dag_run'].conf
                    .get('customerId')
                ),
            }

        process_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_records',
            items=lambda: rail.result('cap_records_for_audit'),
            trigger_dag_id=(
                f'vp_qbo_unit_transaction_sync_create_{config.instance}'
            ),
            conf=build_create_dag_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        # Cover every terminal child-dag state so the sensor never stalls.
        # failed_states=[] keeps the sensor from short-circuiting on the
        # first failure — we want to gather errors from ALL children.
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
            yes_task='fail_unit_transaction_sync',
            no_task='update_last_sync_time'
        )

        fail_unit_transaction_sync = rail.FailOperator(
            task_id='fail_unit_transaction_sync',
            message=(
                "{{ result('gather_create_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        # trigger_rule='all_done' so the watermark advances on every
        # terminal state (including FailOperator). The disabled path is
        # handled INSIDE update_last_sync_time_method by checking the
        # check_disabled_flag XCom.
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
        check_disabled_flag >> rail.Label('Disabled') >> skip_run
        skip_run >> post_dag_run_details

        check_disabled_flag >> rail.Label('Enabled') >> poll_psa_ledger
        poll_psa_ledger >> cap_records >> check_if_records_exist

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
            fail_unit_transaction_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
