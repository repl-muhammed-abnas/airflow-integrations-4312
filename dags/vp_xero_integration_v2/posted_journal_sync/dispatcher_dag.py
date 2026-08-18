# dags/vp_xero_integration_v2/posted_journal_sync/dispatcher_dag.py
"""Dispatcher DAG for VP -> Xero Posted Journal Entry Sync (V2 IPA GitSync architecture).

Per-tenant: applies the polling watermark, queries the VP PSA Ledger
for new/updated journal entry records (TransType='je'), and triggers
the per-record `journal_export_create` child DAG for each row.
Replaces the Workato `014_501_psa_poll_vantagepoint_posted_journal_for_xero`
trigger recipe.

Single-leaf topology: each PSA Ledger row produces exactly one Xero
ManualJournal, so the dispatcher fans out directly to the create child
(no router). TransType='je' (journal entries).

V2 changes from V1:
  - schedule_interval from config.schedule_interval (not None)
  - vp_conn_id from config.connections['vantagepoint'] (not Jinja dag_run.conf)
  - connections/customerId in build_create_dag_conf from config (not dag_run.conf)
  - middleware_api_base_url via Jinja var.value.get (not parse-time Variable.get)
  - check_disabled_flag / skip_run removed (RAIL handles disabled=True at parse time)
  - prepare_sync_timestamps / update_last_sync_time from V2 common directly
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration_v2.posted_journal_sync.config import (
    TRANS_TYPE,
    watermark_variable_key_template,
)
from vp_xero_integration_v2.common.python_callable_method import (
    get_connections,
    prepare_sync_timestamps,
    update_last_sync_time,
)
from vp_xero_integration_v2.posted_journal_sync.utils.python_callable_method import (  # noqa: E501
    build_psa_ledger_filter_method,
)

_log = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll, fan out, gather errors, advance watermark."""
    connections = get_connections(config)
    with rail.create_airflow_dag(
        dag_id=(
            f'vp_xero_posted_journal_sync_dispatcher_{config.instance}'
        ),
        description=(
            'Poll VP PSA Ledger for posted journal entries '
            '(TransType=je) and trigger per-record Xero manual journal create'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        # Backstop for stuck deferred sensors so a hung child fanout
        # doesn't pin this DAG's only slot forever.
        dagrun_timeout=timedelta(hours=2),
        tags=[
            'vantagepoint_xero',
            'journal_sync',
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
                config.initial_sync_time,
            )
        )

        poll_psa_ledger = rail.VantagepointPsaledgerOperator(
            task_id='poll_psa_ledger',
            vp_conn_id=connections['vantagepoint'],
            request_method='GET',
            filters=build_psa_ledger_filter_method,
            trans_type=TRANS_TYPE
        )

        check_if_records_exist = rail.IfOperator(
            task_id='check_if_records_exist',
            test=lambda: len(rail.result('poll_psa_ledger') or []) > 0,
            yes_task='process_records',
            no_task='log_no_records'
        )

        log_no_records = rail.PythonOperator(
            task_id='log_no_records',
            python_callable=lambda: _log.info(
                'No new/updated PSA journal-entry records in this poll window.'
            )
        )

        def build_create_dag_conf(item):
            return {
                # Period/PostSeq stay STRINGS end-to-end through this
                # conf hop — do NOT cast to int here. Only the create
                # DAG's build_record_filter narrowly casts PostSeq to
                # int when re-querying PSALedger.
                'PostSeq': item.get('PostSeq'),
                'Period': item.get('Period'),
                'TransType': item.get('TransType'),
                'connections': connections,
                'customerId': config.customer_id,
            }

        process_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_records',
            items=lambda: rail.result('poll_psa_ledger'),
            trigger_dag_id=(
                f'vp_xero_journal_sync_journal_export_create_'
                f'{config.instance}'
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
            yes_task='fail_journal_sync',
            no_task='update_last_sync_time'
        )

        fail_journal_sync = rail.FailOperator(
            task_id='fail_journal_sync',
            message=(
                "{{ result('gather_create_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        # trigger_rule='all_done': watermark advances even when some journal
        # exports fail. Intentional — a record that fails persistently
        # (bad account mapping, Xero validation rejection) would otherwise
        # re-enter every subsequent poll window indefinitely. Failures are
        # reported to middleware via PostDagRunDetailsToMiddlewareApiOperator
        # so ops can investigate; the DAG itself is still marked failed by
        # FailOperator so the failure is visible in Airflow.
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

        prepare_timestamps >> poll_psa_ledger >> check_if_records_exist

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
            fail_journal_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
