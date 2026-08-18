"""
Dispatcher DAG for Xero -> VP Poll Contact Updates Sync (V2 IPA GitSync architecture).

Per-tenant: applies the polling watermark, calls the Xero Contacts API for
contacts modified since the last run (If-Modified-Since header), and fans out
one processor DAG per contact for the employee-filter + firm-sync pipeline.

Replaces Workato `014-501 PSA Poll Xero Contact updates Vantagepoint` trigger
(poll interval 5 minutes, since_offset 0).

max_active_runs=1 prevents duplicate-poll overlap — each 5-minute run must
complete before the next fires.

V2 changes from V1:
  - schedule_interval from config.schedule_interval (not None)
  - xero_conn_id from config.connections (not Jinja dag_run.conf)
  - connections/customerId in build_processor_conf injected from config
  - middleware_api_base_url via Jinja var.value.get (not parse-time Variable.get)
  - check_disabled_flag / skip_run removed (RAIL handles disabled=True at parse time)
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta

import rail

from vp_xero_integration_v2.common.python_callable_method import get_connections
from vp_xero_integration_v2.poll_contact_updates_sync.utils.python_callable_method import (
    prepare_sync_timestamps_method,
    update_last_sync_time_method,
)

_log = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll Xero contacts, fan out per contact."""
    connections = get_connections(config)
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_poll_contact_updates_sync_dispatcher_{config.instance}',
        description=(
            'Poll Xero for updated contacts and trigger per-contact '
            'employee-filter + firm-sync processor'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        dagrun_timeout=timedelta(hours=1),
        tags=['vantagepoint_xero', 'poll_contact_updates', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps_method(
                config.instance, config.initial_sync_time
            ),
        )

        # Poll Xero for contacts modified since the last watermark.
        # modified_since maps to the If-Modified-Since HTTP header on the
        # GET /Contacts request — Xero returns only contacts updated after
        # that timestamp, matching the Workato updated_contact trigger behaviour.
        poll_xero_updated_contacts = rail.XeroContactOperator(
            task_id='poll_xero_updated_contacts',
            xero_conn_id=connections['xero'],
            operation='search',
            paginate=True,
            modified_since="{{ result('prepare_sync_timestamps')['last_sync_time'] }}",
        )

        check_if_contacts_exist = rail.IfOperator(
            task_id='check_if_contacts_exist',
            test=lambda: len(
                (rail.result('poll_xero_updated_contacts') or {}).get('data') or []
            ) > 0,
            yes_task='process_contacts',
            no_task='log_no_contacts',
        )

        log_no_contacts = rail.PythonOperator(
            task_id='log_no_contacts',
            python_callable=lambda: _log.info(
                'No Xero contacts updated since last poll window.'
            ),
        )

        def build_processor_conf(item):
            return {
                'ContactID': item.get('ContactID'),
                'UpdatedDateUTC': item.get('UpdatedDateUTC'),
                'connections': connections,
                'customerId': config.customer_id,
            }

        process_contacts = rail.TriggerDagRunForEachItemOperator(
            task_id='process_contacts',
            items=lambda: (
                (rail.result('poll_xero_updated_contacts') or {}).get('data') or []
            ),
            trigger_dag_id=(
                f'vp_xero_poll_contact_updates_sync_processor_{config.instance}'
            ),
            conf=build_processor_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Cover all terminal processor states; failed_states=[] lets us gather
        # errors from every child before deciding to fail the dispatcher.
        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_contacts') }}",
            allowed_states=[
                'success', 'failed', 'upstream_failed', 'removed'
            ],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_processor_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_errors',
            dag_runs="{{ result('process_contacts') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True,
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: len(
                rail.result('gather_processor_errors') or []
            ) > 0,
            yes_task='fail_contact_sync',
            no_task='update_last_sync_time',
        )

        fail_contact_sync = rail.FailOperator(
            task_id='fail_contact_sync',
            message=(
                "{{ result('gather_processor_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            ),
        )

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            trigger_rule='all_done',
            python_callable=lambda: update_last_sync_time_method(config.instance),
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url="{{ var.value.get('middleware_api_base_url', '') }}",
            trigger_rule='all_done',
        )

        # --- wiring ---
        prepare_timestamps >> poll_xero_updated_contacts
        poll_xero_updated_contacts >> check_if_contacts_exist

        (
            check_if_contacts_exist >> rail.Label('No contacts') >>
            log_no_contacts >> update_sync_time
        )
        (
            check_if_contacts_exist >> rail.Label('Contacts found') >>
            process_contacts >> wait_for_processor_dag_runs >>
            gather_processor_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_contact_sync >> update_sync_time
        )
        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
