"""
Dispatcher DAG for Xero -> VP Poll Contact Updates Sync.

Per-tenant: applies the polling watermark, calls the Xero Contacts API for
contacts modified since the last run (If-Modified-Since header), and fans out
one processor DAG per contact for the employee-filter + firm-sync pipeline.

Replaces Workato `014-501 PSA Poll Xero Contact updates Vantagepoint` trigger
(poll interval 5 minutes, since_offset 0).

max_active_runs=1 prevents duplicate-poll overlap — each 5-minute run must
complete before the next fires.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta

from airflow.models import Variable
import rail

from vp_xero_integration.poll_contact_updates_sync.utils.python_callable_method import (
    prepare_sync_timestamps_method,
    update_last_sync_time_method,
    is_integration_enabled_method,
)


def create_dag(config):
    """Per-tenant dispatcher: poll Xero contacts, fan out per contact."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_poll_contact_updates_dispatcher_{config.instance}',
        description=(
            'Poll Xero for updated contacts and trigger per-contact '
            'employee-filter + firm-sync processor'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
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

        # Allow disabling the integration per-instance via Airflow Variable
        # CFG_DisablePollContactUpdatesXeroIntegration_{instance}.
        check_disabled_flag = rail.IfOperator(
            task_id='check_disabled_flag',
            test=lambda: is_integration_enabled_method(config.instance),
            yes_task='poll_xero_updated_contacts',
            no_task='skip_run',
        )

        skip_run = rail.EmptyOperator(task_id='skip_run')

        # Poll Xero for contacts modified since the last watermark.
        # modified_since maps to the If-Modified-Since HTTP header on the
        # GET /Contacts request — Xero returns only contacts updated after
        # that timestamp, matching the Workato updated_contact trigger behaviour.
        poll_xero_updated_contacts = rail.XeroContactOperator(
            task_id='poll_xero_updated_contacts',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
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
            python_callable=lambda: print(
                'No Xero contacts updated since last poll window.'
            ),
        )

        def build_processor_conf(item):
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            return {
                'ContactID': item.get('ContactID'),
                'UpdatedDateUTC': item.get('UpdatedDateUTC'),
                'connections': dag_run_conf.get('connections'),
                'customerId': dag_run_conf.get('customerId'),
            }

        process_contacts = rail.TriggerDagRunForEachItemOperator(
            task_id='process_contacts',
            items=lambda: (
                (rail.result('poll_xero_updated_contacts') or {}).get('data') or []
            ),
            trigger_dag_id=(
                f'vp_xero_poll_contact_updates_processor_{config.instance}'
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
            middleware_api_base_url=Variable.get(
                'middleware_api_base_url', default_var=''
            ),
            trigger_rule='all_done',
        )

        # --- wiring ---
        prepare_timestamps >> check_disabled_flag
        check_disabled_flag >> rail.Label('Disabled') >> skip_run
        skip_run >> post_dag_run_details

        check_disabled_flag >> rail.Label('Enabled') >> poll_xero_updated_contacts
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
            fail_contact_sync >> post_dag_run_details
        )
        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
