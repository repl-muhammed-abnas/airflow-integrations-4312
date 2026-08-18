"""
Processor DAG for VP -> Xero Firm Sync Upsert.

Runs once per VP firm surfaced by the dispatcher. Reproduces the per-firm
FOREACH body of the Workato worker `014_501_psa_upsert_contact_in_xero`
(34 steps) as a RAIL IfOperator decision tree:

  get_vp_firm_data      → VP GET /firm/{FirmID}; None on 404 (firm deleted)
  is_vp_firm_found      → branch on VP found vs. not-found (deleted)

  VP found path:
    get_vp_addresses    → VP GET /firm/{FirmID}/address (for TaxRegistrationNumber)
    decide_action       → ReadyForApproval gate + map_firm lookup + field derivation;
                          returns action dict ('create'/'update'/'skip_*')
    is_create_action    → yes: create_xero_contact → upsert_map_firm_after_create
    is_update_action    → yes: update_xero_contact → upsert_map_firm_after_update
                          no:  log_skip (skip_archived or skip_not_ready)

  VP not-found path (firm deleted in VP):
    lookup_map_firm_for_delete → is_in_map
      → yes: archive_xero_contact (ContactStatus=ARCHIVED) → update_map_archived
      → no:  log_skip_not_in_map  (no Xero contact to archive)

  catch_processor_dag_error (trigger_rule='one_failed', SOLE LEAF):
    Returns error dict; does NOT raise. Every work task has a direct edge to
    this task — on the happy path it is skipped; on any failure it fires and
    captures the error for GatherResultsFromDagRunsOperator.

Re-run safety: map_firm UNIQUE(ContactID) ensures upsert is idempotent.
The dispatcher's filterHash watermark prevents reprocessing the same window.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_xero_integration_v2.firm_sync_upsert.utils.python_callable_method import (
    get_vp_firm_data_method,
    is_vp_firm_found_method,
    decide_action_method,
    is_create_action_method,
    is_update_action_method,
    build_create_contact_body,
    build_update_contact_body,
    build_archive_from_delete_body,
    upsert_map_firm_after_create_method,
    upsert_map_firm_after_update_method,
    lookup_map_firm_for_delete_method,
    is_in_map_for_delete_method,
    update_map_archived_method,
    log_skip_method,
    log_skip_not_in_map_method,
    capture_processor_error,
)


def create_dag(config):
    """Per-VP-firm processor DAG."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_firm_sync_upsert_processor_{config.instance}',
        description=(
            'Upsert one VP firm into Xero as a contact '
            '(create / update / skip-archived / archive-deleted)'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_xero', 'firm_sync_upsert', 'processor'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        # ---- Step 1: GET VP firm (None on 404) ----
        get_vp_firm_data = rail.PythonOperator(
            task_id='get_vp_firm_data',
            python_callable=get_vp_firm_data_method,
        )

        # ---- Branch: VP firm found? ----
        is_vp_firm_found = rail.IfOperator(
            task_id='is_vp_firm_found',
            test=is_vp_firm_found_method,
            yes_task='get_vp_addresses',
            no_task='lookup_map_firm_for_delete',
        )

        # ==============================================================
        # VP NOT-FOUND path (firm deleted in VP)
        # ==============================================================

        lookup_map_for_delete = rail.PythonOperator(
            task_id='lookup_map_firm_for_delete',
            python_callable=lookup_map_firm_for_delete_method,
        )

        is_in_map = rail.IfOperator(
            task_id='is_in_map',
            test=is_in_map_for_delete_method,
            yes_task='archive_xero_contact',
            no_task='log_skip_not_in_map',
        )

        # Archive the Xero contact (ContactStatus=ARCHIVED).
        archive_xero_contact = rail.XeroContactOperator(
            task_id='archive_xero_contact',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='update',
            request_body=build_archive_from_delete_body,
        )

        update_map_archived = rail.PythonOperator(
            task_id='update_map_archived',
            python_callable=update_map_archived_method,
        )

        log_skip_not_in_map = rail.PythonOperator(
            task_id='log_skip_not_in_map',
            python_callable=log_skip_not_in_map_method,
        )

        # ==============================================================
        # VP FOUND path (normal upsert)
        # ==============================================================

        # Step 6 (Workato): GET /firm/{FirmID}/address for TaxRegistrationNumber.
        # client_id IS a template_field on VantagepointFirmAddressOperator.
        # pagination=False: this is a single-firm resource endpoint; the VP
        # address endpoint returns all addresses in one non-paginated response.
        get_vp_addresses = rail.VantagepointFirmAddressOperator(
            task_id='get_vp_addresses',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            request_method='GET',
            client_id="{{ dag_run.conf.FirmID }}",
            pagination=False,
        )

        # Steps 2–9 (Workato): ReadyForApproval gate, map_firm lookup,
        # field derivation, routing decision.
        decide_action = rail.PythonOperator(
            task_id='decide_action',
            python_callable=decide_action_method,
        )

        # ---- Create branch ----
        is_create = rail.IfOperator(
            task_id='is_create_action',
            test=is_create_action_method,
            yes_task='create_xero_contact',
            no_task='is_update_action',
        )

        create_xero_contact = rail.XeroContactOperator(
            task_id='create_xero_contact',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='create',
            request_body=build_create_contact_body,
        )

        upsert_map_firm_create = rail.PythonOperator(
            task_id='upsert_map_firm_after_create',
            python_callable=upsert_map_firm_after_create_method,
        )

        # ---- Update branch ----
        is_update = rail.IfOperator(
            task_id='is_update_action',
            test=is_update_action_method,
            yes_task='update_xero_contact',
            no_task='log_skip',
        )

        update_xero_contact = rail.XeroContactOperator(
            task_id='update_xero_contact',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='update',
            request_body=build_update_contact_body,
        )

        upsert_map_firm_update = rail.PythonOperator(
            task_id='upsert_map_firm_after_update',
            python_callable=upsert_map_firm_after_update_method,
        )

        # ---- Skip branch (skip_archived or skip_not_ready) ----
        log_skip = rail.PythonOperator(
            task_id='log_skip',
            python_callable=log_skip_method,
        )

        # ---- Sole leaf: error capture ----
        # trigger_rule='one_failed' fires on a `failed` DIRECT upstream; every
        # work task must therefore have a direct edge here. On the happy path
        # this task is SKIPPED (no failures), keeping the child DAG SUCCESS so
        # the dispatcher's WaitForDagRunsSensor sees success and
        # GatherResultsFromDagRunsOperator only aggregates real errors.
        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
            op_args=[
                "{{ dag_run.conf.get('FirmID') or '' }}",
                "{{ dag_run.conf.get('FirmName') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        # ---- Wiring: decision spine ----
        get_vp_firm_data >> is_vp_firm_found

        # VP not-found branch.
        (
            is_vp_firm_found >> rail.Label('Not found') >>
            lookup_map_for_delete >> is_in_map
        )
        is_in_map >> rail.Label('In map') >> archive_xero_contact >> update_map_archived
        is_in_map >> rail.Label('Not in map') >> log_skip_not_in_map

        # VP found branch.
        (
            is_vp_firm_found >> rail.Label('Found') >>
            get_vp_addresses >> decide_action >> is_create
        )
        is_create >> rail.Label('Create') >> create_xero_contact >> upsert_map_firm_create
        is_create >> rail.Label('Not create') >> is_update
        is_update >> rail.Label('Update') >> update_xero_contact >> upsert_map_firm_update
        is_update >> rail.Label('Skip') >> log_skip

        # ---- Every work task → catch_processor_dag_error ----
        # Ensures catch fires on ANY single-task failure (trigger_rule='one_failed'
        # only fires when a DIRECT upstream failed, not on upstream_failed).
        get_vp_firm_data >> catch_processor_dag_error
        is_vp_firm_found >> catch_processor_dag_error
        lookup_map_for_delete >> catch_processor_dag_error
        is_in_map >> catch_processor_dag_error
        archive_xero_contact >> catch_processor_dag_error
        update_map_archived >> catch_processor_dag_error
        log_skip_not_in_map >> catch_processor_dag_error
        get_vp_addresses >> catch_processor_dag_error
        decide_action >> catch_processor_dag_error
        is_create >> catch_processor_dag_error
        create_xero_contact >> catch_processor_dag_error
        upsert_map_firm_create >> catch_processor_dag_error
        is_update >> catch_processor_dag_error
        update_xero_contact >> catch_processor_dag_error
        upsert_map_firm_update >> catch_processor_dag_error
        log_skip >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
