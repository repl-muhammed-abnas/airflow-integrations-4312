"""
Processor DAG for Xero -> VP Poll Contact Updates Sync.

Per-contact child DAG triggered by the dispatcher. Implements the conditional
logic from Workato `014-501 PSA Poll Xero Contact updates Vantagepoint`:

  Step 1: Lookup map_employee by ContactID.
  Step 2: IF contact IS an employee → stop (skip sync).
  Step 3: Lookup map_firm by ContactID.
  Step 4: IF no firm map row OR UpdatedDateUTC > stored ModDate → proceed.
  Step 5: Fetch full Xero contact record, then sync to VP Firm + upsert map_firm.

Receives in dag_run.conf:
  ContactID       - Xero contact GUID
  UpdatedDateUTC  - ISO-8601 timestamp from the Xero polling trigger
  connections     - dict with 'xero' and 'vantagepoint' conn IDs
  customerId      - tenant customer ID
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta

import rail

from vp_xero_integration.poll_contact_updates_sync.utils.python_callable_method import (
    check_employee_map_method,
    is_employee_contact_method,
    check_firm_map_method,
    firm_needs_sync_method,
    sync_single_xero_firm_to_vp,
    capture_processor_dag_error,
)


def create_dag(config):
    """Per-contact processor: employee filter → change detection → firm sync."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_poll_contact_updates_processor_{config.instance}',
        description=(
            'Per-contact: employee filter, firm change detection, VP firm sync'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'poll_contact_updates', 'processor'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        # Step 1 — Workato lookup map_employee by ContactID.
        # Returns the row dict if the contact is a known employee, else None.
        check_employee_map = rail.PythonOperator(
            task_id='check_employee_map',
            python_callable=check_employee_map_method,
        )

        # Step 2 — if IS employee: stop (no firm sync for employee contacts).
        is_employee_contact = rail.IfOperator(
            task_id='is_employee_contact',
            test=is_employee_contact_method,
            yes_task='skip_employee_contact',
            no_task='check_firm_map',
        )

        skip_employee_contact = rail.PythonOperator(
            task_id='skip_employee_contact',
            python_callable=lambda: print(
                "Contact is a mapped employee — skipping firm sync."
            ),
        )

        # Step 3 — Workato lookup map_firm by ContactID.
        # Returns the row dict (with ModDate) if already mapped, else None.
        check_firm_map = rail.PythonOperator(
            task_id='check_firm_map',
            python_callable=check_firm_map_method,
        )

        # Step 4 — proceed only if: no existing firm row, OR Xero timestamp
        # is newer than the stored ModDate (Workato condition: blank OR UpdatedDateUTC > col8).
        needs_sync = rail.IfOperator(
            task_id='needs_sync',
            test=firm_needs_sync_method,
            yes_task='fetch_xero_contact',
            no_task='skip_no_change',
        )

        skip_no_change = rail.PythonOperator(
            task_id='skip_no_change',
            python_callable=lambda: print(
                "Firm map entry exists and Xero timestamp is not newer — skipping."
            ),
        )

        # Fetch the full Xero contact record by ContactID.
        # `where` is embedded in `filters` (a template field) at __init__ time,
        # so Jinja renders the ContactID GUID correctly at task execution.
        fetch_xero_contact = rail.XeroContactOperator(
            task_id='fetch_xero_contact',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='search',
            where='ContactID=Guid("{{ dag_run.conf.ContactID }}")',
            paginate=False,
        )

        # Step 5 — create/update VP Firm and upsert map_firm collection row.
        sync_firm_to_vp = rail.PythonOperator(
            task_id='sync_firm_to_vp',
            python_callable=sync_single_xero_firm_to_vp,
            op_args=[config.instance],
        )

        # Error capture — trigger_rule='one_failed' fires on any upstream failure,
        # collects the error dict, and never re-raises (so the dispatcher's
        # gather task receives the error rather than a hard DAG failure).
        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_dag_error,
            op_args=['{{ get_error_message() }}'],
        )

        # --- wiring ---
        # view_dagrun_config is a root diagnostic task; runs in parallel with the main chain.
        check_employee_map >> is_employee_contact

        is_employee_contact >> rail.Label('Is employee') >> skip_employee_contact
        is_employee_contact >> rail.Label('Not employee') >> check_firm_map

        check_firm_map >> needs_sync

        needs_sync >> rail.Label('No change') >> skip_no_change
        (
            needs_sync >> rail.Label('Needs sync') >>
            fetch_xero_contact >> sync_firm_to_vp
        )

        [
            skip_employee_contact,
            skip_no_change,
            sync_firm_to_vp,
        ] >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
