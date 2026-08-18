"""
Map Employee child DAG for VP QBO Mapping Sync.

Direction (per integration_vantagepoint_quickbooks/docs/mapping/PHASE_3_STEP_2):
- Unidirectional: QBO Employee → VP Employee (QBO is master)
- Each QBO Employee also needs a paired QBO Vendor named
  '<DisplayName> (Employee)' so VP can post expense reimbursements through
  AP. The vendor is found-or-created during the per-employee loop.

Writes into the `map_employee` S3 collection. The dispatcher creates the
table up front; this DAG only populates rows. Columns:
    Employee      — VP Employee ID
    QBOID         — QBO Employee Id
    QBOVendorID   — QBO Vendor Id for expense processing
    QBOVendorName — QBO Vendor display name
    Name          — display name

Flow:
    check_map_employee_populated → is_map_employee_populated
       ├─ Yes → skip_populate_map_employee
       └─ No  → fetch_qbo_employees      (QuickBooksEmployeeOperator search)
              → fetch_qbo_vendors        (QuickBooksVendorOperator search,
                                          used as a name-index for the
                                          expense-vendor find-or-create)
              → process_qbo_employees    (PythonOperator: per-record
                                          vendor + VP employee + map row)
       catch_map_employee_dag_error      (one_failed; returns dict)
"""
from vp_quickbooks_integration.common.tables import (
    MAPPING_STEP_EMPLOYEE,
)
from vp_quickbooks_integration.mapping_sync.utils.python_callable_method import (
    is_table_populated,
    capture_dag_error,
    check_step_status,
    mark_step_status,
    sync_qbo_employees_to_vp,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
import logging
from datetime import timedelta
from airflow.models import Variable
import rail

_log = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Per-instance map_employee child DAG."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('map_employee', config.instance),
        description=(
            'Sync QBO employees to VP employees; create a paired QBO vendor '
            'per employee for expense processing. Writes the map_employee '
            'cross-reference table.'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_quickbooks', 'mapping_sync', 'map_employee'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ---- Batch-task gate (perf opt-out) ----
        # See map_firm_dag for the rationale on the shared Variable.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                IntegrationConfig.CAN_RUN_BATCH_VARIABLE_NAME,
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='is_map_employee_populated',
        )

        # ---- Skip gates (layered) ----
        # Primary: mapping_table_state.Status == 'Complete' for this step
        # (set by `apply_premapping_state` or a prior successful run).
        # Secondary: is_table_populated as defensive fallback.
        check_step_complete = rail.PythonOperator(
            task_id='check_map_employee_step_complete',
            python_callable=lambda: check_step_status(MAPPING_STEP_EMPLOYEE),
        )

        check_populated = rail.PythonOperator(
            task_id='check_map_employee_populated',
            python_callable=lambda: is_table_populated('map_employee'),
        )

        is_populated = rail.IfOperator(
            task_id='is_map_employee_populated',
            test=lambda: (
                rail.result('check_map_employee_step_complete') or
                rail.result('check_map_employee_populated')
            ),
            yes_task='skip_populate_map_employee',
            no_task='fetch_qbo_employees',
        )

        skip_populate = rail.PythonOperator(
            task_id='skip_populate_map_employee',
            python_callable=lambda: _log.info(
                'map_employee already populated for this customer — skipping'
            ),
        )

        # ---- Mark step Complete on successful population ----
        mark_step_complete = rail.PythonOperator(
            task_id='mark_map_employee_step_complete',
            python_callable=lambda: mark_step_status(
                MAPPING_STEP_EMPLOYEE, 'Complete'
            ),
        )

        # ---- Forward sync (QBO → VP).
        # Both fetches are batch RAIL operators at DAG-task level; the
        # per-employee VP and QBO calls happen inside process_qbo_employees
        # via the helper, so the whole batch round-trips S3 once.
        fetch_qbo_employees = rail.QuickBooksEmployeeOperator(
            task_id='fetch_qbo_employees',
            intuit_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('intuit', 'quickbooks_default') }}"
            ),
            query='select * from Employee where Active = true',
        )

        fetch_qbo_vendors = rail.QuickBooksVendorOperator(
            task_id='fetch_qbo_vendors',
            intuit_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('intuit', 'quickbooks_default') }}"
            ),
            operation='search',
            query='select * from Vendor where Active = true',
        )

        process_qbo_employees = rail.PythonOperator(
            task_id='process_qbo_employees',
            python_callable=sync_qbo_employees_to_vp,
            op_args=[config.instance],
        )

        catch_map_employee_dag_error = rail.PythonOperator(
            task_id='catch_map_employee_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_dag_error,
            op_args=[
                'map_employee',
                "{{ dag_run.conf.get('customerId') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_map_employee_populated',
            end_task='catch_map_employee_dag_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        [check_step_complete, check_populated] >> can_run_batch_task
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_map_employee_dag_error
        can_run_batch_task >> rail.Label('No') >> is_populated
        is_populated >> rail.Label(
            'Already populated') >> skip_populate >> catch_map_employee_dag_error
        (
            is_populated >> rail.Label('Needs population') >>
            fetch_qbo_employees >> fetch_qbo_vendors >>
            process_qbo_employees >> mark_step_complete >> catch_map_employee_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
