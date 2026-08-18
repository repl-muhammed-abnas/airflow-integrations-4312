"""
Processor DAG for VP -> QBO Employee Sync.

Runs once per VP employee that changed in the dispatcher's polling
window. Ports the Workato sub-recipe
`014_503_psa_vantagepoint_employee_to_quickbooks`:

  GET /employee/{Employee}                    (Get Single Employees)
  IF the employee_map already has this VP code:
      refresh QBOID + Name on the existing row
  ELSE:
      add a new row keyed by VP Employee code (Employee + Name only)

No QuickBooks API call is made — this integration is map-maintenance
only. The map lives in the shared mapping_sync `map_employee` S3 collection
(keyed here by VP Employee code); downstream integrations consume it to
resolve VP Employee -> QBO Employee Id.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.employee_sync_upsert.utils.python_callable_method import (  # noqa: E501
    get_employee_from_map_method,
    check_employee_exists_in_map_method,
    update_employee_in_map_method,
    add_employee_to_map_method,
    capture_processor_error,
)


def create_dag(config):
    """Per-employee processor DAG."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_employee_sync_upsert_processor_{config.instance}',
        description=(
            'Maintain VP <-> QBO employee map row for one changed employee'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_quickbooks',
            'employee_sync_upsert',
            'processor',
        ],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        get_single_employee_from_vp = rail.VantagepointEmployeeOperator(
            task_id='get_single_employee_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            employee="{{ dag_run.conf.Employee }}",
        )

        get_employee_from_map = rail.PythonOperator(
            task_id='get_employee_from_map',
            python_callable=get_employee_from_map_method,
        )

        employee_exists_in_map = rail.IfOperator(
            task_id='employee_exists_in_map',
            test=check_employee_exists_in_map_method,
            yes_task='update_employee_in_map',
            no_task='add_employee_to_map',
        )

        update_employee_in_map = rail.PythonOperator(
            task_id='update_employee_in_map',
            python_callable=update_employee_in_map_method,
        )

        add_employee_to_map = rail.PythonOperator(
            task_id='add_employee_to_map',
            python_callable=add_employee_to_map_method,
        )

        catch_processor_dag_error = rail.PythonOperator(
            task_id='catch_processor_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_processor_error,
            op_args=[
                '{{ dag_run.conf.Employee }}',
                "{{ dag_run.conf.get('Name') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        (
            get_single_employee_from_vp >>
            get_employee_from_map >>
            employee_exists_in_map
        )

        (
            employee_exists_in_map >>
            rail.Label('Employee found in map') >>
            update_employee_in_map >>
            catch_processor_dag_error
        )
        (
            employee_exists_in_map >>
            rail.Label('Employee not in map') >>
            add_employee_to_map >>
            catch_processor_dag_error
        )

        # one_failed fires on `failed` but NOT on `upstream_failed`. Without
        # these direct edges, a failure in get_single_employee_from_vp,
        # get_employee_from_map, or employee_exists_in_map leaves the
        # map-write tasks in upstream_failed — catch never runs, the
        # dispatcher gathers no error dict, and the watermark advances
        # past employees that were never synced.
        get_single_employee_from_vp >> catch_processor_dag_error
        get_employee_from_map >> catch_processor_dag_error
        employee_exists_in_map >> catch_processor_dag_error

        return dag


rail.for_each_instance(create_dag)
