"""
Employee Update DAG for VP QBO Employee Sync.
Updates an existing employee in Vantagepoint from a QBO Employee record.

Mirrors the Workato update branch (`014_503_psa_vantagepoint_upsert_employee`):
  GET  /codeTable/CFGEmployeeTitle             (look up Title code)
  POST /codeTable/CFGEmployeeTitle             (create Title if missing)
  PUT  /employee/{vp_employee_id}              (update employee)

Status / TerminationDate / HireDate are always derived from the QBO payload,
so a single PUT covers update, termination, and rehire transitions:
  - QBO Active=True  -> Status='A', TerminationDate='' (cleared)
  - QBO Active=False -> Status='T', TerminationDate=ReleasedDate
"""
from datetime import timedelta
import rail
from vp_quickbooks_integration.employee_sync.utils.python_callable_method import (
    has_title_input,
    check_job_title_match,
    build_create_job_title_body,
    build_update_employee_body,
    refresh_employee_in_employee_map,
    capture_update_error
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """
    Create DAG for updating existing employees in Vantagepoint.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_employee_sync_update_{config.instance}',
        description='Update employee in Vantagepoint from QuickBooks',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'employee_sync', 'update_employee'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # --- CFGEmployeeTitle pre-flight (Title lookup-and-add) ---

        check_has_title = rail.IfOperator(
            task_id='has_qbo_title',
            test=has_title_input,
            yes_task='get_job_titles_from_vp',
            no_task='update_employee_in_vp'
        )

        get_job_titles_from_vp = rail.VantagepointSettingsListOperator(
            task_id='get_job_titles_from_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            endpoint='/codeTable/CFGEmployeeTitle',
            request_method='GET'
        )

        check_title_in_codetable = rail.IfOperator(
            task_id='is_title_found_in_vp',
            test=check_job_title_match,
            yes_task='update_employee_in_vp',
            no_task='create_job_title_in_vp'
        )

        create_job_title_in_vp = rail.VantagepointSettingsListOperator(
            task_id='create_job_title_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            endpoint='/codeTable/CFGEmployeeTitle',
            request_method='POST',
            request_body=build_create_job_title_body
        )

        # --- VP organizations fetch (Workato parity — first row is Org fallback) ---

        get_vp_organizations = rail.VantagepointSettingsListOperator(
            task_id='get_vp_organizations',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            endpoint='/organization',
            request_method='GET'
        )

        # --- Employee update ---

        update_employee_in_vp = rail.VantagepointEmployeeOperator(
            task_id='update_employee_in_vp',
            vp_conn_id=(
                "{{ dag_run.conf.connections.vantagepoint }}"
            ),
            request_method='PUT',
            employee="{{ dag_run.conf.vp_employee_id }}",
            request_body=lambda: build_update_employee_body(config.instance),
            trigger_rule='none_failed'
        )

        refresh_employee_map_lookup = rail.PythonOperator(
            task_id='refresh_employee_map_lookup',
            python_callable=refresh_employee_in_employee_map
        )

        catch_employee_dag_error = rail.PythonOperator(
            task_id='catch_employee_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_update_error,
            op_args=[
                '{{ dag_run.conf.Id }}',
                "{{ dag_run.conf.get('DisplayName') or '' }}",
                '{{ get_error_message() }}'
            ]
        )

        # --- Wiring ---

        get_vp_organizations >> check_has_title

        (
            check_has_title >>
            rail.Label('Title supplied') >>
            get_job_titles_from_vp >>
            check_title_in_codetable
        )
        (
            check_has_title >>
            rail.Label('No Title') >>
            update_employee_in_vp
        )

        (
            check_title_in_codetable >>
            rail.Label('Title already in CFGEmployeeTitle') >>
            update_employee_in_vp
        )
        (
            check_title_in_codetable >>
            rail.Label('Title missing — add it') >>
            create_job_title_in_vp >>
            update_employee_in_vp
        )

        (
            update_employee_in_vp >>
            refresh_employee_map_lookup >>
            catch_employee_dag_error
        )

        return dag


rail.for_each_instance(create_dag)
