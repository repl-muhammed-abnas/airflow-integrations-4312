"""
Router DAG for UKG Pro → Maconomy Employee Sync.
Detects the required action (create / update / terminate / rehire / transfer)
and triggers the appropriate child DAG. All Maconomy writes happen in child DAGs.
"""
import logging
from datetime import timedelta
import rail
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)
from ukgpro_maconomy_integration.employee_sync.utils.python_callable_method import (
    MN_HEADERS_V6,
    mn_quote,
    build_company_restriction,
    build_employee_loop_restriction,
    resolve_maconomy_company_number,
    check_status_for_update,
    check_status_for_rehire,
    check_status_for_termination,
    check_employee_active_in_ukgpro,
    check_transfer_detected,
    build_router_conf,
    build_create_conf,
    build_transfer_conf,
    collect_triggered_dagrun_ids,
    capture_router_dag_error,
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create router DAG for routing each employee to the correct child DAG.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'ukgpro_mn_employee_sync_router_{config.instance}',
        description='Route each employee to create, update, rehire, terminate, or transfer DAG',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['ukgpro_maconomy', 'employee_sync', 'router'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        def get_company_id_from_conf(**context):
            return context['dag_run'].conf.get('companyID')

        def get_employee_id_from_conf(**context):
            return context['dag_run'].conf.get('employeeID')

        get_person_details_from_ukgpro = rail.UKGProDemographicOperator(
            task_id='get_person_details_from_ukgpro',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            company_id=get_company_id_from_conf,
            employee_id=get_employee_id_from_conf,
            additional_fields=[
                'homePhone', 'homePhoneCountry', 'emailAddress',
                'employeeId', 'addressLine1', 'addressLine2',
                'addressLine3', 'addressLine4', 'addressCity',
                'addressState', 'addressZipCode', 'addressCountry',
                'addressCounty', 'dateOfBirth', 'middleName'
            ]
        )

        def get_employment_details_endpoint(**context):
            company_id = context['dag_run'].conf.get('companyID')
            employee_id = context['dag_run'].conf.get('employeeID')
            return (
                f'/personnel/v1/companies/{company_id}'
                f'/employees/{employee_id}/employment-details'
            )

        get_employment_details_from_ukgpro = rail.UKGProGenericOperator(
            task_id='get_employment_details_from_ukgpro',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            endpoint=get_employment_details_endpoint,
            method='GET',
            required_fields=[
                'lastHireDate', 'dateOfTermination', 'workPhoneNumber',
                'supervisorEmployeeNumber', 'jobDescription', 'orgLevel2Code',
                'companyCode', 'orgLevel3Code', 'jobTitle', 'supervisorEmployeeNumber',
                'originalHireDate', 'lastHireDate', 'dateOfTermination',
                'employeeStatusCode', 'jobChangeReasonCode',
            ],
            extract_from_array=True,
            dag=dag
        )

        def _org_level2_endpoint(**ctx):
            code = (
                (ctx['task_instance'].xcom_pull(
                    task_ids='get_employment_details_from_ukgpro'
                ) or {}).get('orgLevel2Code', '') or ''
            ).strip()
            if not code:
                raise RuntimeError(
                    f"Employee {ctx['dag_run'].conf.get('employeeNumber')}: "
                    "orgLevel2Code absent from employment details — "
                    "cannot resolve org level description"
                )
            return f'/configuration/v1/org-levels/2/{code}'

        get_org_level2_from_ukgpro = rail.UKGProGenericOperator(
            task_id='get_org_level2_from_ukgpro',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            endpoint=_org_level2_endpoint,
            method='GET',
            required_fields=['code', 'description'],
        )

        get_company_info_from_maconomy = rail.MaconomyCustomActionOperator(
            task_id='get_company_info_from_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/companyinfo/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: {
                'fields': ['companynumber', 'name1'],
                'restriction': build_company_restriction(
                    (ctx['task_instance'].xcom_pull(
                        task_ids='get_employment_details_from_ukgpro'
                    ) or {}).get('companyCode', ''),
                    ctx['dag_run'].conf.get('companyName', ''),
                ),
                'limit': 0,
            }
        )

        validate_company_found = rail.IfOperator(
            task_id='validate_company_found',
            test=lambda: bool(
                rail.result('get_company_info_from_maconomy')
                .get('data', {})
                .get('panes', {})
                .get('filter', {})
                .get('records', [])
            ),
            yes_task='search_employee_in_maconomy',
            no_task='create_company_instance'
        )

        # ── CREATE COMPANY: 2-step (company not found in Maconomy) ────────────

        create_company_instance = rail.MaconomyCustomActionOperator(
            task_id='create_company_instance',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/companyinfo/instances',
            method='POST',
            headers=MN_HEADERS_V6,
            payload={'panes': {'card': {'fields': ['name1', 'companynumber']}}}
        )

        create_company_card = rail.MaconomyCustomActionOperator(
            task_id='create_company_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/companyinfo/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='create_company_instance'
                )['data']['meta']['containerInstanceId']
                + "/data/panes/card"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='create_company_instance'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='create_company_instance'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload=lambda **ctx: {
                'data': {
                    'companynumber': (ctx['task_instance'].xcom_pull(
                        task_ids='get_employment_details_from_ukgpro'
                    ) or {}).get('companyCode', ''),
                    'name1': ctx['dag_run'].conf.get('companyName', ''),
                }
            }
        )

        # ── EMPLOYEE LOOP SEARCH: base + _r1.._r9 in one query ────────────────

        search_employee_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='search_employee_in_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employees/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: {
                'fields': ['employeenumber', 'blocked', 'dateendemployment', 'companynumber'],
                'restriction': build_employee_loop_restriction(
                    ctx['dag_run'].conf.get('employeeNumber'),
                    resolve_maconomy_company_number(),
                ),
                'limit': 0,
            }
        )

        check_employee_exists_in_maconomy = rail.IfOperator(
            task_id='check_employee_exists_in_maconomy',
            test=lambda: bool(
                rail.result('search_employee_in_maconomy')
                .get('data', {})
                .get('panes', {})
                .get('filter', {})
                .get('records', [])
            ),
            yes_task='check_status_for_update',
            no_task='search_by_employeeid_in_maconomy'
        )

        # ── STATUS ROUTING (employee found via number loop) ────────────────────

        check_status_for_update_task = rail.IfOperator(
            task_id='check_status_for_update',
            test=check_status_for_update,
            yes_task='trigger_employee_update',
            no_task='check_status_for_rehire'
        )

        check_status_for_rehire_task = rail.IfOperator(
            task_id='check_status_for_rehire',
            test=check_status_for_rehire,
            yes_task='trigger_employee_create',
            no_task='check_status_for_termination'
        )

        check_status_for_termination_task = rail.IfOperator(
            task_id='check_status_for_termination',
            test=check_status_for_termination,
            yes_task='trigger_employee_update',
            no_task='log_status_not_matched'
        )

        def log_skip_status_not_matched():
            conf = rail.get_current_context()['dag_run'].conf
            log.warning(
                "Employee %s skipped: status combination not valid for any "
                "sync scenario (UKG=%s)",
                conf.get('employeeNumber'),
                conf.get('employeeStatusCode', 'Unknown'),
            )

        log_status_not_matched = rail.PythonOperator(
            task_id='log_status_not_matched',
            python_callable=log_skip_status_not_matched
        )

        # ── TRANSFER DETECTION (employee not found via number loop) ────────────
        # Search by remark1=employeeID to find any MN record for this person
        # regardless of company or old employee number.

        search_by_employeeid_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='search_by_employeeid_in_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employees/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: {
                'fields': ['employeenumber', 'blocked', 'dateendemployment', 'companynumber', 'remark1'],
                'restriction': (
                    'remark1=' + mn_quote(ctx['dag_run'].conf.get('employeeID'))
                    if ctx['dag_run'].conf.get('employeeID')
                    else "remark1='__no_employee_id__'"
                ),
                'limit': 0,
            }
        )

        # Transfer: found a record with a different base employee number → route to update DAG (type=transfer).
        # No transfer: new employee or inactive → route to create / skip.
        check_transfer_detected_task = rail.IfOperator(
            task_id='check_transfer_detected',
            test=check_transfer_detected,
            yes_task='trigger_employee_update',
            no_task='check_employee_status_in_ukgpro'
        )

        # ── NEW EMPLOYEE (no MN record, no transfer) ───────────────────────────

        check_employee_status_in_ukgpro = rail.IfOperator(
            task_id='check_employee_status_in_ukgpro',
            test=check_employee_active_in_ukgpro,
            yes_task='trigger_employee_create',
            no_task='log_inactive_employee_not_in_maconomy'
        )

        def log_skip_inactive_not_in_maconomy():
            conf = rail.get_current_context()['dag_run'].conf
            log.warning(
                "Employee %s skipped: inactive in UKG Pro (status=%s) "
                "and not found in Maconomy",
                conf.get('employeeNumber'),
                conf.get('employeeStatusCode', 'Unknown'),
            )

        log_inactive_employee_not_in_maconomy = rail.PythonOperator(
            task_id='log_inactive_employee_not_in_maconomy',
            python_callable=log_skip_inactive_not_in_maconomy
        )

        # ── CHILD DAG TRIGGERS ─────────────────────────────────────────────────

        def determine_update_type():
            """Select update / termination / transfer conf at runtime based on XCom state.

            Raises AirflowSkipException (not RuntimeError) when no update scenario matches.
            This handles the case where none_failed_min_one_success trigger_rule causes
            trigger_employee_update to be scheduled even when a branch operator routed away
            from it (e.g. for rehire → create, or new employee → create paths).
            AirflowSkipException propagates through TriggerDagRunOperator.execute before the
            try/except block, so Airflow marks the task SKIPPED rather than FAILED.
            """
            if check_status_for_update():
                return build_router_conf('update')
            elif check_status_for_termination():
                return build_router_conf('termination')
            elif check_transfer_detected():
                return build_transfer_conf()
            else:
                conf = rail.get_current_context()['dag_run'].conf
                message = (
                    f"Employee {conf.get('employeeNumber')}: no update scenario matched "
                    f"(status={conf.get('employeeStatusCode')}) — skipping trigger_employee_update"
                )
                log.warning("%s (expected on create/rehire paths)", message)
                raise AirflowSkipException(message)

        # Triggered for: update, termination, and transfer.
        trigger_employee_update = rail.TriggerDagRunOperator(
            task_id='trigger_employee_update',
            retries=0,
            trigger_dag_id=(
                f'ukgpro_mn_employee_sync_update_{config.instance}'
            ),
            conf=determine_update_type,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Triggered for: new employee and rehire (suffixed number).
        trigger_employee_create = rail.TriggerDagRunOperator(
            task_id='trigger_employee_create',
            retries=0,
            trigger_dag_id=(
                f'ukgpro_mn_employee_sync_create_{config.instance}'
            ),
            conf=build_create_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        collect_triggered_dagrun_id = rail.PythonOperator(
            task_id='collect_triggered_dagrun_id',
            trigger_rule='all_done',
            python_callable=collect_triggered_dagrun_ids
        )

        gather_employee_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_employee_dag_errors',
            dag_runs="{{ result('collect_triggered_dagrun_id') }}",
            dagrun_task_id='catch_employee_dag_error',
            flatten=True
        )

        catch_router_dag_error = rail.PythonOperator(
            task_id='catch_router_dag_error',
            trigger_rule='all_done',
            python_callable=capture_router_dag_error,
            op_args=[
                '{{ dag_run.conf.employeeNumber }}',
                '{{ get_error_message() }}'
            ]
        )

        batch_router = rail.BatchTaskRunOperator(
            task_id='batch_router',
            start_task='get_person_details_from_ukgpro',
            end_task='get_company_info_from_maconomy',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # ── Task chains ────────────────────────────────────────────────────────

        batch_router >> catch_router_dag_error
        batch_router >> get_company_info_from_maconomy
        batch_router >> get_person_details_from_ukgpro

        # Main UKG fetch chain
        (
            get_person_details_from_ukgpro >>
            get_employment_details_from_ukgpro >>
            get_org_level2_from_ukgpro >>
            get_company_info_from_maconomy >>
            validate_company_found
        )

        # Company branches
        (
            validate_company_found >> rail.Label('Company found') >>
            search_employee_in_maconomy >>
            check_employee_exists_in_maconomy
        )
        (
            validate_company_found >> rail.Label('Company not found') >>
            create_company_instance >> create_company_card >>
            search_employee_in_maconomy
        )
        # Note: no direct create_company_card >> catch_router_dag_error edge needed;
        # catch_router_dag_error uses trigger_rule='all_done' so it fires regardless.

        # Employee found via loop → status routing
        (
            check_employee_exists_in_maconomy >>
            rail.Label('Employee exists') >>
            check_status_for_update_task
        )
        (
            check_status_for_update_task >>
            rail.Label('Active-Active (update)') >>
            trigger_employee_update
        )
        (
            check_status_for_update_task >>
            rail.Label('Not Active-Active') >>
            check_status_for_rehire_task
        )
        (
            check_status_for_rehire_task >>
            rail.Label('Rehire (all blocked)') >>
            trigger_employee_create
        )
        (
            check_status_for_rehire_task >>
            rail.Label('Not rehire') >>
            check_status_for_termination_task
        )
        (
            check_status_for_termination_task >>
            rail.Label('Termination') >>
            trigger_employee_update
        )
        (
            check_status_for_termination_task >>
            rail.Label('No match') >>
            log_status_not_matched
        )

        # Employee not found via loop → transfer detection
        (
            check_employee_exists_in_maconomy >>
            rail.Label('Not found — check transfer') >>
            search_by_employeeid_in_maconomy >>
            check_transfer_detected_task
        )
        (
            check_transfer_detected_task >>
            rail.Label('Transfer') >>
            trigger_employee_update
        )
        (
            check_transfer_detected_task >>
            rail.Label('No transfer — new employee') >>
            check_employee_status_in_ukgpro
        )
        (
            check_employee_status_in_ukgpro >>
            rail.Label('Active (A)') >>
            trigger_employee_create
        )
        (
            check_employee_status_in_ukgpro >>
            rail.Label('Inactive') >>
            log_inactive_employee_not_in_maconomy
        )

        # Child DAG error propagation
        trigger_employee_create >> collect_triggered_dagrun_id
        trigger_employee_update >> collect_triggered_dagrun_id
        (
            collect_triggered_dagrun_id >>
            gather_employee_dag_errors >>
            catch_router_dag_error
        )

        # Terminal tasks that never reach collect_triggered_dagrun_id
        log_status_not_matched >> catch_router_dag_error
        log_inactive_employee_not_in_maconomy >> catch_router_dag_error

        return dag


rail.for_each_instance(create_dag)
