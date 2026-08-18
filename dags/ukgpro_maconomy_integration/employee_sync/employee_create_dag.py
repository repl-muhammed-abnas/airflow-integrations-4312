"""
Create DAG for UKG Pro → Maconomy Employee Sync.
Creates a new employee record in Maconomy via 2-step POST protocol.
"""
from datetime import timedelta
import rail
from ukgpro_maconomy_integration.employee_sync.utils.python_callable_method import (
    MN_HEADERS_V6,
    mn_quote,
    build_create_payload,
    warn_supervisor_not_found_for_create,
    capture_create_error,
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create employee create DAG — resolves supervisor/jobpricegroup/category
    lookups then opens a 2-step Maconomy card entry.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'ukgpro_mn_employee_sync_create_{config.instance}',
        description='Create new employee in Maconomy from UKG Pro',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['ukgpro_maconomy', 'employee_sync', 'create'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # ── Supervisor lookup (optional) ───────────────────────────────────

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test=lambda: bool(
                rail.get_current_context()['dag_run'].conf
                .get('supervisorEmployeeNumber')
            ),
            yes_task='search_supervisor_in_maconomy',
            no_task='search_userrole_in_maconomy'
        )

        search_supervisor_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='search_supervisor_in_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employees/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: {
                'fields': ['employeenumber'],
                'restriction': (
                    'employeenumber='
                    + mn_quote(ctx['dag_run'].conf.get('supervisorEmployeeNumber'))
                ),
                'limit': 0,
            }
        )

        check_supervisor_found = rail.IfOperator(
            task_id='check_supervisor_found',
            test=lambda: bool(
                rail.result('search_supervisor_in_maconomy')
                .get('data', {})
                .get('panes', {})
                .get('filter', {})
                .get('records', [])
            ),
            yes_task='search_userrole_in_maconomy',
            no_task='warn_supervisor_not_found'
        )

        warn_supervisor_not_found = rail.PythonOperator(
            task_id='warn_supervisor_not_found',
            python_callable=warn_supervisor_not_found_for_create
        )

        # ── Userrole / jobpricegroup lookups ───────────────────────────────

        def _build_userrole_payload():
            from airflow.models import Variable  # pylint: disable=import-outside-toplevel
            username = Variable.get(
                f'ukgpro_mn_employee_sync_maconomy_username_{config.instance}',
                default_var=''
            )
            return {
                'fields': ['nameofuser', 'instancekey'],
                'restriction': 'nameofuser=' + mn_quote(username),
                'limit': 0,
            }

        search_userrole_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='search_userrole_in_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/userroles/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: _build_userrole_payload()
        )

        check_userrole_found = rail.IfOperator(
            task_id='check_userrole_found',
            test=lambda: bool(
                rail.result('search_userrole_in_maconomy')
                .get('data', {})
                .get('panes', {})
                .get('filter', {})
                .get('records', [])
            ),
            yes_task='search_jobpricegroups_in_maconomy',
            no_task='search_employeecategory_in_maconomy'
        )

        search_jobpricegroups_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='search_jobpricegroups_in_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/jobpricegroups/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: {
                'fields': ['description', 'jobpricegroupnumber'],
                'restriction': (
                    'userroleinformationinstancekey='
                    + mn_quote(
                        rail.result('search_userrole_in_maconomy')
                        .get('data', {})
                        .get('panes', {})
                        .get('filter', {})
                        .get('records', [{}])[0]
                        .get('data', {})
                        .get('instancekey', '')
                    )
                ),
                'limit': 0,
            }
        )

        # ── Employee category lookup ───────────────────────────────────────

        search_employeecategory_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='search_employeecategory_in_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employeecategories/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: {
                'fields': ['name', 'employeecategorynumber'],
                'restriction': (
                    'name='
                    + mn_quote(ctx['dag_run'].conf.get('jobDescription'))
                ),
                'limit': 0,
            }
        )

        # ── Entity lookup / creation ───────────────────────────────────────

        is_entity_code_present = rail.IfOperator(
            task_id='is_entity_code_present',
            test=lambda: bool(
                rail.get_current_context()['dag_run'].conf.get('orgLevel2Code')
            ),
            yes_task='search_entity_in_maconomy',
            no_task='build_create_payload',
        )

        search_entity_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='search_entity_in_maconomy',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/entities/filter',
            method='POST',
            headers=MN_HEADERS_V6,
            payload=lambda **ctx: {
                'fields': ['entityname', 'description', 'blocked'],
                'restriction': (
                    'entityname='
                    + mn_quote(ctx['dag_run'].conf.get('orgLevel2Code'))
                ),
                'limit': 0,
            }
        )

        validate_entity_found = rail.IfOperator(
            task_id='validate_entity_found',
            test=lambda: bool(
                rail.result('search_entity_in_maconomy')
                .get('data', {})
                .get('panes', {})
                .get('filter', {})
                .get('records', [])
            ),
            yes_task='build_create_payload',
            no_task='create_entity_instance',
        )

        create_entity_instance = rail.MaconomyCustomActionOperator(
            task_id='create_entity_instance',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/entities/instances',
            method='POST',
            headers=MN_HEADERS_V6,
            payload={'panes': {'card': {'fields': ['entityname', 'description', 'blocked']}}}
        )

        create_entity_card = rail.MaconomyCustomActionOperator(
            task_id='create_entity_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/entities/instances/"
                + ctx['task_instance'].xcom_pull(task_ids='create_entity_instance')
                ['data']['meta']['containerInstanceId']
                + "/data/panes/card"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(task_ids='create_entity_instance')
                    ['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(task_ids='create_entity_instance')
                    ['headers']['Maconomy-Reconnect']
                ),
            },
            payload=lambda **ctx: {
                'data': {
                    'entityname': ctx['dag_run'].conf.get('orgLevel2Code'),
                    'description': (
                        ctx['dag_run'].conf.get('orgLevel2Description')
                        or ctx['dag_run'].conf.get('orgLevel2Code')
                    ),
                    'blocked': False,
                }
            }
        )

        # ── Build payload ──────────────────────────────────────────────────

        build_payload = rail.PythonOperator(
            task_id='build_create_payload',
            python_callable=build_create_payload,
            op_args=[config.instance, {
                'salesemployee': getattr(config, 'employee_salesemployee', True),
                'accountmanager': getattr(config, 'employee_accountmanager', True),
                'mustusetimesheets': getattr(config, 'employee_mustusetimesheets', True),
                'maxworkingtimeperday': getattr(config, 'employee_maxworkingtimeperday', 24),
                'standardbillingprice': getattr(config, 'employee_standardbillingprice', 0),
            }]
        )

        # ── 2-step Maconomy CREATE ─────────────────────────────────────────
        # Step 1: Open container instance with field list
        create_employee_instance = rail.MaconomyCustomActionOperator(
            task_id='create_employee_instance',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employees/instances',
            method='POST',
            headers=MN_HEADERS_V6,
            payload={'panes': {'card': {'fields': [
                'employeenumber', 'templateemployeenumber', 'firstname', 'middlename',
                'lastname', 'formalfirstname', 'formalmiddlename', 'formallastname', 'telephone',
                'country', 'name2', 'name3', 'name4', 'name5', 'zipcode', 'postaldistrict',
                'blocked', 'salesemployee', 'dateofbirth', 'dateemployed',
                'superioremployee', 'maxworkingtimeperday',
                'companynumber', 'jobpricegroupnumber',
                'primaryemployeecategorynumber', 'electronicmailaddress',
                'standardbillingprice', 'mobilephone', 'mustusetimesheets',
                'absenceapprover', 'substitute1', 'accountmanager',
                'timesheetstartdate', 'entityname', 'position', 'remark1',
            ]}}},
        )

        # Step 2: Write field values using {'data': {...}} wrapper and token from step 1.
        create_employee_card = rail.MaconomyCustomActionOperator(
            task_id='create_employee_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='create_employee_instance'
                )['data']['meta']['containerInstanceId']
                + "/data/panes/card"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='create_employee_instance'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='create_employee_instance'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload=lambda **ctx: {
                'data': ctx['task_instance'].xcom_pull(
                    task_ids='build_create_payload'
                )
            },
        )

        # ── Error capture ──────────────────────────────────────────────────

        catch_employee_dag_error = rail.PythonOperator(
            task_id='catch_employee_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_create_error,
            op_args=[
                '{{ dag_run.conf.employeeNumber }}',
                '{{ get_error_message() }}'
            ]
        )

        # ── Task chain ─────────────────────────────────────────────────────

        is_supervisor_present >> rail.Label('Supervisor set') >> search_supervisor_in_maconomy
        is_supervisor_present >> rail.Label('No supervisor') >> search_userrole_in_maconomy

        search_supervisor_in_maconomy >> check_supervisor_found
        check_supervisor_found >> rail.Label('Found') >> search_userrole_in_maconomy
        check_supervisor_found >> rail.Label('Not found') >> warn_supervisor_not_found >> search_userrole_in_maconomy

        search_userrole_in_maconomy >> check_userrole_found
        check_userrole_found >> rail.Label('Found') >> search_jobpricegroups_in_maconomy >> search_employeecategory_in_maconomy
        check_userrole_found >> rail.Label('Not found') >> search_employeecategory_in_maconomy

        search_employeecategory_in_maconomy >> is_entity_code_present
        is_entity_code_present >> rail.Label('Entity code present') >> search_entity_in_maconomy
        is_entity_code_present >> rail.Label('No entity code') >> build_payload
        search_entity_in_maconomy >> validate_entity_found
        validate_entity_found >> rail.Label('Entity found') >> build_payload
        validate_entity_found >> rail.Label('Entity not found') >> create_entity_instance
        create_entity_instance >> create_entity_card >> build_payload
        create_entity_instance >> catch_employee_dag_error
        create_entity_card >> catch_employee_dag_error

        build_payload >> create_employee_instance >> create_employee_card >> catch_employee_dag_error

        return dag


rail.for_each_instance(create_dag)
