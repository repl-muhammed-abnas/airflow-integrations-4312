"""
Update DAG for UKG Pro → Maconomy Employee Sync.
Handles update, termination, and transfer operations on existing Maconomy employees.

  - termination: 3-step card UPDATE (blocked=True, dateendemployment set)
  - update:      3-step card UPDATE (card fields refreshed)
  - transfer:    3-step terminate old record + entity lookup + 2-step CREATE new record
"""
from datetime import timedelta
import rail
from ukgpro_maconomy_integration.employee_sync.utils.python_callable_method import (
    MN_HEADERS_V6,
    mn_quote,
    build_update_payload,
    build_termination_card_payload,
    build_transfer_termination_payload,
    build_create_payload,
    warn_supervisor_not_found_for_update,
    warn_supervisor_not_found_for_transfer,
    capture_update_error,
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create employee update DAG — handles update, termination, and transfer.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'ukgpro_mn_employee_sync_update_{config.instance}',
        description=(
            'Update, terminate, or transfer employee in Maconomy from UKG Pro'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['ukgpro_maconomy', 'employee_sync', 'update'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        # ── Route by type ──────────────────────────────────────────────────

        check_type_termination = rail.IfOperator(
            task_id='check_type_termination',
            test=lambda: (
                rail.get_current_context()['dag_run'].conf.get('type')
                == 'termination'
            ),
            yes_task='terminate_instance',
            no_task='check_type_transfer'
        )

        # ── TERMINATION: 3-step card update ───────────────────────────────────

        terminate_instance = rail.MaconomyCustomActionOperator(
            task_id='terminate_instance',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employees/instances',
            method='POST',
            headers=MN_HEADERS_V6,
            payload={'panes': {'card': {'fields': [
                'employeenumber', 'blocked', 'dateendemployment',
            ]}}}
        )

        terminate_load = rail.MaconomyCustomActionOperator(
            task_id='terminate_load',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='terminate_instance'
                )['data']['meta']['containerInstanceId']
                + "/data;employeenumber="
                + str(ctx['dag_run'].conf.get('maconomy_employee_number', ''))
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='terminate_instance'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='terminate_instance'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload={}
        )

        terminate_card = rail.MaconomyCustomActionOperator(
            task_id='terminate_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='terminate_instance'
                )['data']['meta']['containerInstanceId']
                + "/data/panes/card/0"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='terminate_load'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='terminate_load'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload=lambda **ctx: {'data': build_termination_card_payload()}
        )

        # ── TRANSFER: terminate old record + create new record ─────────────────

        check_type_transfer = rail.IfOperator(
            task_id='check_type_transfer',
            test=lambda: (
                rail.get_current_context()['dag_run'].conf.get('type')
                == 'transfer'
            ),
            yes_task='transfer_check_has_old_record',
            no_task='is_supervisor_present'
        )

        # Guard: maconomy_employee_number may be None if old record was already terminated.
        transfer_check_has_old_record = rail.IfOperator(
            task_id='transfer_check_has_old_record',
            test=lambda: bool(
                rail.get_current_context()['dag_run'].conf.get('maconomy_employee_number')
            ),
            yes_task='transfer_terminate_instance',
            no_task='transfer_is_entity_code_present'
        )

        # Transfer step 1a: Open container instance for old record termination
        transfer_terminate_instance = rail.MaconomyCustomActionOperator(
            task_id='transfer_terminate_instance',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employees/instances',
            method='POST',
            headers=MN_HEADERS_V6,
            payload={'panes': {'card': {'fields': [
                'employeenumber', 'blocked', 'dateendemployment',
            ]}}}
        )

        # Transfer step 1b: Load old active record
        transfer_terminate_load = rail.MaconomyCustomActionOperator(
            task_id='transfer_terminate_load',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='transfer_terminate_instance'
                )['data']['meta']['containerInstanceId']
                + "/data;employeenumber="
                + str(ctx['dag_run'].conf.get('maconomy_employee_number', ''))
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='transfer_terminate_instance'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='transfer_terminate_instance'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload={}
        )

        # Transfer step 1c: Block old record (dateendemployment = new employee's lastHireDate)
        transfer_terminate_card = rail.MaconomyCustomActionOperator(
            task_id='transfer_terminate_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='transfer_terminate_instance'
                )['data']['meta']['containerInstanceId']
                + "/data/panes/card/0"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='transfer_terminate_load'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='transfer_terminate_load'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload=lambda **ctx: {'data': build_transfer_termination_payload()}
        )

        # Transfer step 2: Entity lookup for the new record
        transfer_is_entity_code_present = rail.IfOperator(
            task_id='transfer_is_entity_code_present',
            test=lambda: bool(
                rail.get_current_context()['dag_run'].conf.get('orgLevel2Code')
            ),
            yes_task='transfer_search_entity_in_maconomy',
            no_task='transfer_is_supervisor_present',
        )

        transfer_search_entity_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='transfer_search_entity_in_maconomy',
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

        transfer_validate_entity_found = rail.IfOperator(
            task_id='transfer_validate_entity_found',
            test=lambda: bool(
                rail.result('transfer_search_entity_in_maconomy')
                .get('data', {})
                .get('panes', {})
                .get('filter', {})
                .get('records', [])
            ),
            yes_task='transfer_is_supervisor_present',
            no_task='transfer_create_entity_instance',
        )

        transfer_create_entity_instance = rail.MaconomyCustomActionOperator(
            task_id='transfer_create_entity_instance',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/entities/instances',
            method='POST',
            headers=MN_HEADERS_V6,
            payload={'panes': {'card': {'fields': ['entityname', 'description', 'blocked']}}}
        )

        transfer_create_entity_card = rail.MaconomyCustomActionOperator(
            task_id='transfer_create_entity_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/entities/instances/"
                + ctx['task_instance'].xcom_pull(task_ids='transfer_create_entity_instance')
                ['data']['meta']['containerInstanceId']
                + "/data/panes/card"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(task_ids='transfer_create_entity_instance')
                    ['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(task_ids='transfer_create_entity_instance')
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

        # Transfer supervisor lookup (mirrors create_dag; prefixed to avoid collision with update path)
        transfer_is_supervisor_present = rail.IfOperator(
            task_id='transfer_is_supervisor_present',
            test=lambda: bool(
                rail.get_current_context()['dag_run'].conf
                .get('supervisorEmployeeNumber')
            ),
            yes_task='transfer_search_supervisor_in_maconomy',
            no_task='transfer_create_instance'
        )

        transfer_search_supervisor_in_maconomy = rail.MaconomyCustomActionOperator(
            task_id='transfer_search_supervisor_in_maconomy',
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

        transfer_check_supervisor_found = rail.IfOperator(
            task_id='transfer_check_supervisor_found',
            test=lambda: bool(
                rail.result('transfer_search_supervisor_in_maconomy')
                .get('data', {})
                .get('panes', {})
                .get('filter', {})
                .get('records', [])
            ),
            yes_task='transfer_create_instance',
            no_task='transfer_warn_supervisor_not_found'
        )

        transfer_warn_supervisor_not_found = rail.PythonOperator(
            task_id='transfer_warn_supervisor_not_found',
            python_callable=warn_supervisor_not_found_for_transfer
        )

        # Transfer step 3a: Open container instance for new employee record
        transfer_create_instance = rail.MaconomyCustomActionOperator(
            task_id='transfer_create_instance',
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
            ]}}}
        )

        # Transfer step 3b: Write new employee card (remark1=employeeID stored for future transfer detection)
        transfer_create_card = rail.MaconomyCustomActionOperator(
            task_id='transfer_create_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='transfer_create_instance'
                )['data']['meta']['containerInstanceId']
                + "/data/panes/card"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='transfer_create_instance'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='transfer_create_instance'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload=lambda **ctx: {
                'data': build_create_payload(
                    config.instance,
                    {
                        'salesemployee': getattr(config, 'employee_salesemployee', True),
                        'accountmanager': getattr(config, 'employee_accountmanager', True),
                        'mustusetimesheets': getattr(config, 'employee_mustusetimesheets', True),
                        'maxworkingtimeperday': getattr(config, 'employee_maxworkingtimeperday', 24),
                        'standardbillingprice': getattr(config, 'employee_standardbillingprice', 0),
                    },
                    supervisor_task_id='transfer_search_supervisor_in_maconomy',
                )
            }
        )

        # ── Supervisor lookup (update path) ───────────────────────────────

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test=lambda: bool(
                rail.get_current_context()['dag_run'].conf
                .get('supervisorEmployeeNumber')
            ),
            yes_task='search_supervisor_in_maconomy',
            no_task='is_entity_code_present'
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
            yes_task='is_entity_code_present',
            no_task='warn_supervisor_not_found'
        )

        warn_supervisor_not_found = rail.PythonOperator(
            task_id='warn_supervisor_not_found',
            python_callable=warn_supervisor_not_found_for_update
        )

        # ── Entity lookup / creation (update path) ────────────────────────

        is_entity_code_present = rail.IfOperator(
            task_id='is_entity_code_present',
            test=lambda: bool(
                rail.get_current_context()['dag_run'].conf.get('orgLevel2Code')
            ),
            yes_task='search_entity_in_maconomy',
            no_task='check_type_update',
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
            yes_task='check_type_update',
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

        check_type_update = rail.IfOperator(
            task_id='check_type_update',
            test=lambda: (
                rail.get_current_context()['dag_run'].conf.get('type')
                == 'update'
            ),
            yes_task='update_instance',
            no_task='fail_unknown_type'
        )

        # ── UPDATE: 3-step card update ─────────────────────────────────────

        update_instance = rail.MaconomyCustomActionOperator(
            task_id='update_instance',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint='containers/$shortname$/employees/instances',
            method='POST',
            headers=MN_HEADERS_V6,
            payload={'panes': {'card': {'fields': [
                'telephone', 'mobilephone', 'companynumber', 'country',
                'name2', 'name3', 'name4', 'name5', 'zipcode', 'postaldistrict',
                'electronicmailaddress', 'dateofbirth', 'middlename',
                'substitute1', 'superioremployee',
                'absenceapprover', 'entityname', 'position',
            ]}}}
        )

        update_load = rail.MaconomyCustomActionOperator(
            task_id='update_load',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='update_instance'
                )['data']['meta']['containerInstanceId']
                + "/data;employeenumber="
                + str(ctx['dag_run'].conf.get('maconomy_employee_number', ''))
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='update_instance'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='update_instance'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload={}
        )

        update_card = rail.MaconomyCustomActionOperator(
            task_id='update_card',
            mn_conn_id="{{ dag_run.conf.connections.maconomy }}",
            endpoint=lambda **ctx: (
                "containers/$shortname$/employees/instances/"
                + ctx['task_instance'].xcom_pull(
                    task_ids='update_instance'
                )['data']['meta']['containerInstanceId']
                + "/data/panes/card/0"
            ),
            method='POST',
            headers=lambda **ctx: {
                **MN_HEADERS_V6,
                'Maconomy-Concurrency-Control': (
                    ctx['task_instance'].xcom_pull(
                        task_ids='update_load'
                    )['headers']['Maconomy-Concurrency-Control']
                ),
                'Authorization': (
                    'X-Reconnect '
                    + ctx['task_instance'].xcom_pull(
                        task_ids='update_load'
                    )['headers']['Maconomy-Reconnect']
                ),
            },
            payload=lambda **ctx: {'data': build_update_payload()}
        )

        def raise_unknown_type():
            conf = rail.get_current_context()['dag_run'].conf
            raise RuntimeError(
                f"Employee {conf.get('employeeNumber')}: "
                f"unsupported sync type '{conf.get('type')}'"
            )

        fail_unknown_type = rail.PythonOperator(
            task_id='fail_unknown_type',
            python_callable=raise_unknown_type
        )

        # ── Error capture ──────────────────────────────────────────────────

        catch_employee_dag_error = rail.PythonOperator(
            task_id='catch_employee_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_update_error,
            op_args=[
                '{{ dag_run.conf.employeeNumber }}',
                '{{ dag_run.conf.type }}',
                '{{ get_error_message() }}'
            ]
        )

        # ── Task chains ────────────────────────────────────────────────────

        # Termination path
        (
            check_type_termination >> rail.Label('Termination') >>
            terminate_instance >> terminate_load >> terminate_card >>
            catch_employee_dag_error
        )

        # Transfer path
        check_type_termination >> rail.Label('Not termination') >> check_type_transfer
        check_type_transfer >> rail.Label('Transfer') >> transfer_check_has_old_record
        (
            transfer_check_has_old_record >>
            rail.Label('Has active old record') >>
            transfer_terminate_instance >> transfer_terminate_load >>
            transfer_terminate_card >> transfer_is_entity_code_present
        )
        (
            transfer_check_has_old_record >>
            rail.Label('Old already terminated') >>
            transfer_is_entity_code_present
        )

        transfer_is_entity_code_present >> rail.Label('Entity code present') >> transfer_search_entity_in_maconomy
        transfer_is_entity_code_present >> rail.Label('No entity code') >> transfer_is_supervisor_present
        transfer_search_entity_in_maconomy >> transfer_validate_entity_found
        transfer_validate_entity_found >> rail.Label('Entity found') >> transfer_is_supervisor_present
        transfer_validate_entity_found >> rail.Label('Entity not found') >> transfer_create_entity_instance
        transfer_create_entity_instance >> transfer_create_entity_card >> transfer_is_supervisor_present
        transfer_create_entity_instance >> catch_employee_dag_error
        transfer_create_entity_card >> catch_employee_dag_error

        transfer_is_supervisor_present >> rail.Label('Supervisor set') >> transfer_search_supervisor_in_maconomy
        transfer_is_supervisor_present >> rail.Label('No supervisor') >> transfer_create_instance
        transfer_search_supervisor_in_maconomy >> transfer_check_supervisor_found
        transfer_check_supervisor_found >> rail.Label('Found') >> transfer_create_instance
        (
            transfer_check_supervisor_found >> rail.Label('Not found') >>
            transfer_warn_supervisor_not_found >> transfer_create_instance
        )
        transfer_create_instance >> transfer_create_card >> catch_employee_dag_error
        transfer_create_instance >> catch_employee_dag_error

        # Update path
        check_type_transfer >> rail.Label('Not transfer') >> is_supervisor_present
        is_supervisor_present >> rail.Label('Supervisor set') >> search_supervisor_in_maconomy
        is_supervisor_present >> rail.Label('No supervisor') >> is_entity_code_present

        search_supervisor_in_maconomy >> check_supervisor_found
        check_supervisor_found >> rail.Label('Found') >> is_entity_code_present
        (
            check_supervisor_found >> rail.Label('Not found') >>
            warn_supervisor_not_found >> is_entity_code_present
        )

        is_entity_code_present >> rail.Label('Entity code present') >> search_entity_in_maconomy
        is_entity_code_present >> rail.Label('No entity code') >> check_type_update
        search_entity_in_maconomy >> validate_entity_found
        validate_entity_found >> rail.Label('Entity found') >> check_type_update
        validate_entity_found >> rail.Label('Entity not found') >> create_entity_instance
        create_entity_instance >> create_entity_card >> check_type_update
        create_entity_instance >> catch_employee_dag_error
        create_entity_card >> catch_employee_dag_error

        check_type_update >> rail.Label('Update') >> update_instance >> update_load >> update_card >> catch_employee_dag_error
        check_type_update >> rail.Label('Not update') >> fail_unknown_type >> catch_employee_dag_error

        return dag


rail.for_each_instance(create_dag)
