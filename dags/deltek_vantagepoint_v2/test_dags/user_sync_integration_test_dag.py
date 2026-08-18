import uuid
from datetime import timedelta

import rail

# pylint:disable = too-many-statements, line-too-long

# Scenarios — maps each test scenario to its assert task and trigger task.
# Used by collect_test_results to check pass/fail and gather coverage dag_run_ids.
SCENARIOS = [
    {
        'name': 'Scenario 1',
        'description': 'Initial Sync - new user created in Replicon',
        'assert_task_id': 'assert_user_created',
        'trigger_task_id': 'trigger_initial_sync',
    },
    {
        'name': 'Scenario 2a',
        'description': 'Webhook Create - user synced to Replicon',
        'assert_task_id': 'assert_webhook_create',
        'trigger_task_id': 'trigger_webhook_create',
    },
    {
        'name': 'Scenario 2b',
        'description': 'Webhook Create - supervisor permission assigned',
        'assert_task_id': 'assert_supervisor_permission_assigned',
        'trigger_task_id': 'trigger_webhook_create',
    },
    {
        'name': 'Scenario 3',
        'description': 'Webhook Update - field changes propagated to Replicon',
        'assert_task_id': 'assert_webhook_update',
        'trigger_task_id': 'trigger_webhook_update',
    },
    {
        'name': 'Scenario 4',
        'description': 'Webhook Delete - user disabled in Replicon',
        'assert_task_id': 'assert_user_disabled',
        'trigger_task_id': 'trigger_webhook_delete_exists',
    },
    {
        'name': 'Scenario 5',
        'description': 'Webhook Reactivate - user re-enabled in Replicon',
        'assert_task_id': 'assert_user_reactivated',
        'trigger_task_id': 'trigger_webhook_reactivate',
    },
]


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vp_replicon_user_sync_integration_test_{config.instance}',
        description='Integration test: VP -> Replicon user sync end-to-end validation',
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        company_key=config.company_key,
        multi_tenant=True,
    ) as dag:

        # ── SETUP: Generate unique test employee ID and create VP employee ─────────
        # A UUID-based employee number is generated once per run, used to:
        #   1. Create a fresh VP employee (POST /employee) scoped to this run.
        #   2. Filter get_all_users_from_vp in the main DAG via customSettings.userSyncFilters
        #      so the initial sync only picks up this one test employee.
        #   3. Assert the employee was created in Replicon after the initial sync.
        # VP Employee API: https://vantagepointapi.deltek.com/#f1614afc-ccb2-49e2-ba4a-d9fd11f712b2

        generate_test_id = rail.PythonOperator(
            task_id='generate_test_id',
            python_callable=lambda: f'{uuid.uuid4().hex[:3].upper()}',
        )

        # POST /employee — creates a new employee in Vantagepoint.
        # EmployeeCompany, HomeCompany, Org come from instance config (qa_us_east_1.py),
        # values taken from the GET /employee response of an existing QA tenant employee.
        # VP Employee API: https://vantagepointapi.deltek.com/#f1614afc-ccb2-49e2-ba4a-d9fd11f712b2
        create_vp_employee = rail.VantagepointAPIOperator(
            task_id='create_vp_employee',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/employee',
            request_method='POST',
            request_body=lambda: {
                'Employee': f"IT{rail.result('generate_test_id')}",
                'EmployeeCompany': config.vp_employee_company,
                'HomeCompany': config.vp_employee_company,
                'Org': config.vp_employee_org,
                'Status': config.employee_status,
                'FirstName': config.initial_sync_name_prefix,
                'LastName': f"IT{rail.result('generate_test_id')}",
                'PreferredName': f"{config.initial_sync_name_prefix} User",
                'HireDate': config.employee_hire_date,
                'TerminationDate': '',
                'ReadyForProcessing': config.employee_ready_for_processing,
                'Type': config.employee_type,
                'PayType': config.employee_pay_type,
                'EMail': f"it{rail.result('generate_test_id').lower()}@{config.employee_email_domain}",
                'Title': 'Integration Test User',
                'ChangeDefaultLC': config.employee_change_default_lc,
                'Locale': config.employee_locale,
                'State': config.employee_state,
                'Country': config.employee_country,
            },
        )

        create_webhook_vp_employee = rail.VantagepointAPIOperator(
            task_id='create_webhook_vp_employee',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/employee',
            request_method='POST',
            request_body=lambda: {
                'Employee': f"WI{rail.result('generate_test_id')}",
                'EmployeeCompany': config.vp_employee_company,
                'HomeCompany': config.vp_employee_company,
                'Org': config.vp_employee_org,
                'Status': config.employee_status,
                'FirstName': config.webhook_name_prefix,
                'LastName': f"WI{rail.result('generate_test_id')}",
                'PreferredName': 'Webhook User',
                'HireDate': config.employee_hire_date,
                'TerminationDate': '',
                'ReadyForProcessing': config.employee_ready_for_processing,
                'Type': config.employee_type,
                'PayType': config.employee_pay_type,
                'EMail': f"wi{rail.result('generate_test_id').lower()}@{config.employee_email_domain}",
                'Title': 'Webhook Integration Test User',
                'ChangeDefaultLC': config.employee_change_default_lc,
                'Locale': config.employee_locale,
                'State': config.employee_state,
                'Country': config.employee_country,
                'Supervisor': f"IT{rail.result('generate_test_id')}",
            },
        )

        # ── SCENARIO 1: Initial Sync — new user created in Replicon ──────────────
        # Path: force_initial_run=True → get_all_users_from_vp (filtered to test employee via
        #       customSettings.userSyncFilters) → process_each_user
        #       child: currentdetails=null → create_or_update_user (no target.uri)
        # Assert: dynamically-created VP employee appears in Replicon with loginEnabled=True

        trigger_initial_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_initial_sync',
            items=lambda: [{}],
            execution_timeout=timedelta(hours=2),
            trigger_dag_id=config.target_user_sync_dag_id,
            conf=lambda item, dag_run: {
                'company_key': config.company_key,
                'replicon_conn_id': config.replicon_conn_id,
                'vantagepoint_conn_id': config.vantagepoint_conn_id,
                # Bypasses the Airflow Variable check in check_initial_run (user_sync_main_dag.py)
                'force_initial_run': True,
                # Scopes get_all_users_from_vp to only the dynamically-created test employee
                'customSettings': {
                    'userSyncFilters': [
                        {'key': 'Employee', 'value': 'IT' +
                            rail.result('generate_test_id')}
                    ]
                },
            },
        )

        wait_initial_sync = rail.WaitForDagRunsSensor(
            task_id='wait_initial_sync',
            execution_timeout=timedelta(hours=2),
            dag_runs='{{ result("trigger_initial_sync") }}',
        )

        read_user_after_initial_sync = rail.RepliconServiceOperator(
            task_id='read_user_after_initial_sync',
            replicon_conn_id=config.replicon_conn_id,
            endpoint='/services/ImportService2.svc/GetUserDetails',
            data=lambda: {
                'user': {
                    'uri': None,
                    'loginName': f"IT{rail.result('generate_test_id')}",
                    'employeeId': None,
                    'parameterCorrelationId': None
                },
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            }
        )

        def _assert_user_created():
            user = rail.result('read_user_after_initial_sync')
            if not user:
                raise AssertionError(
                    'Scenario 1 FAIL: dynamically-created VP employee not found in Replicon after initial sync')

            test_id = rail.result('generate_test_id')
            full_test_id = f"IT{test_id}"

            # Core identity properties
            assert user.get('loginName') == full_test_id, \
                f"loginName mismatch: expected {full_test_id}, got {user.get('loginName')}"
            assert user.get('firstName') == 'IntegTest', \
                f"firstName mismatch: expected 'IntegTest', got {user.get('firstName')}"
            assert user.get('lastName') == full_test_id, \
                f"lastName mismatch: expected {full_test_id}, got {user.get('lastName')}"
            assert user.get('displayName'), \
                f"displayName should not be empty"

            # Email validation
            expected_email = f"it{test_id.lower()}@test.local"
            assert user.get('emailAddress') == expected_email, \
                f"emailAddress mismatch: expected {expected_email}, got {user.get('emailAddress')}"

            # Login and access properties
            assert user.get('loginEnabled') is True, \
                f"loginEnabled should be True, got {user.get('loginEnabled')}"
            assert user.get('uri'), \
                "uri should exist (user not properly created)"

            # Start date properties
            start_date = user.get('startDate')
            assert start_date, "startDate should exist"
            assert start_date.get('year') == 2024 and start_date.get('month') == 1 and start_date.get('day') == 1, \
                f"startDate should be 2024-01-01, got {start_date}"

            # Permissions and policies
            permission_sets = user.get('permissionSets', [])
            assert len(permission_sets) > 0, \
                "User should have at least one permission set assigned"

            policy_sets = user.get('policySets', [])
            assert len(policy_sets) > 0, \
                "User should have at least one policy set assigned"

            # Security configuration
            security_config = user.get('securityConfiguration', {})
            assert security_config.get('loginName') == full_test_id, \
                f"Security config loginName mismatch: expected {full_test_id}, got {security_config.get('loginName')}"
            assert security_config.get('authenticationProviders'), \
                "User should have authentication providers configured"

            # Schedule and timezone
            assert user.get('timeZone'), \
                "User should have timezone configured"
            assert user.get('scheduleTypeSchedule'), \
                "User should have schedule type assigned"
            assert user.get('serviceCenterSchedule'), \
                "User should have service center assigned"

            # Formatting preferences
            formattings = user.get('formattings', {})
            assert formattings.get('generalFormats'), \
                "User should have general format preferences"
            assert formattings.get('nameFormats'), \
                "User should have name format preferences"

        assert_user_created = rail.PythonOperator(
            task_id='assert_user_created',
            python_callable=_assert_user_created,
        )

        # ── SCENARIO 2: Webhook — New User Created ───────────────────────────────
        # Path: is_user_deleted_in_vp=No → if_user_not_to_process=No → process_each_user
        #       child: currentdetails=null → create_or_update_user (no target.uri)
        # Assert: user exists in Replicon with loginEnabled=True

        # ── SCENARIO 3: Webhook — Update Existing User Fields ────────────────────

        trigger_webhook_create = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_webhook_create',
            items=lambda: [{}],
            execution_timeout=timedelta(hours=2),
            trigger_dag_id=config.target_user_sync_dag_id,
            conf=lambda item, dag_run: {
                'company_key': config.company_key,
                'replicon_conn_id': config.replicon_conn_id,
                'vantagepoint_conn_id': config.vantagepoint_conn_id,
                'webhook': {
                    'data': {
                        'Action': 'insert',
                        'Employee Number': f"WI{rail.result('generate_test_id')}",
                    },
                },
            },
        )

        wait_webhook_create = rail.WaitForDagRunsSensor(
            task_id='wait_webhook_create',
            execution_timeout=timedelta(hours=2),
            dag_runs='{{ result("trigger_webhook_create") }}',
        )

        read_user_after_webhook_create = rail.RepliconServiceOperator(
            task_id='read_user_after_webhook_create',
            replicon_conn_id=config.replicon_conn_id,
            endpoint='/services/ImportService2.svc/GetUserDetails',
            data=lambda: {
                'user': {
                    'uri': None,
                    'loginName': f"WI{rail.result('generate_test_id')}",
                    'employeeId': None,
                    'parameterCorrelationId': None
                },
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            }
        )

        def _assert_webhook_create():
            user = rail.result('read_user_after_webhook_create')
            if not user:
                raise AssertionError(
                    'Scenario 2 FAIL: WI employee not found in Replicon after webhook create')
            if not user.get('loginEnabled'):
                raise AssertionError(
                    f"Scenario 2 FAIL: expected loginEnabled=True, got {user.get('loginEnabled')}")

        assert_webhook_create = rail.PythonOperator(
            task_id='assert_webhook_create',
            python_callable=_assert_webhook_create,
        )

        # ── SUPERVISOR ASSIGNMENT CHECK: Verify IT employee (supervisor) has permission assigned ──
        # When WI employee is synced with IT employee as supervisor, the main DAG should assign
        # the supervision permission to the IT employee.

        read_it_employee_from_replicon = rail.RepliconServiceOperator(
            task_id='read_it_employee_from_replicon',
            replicon_conn_id=config.replicon_conn_id,
            endpoint='/services/ImportService2.svc/GetUserDetails',
            data=lambda: {
                'user': {
                    'uri': None,
                    'loginName': f"IT{rail.result('generate_test_id')}",
                    'employeeId': None,
                    'parameterCorrelationId': None
                },
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            }
        )

        read_supervisor_permission_from_replicon = rail.RepliconServiceOperator(
            task_id='read_supervisor_permission_from_replicon',
            replicon_conn_id=config.replicon_conn_id,
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {
                'userUri': rail.result('read_it_employee_from_replicon') and rail.result('read_it_employee_from_replicon').get('uri')
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision'
            ) if response else None,
        )

        def _assert_supervisor_permission_assigned():
            it_user = rail.result('read_it_employee_from_replicon')
            test_id = rail.result('generate_test_id')
            if not it_user:
                raise AssertionError(
                    f'Scenario 2 FAIL: IT employee (IT{test_id}) not found in Replicon')
            permission = rail.result(
                'read_supervisor_permission_from_replicon')
            if not permission:
                raise AssertionError(
                    f'Scenario 2 FAIL: IT employee (supervisor) does not have supervision permission assigned after WI employee webhook create')

        assert_supervisor_permission_assigned = rail.PythonOperator(
            task_id='assert_supervisor_permission_assigned',
            python_callable=_assert_supervisor_permission_assigned,
        )

        # ── SCENARIO 3: Webhook Update — update existing user in VP and sync to Replicon ──
        # Update the WI employee with new FirstName, EMail, and PreferredName, then trigger update webhook

        update_vp_employee_for_webhook = rail.VantagepointAPIOperator(
            task_id='update_vp_employee_for_webhook',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/employee/WI{{result("generate_test_id")}}',
            request_method='PUT',
            request_body=lambda: {
                'Employee': f"WI{rail.result('generate_test_id')}",
                'FirstName': config.webhook_updated_name_prefix,
                'LastName': f"WI{rail.result('generate_test_id')}-Updated",
                'PreferredName': 'Webhook User Updated',
                'EMail': f"wi-updated-{rail.result('generate_test_id').lower()}@{config.employee_email_domain}",
                'Title': 'Webhook Integration Test User - Updated',
            },
        )

        trigger_webhook_update = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_webhook_update',
            items=lambda: [{}],
            execution_timeout=timedelta(hours=2),
            trigger_dag_id=config.target_user_sync_dag_id,
            conf=lambda item, dag_run: {
                'company_key': config.company_key,
                'replicon_conn_id': config.replicon_conn_id,
                'vantagepoint_conn_id': config.vantagepoint_conn_id,
                'webhook': {
                    'data': {
                        'Action': 'update',
                        'Employee Number': f"WI{rail.result('generate_test_id')}",
                    },
                },
            },
        )

        wait_webhook_update = rail.WaitForDagRunsSensor(
            task_id='wait_webhook_update',
            execution_timeout=timedelta(hours=2),
            dag_runs='{{ result("trigger_webhook_update") }}',
        )

        read_user_after_webhook_update = rail.RepliconServiceOperator(
            task_id='read_user_after_webhook_update',
            replicon_conn_id=config.replicon_conn_id,
            endpoint='/services/ImportService2.svc/GetUserDetails',
            data=lambda: {
                'user': {
                    'uri': None,
                    'loginName': f"WI{rail.result('generate_test_id')}",
                    'employeeId': None,
                    'parameterCorrelationId': None
                },
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            }
        )

        def _assert_webhook_update():
            user = rail.result('read_user_after_webhook_update')
            if not user:
                raise AssertionError(
                    'Scenario 3 FAIL: WI employee not found in Replicon after webhook update')
            if not user.get('loginEnabled'):
                raise AssertionError(
                    f"Scenario 3 FAIL: expected loginEnabled=True after field update, got {user.get('loginEnabled')}")
            test_id = rail.result('generate_test_id')
            if user.get('firstName') != config.webhook_updated_name_prefix:
                raise AssertionError(
                    f"Scenario 3 FAIL: expected firstName='{config.webhook_updated_name_prefix}', got {user.get('firstName')}")
            if user.get('lastName') != f"WI{test_id}-Updated":
                raise AssertionError(
                    f"Scenario 3 FAIL: expected lastName='WI{test_id}-Updated', got {user.get('lastName')}")
            if user.get('displayName') != 'Webhook User Updated':
                raise AssertionError(
                    f"Scenario 3 FAIL: expected displayName='Webhook User Updated', got {user.get('displayName')}")
            if user.get('emailAddress') != f"wi-updated-{test_id.lower()}@test.local":
                raise AssertionError(
                    f"Scenario 3 FAIL: expected emailAddress='wi-updated-{test_id.lower()}@test.local', got {user.get('emailAddress')}")

        assert_webhook_update = rail.PythonOperator(
            task_id='assert_webhook_update',
            python_callable=_assert_webhook_update,
        )

        # ── SCENARIO 4: Webhook Delete — delete user in VP, then disable in Replicon ──
        # Mark the WI employee as deleted in VP by setting TerminationDate, then trigger delete webhook
        # Path: update TerminationDate in VP → webhook Action='delete' → search_user_in_replicon
        #       → if_user_found=Yes → disable_user → loginEnabled=False in Replicon
        # Assert: User still exists in Replicon but loginEnabled=False

        update_vp_employee_for_delete = rail.VantagepointAPIOperator(
            task_id='update_vp_employee_for_delete',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/employee/WI{{result("generate_test_id")}}',
            request_method='PUT',
            request_body=lambda: {
                'Employee': f"WI{rail.result('generate_test_id')}",
                'TerminationDate': config.employee_termination_date,
            },
        )

        trigger_webhook_delete_exists = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_webhook_delete_exists',
            items=lambda: [{}],
            execution_timeout=timedelta(hours=2),
            trigger_dag_id=config.target_user_sync_dag_id,
            conf=lambda item, dag_run: {
                'company_key': config.company_key,
                'replicon_conn_id': config.replicon_conn_id,
                'vantagepoint_conn_id': config.vantagepoint_conn_id,
                'webhook': {
                    'data': {
                        'Action': 'delete',
                        'Employee Number': f"WI{rail.result('generate_test_id')}",
                    },
                },
            },
        )

        wait_webhook_delete_exists = rail.WaitForDagRunsSensor(
            task_id='wait_webhook_delete_exists',
            execution_timeout=timedelta(hours=2),
            dag_runs='{{ result("trigger_webhook_delete_exists") }}',
        )

        read_user_after_delete = rail.RepliconServiceOperator(
            task_id='read_user_after_delete',
            replicon_conn_id=config.replicon_conn_id,
            endpoint='/services/ImportService2.svc/GetUserDetails',
            data=lambda: {
                'user': {
                    'uri': None,
                    'loginName': f"WI{rail.result('generate_test_id')}",
                    'employeeId': None,
                    'parameterCorrelationId': None
                },
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            }
        )

        def _assert_user_disabled():
            user = rail.result('read_user_after_delete')
            test_id = rail.result('generate_test_id')
            if not user:
                raise AssertionError(
                    'Scenario 4 FAIL: WI employee not found in Replicon after delete webhook')
            if user.get('loginEnabled') is not False:
                raise AssertionError(
                    f"Scenario 4 FAIL: expected loginEnabled=False (user disabled), got {user.get('loginEnabled')}")
            if user.get('loginName') != f"WI{test_id}":
                raise AssertionError(
                    f"Scenario 4 FAIL: expected loginName='WI{test_id}', got {user.get('loginName')}")
            # Verify user still exists (not deleted) but is disabled
            if user.get('uri') is None:
                raise AssertionError(
                    'Scenario 4 FAIL: user uri is None - user may have been deleted instead of disabled')

        assert_user_disabled = rail.PythonOperator(
            task_id='assert_user_disabled',
            python_callable=_assert_user_disabled,
        )

        # ── SCENARIO 5: Webhook — User Reactivated (Clear TerminationDate) ──
        # Clear the TerminationDate in VP to mark user as active again, then trigger update webhook
        # Path: clear TerminationDate in VP → webhook Action='update' → fetch user from VP
        #       → user is now active again (Status='A', no TerminationDate) → sync to Replicon
        #       → loginEnabled=True in Replicon
        # Assert: loginEnabled=True (user reactivated)

        update_vp_employee_for_reactivate = rail.VantagepointAPIOperator(
            task_id='update_vp_employee_for_reactivate',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/employee/WI{{result("generate_test_id")}}',
            request_method='PUT',
            request_body=lambda: {
                'Employee': f"WI{rail.result('generate_test_id')}",
                'TerminationDate': '',
            },
        )

        trigger_webhook_reactivate = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_webhook_reactivate',
            items=lambda: [{}],
            execution_timeout=timedelta(hours=2),
            trigger_dag_id=config.target_user_sync_dag_id,
            conf=lambda item, dag_run: {
                'company_key': config.company_key,
                'replicon_conn_id': config.replicon_conn_id,
                'vantagepoint_conn_id': config.vantagepoint_conn_id,
                'webhook': {
                    'data': {
                        'Action': 'update',
                        'Employee Number': f"WI{rail.result('generate_test_id')}",
                    },
                },
            },
        )

        wait_webhook_reactivate = rail.WaitForDagRunsSensor(
            task_id='wait_webhook_reactivate',
            execution_timeout=timedelta(hours=2),
            dag_runs='{{ result("trigger_webhook_reactivate") }}',
        )

        read_user_after_reactivate = rail.RepliconServiceOperator(
            task_id='read_user_after_reactivate',
            replicon_conn_id=config.replicon_conn_id,
            endpoint='/services/ImportService2.svc/GetUserDetails',
            data=lambda: {
                'user': {
                    'uri': None,
                    'loginName': f"WI{rail.result('generate_test_id')}",
                    'employeeId': None,
                    'parameterCorrelationId': None
                },
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            }
        )

        def _assert_user_reactivated():
            user = rail.result('read_user_after_reactivate')
            test_id = rail.result('generate_test_id')
            if not user:
                raise AssertionError(
                    'Scenario 5 FAIL: WI employee not found in Replicon after reactivation')
            if user.get('loginEnabled') is not True:
                raise AssertionError(
                    f"Scenario 5 FAIL: expected loginEnabled=True (user reactivated), got {user.get('loginEnabled')}")
            if user.get('loginName') != f"WI{test_id}":
                raise AssertionError(
                    f"Scenario 5 FAIL: expected loginName='WI{test_id}', got {user.get('loginName')}")
            # Verify endDate is cleared (user is active)
            if user.get('endDate'):
                raise AssertionError(
                    f"Scenario 5 FAIL: expected endDate to be empty/null after reactivation, got {user.get('endDate')}")

        assert_user_reactivated = rail.PythonOperator(
            task_id='assert_user_reactivated',
            python_callable=_assert_user_reactivated,
        )

        # ── COVERAGE + EMAIL ──────────────────────────────────────────────────────
        # Uses test_coverage_utils to collect scenario results and recursively
        # traverse the full DAG hierarchy (main → child → nested child) for coverage.

        def _collect_test_results():
            from urllib.parse import urlencode
            from airflow.configuration import conf
            from airflow.utils.session import create_session
            from system.integration_testing.test_coverage_utils import (
                collect_scenario_results,
                collect_dag_run_coverage,
                build_coverage_report,
            )
            test_dag_id = f'vp_replicon_user_sync_integration_test_{config.instance}'
            base_url = conf.get('webserver', 'BASE_URL').rstrip('/')
            run_id = rail.result('generate_test_id') and \
                rail.render_template('{{ dag_run.run_id }}')
            dag_run_link = (
                f'{base_url}/dags/{test_dag_id}/grid?'
                + urlencode({'dag_run_id': run_id, 'tab': 'graph'})
            ) if run_id else None

            with create_session() as session:
                scenario_rows, all_passed, root_dagrun_ids = collect_scenario_results(
                    session, test_dag_id, SCENARIOS
                )
                raw_coverage = collect_dag_run_coverage(session, root_dagrun_ids)
                coverage_report, missed_tasks = build_coverage_report(raw_coverage)

            return {
                'overall_status': 'PASSED' if all_passed else 'FAILED',
                'scenarios': scenario_rows,
                'coverage': coverage_report,
                'missed_task_names': missed_tasks,
                'all_passed': all_passed,
                'dag_run_link': dag_run_link,
            }

        collect_test_results = rail.PythonOperator(
            task_id='collect_test_results',
            python_callable=_collect_test_results,
            trigger_rule='all_done',
        )

        send_test_results_email = rail.EmailOperator(
            task_id='send_test_results_email',
            to='{{ var.value.' + config.email_recipients_variable + ' }}',
            subject=(
                f'{config.company_key} | VP-Replicon User Sync Integration Test - '
                '{{ result("collect_test_results").overall_status }} - '
                '{{ current_time_in_specified_tz(fmt="%Y-%m-%d") }}'
            ),
            html_content='templates/emails/test_results.html',
            trigger_rule='all_done',
            sumo_conn_id=None,
        )

        # Fails the DAG explicitly if any scenario failed, so the run is marked red.
        def _fail_if_errors():
            results = rail.result('collect_test_results')
            if not results:
                raise Exception("Integration test FAILED: collect_test_results did not complete successfully")
            if not results.get('all_passed'):
                failed = [s['name'] for s in results['scenarios'] if s['status'] != 'SUCCESS']
                raise Exception(f"Integration test FAILED. Failed scenarios: {', '.join(failed)}")

        fail_on_test_errors = rail.PythonOperator(
            task_id='fail_on_test_errors',
            python_callable=_fail_if_errors,
            trigger_rule='all_done',
        )

        # ── CLEANUP: Delete WI and IT test employees from VP (in order) ──────────────
        # Delete WI{test_id} first (may reference IT as supervisor), then IT{test_id}
        cleanup_wi_employee = rail.VantagepointAPIOperator(
            task_id='cleanup_wi_employee',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/employee/WI{{result("generate_test_id")}}',
            request_method='DELETE'
        )

        cleanup_it_employee = rail.VantagepointAPIOperator(
            task_id='cleanup_it_employee',
            vp_conn_id=config.vantagepoint_conn_id,
            endpoint='/employee/IT{{result("generate_test_id")}}',
            request_method='DELETE'
        )

        # ── Task Dependencies ─────────────────────────────────────────────────────
        # SETUP (generate_test_id → create_vp_employee)
        #   → S1 (initial sync, filtered to test employee, force_initial_run=True)
        #   → S2 (webhook create, supervisor assignment validation)
        #   → S3 (webhook update, field changes)
        #   → S4 (webhook delete, user exists, loginEnabled=False)
        #   → S5 (webhook reactivate, loginEnabled=True)
        #   → cleanup WI employee → cleanup IT employee

        generate_test_id >> create_vp_employee >> trigger_initial_sync >> wait_initial_sync
        wait_initial_sync >> read_user_after_initial_sync >> assert_user_created

        assert_user_created >> create_webhook_vp_employee >> trigger_webhook_create >> wait_webhook_create
        wait_webhook_create >> read_user_after_webhook_create >> assert_webhook_create

        assert_webhook_create >> read_it_employee_from_replicon >> read_supervisor_permission_from_replicon >> assert_supervisor_permission_assigned >> update_vp_employee_for_webhook >> trigger_webhook_update >> wait_webhook_update >> read_user_after_webhook_update >> assert_webhook_update

        assert_webhook_update >> update_vp_employee_for_delete >> trigger_webhook_delete_exists >> wait_webhook_delete_exists
        wait_webhook_delete_exists >> read_user_after_delete >> assert_user_disabled

        assert_user_disabled >> update_vp_employee_for_reactivate >> trigger_webhook_reactivate >> wait_webhook_reactivate >> read_user_after_reactivate >> assert_user_reactivated

        assert_user_reactivated >> collect_test_results
        collect_test_results >> send_test_results_email >> fail_on_test_errors
        fail_on_test_errors >> cleanup_wi_employee >> cleanup_it_employee

        return dag


rail.for_each_instance(create_dag)
