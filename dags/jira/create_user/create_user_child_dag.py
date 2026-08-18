from datetime import timedelta
import logging
import uuid
import rail
from airflow.models import Variable

log = logging.getLogger(__name__)

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_jira_{config.region.replace('-', '_')}_create_user_child_dag_{config.instance}",
        description=f'Jira {config.region} Create User Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: (Variable.get(
                config.can_run_batch_task_var_name, default_var='true'
            ) or 'true').lower() == 'true',
            yes_task='batch_task',
            no_task='view_dagrun_config'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='view_dagrun_config',
            end_task='catch_create_user_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config'
        )

        # Check duplicate user by loginName
        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def check_user_exists(all_results):
            import itertools

            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x.get('rows', []), all_results))
            ))
            if not flatten_rows:
                return None
            first_row = flatten_rows[0]
            cells = first_row.get('cells', [])
            if len(cells) >= 2:
                user_cell = cells[1]
                user_uri = user_cell.get('uri')
                if user_uri:
                    replicon_enabled = cells[2].get('boolValue', False) if len(cells) > 2 else False
                    return {'uri': user_uri, 'enabled': replicon_enabled}
            return None

        check_duplicate_user = rail.RepliconServicePageOperator(
            task_id='check_duplicate_user',
            endpoint='/services/UserListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": (dag_run.conf or {}).get('user', {}).get('emailAddress', '')
                        },
                        "filterDefinitionUri": None
                    },
                    "value": None,
                    "filterDefinitionUri": None
                }
            },
            page_handler=page_handler,
            all_result_data_handler=check_user_exists
        )

        # Check if the Replicon enabled status needs to match the Jira active status
        def _parse_jira_active(value, default=True):
            """Parse active value that may be bool or string ('true'/'false')."""
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() not in ('false', '0', 'no', '')
            return bool(value) if value is not None else default

        def _needs_enabled_toggle():
            user_result = rail.result('check_duplicate_user')
            if not user_result:
                return False  # new user – no toggle needed
            replicon_enabled = bool(user_result.get('enabled', False))
            raw_active = (rail.get_dag_run_conf() or {}).get('user', {}).get('active', True)
            jira_active = _parse_jira_active(raw_active)
            return replicon_enabled != jira_active

        should_toggle_enabled = rail.IfOperator(
            task_id='should_toggle_enabled',
            test=_needs_enabled_toggle,
            yes_task='is_jira_user_active',
            no_task='get_jira_permissions'
        )

        is_jira_user_active = rail.IfOperator(
            task_id='is_jira_user_active',
            test=lambda: _parse_jira_active(
                (rail.get_dag_run_conf() or {}).get('user', {}).get('active', True)
            ),
            yes_task='enable_user_in_replicon',
            no_task='disable_user_in_replicon'
        )

        enable_user_in_replicon = rail.RepliconServiceOperator(
            task_id='enable_user_in_replicon',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "userUri": rail.result('check_duplicate_user')['uri']
            }
        )

        disable_user_in_replicon = rail.RepliconServiceOperator(
            task_id='disable_user_in_replicon',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "userUri": rail.result('check_duplicate_user')['uri']
            }
        )

        # Get available timezones from Replicon
        def get_integration_admin_flag():
            """Read the is_admin flag passed from the main DAG."""
            dag_conf = rail.get_dag_run_conf() or {}
            return bool(dag_conf.get('is_admin', False))

        get_jira_permissions = rail.PythonOperator(
            task_id='get_jira_permissions',
            python_callable=get_integration_admin_flag
        )

        # Create user in Replicon using CreateUserOrApplyModifications
        def create_user_payload(dag_run):
            user = (dag_run.conf or {}).get('user', {})
            
            _check_result = rail.result('check_duplicate_user')
            existing_user_uri = _check_result.get('uri') if _check_result else None
            is_update = existing_user_uri is not None

            if user.get('accountType') != 'atlassian':
                log.warning(f"Non-atlassian account type: {user.get('accountType')}")

            display_name = user.get('displayName', '').strip()
            full_name = display_name.split()

            first_name = full_name[0][:50] if full_name else 'Unknown'
            last_name = full_name[-1][:50] if len(full_name) > 1 else first_name

            login_name = user.get('loginName') or user.get('emailAddress', '')
            email_address = user.get('emailAddress', '')

            timezone_raw = user.get('timeZone')

            # Translate Jira timezone key to the IANA name expected by Replicon.
            # If the user has no timezone, leave it undefined (omitted from payload).
            timezone_iana = None
            if timezone_raw:
                timezone_iana = config.JIRA_TIMEZONE_MAP.get(timezone_raw)
                if timezone_iana:
                    log.debug(f"Timezone: '{timezone_raw}' -> '{timezone_iana}'")

            # Get permission sets based on Jira admin status
            is_admin = bool(rail.result('get_jira_permissions'))
            permission_map = rail.result('get_available_permission_sets') or {}

            # Build permission sets array
            def build_permission_sets():
                """Build permission sets array in CreateUserOrApplyModifications format"""
                if is_admin:
                    required_slugs = [
                        "system-administrator",
                        "cost-manager",
                        "billing-manager",
                        "project-management-administrator",
                        "resource-manager",
                        "team-manager",
                        "supervisor",
                        "dashboard-user"
                    ]
                else:
                    required_slugs = [
                        "dashboard-user"
                    ]
                
                permission_items = []
                for slug in required_slugs:
                    uri = permission_map.get(slug.lower())
                    if uri:
                        permission_items.append({
                            "permissionSetPolicy": {
                                "uri": uri,
                                "name": None
                            },
                            "groupAccessFilter": None
                        })
                    else:
                        log.warning(f"Permission set '{slug}' not found")
                
                if permission_items:
                    return [{
                        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                        "items": permission_items
                    }]
                else:
                    return []

            permission_sets = build_permission_sets()

            log.debug(f"Timezone raw value: {timezone_raw}, IANA name: {timezone_iana}")

            if is_update:
                # For updates: only firstName, lastName, and timezone
                modifications = {
                    "firstName": {
                        "value": first_name
                    },
                    "lastName": {
                        "value": last_name
                    },
                    "timeZone": {
                        "value": {
                            "uri": None,
                            "IANAName": timezone_iana
                        }
                    } if timezone_iana else None
                }
            else:
                # For new users: include all fields
                # Get URIs from lookup tasks for templates, approval paths, department, and period
                policy_sets = rail.result('get_all_policy_sets') or {}
                timesheet_template_uri = policy_sets.get('timesheet_template_uri')
                timeoff_template_uri = policy_sets.get('timeoff_template_uri')
                expense_template_uri = policy_sets.get('expense_template_uri')
                
                company_department = rail.result('get_company_department') or {}
                department_uri = company_department.get('uri')
                
                timesheet_approval_path_uri = rail.result('get_timesheet_approval_paths')
                timeoff_approval_path_uri = rail.result('get_timeoff_approval_paths')
                timesheet_period_uri = rail.result('get_timesheet_period')

                modifications = {
                    "firstName": {
                        "value": first_name
                    },
                    "lastName": {
                        "value": last_name
                    },
                    "loginName": {
                        "value": login_name
                    },
                    "emailAddress": {
                        "value": email_address
                    },
                    "employeeId": None,
                    "employmentDateRange": None,
                    "securitySettings": {
                        "value": {
                            "loginEnabled": {
                                "value": bool(user.get("active", True))
                            },
                            "forcePasswordChange": {
                                "value": True
                            },
                            "ssoName": {
                                "value": None
                            },
                            "ssoNameModificationOptionUri": None,
                            "password": {"value": Variable.get('jira_default_user_password')},
                            "authenticationProviders": [],
                            "emailMFAResendVerificationEmail": None,
                            "emailMFATryAddMethodFromUsersEmail": None,
                            "isMFAMethodRequired": None,
                            "clearIsLockedOut": None
                        }
                    },
                    "timeZone": {
                        "value": {
                            "uri": None,
                            "IANAName": timezone_iana
                        }
                    } if timezone_iana else None,
                    "workWeekStartDay": {
                        "value": {
                            "uri": "urn:replicon:day-of-week:monday"
                        }
                    },
                    "permissionSets": permission_sets if permission_sets else []
                }
                
                # Add template and approval settings (only for new user creation)
                additional_modifications = {}
                
                # 1. Department (Group) - schedule format
                if department_uri:
                    additional_modifications["departmentGroupSchedule"] = [
                        {
                            "dateRange": None,
                            "item": {
                                "uri": department_uri,
                                "parent": None,
                                "name": None
                            }
                        }
                    ]
                
                # 2. Timesheet Template
                if timesheet_template_uri:
                    additional_modifications["timesheetTemplate"] = {
                        "value": {
                            "uri": timesheet_template_uri,
                            "name": None
                        }
                    }
                
                # 3. Timesheet Approval Path
                if timesheet_approval_path_uri:
                    additional_modifications["timesheetApprovalPath"] = {
                        "value": {
                            "uri": timesheet_approval_path_uri,
                            "name": None
                        }
                    }
                
                # 4. Timesheet Period - schedule format
                if timesheet_period_uri:
                    additional_modifications["timesheetPeriodSchedule"] = [
                        {
                            "dateRange": None,
                            "item": {
                                "uri": timesheet_period_uri,
                                "name": None
                            }
                        }
                    ]
                
                # 5. Time Off Template
                if timeoff_template_uri:
                    additional_modifications["timeoffTemplate"] = {
                        "value": {
                            "uri": timeoff_template_uri,
                            "name": None
                        }
                    }
                
                # 6. Time Off Approval Path
                if timeoff_approval_path_uri:
                    additional_modifications["timeoffApprovalPath"] = {
                        "value": {
                            "uri": timeoff_approval_path_uri,
                            "name": None
                        }
                    }
                
                # 7. Expense Template
                if expense_template_uri:
                    additional_modifications["expenseTemplate"] = {
                        "value": {
                            "uri": expense_template_uri,
                            "name": None
                        }
                    }
                
                # Merge additional modifications
                modifications.update(additional_modifications)
            
            # Remove None values for cleaner payload
            modifications = {k: v for k, v in modifications.items() if v is not None}

            payload = {
                "target": {"uri": existing_user_uri} if is_update else None,
                "template": None,
                "modifications": modifications,
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

            return payload

        def extract_created_user_uri(response):
            try:
                data = response.json()
                d = data.get('d', {})

                errors = d.get('errors', [])
                if errors:
                    for err in errors:
                        notifications = err.get('notifications', [])
                        if notifications:
                            error_msg = notifications[0].get('displayText', 'Unknown error')
                            log.warning(f"API error: {error_msg}")
                            
                            # For update operations, "user already exists" is not a fatal error
                            if 'already exists' in error_msg.lower():
                                # Return the existing user URI for updates
                                _check_result = rail.result('check_duplicate_user')
                                existing_uri = _check_result.get('uri') if _check_result else None
                                if existing_uri:
                                    return existing_uri
                            
                            raise ValueError(f"CreateUserOrApplyModifications errors: {error_msg}")

                user_obj = d.get('user', {})
                user_uri = user_obj.get('uri')

                if user_uri:
                    return user_uri

                # For successful updates: response may not include user.uri,
                # fall back to the existing URI from check_duplicate_user
                _check_result = rail.result('check_duplicate_user')
                existing_uri = _check_result.get('uri') if _check_result else None
                if existing_uri:
                    return existing_uri

                return None
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                log.error(f"Error parsing created user URI from response: {e}")
                return None

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=create_user_payload,
            response_filter=extract_created_user_uri
        )

        # Get available permission sets
        def get_permission_sets_payload(dag_run):
            """Get all available permission sets"""
            return {}

        def extract_permission_set_map(response):
            """Extract permission sets and create a mapping from slug to URI"""
            try:
                if hasattr(response, 'json'):
                    data = response.json()
                else:
                    data = response

                if isinstance(data, dict):
                    if 'd' in data and isinstance(data['d'], list):
                        permission_sets = data['d']
                    elif 'd' in data and isinstance(data['d'], dict):
                        permission_sets = (
                            data['d'].get('permissionSets', [])
                            or data['d'].get('rows', [])
                        )
                    else:
                        permission_sets = (
                            data.get('permissionSets', [])
                            or data.get('rows', [])
                        )
                elif isinstance(data, list):
                    permission_sets = data
                else:
                    return {}

                slug_to_uri = {}

                for perm_set in permission_sets:
                    uri = perm_set.get('uri', '')
                    slug = perm_set.get('slug', '') or perm_set.get('name', '')

                    if uri and slug:
                        slug_to_uri[slug.lower()] = uri

                return slug_to_uri
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                log.error(f"Error parsing permission sets from response: {e}")
                return {}

        get_available_permission_sets = rail.RepliconServiceOperator(
            task_id='get_available_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_permission_sets_payload,
            response_filter=extract_permission_set_map
        )

        # Get company department for group assignment
        get_company_department = rail.RepliconServiceOperator(
            task_id='get_company_department',
            endpoint='/services/DepartmentService1.svc/GetCompanyDepartment',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        # Get all policy sets (templates) for timesheet, time off, and expenses
        def extract_policy_set_uris(response):
            """Extract template URIs by slug"""
            try:
                data = response.json() if hasattr(response, 'json') else response
                policy_sets = data.get('d', []) if isinstance(data.get('d'), list) else []
                
                return {
                    'timesheet_template_uri': rail.find_first_by_attr_and_get_attr(
                        policy_sets, 'slug', 'standard-timesheet', 'uri', None
                    ),
                    'timeoff_template_uri': rail.find_first_by_attr_and_get_attr(
                        policy_sets, 'slug', 'time-off', 'uri', None
                    ),
                    'expense_template_uri': rail.find_first_by_attr_and_get_attr(
                        policy_sets, 'slug', 'expenses', 'uri', None
                    )
                }
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                log.error(f"Error parsing policy sets from response: {e}")
                return {
                    'timesheet_template_uri': None,
                    'timeoff_template_uri': None,
                    'expense_template_uri': None
                }

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            response_filter=extract_policy_set_uris
        )

        # Get timesheet approval paths
        def extract_supervisor_approval_path(response):
            """Extract Supervisor approval path URI"""
            try:
                data = response.json() if hasattr(response, 'json') else response
                approval_paths = data.get('d', []) if isinstance(data.get('d'), list) else []
                
                return rail.find_first_by_attr_and_get_attr(
                    approval_paths, 'displayText', 'Supervisor', 'uri', None
                )
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                log.error(f"Error parsing approval path from response: {e}")
                return None

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            response_filter=extract_supervisor_approval_path
        )

        # Get time off approval paths
        get_timeoff_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timeoff_approval_paths',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            response_filter=extract_supervisor_approval_path
        )

        # Get timesheet period URI from slug
        def extract_timesheet_period_uri_from_slug(response):
            """Extract timesheet period URI from GetUriFromSlug response"""
            try:
                data = response.json() if hasattr(response, 'json') else response
                
                # Direct response with 'd' containing the URI
                if isinstance(data, dict) and 'd' in data:
                    return data['d']
                
                return None
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                log.error(f"Error parsing timesheet period URI from response: {e}")
                return None

        get_timesheet_period = rail.RepliconServiceOperator(
            task_id='get_timesheet_period',
            endpoint='/services/TimesheetPeriodService2.svc/GetUriFromSlug',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "timesheetPeriodSlug": "weekly-starting-on-monday"
            },
            response_filter=extract_timesheet_period_uri_from_slug
        )

        # Return success/error
        def log_success():
            _check_result = rail.result("check_duplicate_user")
            existing_user_uri = _check_result.get('uri') if _check_result else None
            is_update = existing_user_uri is not None
            
            operation = "updated" if is_update else "created"
            return f'User {operation} successfully'

        should_return_success = rail.IfOperator(
            task_id='should_return_success',
            test=lambda: rail.result('create_user_in_replicon') is not None,
            yes_task='return_success',
            no_task='return_error'
        )
        
        return_success = rail.PythonOperator(
            task_id='return_success',
            python_callable=log_success
        )

        def log_failure(**context):
            """Raise exception on user creation/update failure to fail the DAG."""
            exception = context.get('exception')
            task_instance = context.get('task_instance')
            
            if exception:
                log.error(f"Exception: {exception}")
            if task_instance:
                log.error(f"Failed task: {task_instance.task_id}")
            
            raise Exception('User creation/update failed - check logs')

        return_error = rail.PythonOperator(
            task_id='return_error',
            python_callable=log_failure
        )
        
        def catch_user_error(**context):
            """Propagate upstream failures to ensure DAG fails."""
            from airflow.utils.state import State
            dag_run = context['dag_run']
            failed_tasks = [
                ti.task_id for ti in dag_run.get_task_instances()
                if ti.state == State.FAILED and ti.task_id != 'catch_create_user_error'
            ]
            if failed_tasks:
                raise Exception(f"Upstream task(s) failed: {', '.join(failed_tasks)}")

        catch_create_user_error = rail.PythonOperator(
            task_id='catch_create_user_error',
            python_callable=catch_user_error,
            trigger_rule='all_done'
        )
        can_run_batch_task >> [batch_task, view_dagrun_config]
        view_dagrun_config >> check_duplicate_user >> should_toggle_enabled
        should_toggle_enabled >> rail.Label('Yes') >> is_jira_user_active
        should_toggle_enabled >> rail.Label('No') >> get_jira_permissions
        is_jira_user_active >> rail.Label('Yes') >> enable_user_in_replicon >> get_jira_permissions
        is_jira_user_active >> rail.Label('No') >> disable_user_in_replicon >> get_jira_permissions
        get_jira_permissions >> get_available_permission_sets
        # Run all lookup tasks sequentially (batch task doesn't support parallel execution)
        get_available_permission_sets >> get_company_department
        get_company_department >> get_all_policy_sets
        get_all_policy_sets >> get_timesheet_approval_paths
        get_timesheet_approval_paths >> get_timeoff_approval_paths
        get_timeoff_approval_paths >> get_timesheet_period
        get_timesheet_period >> create_user_in_replicon >> should_return_success
        should_return_success >> rail.Label('Yes') >> return_success >> rail.Label('on Error') >> catch_create_user_error
        should_return_success >> rail.Label('No') >> return_error >> rail.Label('on Error') >> catch_create_user_error
        batch_task >> catch_create_user_error

    return dag


rail.for_each_instance(create_child_dag)