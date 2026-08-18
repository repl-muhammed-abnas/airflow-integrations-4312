from datetime import timedelta
import json
import logging
from typing import Dict, Any, Optional
import rail
from airflow.models import Variable


def build_currency_object(default_currency: Dict[str, Any]) -> Dict[str, str]:
    return {
        "id": default_currency.get('uri', get_default_currency_uri()),
        "uri": default_currency.get('uri', get_default_currency_uri()),
        "displayText": default_currency.get('symbol', '$'),
        "name": default_currency.get('name', 'US Dollar'),
        "symbol": default_currency.get('symbol', '$')
    }


def get_default_currency_uri() -> str:
    return "urn:replicon-tenant:{{ get_tenant_slug() }}:currency:1"


def create_batch_and_user_tasks(config):
    rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

    def can_run_batch():
        return Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true'

    can_run_batch_task = rail.IfOperator(
        task_id='can_run_batch_task',
        test=can_run_batch,
        yes_task='batch_task',
        no_task='is_mode_b_gate'
    )

    batch_task = rail.BatchTaskRunOperator(
        task_id='batch_task',
        start_task='is_mode_b_gate',
        end_task='catch_project_import_error',
        execution_timeout=timedelta(days=config.execution_timeout_days)
    )

    def get_jira_user_detail(response) -> Optional[Dict[str, Any]]:
        userdetails = response[0] if response else None
        if not userdetails:
            return None
        return {
            "emailaddress": userdetails.get('emailAddress'),
            "name": userdetails.get('displayName'),
            "active": userdetails.get('active')
        }

    def get_jira_query_params(dag_run) -> Dict[str, str]:
        return {"accountId": dag_run.conf['project_leader_acc_id']}

    search_jira_user_detail = rail.JiraAPIOperator(
        task_id='search_jira_user_detail',
        request_method='GET',
        endpoint="/rest/api/3/user",
        query_params=get_jira_query_params,
        jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
        data_handler=get_jira_user_detail
    )

    def get_base_currency(response) -> Dict[str, str]:
        if not response:
            return {"uri": "urn:replicon-tenant:{{ get_tenant_slug() }}:currency:1"}
        base_currency = next(
            (currency for currency in response if currency.get('isBaseCurrency')),
            None
        )
        return base_currency if base_currency else response[0]

    get_default_currency = rail.RepliconServiceOperator(
        task_id='get_default_currency',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
        data_handler=get_base_currency
    )

    return can_run_batch_task, batch_task, search_jira_user_detail, get_default_currency


def create_user_search_tasks():
    def get_filtered_data(response) -> Dict[str, str]:
        if not response or not hasattr(response, 'json'):
            return {}
        try:
            data = response.json()['d']['rows']
            userinfo = []
            for item in data:
                cells = item.get('cells', [])
                if len(cells) >= 3:
                    userinfo.append({
                        "user_uri": cells[0].get('uri'),
                        "user_name": cells[0].get('textValue'),
                        "user_id": cells[1].get('textValue'),
                        "emailaddress": cells[2].get('textValue'),
                    })
            return userinfo[0] if userinfo else {}
        except (KeyError, IndexError, TypeError):
            return {}

    search_user = rail.RepliconServiceOperator(
        task_id='search_user',
        endpoint='/services/UserListService1.svc/GetData',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        data={
            "page": "1",
            "pagesize": "10000",
            "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:employee-id",
                "urn:replicon:user-list-column:email-address"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                    "leftExpression": None,
                    "operatorUri": None,
                    "rightExpression": None,
                    "value": None,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "leftExpression": None,
                    "operatorUri": None,
                    "rightExpression": None,
                    "value": {
                        "uri": None,
                        "uris": [],
                        "bool": None,
                        "date": None,
                        "money": None,
                        "number": None,
                        "text": "{{ result('search_jira_user_detail').emailaddress }}",
                        "time": None,
                        "calendarDayDurationValue": None,
                        "workdayDurationValue": None,
                        "dateRange": None,
                        "dateTimeUtc": None
                    },
                    "filterDefinitionUri": None
                },
                "value": None,
                "filterDefinitionUri": None
            }
        },
        response_filter=get_filtered_data
    )

    def check_user_uri_present():
        search_result = rail.result('search_user')
        return search_result and search_result.get('user_uri')

    if_user_uri_present = rail.IfOperator(
        task_id='if_user_uri_present',
        test=check_user_uri_present,
        yes_task="check_polaris_permissions",
        no_task="check_polaris_permissions_no_manager",
    )

    return search_user, if_user_uri_present


def get_polaris_project_payload_with_manager_check(dag_run) -> str:
    search_user_result = rail.result('search_user', {})
    user_uri = search_user_result.get('user_uri') if search_user_result else None
    default_currency = rail.result('get_default_currency', {})
    currency_obj = build_currency_object(default_currency)

    project_input = {
        "name": dag_run.conf['project_name'],
        "code": dag_run.conf['project_id'],
        "isTimeEntryAllowed": True,
        "timeAndExpenseEntryType": {
            "id": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
        },
        "isProjectLeaderApprovalRequired": True,
        "estimatedCost": {
            "amount": 0,
            "currency": currency_obj
        },
        "totalEstimatedContract": {
            "amount": 0,
            "currency": currency_obj
        },
        "budgetedCost": {
            "amount": 0,
            "currency": currency_obj
        },
        "defaultBillingCurrency": currency_obj,
        "budgetHours": 0,
        "keyValues": [
            {
                "keyUri": "urn:replicon:project-key-value-key:project-management-type",
                "value": {
                    "uri": "urn:replicon:project-management-type:managed",
                    "id": "urn:replicon:project-management-type:managed"
                }
            }
        ]
    }

    if user_uri:
        project_input["projectManagerReference"] = {
            "id": user_uri,
            "displayText": search_user_result.get('user_name', '')
        }

    return json.dumps([{
        "operationName": "AddProject",
        "variables": {
            "projectInput": project_input
        },
        "query": """mutation AddProject($projectInput: ProjectInput!) {
            addProject2(projectInput: $projectInput) {
                project {
                    id
                    slug
                    name
                    code
                    uri
                    displayText
                    startDate
                    endDate
                    projectManagerReference {
                        id
                        displayText
                    }
                    defaultBillingCurrency {
                        id
                        displayText
                    }
                    billingType {
                        displayText
                        uri
                    }
                }
                errors {
                    id
                    displayText
                    failureUri
                    severityUri
                }
            }
        }"""
    }])


def create_project_tasks():
    check_polaris_permissions = rail.IfOperator(
        task_id='check_polaris_permissions',
        test="{{ dag_run.conf.is_polaris_permissions_present | is_truthy }}",
        yes_task="create_project_polaris",
        no_task="create_project_with_manager"
    )

    check_polaris_permissions_no_manager = rail.IfOperator(
        task_id='check_polaris_permissions_no_manager',
        test="{{ dag_run.conf.is_polaris_permissions_present | is_truthy }}",
        trigger_rule='one_success',
        yes_task="create_project_polaris",
        no_task="create_project_without_manager"
    )

    def handle_polaris_response(response) -> Dict[str, Any]:
        project_data = response[0]['data']['addProject2']['project']
        return {
            'uri': project_data['uri'],
            'id': project_data['id'],
            'name': project_data['name'],
            'code': project_data['code'],
            'errors': response[0]['data']['addProject2'].get('errors', [])
        }

    create_project_polaris = rail.RepliconServiceOperator(
        task_id='create_project_polaris',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint="/graphql",
        app='polaris',
        data=get_polaris_project_payload_with_manager_check,
        data_handler=handle_polaris_response
    )

    check_project_created = rail.IfOperator(
        task_id='check_project_created',
        test=lambda: True,
        yes_task='setup_blank_rate_cards',
        no_task='should_update_project_type'
    )

    def setup_rate_cards() -> Dict[str, str]:
        return {
            'status': 'completed',
            'message': 'Rate Cards setup completed - Polaris projects automatically have blank rate structures'
        }

    setup_blank_rate_cards = rail.PythonOperator(
        task_id='setup_blank_rate_cards',
        python_callable=setup_rate_cards
    )

    return (check_polaris_permissions, check_polaris_permissions_no_manager,
            create_project_polaris, check_project_created, setup_blank_rate_cards)


def create_gen3_and_management_tasks():
    create_project_with_manager = rail.RepliconServiceOperator(
        task_id='create_project_with_manager',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint="/services/ProjectService1.svc/PutProjectInfo2",
        data={
            "target": {
                "uri": None,
                "name": "{{ dag_run.conf.project_name }}",
                "code": None,
                "parameterCorrelationId": None
            },
            "projectInfo": {
                "name": "{{ dag_run.conf.project_name }}",
                "code": "{{ dag_run.conf.project_id }}",
                "description": None,
                "timeEntryDateRange": None,
                "projectStatusLabel": {
                    "uri": None,
                    "name": "In Progress"
                },
                "percentCompleted": "0",
                "client": None,
                "clientRepresentative": None,
                "program": None,
                "projectLeader": {
                    "uri": "{{ result('search_user').user_uri }}",
                    "loginName": None,
                    "employeeId": None,
                    "parameterCorrelationId": None
                },
                "customFieldValues": [],
                "isTimeEntryAllowed": "1",
                "costTypeUri": None,
                "estimatedHours": None,
                "estimatedCost": None,
                "estimatedExpenses": None,
                "budget": None,
                "isProjectLeaderApprovalRequired": "1",
                "estimationModeUri": None,
                "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                "timeAndMaterials": {
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    "billingRateFrequency": None,
                    "billingRateFrequencyDuration": None,
                    "billingRates": []
                },
                "defaultBillingCurrency": None
            }
        }
    )

    create_project_without_manager = rail.RepliconServiceOperator(
        task_id='create_project_without_manager',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint="/services/ProjectService1.svc/PutProjectInfo2",
        data={
            "target": {
                "uri": None,
                "name": "{{ dag_run.conf.project_name }}",
                "code": None,
                "parameterCorrelationId": None
            },
            "projectInfo": {
                "name": "{{ dag_run.conf.project_name }}",
                "code": "{{ dag_run.conf.project_id }}",
                "description": None,
                "timeEntryDateRange": None,
                "projectStatusLabel": {
                    "uri": None,
                    "name": "In Progress"
                },
                "percentCompleted": "0",
                "client": None,
                "clientRepresentative": None,
                "program": None,
                "projectLeader": None,
                "customFieldValues": [],
                "isTimeEntryAllowed": "1",
                "costTypeUri": None,
                "estimatedHours": None,
                "estimatedCost": None,
                "estimatedExpenses": None,
                "budget": None,
                "isProjectLeaderApprovalRequired": "1",
                "estimationModeUri": None,
                "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                "timeAndMaterials": {
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    "billingRateFrequency": None,
                    "billingRateFrequencyDuration": None,
                    "billingRates": []
                },
                "defaultBillingCurrency": None
            }
        }
    )

    should_update_project_type = rail.IfOperator(
        task_id='should_update_project_type',
        test="{{ not dag_run.conf.is_polaris_permissions_present | is_truthy }}",
        yes_task='update_project_type',
        no_task='get_company_department'
    )

    def get_gen3_project_uri() -> str:
        with_manager = rail.result('create_project_with_manager')
        without_manager = rail.result('create_project_without_manager')
        if with_manager:
            return with_manager['uri']
        return without_manager['uri']

    update_project_type = rail.RepliconServiceOperator(
        task_id='update_project_type',
        endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        data=lambda: {
            'projectUri': get_gen3_project_uri(),
            'keyValue': {
                'keyUri': 'urn:replicon:project-key-value-key:project-management-type',
                'value': {
                    'uri': 'urn:replicon:project-management-type:managed'
                }
            }
        }
    )

    get_company_department = rail.RepliconServiceOperator(
        task_id='get_company_department',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint="/services/DepartmentService1.svc/GetCompanyDepartment"
    )

    def get_project_uri() -> str:
        project_tasks = [
            'create_project_with_manager',
            'create_project_without_manager',
            'create_project_polaris'
        ]
        for task_id in project_tasks:
            result = rail.result(task_id, {})
            if result and isinstance(result, dict) and result.get('uri'):
                return result['uri']
        return ""

    def get_team_member_data() -> Dict[str, str]:
        return {
            "projectUri": get_project_uri(),
            "resourceUri": rail.result('get_company_department')['uri'],
            "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
        }

    update_project_team_members = rail.RepliconServiceOperator(
        task_id='update_project_team_members',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
        data=get_team_member_data
    )

    def get_downstreamtasks_error(project_name: str, error_message: str) -> Dict[str, str]:
        return {'error': f'Error with {project_name} - {error_message}'}

    catch_project_import_error = rail.PythonOperator(
        task_id='catch_project_import_error',
        trigger_rule='one_failed',
        python_callable=get_downstreamtasks_error,
        op_args=['{{ dag_run.conf.project_name }}', '{{ get_error_message() }}']
    )

    return (create_project_with_manager, create_project_without_manager, should_update_project_type,
            update_project_type, get_company_department, update_project_team_members, catch_project_import_error)


def create_bulk_assign_tasks():
    def get_jira_member_emails(response) -> list:
        # JiraAPIOperator extracts 'values' from each page and collects as list-of-pages
        # Each item in response is a page (list of user objects)
        emails = []
        for item in (response or []):
            if item is None:
                continue
            users = item if isinstance(item, list) else [item]
            for user in users:
                if user is None:
                    continue
                if (
                    user.get('accountType') == 'atlassian'
                    and user.get('active') is True
                    and (user.get('emailAddress') or '').strip()
                ):
                    emails.append(user['emailAddress'].strip())
        return emails

    def get_jira_member_query_params(dag_run) -> Dict[str, str]:
        return {
            'projectKey': dag_run.conf['project_key'],
            'permissions': 'CREATE_ISSUES,EDIT_ISSUES,WORK_ON_ISSUES'
        }

    fetch_jira_project_members = rail.JiraAPIOperator(
        task_id='fetch_jira_project_members',
        request_method='GET',
        endpoint='/rest/api/3/user/permission/search',
        query_params=get_jira_member_query_params,
        jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
        data_handler=get_jira_member_emails
    )

    def get_resolve_uris_data():
        emails = rail.result('fetch_jira_project_members') or []
        return {'users': [{'loginName': email} for email in emails]}

    def extract_user_uris(response) -> list:
        resolved = []
        unresolved_count = 0
        for user in (response or []):
            if user is None:
                unresolved_count += 1
                continue
            uri = user.get('uri')
            if uri:
                resolved.append(uri)
            else:
                unresolved_count += 1
        if unresolved_count > 0:
            logging.warning(f"{unresolved_count} user(s) could not be resolved to a Replicon URI and will be skipped.")
        return resolved

    resolve_member_uris = rail.RepliconServiceOperator(
        task_id='resolve_member_uris',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint='/services/UserService1.svc/BulkGetUsers2',
        data=get_resolve_uris_data,
        data_handler=extract_user_uris
    )

    def check_has_resolved_uris():
        uris = rail.result('resolve_member_uris') or []
        return len(uris) > 0

    has_resolved_member_uris = rail.IfOperator(
        task_id='has_resolved_member_uris',
        test=check_has_resolved_uris,
        yes_task='bulk_assign_project_team',
        no_task='update_project_team_members'
    )

    def get_bulk_assign_data():
        project_tasks = [
            'create_project_with_manager',
            'create_project_without_manager',
            'create_project_polaris'
        ]
        project_uri = ''
        for task_id in project_tasks:
            result = rail.result(task_id, {})
            if result and isinstance(result, dict) and result.get('uri'):
                project_uri = result['uri']
                break
        return {
            'projectUri': project_uri,
            'userUris': rail.result('resolve_member_uris'),
            'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
        }

    bulk_assign_project_team = rail.RepliconServiceOperator(
        task_id='bulk_assign_project_team',
        replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment2',
        data=get_bulk_assign_data
    )

    return fetch_jira_project_members, resolve_member_uris, has_resolved_member_uris, bulk_assign_project_team


def create_child_dag(config):
    dag_id = f"standard_jira_{config.region.replace('-', '_')}_project_import_child_dag_{config.instance}"
    description = f'Jira {config.region} Project Import Child DAG {config.instance}'

    with rail.create_airflow_dag(
        dag_id=dag_id,
        description=description,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task, batch_task, search_jira_user_detail, get_default_currency = create_batch_and_user_tasks(config)
        search_user, if_user_uri_present = create_user_search_tasks()
        (check_polaris_permissions, check_polaris_permissions_no_manager,
         create_project_polaris, check_project_created, setup_blank_rate_cards) = create_project_tasks()
        (create_project_with_manager, create_project_without_manager, should_update_project_type,
         update_project_type, get_company_department, update_project_team_members,
         catch_project_import_error) = create_gen3_and_management_tasks()
        (fetch_jira_project_members, resolve_member_uris,
         has_resolved_member_uris, bulk_assign_project_team) = create_bulk_assign_tasks()

        def _is_feature_flag_on():
            """Returns True when wbsSyncSetting is present in dag_run.conf (feature flag enabled)."""
            try:
                conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
                conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
                wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
                return bool(wbs)
            except Exception as exc:
                logging.warning(f"_is_feature_flag_on: failed to parse dag_run.conf, defaulting to False: {exc}")
                return False

        def _is_mode_b():
            """Returns True when feature flag is on AND projectSyncSetting is not ['Project'] (issue-as-project mode)."""
            if not _is_feature_flag_on():
                return False
            try:
                conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
                conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
                wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
                project_sync = (wbs.get('mappings') or {}).get('projectSyncSetting', ['Project'])
                return 'Project' not in project_sync
            except Exception as exc:
                logging.warning(f"_is_mode_b: failed to parse dag_run.conf, defaulting to False: {exc}")
                return False

        is_mode_b_gate = rail.IfOperator(
            task_id='is_mode_b_gate',
            test=_is_mode_b,
            yes_task='check_polaris_permissions_no_manager',
            no_task='search_jira_user_detail'
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> rail.Label('on Error') >> catch_project_import_error
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_project_import_error
        can_run_batch_task >> rail.Label('No') >> is_mode_b_gate
        is_mode_b_gate >> rail.Label('Yes') >> check_polaris_permissions_no_manager
        is_mode_b_gate >> rail.Label('No') >> search_jira_user_detail >> get_default_currency >> search_user >> if_user_uri_present

        if_user_uri_present >> rail.Label('Yes') >> check_polaris_permissions
        check_polaris_permissions >> rail.Label('Yes (Polaris)') >> create_project_polaris >> check_project_created
        check_polaris_permissions >> rail.Label('No (Gen3)') >> create_project_with_manager >> should_update_project_type
        if_user_uri_present >> rail.Label('No') >> check_polaris_permissions_no_manager
        check_polaris_permissions_no_manager >> rail.Label('Yes (Polaris)') >> create_project_polaris >> check_project_created
        check_polaris_permissions_no_manager >> rail.Label('No (Gen3)') >> create_project_without_manager >> should_update_project_type
        check_project_created >> rail.Label('Yes (Setup Rate Cards)') >> setup_blank_rate_cards >> should_update_project_type
        check_project_created >> rail.Label('No (Skip Rate Cards)') >> should_update_project_type

        should_update_project_type >> rail.Label('Yes') >> update_project_type >> get_company_department
        should_update_project_type >> rail.Label('No') >> get_company_department >> fetch_jira_project_members >> resolve_member_uris >> has_resolved_member_uris
        has_resolved_member_uris >> rail.Label('Yes') >> bulk_assign_project_team >> update_project_team_members
        has_resolved_member_uris >> rail.Label('No') >> update_project_team_members
        update_project_team_members >> (
            rail.Label('On Error') >> catch_project_import_error
        )

    return dag


rail.for_each_instance(create_child_dag)
