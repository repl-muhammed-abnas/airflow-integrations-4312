import rail
from datetime import timedelta
from airflow.models import Variable
import uuid

null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_zendesk_connector_{config.region.replace('-', '_')}_create_updated_project_import_child{config.instance}",
        description=f"Zendesk Online {config.region} Create/Update Project child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true"
            ).lower()
            == "true",
            yes_task="batch_task",
            no_task="if_get_organization_by_id_name_present",
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="if_get_organization_by_id_name_present",
            end_task="catch_project_error",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_get_organization_by_id_name_present = rail.IfOperator(
            task_id="if_get_organization_by_id_name_present",
            test=lambda dag_run: dag_run.conf["project_items"]["organization_id"],
            yes_task="search_clients",
            no_task="if_assignee_id_present",
        )

        def data_handler(response, dag_run):
            org_id = dag_run.conf["project_items"]["organization_id"]
            filtered_rows = list(filter(
                lambda x: int(x["cells"][2].get("textValue")) == org_id, response["rows"]))
            return filtered_rows

        search_clients = rail.RepliconServiceOperator(
            task_id="search_clients",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:client-list-column:name",
                    "urn:replicon:client-list-column:client",
                    "urn:replicon:client-list-column:code",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": dag_run.conf["project_items"]["organization_id"]
                        }
                    },
                },
            },
            data_handler=data_handler
        )

        if_assignee_id_present = rail.IfOperator(
            task_id="if_assignee_id_present",
            test=lambda dag_run: dag_run.conf["project_items"]["assignee_id"],
            yes_task="get_users",
            no_task="search_projects",
        )

        get_users = rail.ZendeskAPIOperator2(
            task_id="get_users",
            zendesk_conn_id="{{dag_run.conf.zendesk_conn_id}}",
            endpoint="/api/v2/users/" + "{{dag_run.conf.project_items.assignee_id}}",
            request_method="GET",
            pagination=False,
        )

        search_users = rail.RepliconServiceOperator(
            task_id="search_users",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {"text": rail.result("get_users")['user']['name']},
                        "filterDefinitionUri": null,
                    },
                    "value": null,
                    "filterDefinitionUri": null,
                },
            },
        )

        search_projects = rail.RepliconServiceOperator(
            task_id="search_projects",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code",
                    "urn:replicon:project-list-column:name",
                    "urn:replicon:project-list-column:client"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text",
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": (
                                str(dag_run.conf["project_items"]["subject"])
                                + "-"
                                + str(dag_run.conf["project_items"]["id"])
                            ),
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null,
                        },
                        "filterDefinitionUri": null,
                    },
                    "value": null,
                    "filterDefinitionUri": null,
                },
            },
        )

        def extract_project_info(dag_run):
            search_projects = rail.result("search_projects")
            projects_info = []
            if 'rows' in search_projects:
                for row in search_projects['rows']:
                    if row['cells'][0]['textValue'] == (
                            str(dag_run.conf["project_items"]["subject"]) +
                            "-" + str(dag_run.conf["project_items"]["id"])):
                        projects_info.append(
                            {
                                "name": row["cells"][2]["textValue"],
                                "uri": row["cells"][0]["uri"],
                                "clientname": row["cells"][3]["textValue"] if row and row["cells"] and row["cells"][3] and row["cells"][3]["dataType"] != "urn:replicon:list-type:null" else null
                            }
                        )
            return projects_info

        get_matched_projects = rail.PythonOperator(
            task_id="get_matched_projects",
            python_callable=lambda dag_run: extract_project_info(dag_run)
        )

        if_log_project_uri_present = rail.IfOperator(
            task_id="if_log_project_uri_present",
            test=lambda: rail.result('get_matched_projects') and rail.result(
                'get_matched_projects')[0] and rail.result('get_matched_projects')[0]['uri'],
            yes_task="if_organization_name_present",
            no_task="create_project_19",
        )

        if_organization_name_present = rail.IfOperator(
            task_id="if_organization_name_present",
            test=lambda dag_run: dag_run.conf["project_items"]["organization_id"],
            yes_task="get_organizations_data",
            no_task="if_login_name_uri_present",
        )

        get_organizations_data = rail.ZendeskAPIOperator2(
            task_id="get_organizations_data",
            zendesk_conn_id="{{dag_run.conf.zendesk_conn_id}}",
            endpoint="/api/v2/organizations/"
            + "{{dag_run.conf.project_items.organization_id}}",
            request_method="GET",
            pagination=False,
        )

        if_organization_id_not_equals_clientname = rail.IfOperator(
            task_id="if_organization_id_not_equals_clientname",
            test=lambda: (
                rail.result("get_organizations_data")["organization"]["name"]
                != (rail.result("get_matched_projects") and rail.result("get_matched_projects")[0] and rail.result("get_matched_projects")[0]["clientname"])
            )
            and rail.result("search_clients") and rail.result("search_clients")[0] and rail.result("search_clients")[0]["cells"][1]["uri"],
            yes_task="update_project_client",
            no_task="if_login_name_uri_present",
        )

        update_project_client = rail.RepliconServiceOperator(
            task_id="update_project_client",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data=lambda: {
                "projectUri": rail.result('get_matched_projects')[0]['uri'],
                "clientUri": rail.result("search_clients")[0]["cells"][1]["uri"],
                "optionUri": "Keep existing billing rates and Expense codes",
            },
        )

        if_login_name_uri_present = rail.IfOperator(
            task_id="if_login_name_uri_present",
            test=lambda: (
                True
                if rail.result("search_users") and rail.result("search_users")["rows"]
                else False
            ),
            yes_task="assign_user_to_project",
            no_task="if_status_equals_to_solved",
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id="assign_user_to_project",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            data=lambda dag_run: {
                "projectUri": rail.result("get_matched_projects")[0]["uri"],
                "resourceUri": rail.result("search_users")["rows"][0]["cells"][1][
                    "uri"],
                "resourceToReplaceUri": null,
            },
        )

        if_status_equals_to_solved = rail.IfOperator(
            task_id="if_status_equals_to_solved",
            test=lambda dag_run: dag_run.conf["project_items"]["status"] == "solved",
            yes_task="update_project_status",
            no_task="catch_project_error",
        )

        update_project_status = rail.RepliconServiceOperator(
            task_id="update_project_status",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data=lambda: {
                "projectUri": rail.result('get_matched_projects')[0]['uri'],
                "projectStatusUri": "urn:replicon:project-status-type:completed",
            },
        )

        def get_client_details():
            if rail.result("search_clients"):
                return {
                    "clients": [
                        {
                            "client": {
                                "uri": rail.result("search_clients")[0]["cells"][1][
                                    "uri"
                                ],
                                "name": null,
                                "code": null,
                                "parameterCorrelationId": null,
                            },
                            "costAllocationPercentage": "100",
                        }
                    ],
                    "effectiveDate": null,
                }
            return null

        create_project_19 = rail.RepliconServiceOperator(
            task_id="create_project_19",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": (
                            str(dag_run.conf["project_items"]["subject"])
                            + "-"
                            + str(dag_run.conf["project_items"]["id"])
                        ),
                    },
                    "codeToApply": {"value": dag_run.conf["project_items"]["id"]},
                    "descriptionToApply": {
                        "value": (
                            dag_run.conf["project_items"]["description"]
                            if dag_run.conf["project_items"]["description"]
                            else None
                        )
                    },
                    "percentCompletedToApply": null,
                    "startDateToApply": {
                        "date": (
                            {
                                "year": dag_run.conf["project_items"]["created_at"][:4],
                                "month": dag_run.conf["project_items"]["created_at"][
                                    5:7
                                ],
                                "day": dag_run.conf["project_items"]["created_at"][
                                    8:10
                                ],
                            }
                            if dag_run.conf["project_items"]["created_at"]
                            else null
                        )
                    },
                    "endDateToApply": null,
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:time-and-material"
                    },
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": get_client_details(),
                    "statusToApply": {"name": "In Progress"},
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": null,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "budgetedHoursToApply": null,
                    "estimatedCostToApply": {
                        "value": {
                            "amount": "0",
                            "currency": {"uri": null, "name": null, "symbol": "$"},
                        }
                    },
                    "budgetedCostToApply": null,
                    "expenseBudgetedCostToApply": null,
                    "totalEstimatedContractValueToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                    },
                    "billingContractToApply": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "resourceProjectAssignmentModifications": null,
                    "billingContractModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [],
                    "portfolioToApply": null,
                    "locationToApply": null,
                    "divisionToApply": null,
                    "serviceCenterToApply": null,
                    "costCenterToApply": null,
                    "departmentGroupToApply": null,
                    "employeeTypeGroupToApply": null,
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4()),
            },
        )

        is_psa_permission_present = rail.IfOperator(
            task_id="is_psa_permission_present",
            test="{{ dag_run.conf.is_polaris_permissions_present | is_truthy }}",
            yes_task="update_project_type",
            no_task="if_login_name_uri_present_20",
        )

        update_project_type = rail.RepliconServiceOperator(
            task_id="update_project_type",
            endpoint="/services/ProjectService1.svc/PutKeyValueForProject",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id }}",
            data={
                "projectUri": "{{ result('create_project_19').uri }}",
                "keyValue": {
                    "keyUri": "urn:replicon:project-key-value-key:project-management-type",
                    "value": {"uri": "urn:replicon:project-management-type:managed"},
                },
            },
        )

        if_login_name_uri_present_20 = rail.IfOperator(
            task_id="if_login_name_uri_present_20",
            test=lambda: True if rail.result("search_users")
                    and rail.result("search_users")["rows"] else False,
            yes_task="assign_user_to_project_21",
            no_task="if_status_equals_to_solved_22",
        )

        assign_user_to_project_21 = rail.RepliconServiceOperator(
            task_id="assign_user_to_project_21",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            data=lambda: {
                "projectUri": rail.result("create_project_19")["uri"],
                "resourceUri": (
                    rail.result("search_users")["rows"][0]["cells"][1]["uri"]
                    if rail.result("search_users")
                    and rail.result("search_users")["rows"]
                    else null
                ),
                "resourceToReplaceUri": null,
            },
        )

        if_status_equals_to_solved_22 = rail.IfOperator(
            task_id="if_status_equals_to_solved_22",
            test=lambda dag_run: dag_run.conf["project_items"]["status"] == "solved",
            yes_task="update_project_status_23",
            no_task="catch_project_error",
        )

        update_project_status_23 = rail.RepliconServiceOperator(
            task_id="update_project_status_23",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data=lambda: {
                "projectUri": rail.result("create_project_19")["uri"],
                "projectStatusUri": "urn:replicon:project-status-type:completed",
            },
        )

        def get_downstreamtasks_errors(project_name, error_message):
            return {"error": f"Error with {project_name} - {error_message}"}

        catch_project_error = rail.PythonOperator(
            task_id="catch_project_error",
            trigger_rule="one_failed",
            python_callable=get_downstreamtasks_errors,
            op_args=[
                "{{ dag_run.conf.project_items.subject }}",
                "{{ get_error_message() }}",
            ],
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_project_error
        (
            can_run_batch_task
            >> rail.Label("No")
            >> if_get_organization_by_id_name_present
        )

        (
            if_get_organization_by_id_name_present
            >> rail.Label("Yes")
            >> search_clients >> if_assignee_id_present >> rail.Label(
                "Yes") >> get_users >> search_users >> search_projects
        )

        (
            if_assignee_id_present >> rail.Label('No') >> search_projects
        )
        (
            if_get_organization_by_id_name_present
            >> rail.Label("No")
            >> if_assignee_id_present
        )
        search_projects >> get_matched_projects
        get_matched_projects >> if_log_project_uri_present
        if_log_project_uri_present >> rail.Label(
            "Yes") >> if_organization_name_present
        (
            if_organization_name_present
            >> rail.Label("Yes")
            >> get_organizations_data
            >> if_organization_id_not_equals_clientname
        )
        if_organization_name_present >> rail.Label(
            "No") >> if_login_name_uri_present
        if_log_project_uri_present >> rail.Label(
            "No") >> create_project_19 >> is_psa_permission_present

        (
            is_psa_permission_present
            >> rail.Label("Yes")
            >> update_project_type
            >> if_login_name_uri_present_20
        )
        is_psa_permission_present >> rail.Label(
            "No") >> if_login_name_uri_present_20
        (
            if_organization_id_not_equals_clientname
            >> rail.Label("Yes")
            >> update_project_client
            >> if_login_name_uri_present
        )
        (
            if_organization_id_not_equals_clientname
            >> rail.Label("No")
            >> if_login_name_uri_present
        )
        (
            if_login_name_uri_present
            >> rail.Label("Yes")
            >> assign_user_to_project
            >> if_status_equals_to_solved
        )
        if_login_name_uri_present >> rail.Label(
            "No") >> if_status_equals_to_solved
        (
            if_status_equals_to_solved
            >> rail.Label("Yes")
            >> update_project_status
            >> catch_project_error
        )
        if_status_equals_to_solved >> rail.Label("No") >> catch_project_error
        (
            if_login_name_uri_present_20
            >> rail.Label("Yes")
            >> assign_user_to_project_21
            >> if_status_equals_to_solved_22
        )
        (
            if_login_name_uri_present_20
            >> rail.Label("No")
            >> if_status_equals_to_solved_22
        )
        (
            if_status_equals_to_solved_22
            >> rail.Label("Yes")
            >> update_project_status_23
            >> catch_project_error
        )
        if_status_equals_to_solved_22 >> rail.Label(
            "No") >> catch_project_error

        return dag


rail.for_each_instance(create_main_dag)
