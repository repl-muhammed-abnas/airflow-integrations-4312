import json
import rail
from siemens.project_import.utils import custom_methods, request_methods

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_dagid,
        description=f"Siemens Portugal Project Import v1 child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_log = rail.CreateLogOperator(task_id="create_log")

        # Validate required project fields
        if_project_name_and_code = rail.IfOperator(
            task_id="if_project_name_and_code",
            test=lambda dag_run: dag_run.conf.get("name")
            and dag_run.conf.get("projectcode"),
            yes_task="get_client_uri",
            no_task="log_invalid_data",
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id="log_invalid_data",
            log='{{result("create_log")}}',
            message="Invalid project record found",
            severity="Exception",
            properties=lambda dag_run: {
                "projectname": dag_run.conf.get("name", "N/A"),
                "projectcode": dag_run.conf.get("projectcode", "N/A"),
                "status": "Exception",
                "details": "Missing required fields: projectcode or name",
            },
        )

        # Client lookup and creation
        get_client_uri = rail.RepliconServiceOperator(
            task_id="get_client_uri",
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "10",
                "columnUris": ["urn:replicon:client-list-column:client"],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:client-list-filter:name",
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {"text": dag_run.conf.get("client", "")},
                        "filterDefinitionUri": null,
                    },
                    "value": null,
                    "filterDefinitionUri": null,
                },
            },
            data_handler=lambda response, dag_run: (
                list(
                    filter(
                        lambda i: i["name"] == dag_run.conf["client"],
                        map(
                            lambda i: {
                                "name": i["cells"][0]["textValue"],
                                "uri": i["cells"][0]["uri"],
                            },
                            response["rows"],
                        ),
                    )
                )
                if dag_run.conf["client"] and response and response["rows"]
                else null
            ),
        )

        if_client_in_replicon = rail.IfOperator(
            task_id="if_client_in_replicon",
            test=lambda dag_run: (rail.result("get_client_uri") or not dag_run.conf["client"]),
            yes_task="get_project_manager",
            no_task="create_client_in_replicon",
        )

        create_client_in_replicon = rail.RepliconServiceOperator(
            task_id="create_client_in_replicon",
            endpoint="/services/ClientService1.svc/PutClient",
            data=lambda dag_run: {
                "client": {
                    "target": {
                    "uri": null,
                    "name": dag_run.conf["client"],
                    "code": null,
                    "parameterCorrelationId": null
                    },
                    "name":dag_run.conf["client"],
                    "code": null,
                    "comment": null,
                    "clientManager": null,
                    "billingContact": null,
                    "clientAddress": null,
                    "billingAddress": null,
                    "isActive": "true",
                    "customFieldValues": [],
                    "billingRates": [],
                    "expenseCodesAllowedByDefaultOnNewProjects": [],
                    "defaultBillingCurrency": null
                }
            }
        )

        # Project manager lookup
        get_project_manager = rail.RepliconServiceOperator(
            task_id="get_project_manager",
            endpoint="/services/UserlistService1.svc/GetData",
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100",
                "columnUris": ["urn:replicon:user-list-column:user"],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text",
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {"text": dag_run.conf.get("projectmanager", "")},
                    },
                },
            },
            data_handler=custom_methods.get_project_manager_data_handler
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectdetails3",
            data=lambda dag_run: {
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": dag_run.conf["projectcode"],
                        "parameterCorrelationId": null,
                    }
                ]
            },
            data_handler=custom_methods.parse_project_response,
        )

        if_project_exists = rail.IfOperator(
            task_id="if_project_exists",
            test=lambda: rail.result("get_project_details")
            and rail.result("get_project_details").get("uri"),
            no_task="create_project_in_replicon",
            yes_task="if_any_project_updates",
        )

        create_project_in_replicon = rail.RepliconServiceOperator(
            task_id="create_project_in_replicon",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.create_project(dag_run)
        )

        put_task = rail.RepliconServiceCallForEachItemOperator(
            task_id="put_task",
            items="{{dag_run.conf.task_list|to_json}}",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=lambda item: request_methods.put_task(item),
        )

        if_any_project_updates = rail.IfOperator(
            task_id="if_any_project_updates",
            test=lambda dag_run: custom_methods.check_for_project_updates(dag_run),
            yes_task="update_project_custom_details2",
            no_task="get_project_task_details",
        )

        update_project_custom_details2 = rail.RepliconServiceOperator(
            task_id="update_project_custom_details2",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_methods.get_custom_field_update_request(
                dag_run
            )[0],
        )

        get_project_task_details = rail.RepliconServiceOperator(
            task_id="get_project_task_details",
            endpoint="/services/TaskService1.svc/GetDescendantTaskdetails",
            data={"parentUri": '{{result("get_project_details").uri}}'},
            data_handler=lambda response: list(
                map(lambda i: i["task"]["name"], response)
            ),
        )

        if_any_task_updates = rail.IfOperator(
            task_id="if_any_task_updates",
            test=lambda dag_run: custom_methods.check_for_task_updates(dag_run),
            yes_task="put_task2",
            no_task="write_project_update_success",
        )

        put_task2 = rail.RepliconServiceCallForEachItemOperator(
            task_id="put_task2",
            items=lambda dag_run: custom_methods.check_for_task_updates(dag_run),
            endpoint="/services/ProjectService1.svc/PutTask",
            data=lambda item: request_methods.put_task(item),
        )

        write_project_update_success = rail.WriteLogOperator(
            task_id="write_project_update_success",
            log='{{result("create_log")}}',
            message="Project Created Successfully",
            properties=lambda dag_run: {
                "projectname": dag_run.conf["name"],
                "projectcode": dag_run.conf["projectcode"],
                "status": (
                    "Exception" if not rail.result("get_project_manager") else "Success"
                ),
                "details": request_methods.get_custom_field_update_request(dag_run)[1]
                + "Updated",
            },
        )

        write_project_success = rail.WriteLogOperator(
            task_id="write_project_success",
            log='{{result("create_log")}}',
            message="Project Created Successfully",
            properties=lambda dag_run: {
                "projectname": dag_run.conf["name"],
                "projectcode": dag_run.conf["projectcode"],
                "status": (
                    "Exception" if not rail.result("get_project_manager") else "Success"
                ),
                "details": "Project Processed Successfully",
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{result("create_log")}}',
            trigger_rule="one_failed",
            message="Project is not processed",
            severity="Error",
            properties=lambda dag_run: {
                "projectname": dag_run.conf["name"],
                "projectcode": dag_run.conf["projectcode"],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}"),
            },
        )

        # Task dependencies with validation and client/manager lookup
        create_log >> if_project_name_and_code >> rail.Label("No") >> log_invalid_data
        (
            if_project_name_and_code
            >> rail.Label("Yes")
            >> get_client_uri
            >> if_client_in_replicon
            >> rail.Label("Yes")
            >> get_project_manager
            >> get_project_details
        )
        (
            if_client_in_replicon
            >> rail.Label("No")
            >> create_client_in_replicon
            >> get_project_manager
            >> get_project_details
        )

        # Existing project path
        (
            get_project_details
            >> if_project_exists
            >> rail.Label("Yes")
            >> if_any_project_updates
            >> rail.Label("Yes")
            >> update_project_custom_details2
            >> get_project_task_details
            >> if_any_task_updates
            >> rail.Label("Yes")
            >> put_task2
            >> write_project_update_success
        )
        (
            if_any_project_updates
            >> rail.Label("No")
            >> get_project_task_details
            >> if_any_task_updates
        )
        if_any_task_updates >> rail.Label("No") >> write_project_update_success
        write_project_update_success >> catch_and_log_errors

        # New project path
        (
            if_project_exists
            >> rail.Label("No")
            >> create_project_in_replicon >> put_task
            >> write_project_success
        )
        write_project_success >> catch_and_log_errors
        log_invalid_data >> catch_and_log_errors
        return dag


rail.for_each_instance(create_child_dag)
