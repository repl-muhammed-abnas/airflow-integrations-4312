import rail
from datetime import timedelta
from airflow.models import Variable
import uuid

null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_zendesk_connector_{config.region.replace('-', '_')}_create_updated_client_import_child{config.instance}",
        description=f"Zendesk Online {config.region} Create/Update Client child {config.instance}",
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
            no_task="search_clients_with_given_name_in_replicon",
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="search_clients_with_given_name_in_replicon",
            end_task="catch_client_error",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        def get_matching_client(response, dag_run):
            matching_client = {}
            for client in response["rows"]:
                if (
                    client["cells"][0]["textValue"]
                    == dag_run.conf["client_items"]["name"]
                ):
                    matching_client = client
                    break
            return matching_client

        search_clients_with_given_name_in_replicon = rail.RepliconServiceOperator(
            task_id="search_clients_with_given_name_in_replicon",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:client-list-column:name",
                    "urn:replicon:client-list-column:client",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:client-list-filter:name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {"text": dag_run.conf["client_items"]["name"]}
                    },
                },
            },
            data_handler=get_matching_client,
        )

        if_get_client_details_name_blank = rail.IfOperator(
            task_id="if_get_client_details_name_blank",
            test=lambda: (
                not rail.result("search_clients_with_given_name_in_replicon")
            ),
            yes_task="create_client",
            no_task="update_client",
        )

        create_client = rail.RepliconServiceOperator(
            task_id="create_client",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "nameToApply": {"value": dag_run.conf["client_items"]["name"]},
                    "codeToApply": {"value": dag_run.conf["client_items"]["id"]},
                    "descriptionToApply": {
                        "value": dag_run.conf["client_items"]["details"]
                    },
                    "statusToApply": "true",
                    "clientContactToApply": null,
                    "clientAddressToApply": {
                        "address": null,
                        "city": null,
                        "stateProvince": null,
                        "country": null,
                        "zipPostalCode": null,
                        "phoneNumber": null,
                        "faxNumber": null,
                        "email": {
                            "value": dag_run.conf["client_items"]["domain_names"][0]
                        },
                        "website": null,
                    },
                    "billingAddressToApply": null,
                    "billingRatesToApply": null,
                    "clientManagerToApply": null,
                    "clientSharingToApply": null,
                    "expenseCodesToApply": null,
                    "customFieldsToApply": [],
                    "taxProfileToApply": null,
                },
                "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4()),
            },
        )

        update_client = rail.RepliconServiceOperator(
            task_id="update_client",
            replicon_conn_id="{{ dag_run.conf.replicon_conn_id}}",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": rail.result("search_clients_with_given_name_in_replicon")[
                        "cells"
                    ][1]["uri"],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null,
                },
                "modifications": {
                    "nameToApply": null,
                    "codeToApply": {"value": dag_run.conf["client_items"]["id"]},
                    "descriptionToApply": {
                        "value": dag_run.conf["client_items"]["details"]
                    },
                    "statusToApply": "true",
                    "clientContactToApply": null,
                    "clientAddressToApply": {
                        "address": null,
                        "city": null,
                        "stateProvince": null,
                        "country": null,
                        "zipPostalCode": null,
                        "phoneNumber": null,
                        "faxNumber": null,
                        "email": {"value": dag_run.conf["client_items"]["domain_names"][0]},
                        "website": null,
                    },
                    "billingAddressToApply": null,
                    "billingRatesToApply": null,
                    "clientManagerToApply": null,
                    "clientSharingToApply": null,
                    "expenseCodesToApply": null,
                    "customFieldsToApply": [],
                    "taxProfileToApply": null,
                },
                "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4()),
            },
        )

        def get_downstreamtasks_error(client_name, error_message):
            return {"error": f"Error with {client_name} - {error_message}"}

        catch_client_error = rail.PythonOperator(
            task_id="catch_client_error",
            trigger_rule="one_failed",
            python_callable=get_downstreamtasks_error,
            op_args=["{{ dag_run.conf.client_items.name }}",
                     "{{ get_error_message() }}"],
        )

        (
            can_run_batch_task
            >> rail.Label("Yes")
            >> batch_task
            >> catch_client_error
        )
        can_run_batch_task >> rail.Label(
            "No") >> search_clients_with_given_name_in_replicon
        search_clients_with_given_name_in_replicon >> if_get_client_details_name_blank
        (
            if_get_client_details_name_blank
            >> rail.Label("Yes")
            >> create_client
            >> catch_client_error
        )
        (
            if_get_client_details_name_blank
            >> rail.Label("No")
            >> update_client
            >> catch_client_error
        )

        return dag


rail.for_each_instance(create_main_dag)
