import json
import rail
from rosterfy.hubspot_polaris_psa_integration.utils import python_callable, request_payload

def client_process(config):
    with rail.TaskGroup(group_id='process_client', prefix_group_id=False) as process_client:

        if_company_present_in_hubspot = rail.IfOperator(
            task_id="if_company_present_in_hubspot",
            test=lambda : bool((json.loads(rail.result('get_details_of_deal')).get('associations')) and (
                json.loads(rail.result('get_details_of_deal'))['associations'].get('companies'))),
            yes_task="if_client_exists",
            no_task="process_client_end"
        )

        if_client_exists = rail.IfOperator(
            task_id="if_client_exists",
            test="{{ result('get_existing_client_data_based_on_code') | is_truthy }}",
            yes_task="process_client_end",
            no_task="get_country_uri"
        )

        get_country_uri = rail.RepliconServiceOperator(
            task_id='get_country_uri',
            endpoint="/services/InternationalizationService1.svc/GetAllCountries"
        )

        if_client_name_present = rail.IfOperator(
            task_id="if_client_name_present",
            test=lambda : bool(json.loads(rail.result('get_details_of_company_from_hubspot'))['properties']['name']),
            yes_task="if_contacts_available",
            no_task="log_company_name_absent_in_hubspot"
        )

        log_company_name_absent_in_hubspot = rail.PythonOperator(
            task_id = "log_company_name_absent_in_hubspot",
            python_callable=lambda : "Name for Company ID-" + json.loads(rail.result('get_details_of_company_from_hubspot'))['id'] + " not present in Hubspot"
        )

        if_contacts_available = rail.IfOperator(
            task_id="if_contacts_available",
            test=lambda : bool(rail.result('get_required_id_from_company').get('contact_id')),
            yes_task="get_primary_contact_name_from_hubspot",
            no_task="add_client"
        )

        get_primary_contact_name_from_hubspot = rail.SimpleHttpOperator(
            task_id='get_primary_contact_name_from_hubspot',
            method='GET',
            endpoint=config.contact_endpoint + "{{ result('get_required_id_from_company').contact_id }}",
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data={
                "properties": ["firstname","lastname"]
            },
            extra_options={
                'verify': False
            },
        )

        add_client = rail.RepliconServiceOperator(
            task_id='add_client',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data = request_payload.add_client_payload
        )

        if_company_owner_available = rail.IfOperator(
            task_id="if_company_owner_available",
            test=lambda : bool(rail.result('get_required_id_from_company').get('owner_id')),
            yes_task="get_company_owner_details_from_hubspot",
            no_task="process_client_end"
        )

        get_company_owner_details_from_hubspot = rail.SimpleHttpOperator(
            task_id='get_company_owner_details_from_hubspot',
            method='GET',
            endpoint=config.owner_endpoint + "{{ result('get_required_id_from_company').owner_id }}",
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            extra_options={
                'verify': False
            },
        )

        search_owner_present_in_replicon = rail.RepliconServiceOperator(
            task_id='search_owner_present_in_replicon',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_existing_owner_data,
            data_handler=python_callable.get_existing_user_detail
        )

        if_companyowner_present_and_enabled = rail.IfOperator(
            task_id='if_companyowner_present_and_enabled',
            test=lambda: bool(rail.result('search_owner_present_in_replicon') and rail.result(
                'search_owner_present_in_replicon').get('uri') and (
                    rail.result('search_owner_present_in_replicon').get('enabled').lower() == 'true')),
            yes_task="get_assigned_permissionset_for_clientmanager",
            no_task="log_clientmanager_not_presentordisabled",
        )

        log_clientmanager_not_presentordisabled = rail.PythonOperator(
            task_id = "log_clientmanager_not_presentordisabled",
            python_callable=lambda : "Client Manager to be assigned is not present or is disabled"
        )

        get_assigned_permissionset_for_clientmanager = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_for_clientmanager',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_owner_present_in_replicon')['uri'] }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'permissionSet.name', 'Client Manager', 'permissionSet.uri', '')
        )

        if_clientmanager_permission_present = rail.IfOperator(
            task_id='if_clientmanager_permission_present',
            test='''{{ result('get_assigned_permissionset_for_clientmanager') | is_truthy }}''',
            yes_task="assign_clientmanager",
            no_task="log_client_manager_permission_not_available",
        )

        log_client_manager_permission_not_available = rail.PythonOperator(
            task_id = "log_client_manager_permission_not_available",
            python_callable=lambda : "Solution Consultant does not have Project Manager Permission"
        )

        assign_clientmanager = rail.RepliconServiceOperator(
            task_id='assign_clientmanager',
            endpoint="/services/ClientService1.svc/UpdateClientManager",
            data = {
                "clientUri": "{{ result('add_client').uri }}",
                "clientManagerUri":"{{ result('search_owner_present_in_replicon').uri }}"
            }
        )

        process_client_end = rail.EmptyOperator(
            task_id = "process_client_end"
        )

        if_company_present_in_hubspot >> rail.Label('Yes') >> if_client_exists
        if_company_present_in_hubspot >> rail.Label('No') >> process_client_end

        if_client_exists >> rail.Label('Yes') >> process_client_end
        if_client_exists >> rail.Label('No') >> get_country_uri >> if_client_name_present

        if_client_name_present >> rail.Label('Yes') >> if_contacts_available
        if_client_name_present >> rail.Label('No') >> log_company_name_absent_in_hubspot >> process_client_end

        if_contacts_available >> rail.Label('Yes') >> get_primary_contact_name_from_hubspot >> add_client
        if_contacts_available >> rail.Label('No') >> add_client

        add_client >> if_company_owner_available

        if_company_owner_available >> rail.Label('Yes') >> get_company_owner_details_from_hubspot >> search_owner_present_in_replicon >> \
        if_companyowner_present_and_enabled
        if_company_owner_available >> rail.Label('No') >> process_client_end

        if_companyowner_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_for_clientmanager >> if_clientmanager_permission_present
        if_companyowner_present_and_enabled >> rail.Label('No') >> log_clientmanager_not_presentordisabled >> process_client_end

        if_clientmanager_permission_present >> rail.Label('Yes') >> assign_clientmanager >> process_client_end
        if_clientmanager_permission_present >> rail.Label('No') >> log_client_manager_permission_not_available >> process_client_end

        return process_client
