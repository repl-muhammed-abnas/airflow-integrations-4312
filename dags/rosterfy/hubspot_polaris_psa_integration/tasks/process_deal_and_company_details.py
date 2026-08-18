import json
import rail
from rosterfy.hubspot_polaris_psa_integration.utils import python_callable, request_payload

def get_details_of_deals_and_company(config):
    with rail.TaskGroup(group_id='deals_and_company_details', prefix_group_id=False) as deals_and_company_details:

        logger = rail.CreateLogOperator(
            task_id = "logger"
        )

        get_details_of_deal = rail.SimpleHttpOperator(
            task_id='get_details_of_deal',
            method='GET',
            endpoint=config.deals_endpoint + "{{ result('get_project_data').objectId }}",
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data={
                "properties": ["dealname","pipeline","dealstage","arr","closedate","arr_in_company_currency",
                               "industry","contract___rosterfy_entity","non_arr__local_currency_","currency_"] if config.instance == 'trial' else
                               ["dealname","pipeline","dealstage","arr","closedate","arr_in_company_currency",
                               "industry","contract___rosterfy_entity","non_arr","deal_currency_code"],
                "associations": "companies"
            },
            extra_options={
                'verify': False
            },
        )

        if_company_exists_for_deal_in_hubspot = rail.IfOperator(
            task_id="if_company_exists_for_deal_in_hubspot",
            test=lambda : bool((json.loads(rail.result('get_details_of_deal')).get('associations')) and (
                json.loads(rail.result('get_details_of_deal'))['associations'].get('companies'))),
            yes_task="get_company_id",
            no_task="get_pipeline_details"
        )

        get_company_id = rail.PythonOperator(
            task_id = "get_company_id",
            python_callable=lambda: json.loads(rail.result('get_details_of_deal'))['associations']['companies']['results'][0]['id']
        )

        get_details_of_company_from_hubspot = rail.SimpleHttpOperator(
            task_id='get_details_of_company_from_hubspot',
            method='GET',
            endpoint=config.companies_endpoint + "{{ result('get_company_id') }}",
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data={
                "properties": ["name","description","city","phone","state","industry","hubspot_owner_id","contact","hs_contact_id",
                               "website","address","address2","country","zip","division","solutions_consultant"],
                "associations": "contact"
            },
            extra_options={
                'verify': False
            },
        )

        get_required_id_from_company = rail.PythonOperator(
            task_id = "get_required_id_from_company",
            python_callable=python_callable.get_required_id_from_company
        )

        get_existing_client_data_based_on_code = rail.RepliconServiceOperator(
            task_id='get_existing_client_data_based_on_code',
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.get_existing_client_data,
            response_filter=python_callable.check_client_data
        )

        get_pipeline_details = rail.SimpleHttpOperator(
            task_id='get_pipeline_details',
            method='GET',
            endpoint=config.pipeline_endpoint,
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            extra_options={
                'verify': False
            },
        )

        get_pipeline_and_dealstage_name = rail.PythonOperator(
            task_id = "get_pipeline_and_dealstage_name",
            python_callable=python_callable.get_pipeline_dealstage_name
        )

        logger >> get_details_of_deal >> if_company_exists_for_deal_in_hubspot

        if_company_exists_for_deal_in_hubspot >> rail.Label('Yes') >> get_company_id >> get_details_of_company_from_hubspot >> \
            get_required_id_from_company >> get_existing_client_data_based_on_code >> get_pipeline_details
        if_company_exists_for_deal_in_hubspot >> rail.Label('No') >> get_pipeline_details

        get_pipeline_details >> get_pipeline_and_dealstage_name

        return deals_and_company_details
