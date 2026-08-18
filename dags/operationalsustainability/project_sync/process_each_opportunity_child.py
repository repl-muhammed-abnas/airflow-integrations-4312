
import rail

from operationalsustainability.project_sync.utils import request_payload, response_handler, custom_methods
from airflow.models import Variable


null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_opprtunity_child_dag_id,
        description="Process each opportunity from Salesforce and create projects in Replicon",
        company_key=config.company_key,
        max_active_runs=config.max_active_child_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        def salesforce_trigger_data(dag_run):
            item = dag_run.conf.get('item', {})
            response = {
                "Id": item.get('Id'),
                "Name": item.get('Name'),
                "Type": item.get('Type'),
                "StageName": item.get('StageName'),
                "Probability": item.get('Probability'),
                "AccountId": item.get('AccountId'),
                "OwnerId": item.get('OwnerId'),
                "CloseDate": item.get('CloseDate'),
                "Description": item.get('Description'),
                "Amount": item.get('Amount'),
                "Billing_Type__c": item.get('Billing_Type__c'),
                "Amount_of_Amended_Contract__c": item.get('Amount_of_Amended_Contract__c'),
                "Amount_of_Traditional_License__c": item.get('Amount_of_Traditional_License__c'),
                "Annual_Optional_Maintenance_Fee__c": item.get('Annual_Optional_Maintenance_Fee__c'),
                "Annual_Escalation_Fee_Percent__c": item.get('Annual_Escalation_Fee_Percent__c'),
                "Annual_Subscription_Amount__c": item.get('Annual_Subscription_Amount__c'),
                "Auto_Renew_Unless_Notified_Days__c": item.get('Auto_Renew_Unless_Notified_Days__c'),
                "Contract__c": item.get('Contract__c'),
                "Date_Contract_Amended__c": item.get('Date_Contract_Amended__c'),
                "Does_Subscription_Auto_Renew__c": item.get('Does_Subscription_Auto_Renew__c'),
                "How_Many_Subcontractor_Hours__c": item.get('How_Many_Subcontractor_Hours__c'),
                "Is_Any_International_Work_Required__c": item.get('Is_Any_International_Work_Required__c'),
                "License_Fee_Contract_End_Date__c": item.get('License_Fee_Contract_End_Date__c'),
                "MSA_Terminate_Deadline_Before_AutoRenew__c": item.get('MSA_Terminate_Deadline_Before_AutoRenew__c'),
                "OS_Project_Manager__c": item.get('OS_Project_Manager__c'),
                "Percent_Markup_on_Expenses__c": item.get('Percent_Markup_on_Expenses__c'),
                "PO__c": item.get('PO__c'),
                "SaaS_Contract_End_Date__c": item.get('SaaS_Contract_End_Date__c'),
                "Signed_Subscription_Date__c": item.get('Signed_Subscription_Date__c')
            }
            return response


        get_salesforce_trigger_data = rail.PythonOperator(
            task_id='get_salesforce_trigger_data',
            python_callable=lambda dag_run: salesforce_trigger_data(dag_run)
        )

        get_list_of_related_opportunity_products_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_list_of_related_opportunity_products_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query='''SELECT FIELDS(ALL)
            FROM OpportunityLineItem
            WHERE OpportunityId = '{{ result("get_salesforce_trigger_data").Id }}'
            LIMIT 150 OFFSET 0'''
        )

        opportunity_product_name_is_present = rail.IfOperator(
            task_id="opportunity_product_name_is_present",
            test=lambda: (
                rail.result('get_list_of_related_opportunity_products_in_salesforce').get('records', [])
                and len(rail.result('get_list_of_related_opportunity_products_in_salesforce').get('records', [])) > 0
                and rail.result('get_list_of_related_opportunity_products_in_salesforce')['records'][0].get('Name')
            ),
            yes_task='extract_additional_suffix_of_the_opportunity_product_name',
            no_task='types_to_be_synced_doesnt_equal_all'
        )

        extract_additional_suffix_of_the_opportunity_product_name=rail.PythonOperator(
            task_id='extract_additional_suffix_of_the_opportunity_product_name',
            python_callable=lambda: custom_methods.process_product_name(rail.result('get_list_of_related_opportunity_products_in_salesforce')['records'][0]['Name'],
                                                         rail.result('get_salesforce_trigger_data')['Name'])
        )

        types_to_be_synced_doesnt_equal_all=rail.IfOperator(
            task_id="types_to_be_synced_doesnt_equal_all",
            test=lambda: Variable.get(config.types_to_be_synced) != "ALL",
            yes_task='opportunity_type_is_present',
            no_task='opportunity_type_is_not_present'
        )

        opportunity_type_is_present=rail.IfOperator(
            task_id="opportunity_type_is_present",
            test=lambda: rail.result(
                'get_salesforce_trigger_data')['Type'],
            yes_task='types_to_be_synced_doesnt_contain_opportunity_type',
            no_task='opportunity_type_is_not_present'
        )

        types_to_be_synced_doesnt_contain_opportunity_type=rail.IfOperator(
            task_id="types_to_be_synced_doesnt_contain_opportunity_type",
            test=lambda: rail.result(
                'get_salesforce_trigger_data')['Type'] not in Variable.get(config.types_to_be_synced),
            yes_task='no_data',
            no_task='opportunity_type_is_not_present'
        )

        opportunity_type_is_not_present=rail.IfOperator(
            task_id="opportunity_type_is_not_present",
            test=lambda: not rail.result(
                'get_salesforce_trigger_data')['Type'],
            yes_task='check_sync_opportunities_with_no_types',
            no_task='and_flow_should_stop'
        )

        # Step 10 in workato
        check_sync_opportunities_with_no_types=rail.IfOperator(
            task_id="check_sync_opportunities_with_no_types",
            test=lambda: (Variable.get(config.sync_opportunities_with_no_types, default_var='False').lower() == 'true') is not True,
            yes_task='no_data',
            no_task='and_flow_should_stop'
        )

        # AND flow: STOP when operation == AND AND either:
        #   - stages is not ALL AND stage not in configured stages, OR
        #   - probability below threshold
        # If operation != AND or stop conditions not met, proceed to operation_equals_or.
        # This collapses the original 4-task AND flow fan-in into a single check,
        # necessary because Airflow's SkipMixin cannot skip a task that has multiple
        # upstream branch-operator parents — the multi-parent convergence always left
        # operation_equals_or in the "followed" set even when it should have been skipped.
        and_flow_should_stop=rail.IfOperator(
            task_id="and_flow_should_stop",
            test=lambda: (
                Variable.get(config.operation) == 'AND' and (
                    (Variable.get(config.stages_to_be_synced) != 'ALL' and
                     rail.result('get_salesforce_trigger_data')['StageName'] not in Variable.get(config.stages_to_be_synced))
                    or
                    rail.result('get_salesforce_trigger_data')['Probability'] < int(Variable.get(config.probability))
                )
            ),
            yes_task='no_data',
            no_task='operation_equals_or'
        )

        operation_equals_or=rail.IfOperator(
            task_id="operation_equals_or",
            test=lambda: Variable.get(config.operation) == 'OR',
            yes_task='or_flow_should_stop',
            no_task='search_projects_in_replicon'
        )

        # OR flow: STOP (go to no_data) when ALL three conditions hold:
        #   - stage not in configured stages (covers both specific-list and ALL cases,
        #     since a real StageName is never literally contained in the string 'ALL')
        #   - probability below threshold
        #   - opportunity ID is present
        # If any condition fails, proceed to search_projects_in_replicon.
        # This collapses the original 7-task fan-in into a single check, which is
        # necessary because Airflow's SkipMixin cannot skip a task that has multiple
        # upstream branch-operator parents — the multi-parent convergence always left
        # search_projects_in_replicon in the "followed" set even on the STOP path.
        or_flow_should_stop=rail.IfOperator(
            task_id="or_flow_should_stop",
            test=lambda: (
                rail.result('get_salesforce_trigger_data')['StageName'] not in Variable.get(config.stages_to_be_synced)
                and rail.result('get_salesforce_trigger_data')['Probability'] < int(Variable.get(config.probability))
                and rail.result('get_salesforce_trigger_data')['Id']
            ),
            yes_task='no_data',
            no_task='search_projects_in_replicon'
        )

        search_projects_in_replicon=rail.RepliconServiceOperator(
            task_id='search_projects_in_replicon',
            endpoint="/services/ProjectService1.svc/BulkGetProjects2",
            data=lambda: request_payload.get_project_details_payload(rail.result(
                'get_salesforce_trigger_data')['Name'])
        )

        get_project_custom_fields=rail.RepliconServiceOperator(
            task_id='get_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:project'
            },
        )

        search_users_in_salesforce=rail.SalesforceQueryOperator2(
            task_id='search_users_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query='''SELECT FIELDS(ALL) FROM User
            WHERE Id = '{{ result("get_salesforce_trigger_data").OwnerId }}' LIMIT 150'''
        )

        account_id_is_present=rail.IfOperator(
            task_id="account_id_is_present",
            test=lambda: rail.result(
                'get_salesforce_trigger_data')['AccountId'],
            yes_task='get_details_of_specific_account',
            no_task='replicon_project_uri_present'
        )

        get_details_of_specific_account=rail.SalesforceQueryOperator2(
            task_id='get_details_of_specific_account',
            salesforce_conn_id=config.salesforce_conn_id,
            query='''SELECT FIELDS(ALL) FROM Account
            WHERE Id = '{{ result("get_salesforce_trigger_data").AccountId }}' '''
        )

        
        replicon_project_uri_present=rail.IfOperator(
            task_id="replicon_project_uri_present",
            test=lambda: (
                rail.result('search_projects_in_replicon') 
                and isinstance(rail.result('search_projects_in_replicon'), list)
                and len(rail.result('search_projects_in_replicon')) > 0
            ),
            yes_task='to_be_updated',
            no_task='check_account_id_for_new_project'
        )

        to_be_updated=rail.IfOperator(
            task_id="to_be_updated",
            test=lambda: (Variable.get(config.to_update, default_var='False').lower() == 'true') is not True,
            yes_task='no_data',
            no_task='update_project_in_replicon'
        )


        update_project_in_replicon=rail.RepliconServiceOperator(
            task_id="update_project_in_replicon",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: request_payload.update_project_details_payload()
        )

        check_account_id_is_present_inside_loop=rail.IfOperator(
            task_id="check_account_id_is_present_inside_loop",
            test=lambda: rail.result(
                'get_salesforce_trigger_data')['AccountId'],
            yes_task='search_clients_in_replicon',
            no_task='search_users_in_replicon'
        )

        search_clients_in_replicon=rail.RepliconServiceOperator(
            task_id='search_clients_in_replicon',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:client-list-column:client"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: response_handler.get_required_client(response, rail.result(
                'get_details_of_specific_account')['records'][0]['Name'])
        )

        required_client_present=rail.IfOperator(
            task_id="required_client_present",
            test=lambda: rail.result('search_clients_in_replicon').get('client_name'),
            yes_task='update_project_client',
            no_task='search_users_in_replicon'
        )

        update_project_client=rail.RepliconServiceOperator(
            task_id='update_project_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data=lambda: {
                    "projectUri": rail.result('search_projects_in_replicon')[0]['uri'],
                    "clientUri": rail.result('search_clients_in_replicon')['client_uri'],
                    "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
            }
        )

        search_users_in_replicon=rail.RepliconServiceOperator(
            task_id='search_users_in_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: request_payload.search_users_in_replicon_payload(login_name=(rail.result('search_users_in_salesforce').get('records') or [{}])[0].get('Username', '')),
            data_handler=lambda response: response_handler.get_required_user(response)
        )

        replicon_user_uri_is_not_present=rail.IfOperator(
            task_id="replicon_user_uri_is_not_present",
            test=lambda: not rail.result(
                "search_users_in_replicon").get("user_uri"),
            yes_task="no_data",
            no_task="assign_comanager_to_project"
        )

        assign_comanager_to_project=rail.RepliconServiceOperator(
            task_id="assign_comanager_to_project",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": rail.result('search_projects_in_replicon')[0]['uri'],
                "sharedUris": [
                    rail.result('search_users_in_replicon')['user_uri']
                ]
            }
        )

        replicon_project_uri_present_inside_loop=rail.IfOperator(
            task_id="replicon_project_uri_present_inside_loop",
            test=lambda: rail.result(
                'search_projects_in_replicon')[0]['uri'],
            yes_task='no_data',
            no_task='check_account_id_after_update_loop'
        )


        # From replicon_project_uri_present (No) — new project
        check_account_id_for_new_project = rail.IfOperator(
            task_id="check_account_id_for_new_project",
            test=lambda: rail.result('get_salesforce_trigger_data')['AccountId'],
            yes_task='search_clients_in_replicon_when_account_id_is_present',
            no_task='billing_type_contains_time_and_material'
        )

        # From replicon_project_uri_present_inside_loop (No) — after update loop
        check_account_id_after_update_loop = rail.IfOperator(
            task_id="check_account_id_after_update_loop",
            test=lambda: rail.result('get_salesforce_trigger_data')['AccountId'],
            yes_task='search_clients_in_replicon_when_account_id_is_present',
            no_task='billing_type_contains_time_and_material'
        )

        search_clients_in_replicon_when_account_id_is_present=rail.RepliconServiceOperator(
            task_id='search_clients_in_replicon_when_account_id_is_present',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:client-list-column:client"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: response_handler.get_required_client(response,(rail.result('get_details_of_specific_account').get('records') or [{}])[0].get('Name', ''))
        )

        required_client_is_not_present=rail.IfOperator(
            task_id="required_client_is_not_present",
            test=lambda: not rail.result('search_clients_in_replicon_when_account_id_is_present').get('client_name'),
            yes_task='search_for_contacts_in_salesforce',
            no_task='billing_type_contains_time_and_material'
        )

        search_for_contacts_in_salesforce=rail.SalesforceQueryOperator2(
            task_id='search_for_contacts_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: f'''SELECT FIELDS(ALL)
                FROM Contact WHERE AccountId = '{(rail.result("get_details_of_specific_account").get("records") or [{}])[0].get("Id", "") if (rail.result("get_details_of_specific_account").get("records") or [{}]) else ""}'
                LIMIT 150'''
        )

        search_users_in_salesforce_when_account_id_is_present=rail.SalesforceQueryOperator2(
            task_id='search_users_in_salesforce_when_account_id_is_present',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: f'''SELECT FIELDS(ALL) FROM User
                WHERE Id = '{(rail.result("get_details_of_specific_account").get("records") or [{}])[0].get("OwnerId", "") if (rail.result("get_details_of_specific_account").get("records") or [{}]) else ""}'
                LIMIT 150'''
        )

        search_users_in_replicon_when_account_id_is_present=rail.RepliconServiceOperator(
            task_id='search_users_in_replicon_when_account_id_is_present',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: request_payload.search_users_in_replicon_payload(login_name=(rail.result('search_users_in_salesforce_when_account_id_is_present').get('records') or [{}])[0].get('Username', '')),
            data_handler=lambda response: response_handler.get_required_user(response)
        )

        get_all_countries_in_replicon=rail.RepliconServiceOperator(
            task_id='get_all_countries_in_replicon',
            endpoint='/services/InternationalizationService1.svc/GetAllCountries'
        )   

        create_client_in_replicon=rail.RepliconServiceOperator(
            task_id='create_client_in_replicon',
            endpoint='/services/ClientService1.svc/PutClient',
            data=lambda: request_payload.create_client_in_replicon_payload()
        )

        billing_type_contains_time_and_material=rail.IfOperator(
            task_id="billing_type_contains_time_and_material",
            test=lambda: 'Time_and_Material' in rail.result(
                'get_salesforce_trigger_data')['Billing_Type__c'] if rail.result(
                'get_salesforce_trigger_data')['Billing_Type__c'] else None,
            yes_task='create_project_in_replicon_when_billing_type_contains_time_and_material',
            no_task='billing_type_contains_fixed_bid'
        )

        create_project_in_replicon_when_billing_type_contains_time_and_material=rail.RepliconServiceOperator(
            task_id='create_project_in_replicon_when_billing_type_contains_time_and_material',
            endpoint='/services/ProjectService1.svc/PutProjectInfo2',
            data=lambda: request_payload.create_project_in_replicon_when_billing_type_contains_time_and_material_payload()
        )

        search_users_in_replicon_when_billing_type_contains_time_and_material=rail.RepliconServiceOperator(
            task_id='search_users_in_replicon_when_billing_type_contains_time_and_material',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: request_payload.search_users_in_replicon_payload(login_name=(rail.result('search_users_in_salesforce').get('records') or [{}])[0].get('Username', '')),
            data_handler=lambda response: response_handler.get_required_user(response)
        )

        user_is_not_present_when_billing_type_contains_time_and_material=rail.IfOperator(
            task_id="user_is_not_present_when_billing_type_contains_time_and_material",
            test=lambda: not rail.result(
                'search_users_in_replicon_when_billing_type_contains_time_and_material')['user_uri'],
            yes_task='no_data',
            no_task='assign_comanager_to_project_when_billing_type_contains_time_and_material'
        )

        assign_comanager_to_project_when_billing_type_contains_time_and_material=rail.RepliconServiceOperator(
            task_id="assign_comanager_to_project_when_billing_type_contains_time_and_material",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": rail.result('create_project_in_replicon_when_billing_type_contains_time_and_material')['uri'],
                "sharedUris": [
                    rail.result('search_users_in_replicon_when_billing_type_contains_time_and_material')['user_uri']
                ]
            }
        )

        billing_type_contains_fixed_bid=rail.IfOperator(
            task_id="billing_type_contains_fixed_bid",
            test=lambda: 'Fixed_Bid' in rail.result('get_salesforce_trigger_data')[
                'Billing_Type__c'] if rail.result('get_salesforce_trigger_data')[
                'Billing_Type__c'] else None,
            yes_task='create_project_in_replicon_when_billing_type_contains_fixed_bid',
            no_task='billing_type_contains_non_billable'
        )

        create_project_in_replicon_when_billing_type_contains_fixed_bid=rail.RepliconServiceOperator(
            task_id='create_project_in_replicon_when_billing_type_contains_fixed_bid',
            endpoint='/services/ProjectService1.svc/PutProjectInfo2',
            data=lambda: request_payload.create_project_in_replicon_when_billing_type_contains_fixed_bid_payload()
        )

        update_project_client_when_billing_type_contains_fixed_bid=rail.RepliconServiceOperator(
            task_id='update_project_client_when_billing_type_contains_fixed_bid',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data=lambda: {
                    "projectUri": rail.result('create_project_in_replicon_when_billing_type_contains_fixed_bid')['uri'],
                    "clientUri":  (rail.result('search_clients_in_replicon_when_account_id_is_present') or {}).get('client_uri') or
                        (rail.result('create_client_in_replicon') or {}).get('uri'),
                    "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
            }
        )

        update_project_fixed_bid_rate_when_billing_type_contains_fixed_bid=rail.RepliconServiceOperator(
            task_id='update_project_fixed_bid_rate_when_billing_type_contains_fixed_bid',
            endpoint='/services/FixedBidProjectService1.svc/UpdateProjectFixedBidRate',
            data=lambda: {
                "projectUri": rail.result('create_project_in_replicon_when_billing_type_contains_fixed_bid')['uri'],
                "rate": {
                    # amount is converted to string. Referred service call page.
                    "amount": str(rail.result('get_salesforce_trigger_data')['Amount']) if rail.result('get_salesforce_trigger_data')['Amount'] else "0",
                    # Currency -> $
                    "currencyUri": "urn:replicon-tenant:32763ebef136406d8187aca3304648cf:currency:1"
                },
                "projectFixedBidBillingFrequencyUri": "urn:replicon:fixed-bid-frequency:end-of-project"
            }
        )

        search_users_in_replicon_when_billing_type_contains_fixed_bid=rail.RepliconServiceOperator(
            task_id='search_users_in_replicon_when_billing_type_contains_fixed_bid',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: request_payload.search_users_in_replicon_payload(login_name=(rail.result('search_users_in_salesforce').get('records') or [{}])[0].get('Username', '')),
            data_handler=lambda response: response_handler.get_required_user(response)
        )

        user_is_not_present_when_billing_type_contains_fixed_bid=rail.IfOperator(
            task_id="user_is_not_present_when_billing_type_contains_fixed_bid",
            test=lambda: not (rail.result('search_users_in_replicon_when_billing_type_contains_fixed_bid') or {}).get('user_uri'),
            yes_task='no_data',
            no_task='assign_comanager_to_project_when_billing_type_contains_fixed_bid'
        )

        assign_comanager_to_project_when_billing_type_contains_fixed_bid=rail.RepliconServiceOperator(
            task_id="assign_comanager_to_project_when_billing_type_contains_fixed_bid",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": rail.result('create_project_in_replicon_when_billing_type_contains_fixed_bid')['uri'],
                "sharedUris": [
                    rail.result('search_users_in_replicon_when_billing_type_contains_fixed_bid')['user_uri']
                ]
            }
        )

        billing_type_contains_non_billable=rail.IfOperator(
            task_id="billing_type_contains_non_billable",
            test=lambda: 'Non-Billable' in rail.result(
                'get_salesforce_trigger_data')['Billing_Type__c'] if rail.result(
                'get_salesforce_trigger_data')['Billing_Type__c'] else None,
            yes_task='create_project_in_replicon_when_billing_type_contains_non_billable',
            no_task='no_data'
        )

        create_project_in_replicon_when_billing_type_contains_non_billable=rail.RepliconServiceOperator(
            task_id='create_project_in_replicon_when_billing_type_contains_non_billable',
            endpoint='/services/ProjectService1.svc/PutProjectInfo2',
            data=lambda: request_payload.create_project_in_replicon_when_billing_type_contains_non_billable_payload()
        )

        search_users_in_replicon_when_billing_type_contains_non_billable=rail.RepliconServiceOperator(
            task_id='search_users_in_replicon_when_billing_type_contains_non_billable',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: request_payload.search_users_in_replicon_payload(login_name=(rail.result('search_users_in_salesforce').get('records') or [{}])[0].get('Username', '')),
            data_handler=lambda response: response_handler.get_required_user(response)
        )

        user_is_not_present_when_billing_type_contains_non_billable=rail.IfOperator(
            task_id="user_is_not_present_when_billing_type_contains_non_billable",
            test=lambda: not rail.result(
                'search_users_in_replicon_when_billing_type_contains_non_billable')['user_uri'],
            yes_task='no_data',
            no_task='assign_comanager_to_project_when_billing_type_contains_non_billable'
        )

        assign_comanager_to_project_when_billing_type_contains_non_billable=rail.RepliconServiceOperator(
            task_id="assign_comanager_to_project_when_billing_type_contains_non_billable",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": rail.result('create_project_in_replicon_when_billing_type_contains_non_billable')['uri'],
                "sharedUris": [
                    rail.result('search_users_in_replicon_when_billing_type_contains_non_billable')['user_uri']
                ]
            }
        )

    get_salesforce_trigger_data >> get_list_of_related_opportunity_products_in_salesforce >> opportunity_product_name_is_present

    opportunity_product_name_is_present >> rail.Label(
        'Yes') >> extract_additional_suffix_of_the_opportunity_product_name >> types_to_be_synced_doesnt_equal_all
    opportunity_product_name_is_present >> rail.Label(
        'No') >> types_to_be_synced_doesnt_equal_all

    types_to_be_synced_doesnt_equal_all >> rail.Label(
        'Yes') >> opportunity_type_is_present
    types_to_be_synced_doesnt_equal_all >> rail.Label(
        'No') >> opportunity_type_is_not_present

    opportunity_type_is_present >> rail.Label(
        'Yes') >> types_to_be_synced_doesnt_contain_opportunity_type
    opportunity_type_is_present >> rail.Label(
        'No') >> opportunity_type_is_not_present

    types_to_be_synced_doesnt_contain_opportunity_type >> rail.Label(
        'Yes') >> no_data
    types_to_be_synced_doesnt_contain_opportunity_type >> rail.Label(
        'No') >> opportunity_type_is_not_present

    opportunity_type_is_not_present >> rail.Label(
        'Yes') >> check_sync_opportunities_with_no_types
    opportunity_type_is_not_present >> rail.Label(
        'No') >> and_flow_should_stop

    check_sync_opportunities_with_no_types >> rail.Label(
        'Yes') >> no_data
    check_sync_opportunities_with_no_types >> rail.Label(
        'No') >> and_flow_should_stop

    and_flow_should_stop >> rail.Label('Yes') >> no_data
    and_flow_should_stop >> rail.Label('No') >> operation_equals_or

    operation_equals_or >> rail.Label(
        'Yes') >> or_flow_should_stop
    operation_equals_or >> rail.Label(
        'No') >> search_projects_in_replicon

    or_flow_should_stop >> rail.Label(
        'Yes') >> no_data
    or_flow_should_stop >> rail.Label(
        'No') >> search_projects_in_replicon

    search_projects_in_replicon >> get_project_custom_fields >> search_users_in_salesforce >> account_id_is_present

    account_id_is_present >> rail.Label(
        'Yes') >> get_details_of_specific_account >> replicon_project_uri_present
    account_id_is_present >> rail.Label(
        'No') >> replicon_project_uri_present

    replicon_project_uri_present >> rail.Label(
        'Yes') >> to_be_updated
    replicon_project_uri_present >> rail.Label(
        'No') >> check_account_id_for_new_project
    check_account_id_for_new_project >> rail.Label(
        'No') >> billing_type_contains_time_and_material
    check_account_id_for_new_project >> rail.Label(
        'Yes') >> search_clients_in_replicon_when_account_id_is_present >> required_client_is_not_present

    to_be_updated >> rail.Label(
        'Yes') >> no_data
    to_be_updated >> rail.Label(
        'No') >> update_project_in_replicon

    update_project_in_replicon >> check_account_id_is_present_inside_loop

    check_account_id_is_present_inside_loop >> rail.Label(
        'Yes') >> search_clients_in_replicon >> required_client_present
    check_account_id_is_present_inside_loop >> rail.Label(
        'No') >> search_users_in_replicon

    required_client_present >> rail.Label(
        'Yes') >> update_project_client >> search_users_in_replicon
    required_client_present >> rail.Label(
        'No') >> search_users_in_replicon

    search_users_in_replicon >> replicon_user_uri_is_not_present

    replicon_user_uri_is_not_present >> rail.Label(
        'Yes') >> no_data
    replicon_user_uri_is_not_present >> rail.Label(
        'No') >> assign_comanager_to_project

    assign_comanager_to_project >> replicon_project_uri_present_inside_loop

    replicon_project_uri_present_inside_loop >> rail.Label(
        'Yes') >> no_data
    replicon_project_uri_present_inside_loop >> rail.Label(
        'No') >> check_account_id_after_update_loop

    check_account_id_after_update_loop >> rail.Label(
        'Yes') >> search_clients_in_replicon_when_account_id_is_present >> required_client_is_not_present
    check_account_id_after_update_loop >> rail.Label(
        'No') >> billing_type_contains_time_and_material

    required_client_is_not_present >> rail.Label(
        'Yes') >> search_for_contacts_in_salesforce
    required_client_is_not_present >> rail.Label(
        'No') >> billing_type_contains_time_and_material

    search_for_contacts_in_salesforce >> search_users_in_salesforce_when_account_id_is_present >> search_users_in_replicon_when_account_id_is_present >> get_all_countries_in_replicon
    get_all_countries_in_replicon >> create_client_in_replicon >> billing_type_contains_time_and_material

    billing_type_contains_time_and_material >> rail.Label(
        'Yes') >> create_project_in_replicon_when_billing_type_contains_time_and_material
    billing_type_contains_time_and_material >> rail.Label(
        'No') >> billing_type_contains_fixed_bid

    create_project_in_replicon_when_billing_type_contains_time_and_material >> search_users_in_replicon_when_billing_type_contains_time_and_material
    search_users_in_replicon_when_billing_type_contains_time_and_material >> user_is_not_present_when_billing_type_contains_time_and_material

    user_is_not_present_when_billing_type_contains_time_and_material >> rail.Label(
        'Yes') >> no_data
    user_is_not_present_when_billing_type_contains_time_and_material >> rail.Label(
        'No') >> assign_comanager_to_project_when_billing_type_contains_time_and_material >> billing_type_contains_fixed_bid

    billing_type_contains_fixed_bid >> rail.Label(
        'Yes') >> create_project_in_replicon_when_billing_type_contains_fixed_bid
    billing_type_contains_fixed_bid >> rail.Label(
        'No') >> billing_type_contains_non_billable

    create_project_in_replicon_when_billing_type_contains_fixed_bid >> update_project_client_when_billing_type_contains_fixed_bid >> update_project_fixed_bid_rate_when_billing_type_contains_fixed_bid
    update_project_fixed_bid_rate_when_billing_type_contains_fixed_bid >> search_users_in_replicon_when_billing_type_contains_fixed_bid >> user_is_not_present_when_billing_type_contains_fixed_bid

    user_is_not_present_when_billing_type_contains_fixed_bid >> rail.Label(
        'Yes') >> no_data
    user_is_not_present_when_billing_type_contains_fixed_bid >> rail.Label(
        'No') >> assign_comanager_to_project_when_billing_type_contains_fixed_bid >> billing_type_contains_non_billable

    billing_type_contains_non_billable >> rail.Label(
        'Yes') >> create_project_in_replicon_when_billing_type_contains_non_billable
    billing_type_contains_non_billable >> rail.Label(
        'No') >> no_data

    create_project_in_replicon_when_billing_type_contains_non_billable >> search_users_in_replicon_when_billing_type_contains_non_billable
    search_users_in_replicon_when_billing_type_contains_non_billable >> user_is_not_present_when_billing_type_contains_non_billable

    user_is_not_present_when_billing_type_contains_non_billable >> rail.Label(
        'Yes') >> no_data
    user_is_not_present_when_billing_type_contains_non_billable >> rail.Label(
        'No') >> assign_comanager_to_project_when_billing_type_contains_non_billable

    return dag


rail.for_each_instance(create_child_dag)
