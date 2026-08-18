from datetime import timedelta
import rail
from sideplate.project_records_sync.utils import custom_function, request_payload, request_query


def create_child_dag(config):
    """
    Child DAG for processing each Salesforce opportunity record.
    Triggered from master DAG via TriggerDagRunForEachItemOperator.
    """
    with rail.create_airflow_dag(
        dag_id=config.process_opportunity_dag_id,
        description='Sync opportunities from Salesforce as projects to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Extract opportunity record from dag_run.conf
        extract_opportunity_data = rail.PythonOperator(
            task_id='extract_opportunity_data',
            python_callable=lambda dag_run: dag_run.conf.get('project_record', {})
        )

        # Create a wrapper to return full structure like original
        prepare_salesforce_data = rail.PythonOperator(
            task_id='prepare_salesforce_data',
            python_callable=lambda: {
                'records': [rail.result('extract_opportunity_data')],
                'totalSize': 1
            }
        )

        get_all_currency = rail.RepliconServiceOperator(
            task_id='get_all_currency',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data= {},
            replicon_conn_id=config.replicon_conn_id,
        )

        get_all_country = rail.RepliconServiceOperator(
            task_id='get_all_country',
            endpoint="/services/InternationalizationService1.svc/GetAllCountries",
            data= {},
            replicon_conn_id=config.replicon_conn_id,
        )
        
        check_if_project_manager_is_present = rail.IfOperator(
            task_id='check_if_project_manager_is_present',
            test=lambda: bool(rail.result('extract_opportunity_data').get('Project_Manager__c')),
            no_task='project_manager_is_not_present',
            yes_task='project_manager_is_present'
        )

        project_manager_is_not_present = rail.EmptyOperator(
            task_id='project_manager_is_not_present'
        )

        project_manager_is_present = rail.EmptyOperator(
            task_id='project_manager_is_present'
        )

        get_eligible_project_leaders = rail.RepliconServiceOperator(
            task_id='get_eligible_project_leaders',
            endpoint="/services/ProjectService1.svc/GetEligibleProjectLeaders",
            data= {},
            replicon_conn_id=config.replicon_conn_id,
        )

        get_eligible_project_leader = rail.PythonOperator(
            task_id='get_eligible_project_leader',
            python_callable=lambda: custom_function.get_an_eligible_project_leader(
                rail.result("get_eligible_project_leaders"),
                rail.result("extract_opportunity_data")
            )
        )

        search_client_code = rail.RepliconServiceOperator(
            task_id='search_client_code',
            endpoint="/services/ClientListService1.svc/GetData",
            data= lambda: request_payload.search_client_code_payload(
                rail.result("extract_opportunity_data")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        log_search_client_code = rail.PythonOperator(
            task_id='log_search_client_code',
            python_callable=lambda: rail.result("search_client_code")["rows"][0]["cells"][0] if rail.result("search_client_code")["rows"] else [],
        )
        

        check_if_data_type_is_present = rail.IfOperator(
            task_id='check_if_data_type_is_present',
            test=lambda: len(custom_function.check_if_rows_are_present(rail.result("search_client_code"))) > 0,
            no_task='data_type_is_not_present',
            yes_task='data_type_is_present'
        )

        data_type_is_not_present = rail.EmptyOperator(
            task_id='data_type_is_not_present'
        )

        data_type_is_present = rail.EmptyOperator(
            task_id='data_type_is_present'
        )

        accumulate_items_to_searchoutput_list = rail.PythonOperator(
            task_id='accumulate_items_to_searchoutput_list',
            python_callable=lambda: custom_function.create_accumulate_items_to_searchoutput_list(
                rail.result("search_client_code")
            )
        )

        get_details_of_specific_account_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_details_of_specific_account_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_account_in_salesforce_query(
                rail.result("prepare_salesforce_data")
            ),
        )

        check_if_client_uri_is_present = rail.IfOperator(
            task_id='check_if_client_uri_is_present',
            test=lambda: len(custom_function.get_client_uri(rail.result("accumulate_items_to_searchoutput_list"))) > 0,
            no_task='client_uri_is_not_present',
            yes_task='client_uri_is_present')
        
        client_uri_is_not_present = rail.EmptyOperator(
            task_id='client_uri_is_not_present'
        )

        client_uri_is_present = rail.EmptyOperator(
            task_id='client_uri_is_present'
        )

        update_client_name = rail.RepliconServiceOperator(
            task_id='update_client_name',
            endpoint="/services/ClientService1.svc/UpdateName",
            data= lambda: request_payload.update_client_name_payload(
                rail.result("accumulate_items_to_searchoutput_list")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data= lambda: request_payload.create_client_payload(
                rail.result("get_details_of_specific_account_in_salesforce"),
                rail.result("get_all_country") 
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_billing_rate_is_allowed_by_default_on_new_projects = rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_allowed_by_default_on_new_projects',
            endpoint="/services/ClientService1.svc/UpdateBillingRateIsAllowedByDefaultOnNewProjects",
            data= lambda: request_payload.update_billing_rate_is_allowed_by_default_on_new_projects_payload(
                rail.result("create_client")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        project_resource_list = rail.RepliconServiceOperator(
            task_id='project_resource_list',
            endpoint="/services/ResourceListService1.svc/GetData",
            data= lambda: request_payload.project_resource_list_payload(),
            replicon_conn_id=config.replicon_conn_id,
        )

        accumulate_items_to_project_resource_list = rail.PythonOperator(
            task_id='accumulate_items_to_project_resource_list',
            python_callable=lambda: custom_function.create_accumulate_items_to_project_resource_list(
                rail.result("project_resource_list")
            )
        )

        search_project_by_code = rail.RepliconServiceOperator(
            task_id='search_project_by_code',
            endpoint="/services/ProjectListService1.svc/GetData",
            data= lambda: request_payload.search_project_by_code_payload(rail.result("extract_opportunity_data")),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_details_of_specific_account_in_salesforce2 = rail.SalesforceQueryOperator2(
            task_id='get_details_of_specific_account_in_salesforce2',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_account_in_salesforce_query(
                rail.result("prepare_salesforce_data")
            ),
        )

        create_client2 = rail.RepliconServiceOperator(
            task_id='create_client2',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data= lambda: request_payload.create_client_payload(
                rail.result("get_details_of_specific_account_in_salesforce2")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_billing_rate_is_allowed_by_default_on_new_projects2 = rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_allowed_by_default_on_new_projects2',
            endpoint="/services/ClientService1.svc/UpdateBillingRateIsAllowedByDefaultOnNewProjects",
            data= lambda: request_payload.update_billing_rate_is_allowed_by_default_on_new_projects_payload(
                rail.result("create_client2")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_whether_data_type_is_present = rail.IfOperator(
            task_id='check_whether_data_type_is_present',
            test=lambda: len(custom_function.check_if_rows_are_present(rail.result("search_project_by_code"))) > 0,
            no_task='data_type_is_absent',
            yes_task='data_type_present'
        )

        data_type_is_absent = rail.EmptyOperator(
            task_id='data_type_is_absent'
        )

        data_type_present = rail.EmptyOperator(
            task_id='data_type_present'
        )

        accumulate_items_to_project_search_output_list = rail.PythonOperator(
            task_id='accumulate_items_to_project_search_output_list',
            python_callable=lambda: custom_function.create_accumulate_items_to_project_search_output_list(
                rail.result("search_project_by_code")
            )
        )

        check_if_project_uri_is_present = rail.IfOperator(
            task_id='check_if_project_uri_is_present',
            test=lambda: len(custom_function.check_if_rows_are_present(rail.result("search_project_by_code"))) > 0,
            no_task='project_uri_is_absent',
            yes_task='project_uri_present'
        )

        project_uri_is_absent = rail.EmptyOperator(
            task_id='project_uri_is_absent'
        )

        project_uri_present = rail.EmptyOperator(
            task_id='project_uri_present'
        )

        check_if_description_is_present = rail.IfOperator(
            task_id='check_if_description_is_present',
            test=lambda: custom_function.check_if_argument_is_present(rail.result("extract_opportunity_data"), argument = 'MPM4_BASE_Description__c'),
            no_task='check_if_project_leader_is_present',
            yes_task='update_project_description'
        )


        update_project_description = rail.RepliconServiceOperator(
            task_id='update_project_description',
            endpoint="/services/ProjectService1.svc/UpdateDescription",
            data= lambda: request_payload.update_project_description_payload(
                rail.result("accumulate_items_to_project_search_output_list"),
                rail.result("extract_opportunity_data")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_project_leader_is_present = rail.IfOperator(
            task_id='check_if_project_leader_is_present',
            test='{{ result("get_eligible_project_leader") | is_truthy }}',
            no_task='check_if_project_number_and_name_is_present',
            yes_task='assign_project_manager'
        )

        assign_project_manager = rail.RepliconServiceOperator(
            task_id='assign_project_manager',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data= lambda: request_payload.assign_project_manager_payload(
                rail.result("accumulate_items_to_project_search_output_list"),
                rail.result("get_eligible_project_leader")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        check_if_project_number_and_name_is_present = rail.IfOperator(
            task_id='check_if_project_number_and_name_is_present',
            test=lambda: custom_function.check_if_argument_is_present(rail.result("extract_opportunity_data"), argument = 'Project_Number_and_Name__c'),
            no_task='check_project_status',
            yes_task='update_project_name'
        )

        update_project_name = rail.RepliconServiceOperator(
            task_id='update_project_name',
            endpoint="/services/ProjectService1.svc/UpdateName",
            data= lambda: request_payload.update_project_name_payload(
                rail.result("extract_opportunity_data"),
                rail.result("accumulate_items_to_project_search_output_list")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        check_project_status = rail.IfOperator(
            task_id='check_project_status',
            test=lambda: custom_function.project_status_check(rail.result("extract_opportunity_data")),
            no_task='get_project_details',
            yes_task='update_project_status'
        )
        
        update_project_status = rail.RepliconServiceOperator(
            task_id='update_project_status',
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data= lambda: request_payload.update_project_status_payload(
                rail.result("extract_opportunity_data"),
                rail.result("accumulate_items_to_project_search_output_list")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/GetProjectDetails",
            data= lambda: request_payload.get_project_details_payload(
                rail.result("accumulate_items_to_project_search_output_list")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        if_billing_type_contains_fixed_bid = rail.IfOperator(
            task_id='if_billing_type_contains_fixed_bid',
            test=lambda: custom_function.billing_type_fixed_bid_check(rail.result("get_project_details")),
            no_task='if_uri_not_equals_task_based',
            yes_task='update_project_fixed_bid_rate'
        )

        update_project_fixed_bid_rate = rail.RepliconServiceOperator(
            task_id='update_project_fixed_bid_rate',
            endpoint="/services/FixedBidProjectService1.svc/UpdateProjectFixedBidRate",
            data= lambda: request_payload.update_project_fixed_bid_rate_payload(
                rail.result("accumulate_items_to_project_search_output_list"),
                rail.result('extract_opportunity_data'),
                rail.result("get_all_currency")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        if_uri_not_equals_task_based = rail.IfOperator(
            task_id='if_uri_not_equals_task_based',
            test=lambda: custom_function.check_if_uri_not_equals_task_based(rail.result("get_project_details")),
            no_task='extract_client_uri',
            yes_task='update_estimation_mode'
        )

        update_estimation_mode = rail.RepliconServiceOperator(
            task_id='update_estimation_mode',
            endpoint="/services/ProjectService1.svc/UpdateEstimationMode",
            data= lambda: request_payload.update_estimation_mode_payload(
                rail.result("accumulate_items_to_project_search_output_list")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        extract_client_uri = rail.PythonOperator(
            task_id='extract_client_uri',
            python_callable=custom_function.extract_client_uri_from_dag_run,
        )

        get_client_details_from_replicon = rail.RepliconServiceOperator(
            task_id='get_client_details_from_replicon',
            endpoint="/services/ClientService1.svc/GetClientDetails",
            data= lambda: request_payload.get_client_details_payload(
                rail.result("extract_client_uri")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        

        check_if_code_not_equals_account = rail.IfOperator(
            task_id='check_if_code_not_equals_account',
            test=lambda: custom_function.check_code_equals_account(rail.result("get_client_details_from_replicon"),
                                                                            rail.result('extract_opportunity_data')),
            no_task='call_recipe',
            yes_task='apply_new_client2'
        )

        apply_new_client2 = rail.RepliconServiceOperator(
            task_id='apply_new_client2',
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data= lambda: request_payload.apply_new_client2_payload(
                rail.result("get_client_details_from_replicon"),
                rail.result("get_project_details")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        call_recipe = rail.TriggerDagRunForEachItemOperator(
            task_id='call_recipe',
            trigger_dag_id=config.updateprojectoef_sideplate_dag_id,
            items=lambda: rail.result('prepare_salesforce_data')["records"],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {"recipe_input":{
                'Salesforceprojectid': item['Id'],
                'Repliconprojecturi': rail.result("accumulate_items_to_project_search_output_list")[0]["uri"]
            }}
        )
        wait_for_call_recipe_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_call_recipe_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("call_recipe") }}'
        )

        opp_billing_type_contains_fixed_bid = rail.IfOperator(
            task_id='opp_billing_type_contains_fixed_bid',
            test=lambda: custom_function.check_opp_billing_type_contains_fixed_bid(rail.result('extract_opportunity_data')),
            no_task='opp_billing_type_not_equals_fixed_bid',
            yes_task='opp_billing_type_equals_fixed_bid'
        )

        opp_billing_type_equals_fixed_bid = rail.EmptyOperator(
            task_id='opp_billing_type_equals_fixed_bid'
        )
        
        opp_billing_type_not_equals_fixed_bid = rail.EmptyOperator(
            task_id='opp_billing_type_not_equals_fixed_bid'
        )

        opp_billing_type_contains_hourly = rail.IfOperator(
            task_id='opp_billing_type_contains_hourly',
            test=lambda: custom_function.check_opp_billing_type_contains_hourly(rail.result('extract_opportunity_data')),
            no_task='opp_billing_type_is_NA',
            yes_task='opp_billing_type_equals_hourly'

        )

        opp_billing_type_equals_hourly = rail.EmptyOperator(
            task_id='opp_billing_type_equals_hourly'
        )

        opp_billing_type_is_NA = rail.EmptyOperator(
            task_id='opp_billing_type_is_NA'
        )
        
        get_project_status = rail.RepliconServiceOperator(
            task_id='get_project_status',
            endpoint="/services/ProjectStatusService1.svc/GetAllProjectStatusLabels",
            data= {},
            replicon_conn_id=config.replicon_conn_id,
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data= lambda: request_payload.create_project_payload(rail.result('extract_opportunity_data'),
                                                                    rail.result("get_eligible_project_leader")),
            replicon_conn_id=config.replicon_conn_id,
        )

        apply_new_client2_via_http = rail.RepliconServiceOperator(
            task_id='apply_new_client2_via_http',
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data= lambda: request_payload.apply_new_client2_via_http_payload(rail.result('create_project'),
                                                                    rail.result("extract_client_uri2")),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_clients = rail.RepliconServiceOperator(
            task_id='update_clients',
            endpoint="/services/ProjectService1.svc/UpdateClients",
            data= lambda: request_payload.update_clients_payload(rail.result('create_project'),
                                                                    rail.result("extract_client_uri2")),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_allow_time_entry_against_tasks_only = rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data= lambda: request_payload.update_allow_time_entry_against_tasks_only_payload(rail.result('create_project')),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        update_project_fixed_bid_rate_in_replicon = rail.RepliconServiceOperator(
            task_id='update_project_fixed_bid_rate_in_replicon',
            endpoint="/services/FixedBidProjectService1.svc/UpdateProjectFixedBidRate",
            data= lambda: request_payload.update_project_fixed_bid_rate_in_replicon_payload(rail.result('create_project'),
                                                                                             rail.result('extract_opportunity_data'),
                                                                                             rail.result("get_all_currency")),
            replicon_conn_id=config.replicon_conn_id,
        )

        bulk_update_project_team_members_assignment = rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data= lambda: request_payload.bulk_update_project_team_members_assignment_payload(rail.result('accumulate_items_to_project_resource_list'),
                                                                                             rail.result('create_project')),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_project2 = rail.RepliconServiceOperator(
            task_id='create_project2',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data= lambda: request_payload.create_project_payload_time_and_material(rail.result('extract_opportunity_data'),
                                                                    rail.result("get_eligible_project_leader")),
            replicon_conn_id=config.replicon_conn_id,
        )

        apply_new_client2_via_http2 = rail.RepliconServiceOperator(
            task_id='apply_new_client2_via_http2',
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data= lambda: request_payload.apply_new_client2_via_http_payload(rail.result('create_project2'),
                                                                    rail.result("extract_client_uri3")),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_clients2 = rail.RepliconServiceOperator(
            task_id='update_clients2',
            endpoint="/services/ProjectService1.svc/UpdateClients",
            data= lambda: request_payload.update_clients_payload(rail.result('create_project2'),
                                                                    rail.result("extract_client_uri3")),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_allow_time_entry_against_tasks_only2 = rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only2',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data= lambda: request_payload.update_allow_time_entry_against_tasks_only_payload(rail.result('create_project2')),
            replicon_conn_id=config.replicon_conn_id,
        )

        bulk_update_project_team_members_assignment2 = rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment2',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data= lambda: request_payload.bulk_update_project_team_members_assignment_payload(rail.result('accumulate_items_to_project_resource_list'),
                                                                                             rail.result('create_project2')),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        store_project_uri = rail.PythonOperator(
            task_id='store_project_uri',
            python_callable=custom_function.extract_new_project_uri,
        )

        extract_client_uri2 = rail.PythonOperator(
            task_id='extract_client_uri2',
            python_callable=custom_function.extract_client_uri_from_dag_run,
        )

        store_project_uri2 = rail.PythonOperator(
            task_id='store_project_uri2',
            python_callable=custom_function.extract_new_project_uri2,
        )

        extract_client_uri3 = rail.PythonOperator(
            task_id='extract_client_uri3',
            python_callable=custom_function.extract_client_uri_from_dag_run,
        )

        update_estimation_mode_via_http = rail.RepliconServiceOperator(
            task_id='update_estimation_mode_via_http',
            endpoint="/services/ProjectService1.svc/UpdateEstimationMode",
            data= lambda: request_payload.update_estimation_mode_via_http_payload(),
            replicon_conn_id=config.replicon_conn_id,
        )

        store_project_uri3 = rail.PythonOperator(
            task_id='store_project_uri3',
            python_callable=custom_function.extract_new_project_uri3,
        )

        call_recipe2 = rail.TriggerDagRunForEachItemOperator(
            task_id='call_recipe2',
            trigger_dag_id=config.updateprojectoef_sideplate_dag_id,
            items=lambda: rail.result('prepare_salesforce_data')["records"],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {"recipe_input":{
                'Salesforceprojectid': item['Id'],
                'Repliconprojecturi': rail.result("store_project_uri2") if rail.result("store_project_uri2") else rail.result("store_project_uri")
            }}
        )

        wait_for_call_recipe_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_call_recipe_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("call_recipe2") }}'
        )

        get_project_details_via_http = rail.RepliconServiceOperator(
            task_id='get_project_details_via_http',
            endpoint="/services/ProjectService1.svc/GetProjectDetails2",
            data= lambda: request_payload.get_project_details_via_http_payload(rail.result('store_project_uri3')),
            replicon_conn_id=config.replicon_conn_id,
        )

        put_task_hierarchy_via_http =  rail.RepliconServiceOperator(
            task_id='put_task_hierarchy_via_http',
            endpoint="/services/ProjectService1.svc/PutTaskHierarchy",
            data= lambda: request_payload.put_task_hierarchy_via_http_payload(rail.result('store_project_uri3')),
            replicon_conn_id=config.replicon_conn_id,
        )

        collect_resource_uris = rail.PythonOperator(
            task_id='collect_resource_uris',
            python_callable=lambda: custom_function.collect_resource_uris(
                rail.result("accumulate_items_to_project_resource_list")
            ),
        )

        accumulate_items_to_subtasks_list = rail.PythonOperator(
            task_id='accumulate_items_to_subtasks_list',
            python_callable=lambda: custom_function.accumulate_items_to_subtasks_list(
                rail.result("put_task_hierarchy_via_http")
            ),
        )

        bulk_update_resource_assignments_via_http = rail.RepliconServiceCallForEachItemOperator(
            task_id='bulk_update_resource_assignments_via_http',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            items=lambda: rail.result("accumulate_items_to_subtasks_list"),
            data=lambda item: {
                "taskUri": item,
                "resourceUris": rail.result("collect_resource_uris"),
                "isAssigned": "true"
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        create_project3 = rail.RepliconServiceOperator(
            task_id='create_project3',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data= lambda: request_payload.create_project_payload_non_billable(rail.result('extract_opportunity_data'),
                                                                    rail.result("get_eligible_project_leader")),
            replicon_conn_id=config.replicon_conn_id,
        )

        apply_new_client2_via_http3 = rail.RepliconServiceOperator(
            task_id='apply_new_client2_via_http3',
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data= lambda: request_payload.apply_new_client2_via_http_payload(rail.result('create_project3'),
                                                                    rail.result("extract_client_uri")),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_clients3 = rail.RepliconServiceOperator(
            task_id='update_clients3',
            endpoint="/services/ProjectService1.svc/UpdateClients",
            data= lambda: request_payload.update_clients_payload(rail.result('create_project3'),
                                                                    custom_function.extract_client_uri_from_dag_run()),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_allow_time_entry_against_tasks_only3 = rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only3',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data= lambda: request_payload.update_allow_time_entry_against_tasks_only_payload(rail.result('create_project3')),
            replicon_conn_id=config.replicon_conn_id,
        )

        bulk_update_project_team_members_assignment3 = rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment3',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data= lambda: request_payload.bulk_update_project_team_members_assignment_payload(rail.result('accumulate_items_to_project_resource_list'),
                                                                                             rail.result('create_project3')),
            replicon_conn_id=config.replicon_conn_id,
        )

        extract_opportunity_data >> prepare_salesforce_data >> get_all_currency >> get_all_country >> check_if_project_manager_is_present
        check_if_project_manager_is_present >> rail.Label("No") >> project_manager_is_not_present >> search_client_code
        check_if_project_manager_is_present >> rail.Label("Yes") >> project_manager_is_present >> get_eligible_project_leaders
        get_eligible_project_leaders >> get_eligible_project_leader >> search_client_code >> log_search_client_code >> check_if_data_type_is_present
        
        check_if_data_type_is_present >> rail.Label("No") >> data_type_is_not_present >> get_details_of_specific_account_in_salesforce2 >> create_client2 >> update_billing_rate_is_allowed_by_default_on_new_projects2 >> project_resource_list
        check_if_data_type_is_present >> rail.Label("Yes") >> data_type_is_present >> accumulate_items_to_searchoutput_list >> get_details_of_specific_account_in_salesforce >> check_if_client_uri_is_present
        
        check_if_client_uri_is_present >> rail.Label("No") >> client_uri_is_not_present >> create_client >> update_billing_rate_is_allowed_by_default_on_new_projects >> project_resource_list
        check_if_client_uri_is_present >> rail.Label("Yes") >> client_uri_is_present >> update_client_name >> project_resource_list

        project_resource_list >> accumulate_items_to_project_resource_list >> search_project_by_code>> get_project_status >> check_whether_data_type_is_present
        check_whether_data_type_is_present >> rail.Label("No") >> data_type_is_absent >> opp_billing_type_contains_fixed_bid
        check_whether_data_type_is_present >> rail.Label("Yes") >> data_type_present >> accumulate_items_to_project_search_output_list >> check_if_project_uri_is_present

        check_if_project_uri_is_present >> rail.Label("No") >> project_uri_is_absent
        check_if_project_uri_is_present >> rail.Label("Yes") >> project_uri_present >> check_if_description_is_present

        check_if_description_is_present >> rail.Label("No") >> check_if_project_leader_is_present
        check_if_description_is_present >> rail.Label("Yes") >> update_project_description >> check_if_project_leader_is_present

        check_if_project_leader_is_present >> rail.Label("No") >> check_if_project_number_and_name_is_present
        check_if_project_leader_is_present >> rail.Label("Yes") >> assign_project_manager >> check_if_project_number_and_name_is_present

        check_if_project_number_and_name_is_present >> rail.Label("No") >> check_project_status
        check_if_project_number_and_name_is_present >> rail.Label("Yes") >> update_project_name >> check_project_status

        check_project_status >> rail.Label("Yes") >> update_project_status >> get_project_details
        check_project_status >> rail.Label("No") >> get_project_details

        get_project_details >> if_billing_type_contains_fixed_bid
        if_billing_type_contains_fixed_bid >> rail.Label("Yes") >> update_project_fixed_bid_rate >> if_uri_not_equals_task_based
        if_billing_type_contains_fixed_bid >> rail.Label("No") >> if_uri_not_equals_task_based

        if_uri_not_equals_task_based >> rail.Label("Yes") >> update_estimation_mode >> extract_client_uri
        if_uri_not_equals_task_based >> rail.Label("No") >> extract_client_uri
        extract_client_uri >> get_client_details_from_replicon >> check_if_code_not_equals_account

        check_if_code_not_equals_account >> rail.Label("No") >> call_recipe >> wait_for_call_recipe_dags
        check_if_code_not_equals_account >> rail.Label("Yes") >> apply_new_client2 >> call_recipe >> wait_for_call_recipe_dags

        project_uri_is_absent >> opp_billing_type_contains_fixed_bid
        opp_billing_type_contains_fixed_bid >> rail.Label("No") >> opp_billing_type_not_equals_fixed_bid >> opp_billing_type_equals_hourly >> create_project2 >> store_project_uri2 >> extract_client_uri3 >> apply_new_client2_via_http2 >> update_clients2 >> update_allow_time_entry_against_tasks_only2 >> bulk_update_project_team_members_assignment2 >> update_estimation_mode_via_http
        opp_billing_type_contains_fixed_bid >> rail.Label("Yes") >> opp_billing_type_equals_fixed_bid >> create_project >> store_project_uri >> extract_client_uri2 >> apply_new_client2_via_http >> update_clients >> update_allow_time_entry_against_tasks_only >> update_project_fixed_bid_rate_in_replicon >> bulk_update_project_team_members_assignment >> update_estimation_mode_via_http
        
        opp_billing_type_not_equals_fixed_bid >> opp_billing_type_contains_hourly 
        opp_billing_type_contains_hourly >> rail.Label("No") >> opp_billing_type_is_NA >> create_project3 >> apply_new_client2_via_http3 >> update_clients3 >> update_allow_time_entry_against_tasks_only3 >> bulk_update_project_team_members_assignment3
        opp_billing_type_contains_hourly >> rail.Label("Yes") >> opp_billing_type_equals_hourly
        update_estimation_mode_via_http >> store_project_uri3 >> call_recipe2 >> wait_for_call_recipe_dag >> get_project_details_via_http >> put_task_hierarchy_via_http >> collect_resource_uris
        put_task_hierarchy_via_http >> accumulate_items_to_subtasks_list
        collect_resource_uris >> bulk_update_resource_assignments_via_http
        accumulate_items_to_subtasks_list >> bulk_update_resource_assignments_via_http

    return dag


# Create child DAG for each instance
rail.for_each_instance(create_child_dag)