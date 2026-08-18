from datetime import timedelta
import rail
from sideplate.project_records_sync.utils import custom_function, request_payload, request_query


def create_child_dag(config):
    """
    Child DAG for processing each Salesforce opportunity record.
    Triggered from master DAG via TriggerDagRunForEachItemOperator.
    """
    with rail.create_airflow_dag(
        dag_id=config.updateprojectoef_sideplate_dag_id,
        description='Sync opportunities from Salesforce as projects to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_sub_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        extract_data = rail.PythonOperator(
            task_id='extract_data',
            python_callable=lambda dag_run: dag_run.conf.get('recipe_input', {})
        )

        get_details_of_project_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_details_of_project_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_project_in_salesforce_query(
                rail.result("extract_data")
            ),
        )

        get_all_object_extension_field_projects_via_http = rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field_projects_via_http',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data= {
                    "bindingContextUri": "urn:replicon:object-type:project"
                    },
            replicon_conn_id=config.replicon_conn_id,
        )

        get_all_custom_field = rail.RepliconServiceOperator(
            task_id='get_all_custom_field',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data= lambda: {
                    "objectUri": rail.result("extract_data")["Repliconprojecturi"]
                    },
            replicon_conn_id=config.replicon_conn_id,
        )

        log_opp_close_date_custom_uri = rail.PythonOperator(
            task_id = "log_opp_close_date_custom_uri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_all_custom_field"), "displayText", "Opp Close Date", "uri")
        )

        update_date_value = rail.RepliconServiceOperator(
            task_id='update_date_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data= lambda: request_payload.get_update_date_payload(rail.result("extract_data")["Repliconprojecturi"],
                                                                  rail.result("log_opp_close_date_custom_uri"),
                                                                  rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_Close_Date__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        
        
        check_if_project_number_is_present = rail.IfOperator(
            task_id='check_if_project_number_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument = "Project_Number__c"),
            no_task='opp_id_is_present',
            yes_task='update_object_extension_field_value_project_via_http'
        )

        update_object_extension_field_value_project_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_project_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Project #",
                field_uri = rail.result("get_details_of_project_in_salesforce")['records'][0]["Project_Number__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        opp_id_is_present = rail.IfOperator(
            task_id='opp_id_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument = "Opp_ID__c"),
            no_task='finish_task',
            yes_task='update_object_extension_field_value_opportunity_id_via_http'
        )

        update_object_extension_field_value_opportunity_id_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opportunity_id_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opportunity ID",
                field_uri = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_ID__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_opportunity_name_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opportunity_name_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opportunity Name",
                field_uri = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_Name__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_opp_category_is_present = rail.IfOperator(
            task_id='check_if_opp_category_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Opp_Category__c"),
            no_task='check_if_opp_state_is_present',
            yes_task='get_object_extension_tag_definition_details_opp_category_via_http'
        )

        get_object_extension_tag_definition_details_opp_category_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_opp_category_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Category",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_opp_category_tag_matches_salesforce_opp_category = rail.IfOperator(
            task_id='check_if_opp_category_tag_matches_salesforce_opp_category',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_opp_category_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_Category__c"]
                                                                   ),
            no_task='create_object_extension_tags_opp_category_list',
            yes_task='update_object_extension_field_value_opportunity_category_via_http'
        )

        update_object_extension_field_value_opportunity_category_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opportunity_category_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Category",
                field_uri = rail.result("get_object_extension_tag_definition_details_opp_category_via_http"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_Category__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_opp_category_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_opp_category_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_opp_category_via_http"),
            )
        )
        
        put_object_extension_tags_via_http = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_opp_category_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Opp Category",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_Category__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_opp_category_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_opp_category_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Category",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_opportunity_category_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opportunity_category_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Category",
                field_uri = rail.result("get_object_extension_tag_definition_details_opp_category_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_Category__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_opp_state_is_present = rail.IfOperator(
            task_id='check_if_opp_state_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Opp_State__c"),
            no_task='check_if_owner_id_is_present',
            yes_task='get_object_extension_tag_definition_details_opp_state_via_http'
        )

        get_object_extension_tag_definition_details_opp_state_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_opp_state_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp State",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_opp_state_tag_matches_salesforce_opp_state = rail.IfOperator(
            task_id='check_if_opp_state_tag_matches_salesforce_opp_state',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_opp_state_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_State__c"]
                                                                   ),
            no_task='create_object_extension_tags_opp_state_list',
            yes_task='update_object_extension_field_value_opportunity_state_via_http'
        )

        update_object_extension_field_value_opportunity_state_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opportunity_state_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp State",
                field_uri = rail.result("get_object_extension_tag_definition_details_opp_state_via_http"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_State__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_opp_state_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_opp_state_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_opp_state_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_1 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_opp_state_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Opp State",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_State__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_opp_state_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_opp_state_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp State",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_opportunity_state_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opportunity_state_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp State",
                field_uri = rail.result("get_object_extension_tag_definition_details_opp_state_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_State__c"]

            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_owner_id_is_present = rail.IfOperator(
            task_id='check_if_owner_id_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="OwnerId"),
            no_task='check_if_r_value_is_present',
            yes_task='get_object_extension_tag_definition_details_owner_id_via_http'
        )

        get_object_extension_tag_definition_details_owner_id_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_owner_id_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Owner Name",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_owner_id_tag_matches_salesforce_owner_id = rail.IfOperator(
            task_id='check_if_owner_id_tag_matches_salesforce_owner_id',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_owner_id_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_Owner_Name__c"]
                                                                   ),
            no_task='create_object_extension_tags_owner_id_list',
            yes_task='update_object_extension_field_value_owner_id_via_http'
        )

        update_object_extension_field_value_owner_id_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_owner_id_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Owner Name",
                field_uri=rail.result("get_object_extension_tag_definition_details_owner_id_via_http"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_Owner_Name__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_owner_id_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_owner_id_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_owner_id_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_2 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_2',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_owner_id_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Opp Owner Name",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_Owner_Name__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_owner_id_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_owner_id_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Owner Name",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_owner_id_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_owner_id_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Owner Name",
                field_uri = rail.result("get_object_extension_tag_definition_details_owner_id_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_Owner_Name__c"]

            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_r_value_is_present = rail.IfOperator(
            task_id='check_if_r_value_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="R_Value__c"),
            no_task='check_if_sector_is_present',
            yes_task='update_object_extension_field_value_r_value_via_http'
        )

        update_object_extension_field_value_r_value_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_r_value_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_numeric_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                field_name = "R Value",
                salesforce_field_name = "R_Value__c"
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_sector_is_present = rail.IfOperator(
            task_id='check_if_sector_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Sector__c"),
            no_task='check_if_software_is_present',
            yes_task='get_object_extension_tag_definition_details_sector_via_http'
        )
        
        get_object_extension_tag_definition_details_sector_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_sector_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Sector",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_sector_tag_matches_salesforce_sector = rail.IfOperator(
            task_id='check_if_sector_tag_matches_salesforce_sector',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_sector_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Sector__c"]
                                                                   ),
            no_task='create_object_extension_tags_sector_list',
            yes_task='update_object_extension_field_value_sector_via_http'
        )

        update_object_extension_field_value_sector_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_sector_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Sector",
                field_uri=rail.result("get_object_extension_tag_definition_details_sector_via_http"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Sector__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_sector_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_sector_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_sector_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_3 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_3',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_sector_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Sector",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Sector__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_sector_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_sector_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Sector",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_sector_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_sector_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Sector",
                field_uri = rail.result("get_object_extension_tag_definition_details_sector_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Sector__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_software_is_present = rail.IfOperator(
            task_id='check_if_software_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Software__c"),
            no_task='check_if_pc_reqd_is_present',
            yes_task='get_object_extension_tag_definition_details_software_via_http'
        )
        
        get_object_extension_tag_definition_details_software_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_software_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Software",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_software_tag_matches_salesforce_software = rail.IfOperator(
            task_id='check_if_software_tag_matches_salesforce_software',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_software_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Software__c"]
                                                                   ),
            no_task='create_object_extension_tags_software_list',
            yes_task='update_object_extension_field_value_software_via_http'
        )

        update_object_extension_field_value_software_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_software_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Software",
                field_uri = rail.result("get_object_extension_tag_definition_details_software_via_http"),
                salesforce_field_name=rail.result("get_details_of_project_in_salesforce")['records'][0]["Software__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_software_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_software_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_software_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_4 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_4',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_software_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Software",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Software__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_software_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_software_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Software",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_software_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_software_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Software",
                field_uri = rail.result("get_object_extension_tag_definition_details_software_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Software__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_pc_reqd_is_present = rail.IfOperator(
            task_id='check_if_pc_reqd_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="PC_Reqd__c"),
            no_task='check_if_opp_stage_is_present',
            yes_task='get_object_extension_tag_definition_details_pc_reqd_via_http'
        )
        
        get_object_extension_tag_definition_details_pc_reqd_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_pc_reqd_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PC Reqd?",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_pc_reqd_tag_matches_salesforce_pc_reqd = rail.IfOperator(
            task_id='check_if_pc_reqd_tag_matches_salesforce_pc_reqd',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_pc_reqd_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["PC_Reqd__c"]
                                                                   ),
            no_task='create_object_extension_tags_pc_reqd_list',
            yes_task='update_object_extension_field_value_pc_reqd_via_http'
        )

        update_object_extension_field_value_pc_reqd_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_pc_reqd_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PC Reqd?",
                field_uri = rail.result("get_object_extension_tag_definition_details_pc_reqd_via_http"),
                salesforce_field_name=rail.result("get_details_of_project_in_salesforce")['records'][0]["PC_Reqd__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_pc_reqd_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_pc_reqd_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_pc_reqd_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_5 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_5',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_pc_reqd_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="PC Reqd?",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["PC_Reqd__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_pc_reqd_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_pc_reqd_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PC Reqd?",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_pc_reqd_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_pc_reqd_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PC Reqd?",
                field_uri = rail.result("get_object_extension_tag_definition_details_pc_reqd_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["PC_Reqd__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_opp_stage_is_present = rail.IfOperator(
            task_id='check_if_opp_stage_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Opp_Stage__c"),
            no_task='check_if_updated_approx_sq_ft_is_present',
            yes_task='get_object_extension_tag_definition_details_opp_stage_via_http'
        )
        
        get_object_extension_tag_definition_details_opp_stage_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_opp_stage_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Stage",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_opp_stage_tag_matches_salesforce_opp_stage = rail.IfOperator(
            task_id='check_if_opp_stage_tag_matches_salesforce_opp_stage',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_opp_stage_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_Stage__c"]
                                                                   ),
            no_task='create_object_extension_tags_opp_stage_list',
            yes_task='update_object_extension_field_value_opp_stage_via_http'
        )

        update_object_extension_field_value_opp_stage_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opp_stage_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Stage",
                field_uri = rail.result("get_object_extension_tag_definition_details_opp_stage_via_http"),
                salesforce_field_name=rail.result("get_details_of_project_in_salesforce")['records'][0]["Opp_Stage__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_opp_stage_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_opp_stage_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_opp_stage_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_6 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_6',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_opp_stage_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Opp Stage",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Opp_Stage__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_opp_stage_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_opp_stage_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Stage",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_opp_stage_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_opp_stage_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Opp Stage",
                field_uri = rail.result("put_object_extension_tags_via_http_6")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_updated_approx_sq_ft_is_present = rail.IfOperator(
            task_id='check_if_updated_approx_sq_ft_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Approx_Sq_Ftg_for_Fee_Email__c"),
            no_task='check_if_primary_design_criteria_is_present',
            yes_task='update_object_extension_field_value_updated_approx_sq_ft_via_http'
        )

        update_object_extension_field_value_updated_approx_sq_ft_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_updated_approx_sq_ft_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_numeric_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                field_name = "Updated Approx Sq Ft",
                salesforce_field_name = "Approx_Sq_Ftg_for_Fee_Email__c"
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_primary_design_criteria_is_present = rail.IfOperator(
            task_id='check_if_primary_design_criteria_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Primary_design_criteria__c"),
            no_task='check_if_updated_number_of_sp_joints_is_present',
            yes_task='get_object_extension_tag_definition_details_primary_design_criteria_via_http'
        )
        
        get_object_extension_tag_definition_details_primary_design_criteria_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_primary_design_criteria_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Primary Design Criteria",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_primary_design_criteria_tag_matches_salesforce_primary_design_criteria = rail.IfOperator(
            task_id='check_if_primary_design_criteria_tag_matches_salesforce_primary_design_criteria',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_primary_design_criteria_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Primary_design_criteria__c"]
                                                                   ),
            no_task='create_object_extension_tags_primary_design_criteria_list',
            yes_task='update_object_extension_field_value_primary_design_criteria_via_http'
        )

        update_object_extension_field_value_primary_design_criteria_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_primary_design_criteria_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Primary Design Criteria",
                field_uri = rail.result("get_object_extension_tag_definition_details_primary_design_criteria_via_http"),
                salesforce_field_name=rail.result("get_details_of_project_in_salesforce")['records'][0]["Primary_design_criteria__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_primary_design_criteria_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_primary_design_criteria_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_primary_design_criteria_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_7 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_7',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_primary_design_criteria_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Primary Design Criteria",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Primary_design_criteria__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_primary_design_criteria_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_primary_design_criteria_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Primary Design Criteria",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_primary_design_criteria_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_primary_design_criteria_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Primary Design Criteria",
                field_uri = rail.result("get_object_extension_tag_definition_details_primary_design_criteria_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Primary_design_criteria__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_updated_number_of_sp_joints_is_present = rail.IfOperator(
            task_id='check_if_updated_number_of_sp_joints_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Updated_of_SP_Joints__c"),
            no_task='check_if_updated_qty_of_bldgs_is_present',
            yes_task='update_object_extension_field_value_updated_number_of_sp_joints_via_http'
        )

        update_object_extension_field_value_updated_number_of_sp_joints_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_updated_number_of_sp_joints_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_numeric_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                field_name = "Updated # of SP Joints",
                salesforce_field_name = "Updated_of_SP_Joints__c"
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_updated_qty_of_bldgs_is_present = rail.IfOperator(
            task_id='check_if_updated_qty_of_bldgs_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Updated_Qty_of_Bldgs__c"),
            no_task='check_if_sp_bolted_is_present',
            yes_task='update_object_extension_field_value_updated_qty_of_bldgs_via_http'
        )

        update_object_extension_field_value_updated_qty_of_bldgs_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_updated_qty_of_bldgs_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Updated Qty of Bldgs",
                field_uri = rail.result("get_details_of_project_in_salesforce")['records'][0]["Updated_Qty_of_Bldgs__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_sp_bolted_is_present = rail.IfOperator(
            task_id='check_if_sp_bolted_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="SP_Bolted__c"),
            no_task='check_if_fees_per_sq_ft_is_present',
            yes_task='get_object_extension_tag_definition_details_sp_bolted_via_http'
        )
        
        get_object_extension_tag_definition_details_sp_bolted_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_sp_bolted_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "SP bolted?",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_sp_bolted_tag_matches_salesforce_sp_bolted = rail.IfOperator(
            task_id='check_if_sp_bolted_tag_matches_salesforce_sp_bolted',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_sp_bolted_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["SP_Bolted__c"]
                                                                   ),
            no_task='create_object_extension_tags_sp_bolted_list',
            yes_task='update_object_extension_field_value_sp_bolted_via_http'
        )

        update_object_extension_field_value_sp_bolted_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_sp_bolted_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "SP bolted?",
                field_uri = rail.result("get_object_extension_tag_definition_details_sp_bolted_via_http"),
                salesforce_field_name=rail.result("get_details_of_project_in_salesforce")['records'][0]["SP_Bolted__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_sp_bolted_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_sp_bolted_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_sp_bolted_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_8 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_8',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_sp_bolted_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="SP bolted?",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["SP_Bolted__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_sp_bolted_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_sp_bolted_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "SP bolted?",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_sp_bolted_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_sp_bolted_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "SP bolted?",
                field_uri = rail.result("get_object_extension_tag_definition_details_sp_bolted_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["SP_Bolted__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_fees_per_sq_ft_is_present = rail.IfOperator(
            task_id='check_if_fees_per_sq_ft_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Fees_Per_Sq_Ft__c"),
            no_task='check_if_updated_number_of_stories_is_present',
            yes_task='update_object_extension_field_value_fees_per_sq_ft_via_http'
        )

        update_object_extension_field_value_fees_per_sq_ft_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_fees_per_sq_ft_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_numeric_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                field_name = "Fees Per Sq Ft",
                salesforce_field_name = "Fees_Per_Sq_Ft__c"
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_updated_number_of_stories_is_present = rail.IfOperator(
            task_id='check_if_updated_number_of_stories_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Updated_of_Stories__c"),
            no_task='check_if_why_we_won_is_present',
            yes_task='update_object_extension_field_value_updated_number_of_stories_via_http'
        )

        update_object_extension_field_value_updated_number_of_stories_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_updated_number_of_stories_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Updated # of Stories",
                field_uri = rail.result("get_details_of_project_in_salesforce")['records'][0]["Updated_of_Stories__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_why_we_won_is_present = rail.IfOperator(
            task_id='check_if_why_we_won_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Why_we_won__c"),
            no_task='check_if_pd_lead_engineer_is_present',
            yes_task='get_object_extension_tag_definition_details_why_we_won_via_http'
        )
        
        get_object_extension_tag_definition_details_why_we_won_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_why_we_won_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Why we won",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_why_we_won_tag_matches_salesforce_why_we_won = rail.IfOperator(
            task_id='check_if_why_we_won_tag_matches_salesforce_why_we_won',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_why_we_won_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Why_we_won__c"]
                                                                   ),
            no_task='create_object_extension_tags_why_we_won_list',
            yes_task='update_object_extension_field_value_why_we_won_via_http'
        )

        update_object_extension_field_value_why_we_won_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_why_we_won_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Why we won",
                field_uri = rail.result("get_object_extension_tag_definition_details_why_we_won_via_http"),
                salesforce_field_name=rail.result("get_details_of_project_in_salesforce")['records'][0]["Why_we_won__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        create_object_extension_tags_why_we_won_list =  rail.PythonOperator(
            task_id='create_object_extension_tags_why_we_won_list',
            python_callable=lambda: custom_function.get_list_for_object_extension(
                rail.result("get_object_extension_tag_definition_details_why_we_won_via_http"),
            )
        )
        
        put_object_extension_tags_via_http_9 = rail.RepliconServiceOperator(
            task_id='put_object_extension_tags_via_http_9',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data= lambda: request_payload.put_object_extension_tags_via_http_payload(rail.result("create_object_extension_tags_why_we_won_list"),
                                                                                     rail.result("get_all_object_extension_field_projects_via_http"),
                                                                                     argument="Why we won",
                                                                                     new_value=rail.result("get_details_of_project_in_salesforce")["records"][0]["Why_we_won__c"]),
            replicon_conn_id=config.replicon_conn_id,
        )

        get_object_extension_tag_definition_details_why_we_won_via_http_1 = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_why_we_won_via_http_1',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Why we won",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        update_object_extension_field_value_why_we_won_via_http_1 = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_why_we_won_via_http_1',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Why we won",
                field_uri = rail.result("get_object_extension_tag_definition_details_why_we_won_via_http_1"),
                salesforce_field_name = rail.result("get_details_of_project_in_salesforce")['records'][0]["Why_we_won__c"]
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_pd_lead_engineer_is_present = rail.IfOperator(
            task_id='check_if_pd_lead_engineer_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="PD_Lead_Engineer__c"),
            no_task='check_if_project_engineer_is_present',
            yes_task='get_details_of_contact_in_salesforce'
        )

        get_details_of_contact_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_details_of_contact_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_contact_in_salesforce_query(
                rail.result("get_details_of_project_in_salesforce")
            ),
        )

        get_object_extension_tag_definition_details_pd_lead_engineer_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_pd_lead_engineer_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PD Lead Engineer",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_pd_lead_engineer_tag_matches_salesforce_pd_lead_engineer = rail.IfOperator(
            task_id='check_if_pd_lead_engineer_tag_matches_salesforce_pd_lead_engineer',
            test=lambda:custom_function.check_if_user_exists(rail.result("get_object_extension_tag_definition_details_pd_lead_engineer_via_http"),
                                                             rail.result("get_details_of_contact_in_salesforce")["records"][0]["Full_Name__c"]),
            no_task='create_object_extension_tags_pd_lead_engineer_list',
            yes_task='get_pd_lead_engineer_uri'
        )

        create_object_extension_tags_pd_lead_engineer_list =  rail.RepliconServiceOperator(
            task_id='create_object_extension_tags_pd_lead_engineer_list',
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PD Lead Engineer",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        enable_draft_uri_for_pd_lead_engineer = rail.RepliconServiceOperator(
            task_id='enable_draft_uri_for_pd_lead_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_pd_lead_engineer_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        
        update_name_of_new_OEF_drop_down_pd_lead_engineer = rail.RepliconServiceOperator(
            task_id='update_name_of_new_OEF_drop_down_pd_lead_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data= lambda: request_payload.update_name_of_new_OEF_drop_down_payload(
                rail.result("create_object_extension_tags_pd_lead_engineer_list"),
                rail.result("get_details_of_contact_in_salesforce"),
                field_name = 'Full_Name__c'
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        publish_draft_pd_lead_engineer = rail.RepliconServiceOperator(
            task_id='publish_draft_pd_lead_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_pd_lead_engineer_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        log_publish_draft_pd_lead_engineer =  rail.PythonOperator(
            task_id='log_publish_draft_pd_lead_engineer',
            python_callable=lambda: rail.result("publish_draft_pd_lead_engineer")['uri']
        )

        
        get_pd_lead_engineer_uri = rail.PythonOperator(
            task_id='get_pd_lead_engineer_uri',
            python_callable=lambda: custom_function.get_uri_if_user_exists(
                rail.result("get_object_extension_tag_definition_details_pd_lead_engineer_via_http"),
                rail.result("get_details_of_contact_in_salesforce")["records"][0]["Full_Name__c"]
            ) if custom_function.check_if_user_exists(
                rail.result("get_object_extension_tag_definition_details_pd_lead_engineer_via_http"),
                rail.result("get_details_of_contact_in_salesforce")["records"][0]["Full_Name__c"]
            )
            else rail.result("log_publish_draft_pd_lead_engineer"),
        )

        update_project_level_OEF_value_in_project_pd_lead_engineer = rail.RepliconServiceOperator(
            task_id='update_project_level_OEF_value_in_project_pd_lead_engineer',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PD Lead Engineer",
                field_uri = rail.result("get_pd_lead_engineer_uri")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_project_engineer_is_present = rail.IfOperator(
            task_id='check_if_project_engineer_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Project_Engineer__c"),
            no_task='check_if_sum_of_project_fees_is_present',
            yes_task='get_details_of_contact_in_salesforce_1'
        )

        get_details_of_contact_in_salesforce_1 = rail.SalesforceQueryOperator2(
            task_id='get_details_of_contact_in_salesforce_1',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_project_engineer_contact_in_salesforce_query(
                rail.result("get_details_of_project_in_salesforce")
            ),
        )

        get_object_extension_tag_definition_details_project_engineer_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_project_engineer_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Project Engineer",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_project_engineer_tag_matches_salesforce_project_engineer = rail.IfOperator(
            task_id='check_if_project_engineer_tag_matches_salesforce_project_engineer',
            test=lambda:custom_function.check_if_user_exists(rail.result("get_object_extension_tag_definition_details_project_engineer_via_http"),
                                                             rail.result("get_details_of_contact_in_salesforce_1")["records"][0]["Full_Name__c"]),
            no_task='create_object_extension_tags_project_engineer_list',
            yes_task='get_project_engineer_uri'
        )

        create_object_extension_tags_project_engineer_list =  rail.RepliconServiceOperator(
            task_id='create_object_extension_tags_project_engineer_list',
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Project Engineer",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        enable_draft_uri_for_project_engineer = rail.RepliconServiceOperator(
            task_id='enable_draft_uri_for_project_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_project_engineer_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        
        update_name_of_new_OEF_drop_down_project_engineer = rail.RepliconServiceOperator(
            task_id='update_name_of_new_OEF_drop_down_project_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data= lambda: request_payload.update_name_of_new_OEF_drop_down_payload(
                rail.result("create_object_extension_tags_project_engineer_list"),
                rail.result("get_details_of_contact_in_salesforce_1"),
                field_name = 'Full_Name__c'
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        publish_draft_project_engineer = rail.RepliconServiceOperator(
            task_id='publish_draft_project_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_project_engineer_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        log_publish_draft_project_engineer =  rail.PythonOperator(
            task_id='log_publish_draft_project_engineer',
            python_callable=lambda: rail.result("publish_draft_project_engineer")['uri']
        )
        
        get_project_engineer_uri = rail.PythonOperator(
            task_id='get_project_engineer_uri',
            python_callable=lambda: custom_function.get_uri_if_user_exists(
                rail.result("get_object_extension_tag_definition_details_project_engineer_via_http"),
                rail.result("get_details_of_contact_in_salesforce_1")["records"][0]["Full_Name__c"]
            ) if custom_function.check_if_user_exists(
                rail.result("get_object_extension_tag_definition_details_project_engineer_via_http"),
                rail.result("get_details_of_contact_in_salesforce_1")["records"][0]["Full_Name__c"]
            )
            else rail.result("log_publish_draft_project_engineer"),
        )

        update_project_level_OEF_value_in_project_engineer = rail.RepliconServiceOperator(
            task_id='update_project_level_OEF_value_in_project_engineer',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Project Engineer",
                field_uri = rail.result("get_project_engineer_uri")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_sum_of_project_fees_is_present = rail.IfOperator(
            task_id='check_if_sum_of_project_fees_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Sum_of_Project_Amounts__c"),
            no_task='check_if_milestone_status_is_present',
            yes_task='update_object_extension_field_value_sum_of_project_fees_via_http'
        )

        update_object_extension_field_value_sum_of_project_fees_via_http = rail.RepliconServiceOperator(
            task_id='update_object_extension_field_value_sum_of_project_fees_via_http',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_numeric_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                field_name = "Sum of Project's Fees",
                salesforce_field_name = "Sum_of_Project_Amounts__c"
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_milestone_status_is_present = rail.IfOperator(
            task_id='check_if_milestone_status_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Milestone_Status__c"),
            no_task='check_if_active_is_present',
            yes_task='get_object_extension_tag_definition_details_milestone_status_via_http'
        )

        get_object_extension_tag_definition_details_milestone_status_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_milestone_status_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Milestone Status",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_milestone_status_tag_matches_salesforce_milestone_status = rail.IfOperator(
            task_id='check_if_milestone_status_tag_matches_salesforce_milestone_status',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_milestone_status_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Milestone_Status__c"]
                                                                   ),
            no_task='create_object_extension_tags_milestone_status_list',
            yes_task='get_milestone_status_uri'
        )

        create_object_extension_tags_milestone_status_list =  rail.RepliconServiceOperator(
            task_id='create_object_extension_tags_milestone_status_list',
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Milestone Status",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        enable_draft_uri_for_milestone_status = rail.RepliconServiceOperator(
            task_id='enable_draft_uri_for_milestone_status',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_milestone_status_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        
        update_name_of_new_OEF_drop_down_milestone_status = rail.RepliconServiceOperator(
            task_id='update_name_of_new_OEF_drop_down_milestone_status',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data= lambda: request_payload.update_name_of_new_OEF_drop_down_payload(
                rail.result("create_object_extension_tags_milestone_status_list"),
                rail.result("get_details_of_project_in_salesforce"),
                field_name = 'Milestone_Status__c'
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        publish_draft_milestone_status = rail.RepliconServiceOperator(
            task_id='publish_draft_milestone_status',
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_milestone_status_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        log_publish_draft_milestone_status = rail.PythonOperator(
            task_id='log_publish_draft_milestone_status',
            python_callable=lambda: rail.result("publish_draft_milestone_status")['uri']
        )

        get_milestone_status_uri = rail.PythonOperator(
            task_id='get_milestone_status_uri',
            python_callable=lambda: custom_function.get_field_uri(
                rail.result("get_object_extension_tag_definition_details_milestone_status_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Milestone_Status__c"]
            ) if custom_function.check_if_field_uri_matches(
                rail.result("get_object_extension_tag_definition_details_milestone_status_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Milestone_Status__c"]
            )
            else rail.result("log_publish_draft_milestone_status"),
        )

        update_project_level_OEF_value_in_milestone_status = rail.RepliconServiceOperator(
            task_id='update_project_level_OEF_value_in_milestone_status',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Milestone Status",
                field_uri = rail.result("get_milestone_status_uri")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_active_is_present = rail.IfOperator(
            task_id='check_if_active_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="Active__c"),
            no_task='check_if_pd_engineer_is_present',
            yes_task='get_object_extension_tag_definition_details_active_via_http'
        )

        get_object_extension_tag_definition_details_active_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_active_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Active?",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_active_tag_matches_salesforce_active = rail.IfOperator(
            task_id='check_if_active_tag_matches_salesforce_active',
            test=lambda: custom_function.check_if_field_uri_matches(rail.result("get_object_extension_tag_definition_details_active_via_http"),
                                                                   rail.result("get_details_of_project_in_salesforce"),
                                                                   argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Active__c"]
                                                                   ),
            no_task='create_object_extension_tags_active_list',
            yes_task='get_active_uri'
        )

        create_object_extension_tags_active_list =  rail.RepliconServiceOperator(
            task_id='create_object_extension_tags_active_list',
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Active?",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        enable_draft_uri_for_active = rail.RepliconServiceOperator(
            task_id='enable_draft_uri_for_active',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_active_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        
        update_name_of_new_OEF_drop_down_active = rail.RepliconServiceOperator(
            task_id='update_name_of_new_OEF_drop_down_active',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data= lambda: request_payload.update_name_of_new_OEF_drop_down_payload(
                rail.result("create_object_extension_tags_active_list"),
                rail.result("get_details_of_project_in_salesforce"),
                field_name = 'Active__c'
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        publish_draft_active = rail.RepliconServiceOperator(
            task_id='publish_draft_active',
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_active_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        log_publish_draft_active =  rail.PythonOperator(
            task_id='log_publish_draft_active',
            python_callable=lambda: rail.result("publish_draft_active")['uri']
        )

        get_active_uri = rail.PythonOperator(
            task_id='get_active_uri',
            python_callable=lambda: custom_function.get_field_uri(
                rail.result("get_object_extension_tag_definition_details_active_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Active__c"]
            ) if custom_function.check_if_field_uri_matches(
                rail.result("get_object_extension_tag_definition_details_active_via_http"),
                rail.result("get_details_of_project_in_salesforce"),
                argument=rail.result("get_details_of_project_in_salesforce")["records"][0]["Active__c"]
            )
            else rail.result("log_publish_draft_active"),
            trigger_rule='none_failed',
        )



        update_project_level_OEF_value_in_active = rail.RepliconServiceOperator(
            task_id='update_project_level_OEF_value_in_active',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "Active?",
                field_uri = rail.result("get_active_uri")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_pd_engineer_is_present = rail.IfOperator(
            task_id='check_if_pd_engineer_is_present',
            test=lambda: custom_function.check_if_argument_exists(rail.result("get_details_of_project_in_salesforce"), argument="PD_Engineer__c"),
            no_task='finish_task',
            yes_task='get_details_of_contact_in_salesforce_2'
        )

        get_details_of_contact_in_salesforce_2 = rail.SalesforceQueryOperator2(
            task_id='get_details_of_contact_in_salesforce_2',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_pd_engineer_contact_in_salesforce_query(
                rail.result("get_details_of_project_in_salesforce")
            ),
        )

        get_object_extension_tag_definition_details_pd_engineer_via_http = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_pd_engineer_via_http',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PD Engineer",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        check_if_pd_engineer_tag_matches_salesforce_project_engineer = rail.IfOperator(
            task_id='check_if_pd_engineer_tag_matches_salesforce_project_engineer',
            test=lambda:custom_function.check_if_user_exists(rail.result("get_object_extension_tag_definition_details_pd_engineer_via_http"),
                                                             rail.result("get_details_of_contact_in_salesforce_2")["records"][0]["Full_Name__c"]),
            no_task='create_object_extension_tags_pd_engineer_list',
            yes_task='get_pd_engineer_uri'
        )

        create_object_extension_tags_pd_engineer_list =  rail.RepliconServiceOperator(
            task_id='create_object_extension_tags_pd_engineer_list',
            endpoint="/services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data= lambda: request_payload.get_object_extension_tag_definition_details_via_http_payload(
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PD Engineer",
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        enable_draft_uri_for_pd_engineer = rail.RepliconServiceOperator(
            task_id='enable_draft_uri_for_pd_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_pd_engineer_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )
        
        
        update_name_of_new_OEF_drop_down_pd_engineer = rail.RepliconServiceOperator(
            task_id='update_name_of_new_OEF_drop_down_pd_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateName",
            data= lambda: request_payload.update_name_of_new_OEF_drop_down_payload(
                rail.result("create_object_extension_tags_pd_engineer_list"),
                rail.result("get_details_of_contact_in_salesforce_2"),
                field_name = 'Full_Name__c'
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        publish_draft_pd_engineer = rail.RepliconServiceOperator(
            task_id='publish_draft_pd_engineer',
            endpoint="/services/ObjectExtensionTagService1.svc/PublishDraft",
            data= lambda: request_payload.enable_draft_uri_payload(
                rail.result("create_object_extension_tags_pd_engineer_list"),
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        log_publish_draft_pd_engineer =  rail.PythonOperator(
            task_id='log_publish_draft_pd_engineer',
            python_callable=lambda: rail.result("publish_draft_pd_engineer")['uri']
        )
        
        get_pd_engineer_uri = rail.PythonOperator(
            task_id='get_pd_engineer_uri',
            python_callable=lambda: custom_function.get_uri_if_user_exists(
                rail.result("get_object_extension_tag_definition_details_pd_engineer_via_http"),
                rail.result("get_details_of_contact_in_salesforce_2")["records"][0]["Full_Name__c"]
            ) if custom_function.check_if_user_exists(
                rail.result("get_object_extension_tag_definition_details_pd_engineer_via_http"),
                rail.result("get_details_of_contact_in_salesforce_2")["records"][0]["Full_Name__c"]
            )
            else rail.result("log_publish_draft_pd_engineer"),
        )

        update_project_level_OEF_value_in_pd_engineer = rail.RepliconServiceOperator(
            task_id='update_project_level_OEF_value_in_pd_engineer',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data= lambda: request_payload.update_object_extension_field_value_project_via_http_1_payload(
                rail.result("extract_data"),
                rail.result("get_all_object_extension_field_projects_via_http"),
                field_name = "PD Engineer",
                field_uri = rail.result("get_pd_engineer_uri")
            ),
            replicon_conn_id=config.replicon_conn_id,
        )

        finish_task = rail.EmptyOperator(
            task_id='finish_task'
        )

        extract_data >> get_details_of_project_in_salesforce >> get_all_object_extension_field_projects_via_http >> get_all_custom_field >> log_opp_close_date_custom_uri >> update_date_value>> check_if_project_number_is_present
        check_if_project_number_is_present >> rail.Label("Yes") >> update_object_extension_field_value_project_via_http >> opp_id_is_present
        check_if_project_number_is_present >> rail.Label("No") >> opp_id_is_present
        opp_id_is_present >> rail.Label("Yes") >> update_object_extension_field_value_opportunity_id_via_http >> update_object_extension_field_value_opportunity_name_via_http >> check_if_opp_category_is_present
        opp_id_is_present >> rail.Label("No") >> finish_task
        
        check_if_opp_category_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_opp_category_via_http >> check_if_opp_category_tag_matches_salesforce_opp_category
        check_if_opp_category_is_present >> rail.Label("No") >> check_if_opp_state_is_present
        check_if_opp_category_tag_matches_salesforce_opp_category >> rail.Label("Yes") >> update_object_extension_field_value_opportunity_category_via_http >> check_if_opp_state_is_present
        check_if_opp_category_tag_matches_salesforce_opp_category >> rail.Label("No") >> create_object_extension_tags_opp_category_list >> put_object_extension_tags_via_http >> get_object_extension_tag_definition_details_opp_category_via_http_1 >> update_object_extension_field_value_opportunity_category_via_http_1 >> check_if_opp_state_is_present

        check_if_opp_state_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_opp_state_via_http >> check_if_opp_state_tag_matches_salesforce_opp_state
        check_if_opp_state_is_present >> rail.Label("No") >> check_if_owner_id_is_present
        check_if_opp_state_tag_matches_salesforce_opp_state >> rail.Label("Yes") >> update_object_extension_field_value_opportunity_state_via_http >> check_if_owner_id_is_present
        check_if_opp_state_tag_matches_salesforce_opp_state >> rail.Label("No") >> create_object_extension_tags_opp_state_list >> put_object_extension_tags_via_http_1 >> get_object_extension_tag_definition_details_opp_state_via_http_1 >> update_object_extension_field_value_opportunity_state_via_http_1 >> check_if_owner_id_is_present
    
        check_if_owner_id_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_owner_id_via_http >> check_if_owner_id_tag_matches_salesforce_owner_id
        check_if_owner_id_is_present >> rail.Label("No") >> check_if_r_value_is_present
        check_if_owner_id_tag_matches_salesforce_owner_id >> rail.Label("Yes") >> update_object_extension_field_value_owner_id_via_http >> check_if_r_value_is_present
        check_if_owner_id_tag_matches_salesforce_owner_id >> rail.Label("No") >> create_object_extension_tags_owner_id_list >> put_object_extension_tags_via_http_2 >> get_object_extension_tag_definition_details_owner_id_via_http_1 >> update_object_extension_field_value_owner_id_via_http_1 >> check_if_r_value_is_present

        check_if_r_value_is_present >> rail.Label("Yes") >> update_object_extension_field_value_r_value_via_http >> check_if_sector_is_present
        check_if_r_value_is_present >> rail.Label("No") >> check_if_sector_is_present

        check_if_sector_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_sector_via_http >> check_if_sector_tag_matches_salesforce_sector
        check_if_sector_is_present >> rail.Label("No") >> check_if_software_is_present
        check_if_sector_tag_matches_salesforce_sector >> rail.Label("Yes") >> update_object_extension_field_value_sector_via_http >> check_if_software_is_present
        check_if_sector_tag_matches_salesforce_sector >> rail.Label("No") >> create_object_extension_tags_sector_list >> put_object_extension_tags_via_http_3 >> get_object_extension_tag_definition_details_sector_via_http_1 >> update_object_extension_field_value_sector_via_http_1 >> check_if_software_is_present

        check_if_software_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_software_via_http >> check_if_software_tag_matches_salesforce_software
        check_if_software_is_present >> rail.Label("No") >> check_if_pc_reqd_is_present
        check_if_software_tag_matches_salesforce_software >> rail.Label("Yes") >> update_object_extension_field_value_software_via_http >> check_if_pc_reqd_is_present
        check_if_software_tag_matches_salesforce_software >> rail.Label("No") >> create_object_extension_tags_software_list >> put_object_extension_tags_via_http_4 >> get_object_extension_tag_definition_details_software_via_http_1 >> update_object_extension_field_value_software_via_http_1 >> check_if_pc_reqd_is_present

        check_if_pc_reqd_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_pc_reqd_via_http >> check_if_pc_reqd_tag_matches_salesforce_pc_reqd
        check_if_pc_reqd_is_present >> rail.Label("No") >> check_if_opp_stage_is_present
        check_if_pc_reqd_tag_matches_salesforce_pc_reqd >> rail.Label("Yes") >> update_object_extension_field_value_pc_reqd_via_http >> check_if_opp_stage_is_present
        check_if_pc_reqd_tag_matches_salesforce_pc_reqd >> rail.Label("No") >> create_object_extension_tags_pc_reqd_list >> put_object_extension_tags_via_http_5 >> get_object_extension_tag_definition_details_pc_reqd_via_http_1 >> update_object_extension_field_value_pc_reqd_via_http_1 >> check_if_opp_stage_is_present

        check_if_opp_stage_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_opp_stage_via_http >> check_if_opp_stage_tag_matches_salesforce_opp_stage
        check_if_opp_stage_is_present >> rail.Label("No") >> check_if_updated_approx_sq_ft_is_present
        check_if_opp_stage_tag_matches_salesforce_opp_stage >> rail.Label("Yes") >> update_object_extension_field_value_opp_stage_via_http >> check_if_updated_approx_sq_ft_is_present
        check_if_opp_stage_tag_matches_salesforce_opp_stage >> rail.Label("No") >> create_object_extension_tags_opp_stage_list >> put_object_extension_tags_via_http_6 >> get_object_extension_tag_definition_details_opp_stage_via_http_1 >> update_object_extension_field_value_opp_stage_via_http_1 >> check_if_updated_approx_sq_ft_is_present

        check_if_updated_approx_sq_ft_is_present >> rail.Label("Yes") >> update_object_extension_field_value_updated_approx_sq_ft_via_http >> check_if_primary_design_criteria_is_present
        check_if_updated_approx_sq_ft_is_present >> rail.Label("No") >> check_if_primary_design_criteria_is_present

        check_if_primary_design_criteria_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_primary_design_criteria_via_http >> check_if_primary_design_criteria_tag_matches_salesforce_primary_design_criteria
        check_if_primary_design_criteria_is_present >> rail.Label("No") >> check_if_updated_number_of_sp_joints_is_present
        check_if_primary_design_criteria_tag_matches_salesforce_primary_design_criteria >> rail.Label("Yes") >> update_object_extension_field_value_primary_design_criteria_via_http >> check_if_updated_number_of_sp_joints_is_present
        check_if_primary_design_criteria_tag_matches_salesforce_primary_design_criteria >> rail.Label("No") >> create_object_extension_tags_primary_design_criteria_list >> put_object_extension_tags_via_http_7 >> get_object_extension_tag_definition_details_primary_design_criteria_via_http_1 >> update_object_extension_field_value_primary_design_criteria_via_http_1 >> check_if_updated_number_of_sp_joints_is_present

        check_if_updated_number_of_sp_joints_is_present >> rail.Label("Yes") >> update_object_extension_field_value_updated_number_of_sp_joints_via_http >> check_if_updated_qty_of_bldgs_is_present
        check_if_updated_number_of_sp_joints_is_present >> rail.Label("No") >> check_if_updated_qty_of_bldgs_is_present
        
        check_if_updated_qty_of_bldgs_is_present >> rail.Label("Yes") >> update_object_extension_field_value_updated_qty_of_bldgs_via_http >> check_if_sp_bolted_is_present
        check_if_updated_qty_of_bldgs_is_present >> rail.Label("No") >> check_if_sp_bolted_is_present
    
        check_if_sp_bolted_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_sp_bolted_via_http >> check_if_sp_bolted_tag_matches_salesforce_sp_bolted
        check_if_sp_bolted_is_present >> rail.Label("No") >> check_if_fees_per_sq_ft_is_present
        check_if_sp_bolted_tag_matches_salesforce_sp_bolted >> rail.Label("Yes") >> update_object_extension_field_value_sp_bolted_via_http >> check_if_fees_per_sq_ft_is_present
        check_if_sp_bolted_tag_matches_salesforce_sp_bolted >> rail.Label("No") >> create_object_extension_tags_sp_bolted_list >> put_object_extension_tags_via_http_8 >> get_object_extension_tag_definition_details_sp_bolted_via_http_1 >> update_object_extension_field_value_sp_bolted_via_http_1 >> check_if_fees_per_sq_ft_is_present

        check_if_fees_per_sq_ft_is_present >> rail.Label("Yes") >> update_object_extension_field_value_fees_per_sq_ft_via_http >> check_if_updated_number_of_stories_is_present
        check_if_fees_per_sq_ft_is_present >> rail.Label("No") >> check_if_updated_number_of_stories_is_present

        check_if_updated_number_of_stories_is_present >> rail.Label("Yes") >> update_object_extension_field_value_updated_number_of_stories_via_http >> check_if_why_we_won_is_present
        check_if_updated_number_of_stories_is_present >> rail.Label("No") >> check_if_why_we_won_is_present

        check_if_why_we_won_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_why_we_won_via_http >> check_if_why_we_won_tag_matches_salesforce_why_we_won
        check_if_why_we_won_is_present >> rail.Label("No") >> check_if_pd_lead_engineer_is_present
        check_if_why_we_won_tag_matches_salesforce_why_we_won >> rail.Label("Yes") >> update_object_extension_field_value_why_we_won_via_http >> check_if_pd_lead_engineer_is_present
        check_if_why_we_won_tag_matches_salesforce_why_we_won >> rail.Label("No") >> create_object_extension_tags_why_we_won_list >> put_object_extension_tags_via_http_9 >> get_object_extension_tag_definition_details_why_we_won_via_http_1 >> update_object_extension_field_value_why_we_won_via_http_1 >> check_if_pd_lead_engineer_is_present
        
        check_if_pd_lead_engineer_is_present >> rail.Label("Yes") >> get_details_of_contact_in_salesforce >> get_object_extension_tag_definition_details_pd_lead_engineer_via_http >> check_if_pd_lead_engineer_tag_matches_salesforce_pd_lead_engineer
        check_if_pd_lead_engineer_is_present >> rail.Label("No") >> check_if_project_engineer_is_present
        check_if_pd_lead_engineer_tag_matches_salesforce_pd_lead_engineer >> rail.Label("No") >> create_object_extension_tags_pd_lead_engineer_list >> enable_draft_uri_for_pd_lead_engineer >> update_name_of_new_OEF_drop_down_pd_lead_engineer >> publish_draft_pd_lead_engineer >> log_publish_draft_pd_lead_engineer >> get_pd_lead_engineer_uri >> update_project_level_OEF_value_in_project_pd_lead_engineer >> check_if_project_engineer_is_present
        check_if_pd_lead_engineer_tag_matches_salesforce_pd_lead_engineer >> rail.Label("Yes") >> get_pd_lead_engineer_uri >> update_project_level_OEF_value_in_project_pd_lead_engineer >> check_if_project_engineer_is_present

        check_if_project_engineer_is_present >> rail.Label("Yes") >> get_details_of_contact_in_salesforce_1 >> get_object_extension_tag_definition_details_project_engineer_via_http >> check_if_project_engineer_tag_matches_salesforce_project_engineer
        check_if_project_engineer_is_present >> rail.Label("No") >> check_if_sum_of_project_fees_is_present
        check_if_project_engineer_tag_matches_salesforce_project_engineer >> rail.Label("Yes") >> create_object_extension_tags_project_engineer_list >> enable_draft_uri_for_project_engineer >> update_name_of_new_OEF_drop_down_project_engineer >> publish_draft_project_engineer >> log_publish_draft_project_engineer >> get_project_engineer_uri >> update_project_level_OEF_value_in_project_engineer >> check_if_sum_of_project_fees_is_present
        check_if_project_engineer_tag_matches_salesforce_project_engineer >> rail.Label("No") >>  get_project_engineer_uri >> update_project_level_OEF_value_in_project_engineer >> check_if_sum_of_project_fees_is_present

        check_if_sum_of_project_fees_is_present >> rail.Label("Yes") >> update_object_extension_field_value_sum_of_project_fees_via_http >> check_if_milestone_status_is_present
        check_if_sum_of_project_fees_is_present >> rail.Label("No") >> check_if_milestone_status_is_present

        check_if_milestone_status_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_milestone_status_via_http >> check_if_milestone_status_tag_matches_salesforce_milestone_status
        check_if_milestone_status_is_present >> rail.Label("No") >> check_if_active_is_present
        check_if_milestone_status_tag_matches_salesforce_milestone_status >> rail.Label("No") >> create_object_extension_tags_milestone_status_list >> enable_draft_uri_for_milestone_status >> update_name_of_new_OEF_drop_down_milestone_status >> publish_draft_milestone_status >> log_publish_draft_milestone_status >> get_milestone_status_uri >> update_project_level_OEF_value_in_milestone_status >> check_if_active_is_present
        check_if_milestone_status_tag_matches_salesforce_milestone_status >> rail.Label("Yes") >> get_milestone_status_uri >> update_project_level_OEF_value_in_milestone_status >> check_if_active_is_present

        check_if_active_is_present >> rail.Label("Yes") >> get_object_extension_tag_definition_details_active_via_http >> check_if_active_tag_matches_salesforce_active
        check_if_active_is_present >> rail.Label("No") >> check_if_pd_engineer_is_present
        check_if_active_tag_matches_salesforce_active >> rail.Label("No") >> create_object_extension_tags_active_list >> enable_draft_uri_for_active >> update_name_of_new_OEF_drop_down_active >> publish_draft_active >> log_publish_draft_active >> get_active_uri >> update_project_level_OEF_value_in_active >> check_if_pd_engineer_is_present
        check_if_active_tag_matches_salesforce_active >> rail.Label("Yes") >> get_active_uri >> update_project_level_OEF_value_in_active >> check_if_pd_engineer_is_present

        check_if_pd_engineer_is_present >> rail.Label("Yes") >> get_details_of_contact_in_salesforce_2 >> get_object_extension_tag_definition_details_pd_engineer_via_http >> check_if_pd_engineer_tag_matches_salesforce_project_engineer
        check_if_pd_engineer_is_present >> rail.Label("No") >> finish_task
        check_if_pd_engineer_tag_matches_salesforce_project_engineer >> rail.Label("No") >> create_object_extension_tags_pd_engineer_list >> enable_draft_uri_for_pd_engineer >> update_name_of_new_OEF_drop_down_pd_engineer >> publish_draft_pd_engineer >> log_publish_draft_pd_engineer >> get_pd_engineer_uri >> update_project_level_OEF_value_in_pd_engineer >> finish_task
        check_if_pd_engineer_tag_matches_salesforce_project_engineer >> rail.Label("Yes") >> get_pd_engineer_uri >> update_project_level_OEF_value_in_pd_engineer >> finish_task

    return dag

# Create child DAG for each instance
rail.for_each_instance(create_child_dag)