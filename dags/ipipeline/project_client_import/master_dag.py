# Master DAG for iPipeline Salesforce to Replicon Integration

from datetime import timedelta
from pendulum import now, datetime as dt
from airflow.models import Variable
import rail

from ipipeline.project_client_import.utils import request_payload, custom_methods, response_filter


def create_main_dag(config):
    """
    Create the master DAG for Salesforce to Replicon sync
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'iPipeline Salesforce to Replicon Master Sync - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=config.master_dag_interval),
        max_active_runs=config.max_active_run_master,
    ) as dag:

        log_job_start_time = rail.PythonOperator(
            task_id='log_job_start_time',
            python_callable=lambda: now(
                config.time_zone).strftime("%Y-%m-%dT%H:%M:%S%z")
        )

        check_instance = rail.IfOperator(
            task_id='check_instance',
            test=lambda: config.instance == 'trial' and (Variable.get(
                config.bypass_trial_instance_check, default_var='False')).lower() != 'true',
            yes_task='view_dagrun_conf',
            no_task='log_lookback_period_start_timestamp'
        )

        # View incoming configuration
        view_dagrun_conf = rail.ViewDagRunConfOperator(
            task_id='view_dagrun_conf'
        )

        get_account_opportunities_data_from_dag_run_conf = rail.PythonOperator(
            task_id='get_account_opportunities_data_from_dag_run_conf',
            python_callable=lambda dag_run: {
                'accounts': dag_run.conf['salesforce_accounts'],
                'opportunities': dag_run.conf['salesforce_opportunities']
            }
        )

        log_lookback_period_start_timestamp = rail.PythonOperator(
            task_id='log_lookback_period_start_timestamp',
            python_callable=lambda: request_payload.get_lookback_period_start_timestamp(
                config)
        )

        new_created_or_updated_account = rail.SalesforceQueryOperator2(
            task_id="new_created_or_updated_account",
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_payload.get_new_created_or_updated_account_query(
                config)
        )

        new_created_or_updated_opportunity = rail.SalesforceQueryOperator2(
            task_id="new_created_or_updated_opportunity",
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_payload.get_new_created_or_updated_opportunity_query(
                config)
        )

        update_variables_lookback_timestamps = rail.PythonOperator(
            task_id='update_variables_lookback_timestamps',
            python_callable=lambda: request_payload.set_timestamps_based_on_last_modified_record(
                config)
        )

        log_count_accounts_opportunities_records = rail.PythonOperator(
            task_id='log_count_accounts_opportunities_records',
            python_callable=lambda: {
                'accounts_records_count': rail.result('new_created_or_updated_account', 'length') if rail.result('new_created_or_updated_account') else len(rail.result(
                    'get_account_opportunities_data_from_dag_run_conf')['accounts']),
                'opportunities_records_count': rail.result('new_created_or_updated_opportunity', 'length') if rail.result(
                    'new_created_or_updated_opportunity') else len(rail.result(
                        'get_account_opportunities_data_from_dag_run_conf')['opportunities']),
            }
        )

        if_accounts_opportunities_count_0 = rail.IfOperator(
            task_id='if_accounts_opportunities_count_0',
            test=lambda dag_run: (int(rail.result(
                'log_count_accounts_opportunities_records')['accounts_records_count']) + int(rail.result(
                    'log_count_accounts_opportunities_records')['opportunities_records_count'])) == 0,
            yes_task='finish',
            no_task='get_required_permission_set_uris'
        )

        get_required_permission_set_uris = rail.RepliconServiceOperator(
            task_id='get_required_permission_set_uris',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda res: {
                'client_rep_permission_uri': rail.find_first_by_attr_and_get_attr(res, 'name', 'Client Representative', 'uri', ''),
                'project_manager_permission_uri':  rail.find_first_by_attr_and_get_attr(res, 'name', 'Project Manager', 'uri', ''),
                'client_manager_permission_uri': rail.find_first_by_attr_and_get_attr(res, 'name', 'Client Manager', 'uri', ''),
            }
        )

        if_accounts_record_present = rail.IfOperator(
            task_id='if_accounts_record_present',
            test=lambda: int(rail.result(
                'log_count_accounts_opportunities_records')['accounts_records_count']) > 0,
            yes_task='client_import_log',
            no_task='if_opportunity_records_present'
        )

        # Create log for client import tracking
        client_import_log = rail.CreateLogOperator(
            task_id='client_import_log',
        )

        create_collection_new_updated_accounts = rail.CreateCollectionOperator(
            task_id='create_collection_new_updated_accounts',
            source=lambda: rail.result('new_created_or_updated_account').get('records', '') if rail.result('new_created_or_updated_account') else rail.result(
                'get_account_opportunities_data_from_dag_run_conf')['accounts'],
            name='accounts_data_from_salesforce'
        )

        # Query accounts with missing IDs
        query_accounts_with_missing_id = rail.QueryCollectionOperator(
            task_id='query_accounts_with_missing_id',
            query="""SELECT * FROM accounts_data_from_salesforce WHERE (Id IS NULL OR Id = '')""",
            name='accounts_with_missing_id'
        )

        if_accounts_with_missing_id = rail.IfOperator(
            task_id='if_accounts_with_missing_id',
            test='{{ result("query_accounts_with_missing_id", "length") > 0 }}',
            yes_task='log_missing_id_accounts',
            no_task='query_accounts_with_id'
        )

        # Log accounts with missing IDs as errors
        log_missing_id_accounts = rail.WriteLogOperator(
            task_id='log_missing_id_accounts',
            log='{{ result("client_import_log") }}',
            items='{{ result("query_accounts_with_missing_id") }}',
            severity='Exception',
            message='ID is missing',
            properties=lambda item: {
                'name': item.get('Name'),
                'id': item.get('Id'),
                'type': 'account',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'ID is blank for account'
            }
        )

        # Query accounts with valid IDs for processing
        query_accounts_with_id = rail.QueryCollectionOperator(
            task_id='query_accounts_with_id',
            query="""SELECT * FROM accounts_data_from_salesforce WHERE Id IS NOT NULL AND Id != '' """,
            name='accounts_with_id'
        )

        get_all_countries_in_replicon = rail.RepliconServiceOperator(
            task_id='get_all_countries_in_replicon',
            endpoint='/services/InternationalizationService1.svc/GetAllCountries',
        )

        get_all_clients = rail.RepliconServicePageOperator(
            task_id='get_all_clients',
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.payload_to_get_all_replicon_clients,
            page_handler=custom_methods.page_handler,
            all_result_data_handler=response_filter.filter_client_data
        )

        create_replicon_client_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_client_collection',
            source=lambda: rail.result('get_all_clients'),
            name='replicon_clients'
        )

        query_valid_accounts_left_join_replicon_clients = rail.QueryCollectionOperator(
            task_id='query_valid_accounts_left_join_replicon_clients',
            query="""SELECT * 
                FROM  
                    accounts_with_id 
                LEFT JOIN 
                    replicon_clients 
                ON 
                    LOWER(accounts_with_id.Id) = LOWER(replicon_clients.client_code)""",
            name='final_accounts_to_process'
        )

        if_final_accounts_to_process = rail.IfOperator(
            task_id='if_final_accounts_to_process',
            test='{{ result("query_valid_accounts_left_join_replicon_clients", "length") > 0 }}',
            yes_task='dummy_process_clients',
            no_task='if_opportunity_records_present'
        )

        dummy_process_clients = rail.EmptyOperator(
            task_id="dummy_process_clients"
        )

        trigger_process_clients = rail.trigger_parallel_dagrun(
            task_id="trigger_process_clients",
            trigger_dag_id=config.process_client_child_dag_id,
            parallel_count=config.parallel_count_process_clients,
            items=lambda: rail.result(
                "query_valid_accounts_left_join_replicon_clients"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'client_country_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_countries_in_replicon'), 'displayText', item.get('ShippingCountry'), 'uri') if item.get('ShippingCountry') else None,
                'billing_country_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_countries_in_replicon'), 'displayText', item.get('BillingCountry'), 'uri') if item.get('BillingCountry') else None,
                'intacct_id_udf': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_client_custom_fields'), 'displayText', 'Intacct ID') if rail.result('get_all_client_custom_fields') else None,
            }
        )

        # Collect all client processing DAG run IDs for monitoring and log gathering
        get_process_clients_dag_ids = rail.PythonOperator(
            task_id='get_process_clients_dag_ids',
            python_callable=lambda: custom_methods.get_process_dag_ids(
                config.parallel_count_process_clients, 'trigger_process_clients'),
            show_return_value_in_logs=False
        )

        gather_client_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_logs',
            dag_runs='{{ result("get_process_clients_dag_ids") }}',
            dagrun_task_id='create_client_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        if_opportunity_records_present = rail.IfOperator(
            task_id='if_opportunity_records_present',
            test=lambda: int(rail.result(
                'log_count_accounts_opportunities_records')['opportunities_records_count']) > 0,
            yes_task='project_import_log',
            no_task='dummy_trigger_process_log_generation'
        )

        # Create log for project import tracking
        project_import_log = rail.CreateLogOperator(
            task_id='project_import_log',
        )

        create_collection_new_updated_opportunities = rail.CreateCollectionOperator(
            task_id='create_collection_new_updated_opportunities',
            source=lambda: rail.result('new_created_or_updated_opportunity').get('records', '') if rail.result('new_created_or_updated_opportunity') else rail.result(
                'get_account_opportunities_data_from_dag_run_conf')['opportunities'],
            name='opportunities_data_from_salesforce'
        )

        # Query opportunities with missing IDs for validation
        query_opportunities_missing_project_code = rail.QueryCollectionOperator(
            task_id='query_opportunities_missing_project_code',
            query="""SELECT * FROM  opportunities_data_from_salesforce WHERE (Project_Code__c IS NULL OR Project_Code__c = '')""",
            name='opportunities_with_missing_id'
        )

        # Check if there are any opportunities with missing IDs
        if_opportunities_with_missing_project_code = rail.IfOperator(
            task_id='if_opportunities_with_missing_project_code',
            test='{{ result("query_opportunities_missing_project_code", "length") > 0 }}',
            yes_task='log_missing_project_code_opportunities',
            no_task='query_opportunities_with_project_code'
        )

        # Log opportunities with missing IDs as exceptions
        log_missing_project_code_opportunities = rail.WriteLogOperator(
            task_id='log_missing_project_code_opportunities',
            log='{{ result("project_import_log") }}',
            items='{{ result("query_opportunities_missing_project_code") }}',
            severity='Exception',
            message='Project code is missing',
            properties=lambda item: {
                'name': item.get('Name'),
                'id': item.get('Id'),
                'type': 'opportunity',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Project code is blank for opportunity'
            }
        )

        # Query opportunities with valid IDs for further processing
        query_opportunities_with_project_code = rail.QueryCollectionOperator(
            task_id='query_opportunities_with_project_code',
            query="""SELECT * FROM  opportunities_data_from_salesforce WHERE Project_Code__c IS NOT NULL AND Project_Code__c != '' """,
            name='opportunities_with_project_code'
        )

        if_final_opportunities_to_process = rail.IfOperator(
            task_id='if_final_opportunities_to_process',
            test='{{ result("query_opportunities_with_project_code", "length") > 0 }}',
            yes_task='get_all_required_currencies',
            no_task='dummy_trigger_process_log_generation'
        )

        get_all_required_currencies = rail.RepliconServiceOperator(
            task_id='get_all_required_currencies',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data_handler=lambda res: [
                {
                    'currency_iso_code': currency_iso_code,
                    'name': name,
                    'uri': rail.find_first_by_attr_and_get_attr(res, 'name', name, 'uri')
                } for currency_iso_code, name in config.CURRENCIES_MAP.items()] if res else []
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.cost_center_list_service_get_data_payload(),
            data_handler=lambda response: response_filter.extract_cost_center_data(response),
        )

        get_template_project_details = rail.RepliconServiceOperator(
            task_id='get_template_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda: {
                "projects": [
                    {
                        "code": (config.COPY_TEMPLATE_PROJECT_DETAILS).get('code')
                    }
                ]
            },
            data_handler=lambda resp: resp[0]['projectDetails'] if resp and resp[0].get(
                'projectDetails') else None
        )

        get_project_engagement_oef_uri = rail.RepliconServiceOperator(
            task_id='get_project_engagement_oef_uri',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda res: {
                'engagement_type_oef_uri': rail.find_first_by_attr_and_get_attr(res, 'name', 'Engagement Type', 'uri'),
                'engagement_stage_oef_uri': rail.find_first_by_attr_and_get_attr(res, 'name', 'Engagement Stage', 'uri'),
            }
        )

        get_all_client_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_client_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data = {
                "objectUri": "urn:replicon:object-type:client"
            },
            data_handler=lambda res: list(map(lambda elem: {
                'displayText': elem['displayText'],
                'isEnabled': elem['isEnabled'],
                'uri': elem['uri'],
                **({'textConfiguration': elem['textConfiguration']} if 'textConfiguration' in elem else {})
            }, res)) if res else []
        )

        get_all_engagement_type_dd_values = rail.RepliconServiceOperator(
            task_id='get_all_engagement_type_dd_values',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_project_engagement_oef_uri')['engagement_type_oef_uri']
            },
            data_handler=lambda res: list(map(lambda tag: {
                'tag_name': tag['name'],
                'tag_uri': tag['uri'],
                'tag_is_enabled': tag['isEnabled']
            }, res['tags'])) if res and res['tags'] else []
        )

        get_all_engagement_stage_dd_values = rail.RepliconServiceOperator(
            task_id='get_all_engagement_stage_dd_values',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_project_engagement_oef_uri')['engagement_stage_oef_uri']
            },
            data_handler=lambda res: list(map(lambda tag: {
                'tag_name': tag['name'],
                'tag_uri': tag['uri'],
                'tag_is_enabled': tag['isEnabled']
            }, res['tags'])) if res and res['tags'] else []
        )

        # Retrieve all existing projects from Replicon for comparison
        get_all_projects_in_replicon = rail.RepliconServicePageOperator(
            task_id='get_all_projects_in_replicon',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=request_payload.payload_to_get_all_replicon_projects,
            page_handler=custom_methods.page_handler,
            all_result_data_handler=response_filter.filter_project_data
        )

        create_replicon_project_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_project_collection',
            source=lambda: rail.result('get_all_projects_in_replicon'),
            name='replicon_projects'
        )

        get_all_updated_clients = rail.RepliconServicePageOperator(
            task_id='get_all_updated_clients',
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.payload_to_get_all_replicon_clients,
            page_handler=custom_methods.page_handler,
            all_result_data_handler=response_filter.filter_client_data
        )

        create_updated_replicon_client_collection = rail.CreateCollectionOperator(
            task_id='create_updated_replicon_client_collection',
            source=lambda: rail.result('get_all_updated_clients'),
            name='replicon_updated_clients'
        )

        query_valid_opportunities_left_join_replicon_projects = rail.QueryCollectionOperator(
            task_id='query_valid_opportunities_left_join_replicon_projects',
            query="""SELECT *
                FROM
                    opportunities_with_project_code 
                LEFT JOIN
                    replicon_projects ON LOWER(opportunities_with_project_code.Project_Code__c) = LOWER(replicon_projects.project_code)
                LEFT JOIN
                    replicon_updated_clients ON LOWER(opportunities_with_project_code.AccountId) = LOWER(replicon_updated_clients.client_code)""",
            name='final_opportunities_to_process'
        )

        dummy_process_projects = rail.EmptyOperator(
            task_id="dummy_process_projects"
        )

        # Trigger parallel DAGs to process projects (create or update in Replicon)
        trigger_process_projects = rail.trigger_parallel_dagrun(
            task_id="trigger_process_projects",
            trigger_dag_id=config.process_project_child_dag_id,
            parallel_count=config.parallel_count_process_projects,
            items=lambda: rail.result(
                "query_valid_opportunities_left_join_replicon_projects"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'currency_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_required_currencies'), 'currency_iso_code', item['CurrencyIsoCode'], 'uri'),
                'client_rep_permission_set_uri': rail.result('get_required_permission_set_uris')['client_rep_permission_uri'],
                'template_project_uri': rail.result('get_template_project_details').get('uri'),
                'engagement_type_oef_uri': rail.result('get_project_engagement_oef_uri')['engagement_type_oef_uri'],
                'matching_engagement_type_from_oef_dd': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_engagement_type_dd_values'), 'tag_name', item['Engagement_Type__c']) if rail.result(
                    'get_all_engagement_type_dd_values') else {},
                'engagement_stage_oef_uri': rail.result('get_project_engagement_oef_uri')['engagement_stage_oef_uri'],
                'matching_engagement_stage_from_oef_dd': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_engagement_stage_dd_values'), 'tag_name', item['StageName']) if rail.result(
                    'get_all_engagement_stage_dd_values') else {},
                'matching_cost_center': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_cost_centers'), 'cost_center', item['Engagement_Cost_Center__c'])
            }
        )

        # Collect all project processing DAG run IDs for monitoring and log gathering
        get_process_projects_dag_ids = rail.PythonOperator(
            task_id='get_process_projects_dag_ids',
            python_callable=lambda: custom_methods.get_process_dag_ids(
                config.parallel_count_process_projects, 'trigger_process_projects'),
            show_return_value_in_logs=False
        )

        gather_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_logs',
            dag_runs='{{ result("get_process_projects_dag_ids") }}',
            dagrun_task_id='create_project_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        dummy_trigger_process_log_generation = rail.EmptyOperator(
            task_id="dummy_trigger_process_log_generation"
        )

        trigger_process_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_process_log_generation',
            trigger_dag_id=config.process_log_generation_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'client_logs': rail.result('gather_client_logs') if rail.result('gather_client_logs') else [],
                'master_client_log': rail.result('client_import_log') if rail.result('client_import_log') else [],
                'project_logs': rail.result('gather_project_logs') if rail.result('gather_project_logs') else [],
                'master_project_log': rail.result('project_import_log') if rail.result('project_import_log') else [],
                'total_records_accounts': rail.result('log_count_accounts_opportunities_records')['accounts_records_count'],
                'total_records_opportunities': rail.result('log_count_accounts_opportunities_records')['opportunities_records_count'],
                'job_start_time': rail.result('log_job_start_time')
            }
        )

        # Wait for log generation to complete before finishing the workflow
        wait_for_trigger_process_log_generation = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_process_log_generation',
            dag_runs='{{ result("trigger_process_log_generation") }}',
            execution_timeout=timedelta(hours=config.gather_logs_timeout_hours)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        # ============ WORKFLOW DEPENDENCIES ============
        # Define the DAG execution order and conditional branching

        log_job_start_time >> check_instance

        # Instance-based routing: Trial vs UAT/PROD
        check_instance >> rail.Label(
            "Trial instance") >> view_dagrun_conf >> get_account_opportunities_data_from_dag_run_conf >> log_count_accounts_opportunities_records
        check_instance >> rail.Label(
            "UAT/PROD instance") >> log_lookback_period_start_timestamp

        # UAT/PROD flow: Fetch data from Salesforce with lookback period
        log_lookback_period_start_timestamp >> new_created_or_updated_account >> new_created_or_updated_opportunity >> update_variables_lookback_timestamps \
            >> log_count_accounts_opportunities_records

        # Main flow: Check if there are any records to process
        log_count_accounts_opportunities_records >> if_accounts_opportunities_count_0

        if_accounts_opportunities_count_0 >> rail.Label('Yes') >> finish
        if_accounts_opportunities_count_0 >> rail.Label(
            'No') >> get_required_permission_set_uris >> if_accounts_record_present

        if_accounts_record_present >> rail.Label('Yes') >> client_import_log
        if_accounts_record_present >> rail.Label(
            'No') >> if_opportunity_records_present

        client_import_log >> create_collection_new_updated_accounts >> query_accounts_with_missing_id

        # Account validation branching
        query_accounts_with_missing_id >> if_accounts_with_missing_id

        if_accounts_with_missing_id >> rail.Label(
            "Yes") >> log_missing_id_accounts >> query_accounts_with_id
        if_accounts_with_missing_id >> rail.Label(
            "No") >> query_accounts_with_id

        # Get country data for address mapping and proceed with client processing
        query_accounts_with_id >> get_all_countries_in_replicon

        # Client processing flow: Get existing clients and perform left join
        get_all_countries_in_replicon >> get_all_clients >> create_replicon_client_collection \
            >> query_valid_accounts_left_join_replicon_clients >> if_final_accounts_to_process

        if_final_accounts_to_process >> rail.Label("Yes") >> dummy_process_clients >> get_all_client_custom_fields >> trigger_process_clients >> get_process_clients_dag_ids \
            >> gather_client_logs >> if_opportunity_records_present
        if_final_accounts_to_process >> rail.Label(
            "No") >> if_opportunity_records_present

        if_opportunity_records_present >> rail.Label(
            "Yes") >> project_import_log
        if_opportunity_records_present >> rail.Label(
            "No") >> dummy_trigger_process_log_generation

        project_import_log >> create_collection_new_updated_opportunities >> query_opportunities_missing_project_code

        query_opportunities_missing_project_code >> if_opportunities_with_missing_project_code
        if_opportunities_with_missing_project_code >> rail.Label(
            "Yes") >> log_missing_project_code_opportunities >> query_opportunities_with_project_code
        if_opportunities_with_missing_project_code >> rail.Label(
            "No") >> query_opportunities_with_project_code

        query_opportunities_with_project_code >> if_final_opportunities_to_process

        if_final_opportunities_to_process >> rail.Label(
            "Yes") >> get_all_required_currencies
        if_final_opportunities_to_process >> rail.Label(
            "No") >> dummy_trigger_process_log_generation

        get_all_required_currencies >> get_all_cost_centers >> get_template_project_details >> get_project_engagement_oef_uri >> get_all_engagement_type_dd_values >> get_all_engagement_stage_dd_values\
            >> get_all_projects_in_replicon >> create_replicon_project_collection \
            >> get_all_updated_clients >> create_updated_replicon_client_collection >> query_valid_opportunities_left_join_replicon_projects

        query_valid_opportunities_left_join_replicon_projects >> dummy_process_projects

        dummy_process_projects >> trigger_process_projects >> get_process_projects_dag_ids \
            >> gather_project_logs >> dummy_trigger_process_log_generation

        dummy_trigger_process_log_generation >> trigger_process_log_generation >> wait_for_trigger_process_log_generation >> finish

    return dag


rail.for_each_instance(create_main_dag)
