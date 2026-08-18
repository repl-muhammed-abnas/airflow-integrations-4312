
import rail
from pendulum import datetime, now
from datetime import timedelta

null = None


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Create Replicon project manager in Salesforce',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 1, 8, tz=config.time_zone),  
        schedule_interval=config.schedule_interval, 
        max_active_runs=config.max_active_runs
    ) as dag:
        
        def get_specific_user_details(data):
            response = []
            for item in data:
                user = item.get('user', {})
                user_obj = {
                    'displayText': user.get('displayText'),
                    'uri': user.get('uri'),
                    'loginName': user.get('loginName')
                }
                response.append(user_obj)
            return response

        get_eligible_project_leaders=rail.RepliconServiceOperator(
            task_id='get_eligible_project_leaders',
            endpoint="/services/ProjectService1.svc/GetEligibleProjectLeaders",
            data_handler=get_specific_user_details
        )

        foreach_specific_user_details = rail.ForEachOperator(
            task_id='foreach_specific_user_details',
            items=lambda: rail.result('get_eligible_project_leaders'),
            start_task='search_replicon_managers_in_salesforce',
            end_task='foreach_specific_user_details_end'
        )

        search_replicon_managers_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_replicon_managers_in_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query='''SELECT FIELDS(ALL) FROM replicon__Replicon_Project_Manager__c WHERE replicon__Replicon_Id__c = '{{ result("foreach_specific_user_details").uri }}' LIMIT 150''',
        )

        is_project_manager_exisits = rail.IfOperator(
            task_id="is_project_manager_exisits",
            test=lambda: rail.result('search_replicon_managers_in_salesforce')['totalSize'] > 0,
            yes_task='no_data',
            no_task='create_project_manager_in_salesforce'
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        def create_project_manager_payload():
            current_item = rail.result("foreach_specific_user_details")
            return [{
                'Name': current_item['displayText'],
                'replicon__Replicon_Id__c': current_item['uri'],
                'replicon__Replicon_Loginname__c': current_item['loginName']
            }]      

        create_project_manager_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id='create_project_manager_in_salesforce',
            operation= 'insert',
            object_name= 'replicon__Replicon_Project_Manager__c',
            salesforce_conn_id= config.salesforce_conn_id,
            payload= create_project_manager_payload
        )

        foreach_specific_user_details_end = rail.EmptyOperator(
            task_id='foreach_specific_user_details_end'
        )

        get_eligible_project_leaders >> foreach_specific_user_details >> foreach_specific_user_details_end
        foreach_specific_user_details >> search_replicon_managers_in_salesforce >> is_project_manager_exisits

        is_project_manager_exisits >> rail.Label('Yes') >> no_data >> foreach_specific_user_details_end
        is_project_manager_exisits >> rail.Label('No') >> create_project_manager_in_salesforce >> foreach_specific_user_details_end

    return dag

rail.for_each_instance(create_main_airflow_dag)