
import rail
from pendulum import datetime, now
from datetime import timedelta

null = None

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Create Replicon client manager in Salesforce',
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

        get_eligible_client_leaders = rail.RepliconServiceOperator(
            task_id='get_eligible_client_leaders',
            endpoint="/services/UserService1.svc/GetPageOfUsersInPolicyDataAccessScope2",
            data=lambda: {
                "pageIndex": "1",
                "pageSize": "2000",
                "policyUri": "urn:replicon:policy:project-management",
                "userSearch": {
                    "statusOptionUri": "urn:replicon:user-status-option:include-only-enabled-users",
                    "userAccessRoleUri": "urn:replicon:user-access-role:client-manager",
                    "textSearch": null
                }
            },
            data_handler=get_specific_user_details
        )

        foreach_specific_user_details = rail.ForEachOperator(
            task_id='foreach_specific_user_details',
            items=lambda: rail.result('get_eligible_client_leaders'),
            start_task='search_replicon_managers_in_salesforce',
            end_task='foreach_specific_user_details_end'
        )

        search_replicon_managers_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_replicon_managers_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query='''SELECT FIELDS(ALL) FROM replicon__RepliconClientManager__c WHERE replicon__Replicon_Id__c = '{{ result("foreach_specific_user_details").uri }}' LIMIT 150''',
        )

        is_client_manager_exisits = rail.IfOperator(
            task_id="is_client_manager_exisits",
            test=lambda: rail.result('search_replicon_managers_in_salesforce')['totalSize'] > 0,
            yes_task='no_data',
            no_task='create_client_manager_in_salesforce'
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        def create_client_manager_payload():
            current_item = rail.result("foreach_specific_user_details")
            return [{
                'Name': current_item['displayText'],
                'replicon__Replicon_Id__c': current_item['uri'],
                'replicon__Replicon_Loginname__c': current_item['loginName']
            }]

        create_client_manager_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id='create_client_manager_in_salesforce',
            operation='insert',
            object_name='replicon__RepliconClientManager__c',
            salesforce_conn_id=config.salesforce_conn_id,
            payload=create_client_manager_payload
        )

        foreach_specific_user_details_end = rail.EmptyOperator(
            task_id='foreach_specific_user_details_end'
        )

        get_eligible_client_leaders >> foreach_specific_user_details >> foreach_specific_user_details_end
        foreach_specific_user_details >> search_replicon_managers_in_salesforce >> is_client_manager_exisits

        is_client_manager_exisits >> rail.Label(
            'Yes') >> no_data >> foreach_specific_user_details_end
        is_client_manager_exisits >> rail.Label(
            'No') >> create_client_manager_in_salesforce >> foreach_specific_user_details_end

    return dag


rail.for_each_instance(create_main_airflow_dag)
