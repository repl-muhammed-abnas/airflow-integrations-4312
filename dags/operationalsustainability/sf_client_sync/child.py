import rail
from operationalsustainability.sf_client_sync.utils import python_callable
from operationalsustainability.sf_client_sync.utils import request_payload
from operationalsustainability.sf_client_sync.config import *

def escape_soql_string(value):
    if value is None or value == '':
        raise ValueError("SOQL value cannot be None or empty")
    # SOQL uses backslash escaping for single quotes
    return str(value).replace('\\', '\\\\').replace("'", "\\'")



def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.child_dag_id,
        description= f'New or Updated account from Salesforce will Sync as Client in Replicon | process Accounts_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')
        
        is_account_types_to_sync_all = rail.IfOperator(
            task_id = 'is_account_types_to_sync_all',
            test = lambda dag_run: (
                config.account_types_to_sync != 'All' and
                dag_run.conf.get('item').get('Type') is not None and
                dag_run.conf.get('item').get('Type') not in [t.strip() for t in config.account_types_to_sync.split(',')]
            ),
            yes_task= 'empty_task',
            no_task= 'is_account_type_present'
        )

        empty_task = rail.EmptyOperator(
            task_id= 'empty_task'
        )

        is_account_type_present = rail.IfOperator(
            task_id = 'is_account_type_present',
            test = lambda dag_run: (
                dag_run.conf.get('item').get('Type') is None and
                not config.sync_accounts_with_no_types
            ),
            yes_task= 'empty_task',
            no_task= 'search_clients'
        )

        search_clients = rail.RepliconServicePageOperator(
            task_id='search_clients',
            endpoint='/services/ClientListService1.svc/GetData',
            data= request_payload.search_client_payload,
            page_handler= python_callable.page_handler,
            all_result_data_handler= python_callable.get_client_details
        )

        def get_query_for_search_user_sf(dag_run):
            return f"""select FIELDS(ALL) from User where Id = '{dag_run.conf['item']["OwnerId"]}' LIMIT 200"""

        search_user_sf = rail.SalesforceQueryOperator2(
            task_id = 'search_user_sf',
            salesforce_conn_id= config.sf_conn_id,
            query= get_query_for_search_user_sf
        )

        def get_query_for_search_contact_sf(dag_run):
            return f"""select FIELDS(ALL) from Contact where Id = '{dag_run.conf['item']["Id"]}' LIMIT 200"""

        search_contact_sf = rail.SalesforceQueryOperator2(
            task_id = 'search_contact_sf',
            salesforce_conn_id= config.sf_conn_id,
            query= get_query_for_search_contact_sf
        )

        search_user = rail.RepliconServiceOperator(
            task_id = 'search_user',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_search_user_payload
        )

        is_sf_client_present = rail.IfOperator(
            task_id = 'is_sf_client_present',
            test = lambda dag_run: (rail.result("search_clients") and rail.result("search_clients").get("client_name")),
            yes_task= 'is_to_update_false',
            no_task= 'create_clientorapply_modifications'
        )

        is_to_update_false = rail.IfOperator(
            task_id = 'is_to_update_false',
            test= lambda: not config.to_update,
            yes_task= 'empty_task',
            no_task= 'create_clientorapply_modifications'
        )

        create_clientorapply_modifications = rail.RepliconServiceOperator(
            task_id="create_clientorapply_modifications",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=request_payload.apply_client_modifications_payload,
        )

        is_account_types_to_sync_all >> rail.Label("No") >> is_account_type_present >> rail.Label("No") >> search_clients >> \
        search_user_sf >> search_contact_sf >> search_user >> is_sf_client_present >> rail.Label("Yes") >> is_to_update_false

        is_to_update_false >> rail.Label("No") >> create_clientorapply_modifications

        is_sf_client_present >> rail.Label("No") >> create_clientorapply_modifications

        is_to_update_false >> rail.Label("Yes") >> empty_task

        is_account_type_present >> rail.Label("Yes") >> empty_task

        is_account_types_to_sync_all >> rail.Label("Yes") >> empty_task



rail.for_each_instance(create_child_dag)