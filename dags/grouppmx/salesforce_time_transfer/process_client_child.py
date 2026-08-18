import rail
from grouppmx.salesforce_time_transfer.utils import custom_methods,request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.client_dag_id,
        description=f'Grouppmx Time Transfer To Salesforce Client Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = 'view_dagrun_conf')

        def get_account_details(response):
            records = response.get('records', [])
            return {
                'account_id': records[0].get('Id', '') if records else '',
                'account_name': records[0].get('Name', '') if records else ''
                }

        search_accounts_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_accounts_in_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query="SELECT FIELDS(ALL) FROM Account WHERE Replicon_ID__c LIKE '{{ dag_run.conf.client_uri }}' LIMIT 150",
            data_handler=get_account_details
        )

        is_account_available = rail.IfOperator(
            task_id = 'is_account_available',
            test= '{{ result("search_accounts_in_salesforce").account_id | is_truthy }}',
            yes_task= 'log_account_details',
            no_task= 'search_client_in_replicon'
        )

        search_client_in_replicon = rail.RepliconServiceOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            data=lambda dag_run: {
                    "clientUri": dag_run.conf['client_uri']
                }
        )

        create_account_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id='create_account_in_salesforce',
            operation= 'insert',
            object_name= 'Account',
            salesforce_conn_id= config.salesforce_conn_id,
            payload= request_payload.create_account_payload
        )

        log_account_success = rail.WriteLogOperator(
            task_id = 'log_account_success',
            log= '{{ dag_run.conf.log }}',
            message="NA",
            severity="Success",
            properties=lambda: {
                "contact": "",
                "project": "",
                "account": rail.result("search_client_in_replicon")['name'],
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Success",
                "details": "{{ dag_run_ecid() }} Account created with Replicon ID -" + rail.result('search_client_in_replicon')['uri'],
            }
        )

        log_account_details = rail.PythonOperator(
            task_id = 'log_account_details',
            python_callable= lambda dag_run: {
                'account_id': rail.result("search_accounts_in_salesforce")['account_id'] if rail.result(
                    "search_accounts_in_salesforce")['account_id'] else rail.result("create_account_in_salesforce")[0]['id'],
                'account_name': rail.result("search_accounts_in_salesforce")['account_name'] if rail.result(
                    "search_accounts_in_salesforce")['account_id'] else rail.result("search_client_in_replicon")['name'],
                'client_uri': dag_run.conf['client_uri']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.log}}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "contact": "",
                "project": "",
                "account": "{{ dag_run.conf.account }}",
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Success",
                "details": '{{ dag_run_ecid() }} - {{ get_error_message() }}',
            }
        )

        search_accounts_in_salesforce >> is_account_available

        is_account_available >> rail.Label(
            "Yes") >> log_account_details

        is_account_available >> rail.Label(
            "No") >> search_client_in_replicon >> create_account_in_salesforce >> \
            log_account_success >> log_account_details >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
