from datetime import timedelta
import rail
from grouppmx.salesforce_time_transfer.utils import request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.project_dag_id,
        description=f'Grouppmx Time Transfer To Salesforce Project Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = 'view_dagrun_conf')

        def get_project_details(response):
            records = response.get('records', [])
            return {
                'project_id': records[0].get('Id', '') if records else '',
                'project_name': records[0].get('Name', '') if records else ''
                }

        search_projects_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_projects_in_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query="SELECT ID, Name FROM Project__c WHERE Replicon_ID__c LIKE '{{ dag_run.conf.project_uri }}' LIMIT 150",
            data_handler=get_project_details
        )

        is_project_available = rail.IfOperator(
            task_id = 'is_project_available',
            test= '{{ result("search_projects_in_salesforce").project_id | is_truthy }}',
            yes_task= 'log_project_details',
            no_task= 'search_project_in_replicon'
        )

        search_project_in_replicon = rail.RepliconServiceOperator(
            task_id = 'search_project_in_replicon',
            endpoint= 'services/ProjectService1.svc/GetProjectDetails',
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri']
            }
        )

        create_project_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id='create_project_in_salesforce',
            operation= 'insert',
            object_name= 'Project__c',
            salesforce_conn_id= config.salesforce_conn_id,
            payload= request_payload.create_project_payload
        )

        is_client_present_in_project = rail.IfOperator(
            task_id = 'is_client_present_in_project',
            test= '{{ (result("search_project_in_replicon").client.uri if result("search_project_in_replicon").client else "") | is_truthy }}',
            yes_task= 'process_each_client',
            no_task= 'log_project_success'
        )

        process_each_client = rail.TriggerDagRunOperator(
            task_id='process_each_client',
            trigger_dag_id= config.client_dag_id,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'client_uri': rail.result("search_project_in_replicon")['client']['uri'],
                'log': dag_run.conf['log']
            },
            wait_for_completion= True
        )

        gather_accounts_data = rail.GatherResultsFromDagRunsOperator(
            task_id = 'gather_accounts_data',
            dag_runs= '{{ result("process_each_client") }}',
            dagrun_task_id= 'log_account_details',
            flatten= True
        )

        update_project_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id='update_project_in_salesforce',
            operation= 'update',
            object_name= 'Project__c',
            salesforce_conn_id= config.salesforce_conn_id,
            payload=lambda: [{
                'Client_Company__c': rail.result("gather_accounts_data")[0]['account_id'],
                'Id': rail.result("create_project_in_salesforce")[0]['id']
            }]
        )

        log_project_success = rail.WriteLogOperator(
            task_id = 'log_project_success',
            log= '{{ dag_run.conf.log }}',
            message="NA",
            severity="Success",
            properties=lambda dag_run: {
                "project": rail.result("search_project_in_replicon")['name'],
                "contact": "",
                "account": "",
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Success",
                "details": "{{ dag_run_ecid() }} - Project created with Replicon ID - {{ result('search_project_in_replicon') }}",
            }
        )

        log_project_details = rail.PythonOperator(
            task_id = 'log_project_details',
            python_callable= lambda dag_run: {
                'project_id': rail.result("search_projects_in_salesforce")['project_id'] if rail.result(
                    "search_projects_in_salesforce")['project_id'] else rail.result("create_project_in_salesforce")[0]['id'],
                'project_name': rail.result("search_projects_in_salesforce")['project_name'] if rail.result(
                    "search_projects_in_salesforce")['project_id'] else rail.result("search_project_in_replicon")['name'],
                'project_uri': dag_run.conf['project_uri']
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
                "project": "{{ result('search_project_in_replicon').name }}",
                "account": "",
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Error",
                "details": '{{ dag_run_ecid() }} - {{ get_error_message() }}',
            }
        )

        search_projects_in_salesforce >> is_project_available

        is_project_available >> rail.Label(
            "Yes") >> log_project_details

        is_project_available >> rail.Label(
            "No") >> search_project_in_replicon >> create_project_in_salesforce >> is_client_present_in_project

        is_client_present_in_project >> rail.Label(
                "Yes") >> process_each_client >> gather_accounts_data >> update_project_in_salesforce >> log_project_success

        is_client_present_in_project >> rail.Label(
            "No") >> log_project_success >> log_project_details >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)