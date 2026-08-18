from datetime import timedelta
from pendulum import datetime
import rail
from sideplate.project_records_sync.utils import custom_function, request_payload, request_query

def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='sideplate project records sync - Master DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        def get_new_or_updated_account_query():
            return f"""SELECT
                Id,
                Account__c,
                Project_Manager__c,
                Fee_Req_Resp_Email__c,
                Project_Number_and_Name__c,
                MPM4_BASE_Description__c,
                MPM4_BASE_Status__c,
                Opp_Billing_Type__c,
                Sum_of_Project_Amounts__c
            FROM
                Project__c
            WHERE
                LastModifiedDate > {rail.result("get_last_sync_time")['last_synctime']}"""
           
        get_last_sync_time = rail.PythonOperator(
            task_id = 'get_last_sync_time',
            python_callable= lambda: custom_function.last_sync_time(config.last_sync_time_variable)
        )


        new_or_updated_salesforce_object = rail.SalesforceQueryOperator2(
            task_id='new_or_updated_salesforce_object',
            salesforce_conn_id=config.salesforce_conn_id,
            query= get_new_or_updated_account_query
        )

        # Trigger child DAG for each Salesforce roject record
        trigger_process_project = rail.trigger_parallel_dagrun(
            task_id='trigger_process_project',
            trigger_dag_id=config.process_opportunity_dag_id,
            items=lambda: rail.result('new_or_updated_salesforce_object').get('records', []),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.parallel_trigger_dagrun_count,       
            conf=lambda item: {
                'project_record': item
            }
        )

        update_lastsync_time = rail.PythonOperator(
            task_id = 'update_lastsync_time',
            python_callable= lambda: custom_function.update_last_sync(config.last_sync_time_variable)
        )

        collection_task = rail.WriteLogOperator(
            task_id='collection_task',
            message="Project Sync is successfully processed",
            severity='Success',
            properties={
                'status': 'Success',
            }
        )

        ## airflow hierarchy
        get_last_sync_time >> new_or_updated_salesforce_object >> trigger_process_project >> update_lastsync_time >> collection_task 

    return dag

# Create DAG for each instance
rail.for_each_instance(create_main_dag)