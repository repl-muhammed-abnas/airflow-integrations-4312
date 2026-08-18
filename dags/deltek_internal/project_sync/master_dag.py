from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable
from dateutil.parser import parse as date_parser
from deltek_internal.project_sync import config
from deltek_internal.project_sync.utils import request_payload 
from deltek_internal.project_sync.utils import custom_functions

def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"{config.master_dag_id}",
        description=f'Salesforce to Polaris Project Sync Master - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,  # Run daily to check for new/updated opportunities
        max_active_runs=config.max_active_run_master
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_new_or_updated_account_query():
            last_created_time_stamp = Variable.get(config.last_sync_time_variable)
            return f"""select LastModifiedDate,
                Sol_Sales_Billing_Type__c,
                Sol_Sales_CIE_Hours__c,
                Sol_Sales_CIE_Pricing__c,
                Sol_Sales_Final_Approved_Pricing__c,
                Sol_Sales_Implementation_Hours__c,
                Sol_Sales_Implementation_Pricing__c,
                Sol_Sales_Opp__c,
                Sol_Sales_Parent_Opportunity__c,
                Sol_Sales_Parent_opportunity_Owner__c,
                Sol_Sales_PDM_Hours__c,
                Sol_Sales_PDM_Pricing__c,
                Sol_Sales_Probability__c,
                Sol_Sales_Proposed_Project_Kick_Off__c,
                Sol_Sales_RIT_Hours__c,
                Sol_Sales_RIT_Pricing__c,
                Sol_Sales_Services_Add_Ons__c,
                Sol_Sales_Services_Package__c,
                Sol_Sales_Total_Services_Pricing__c,
                Account.Name,
                Growth_type__c,
                SOW_CR_Reference__c,
                Name,
                Implementation_End_Date__c,
                Implementation_Start_Date__c from opportunity
                where Sol_Sales_Probability__c = '{config.probability_filter}'
                AND Sol_Sales_Probability__c != null and Growth_type__c = '{config.growth_type_filter}'
                and LastModifiedDate > {last_created_time_stamp}
                LIMIT {config.query_limit}"""
        
        query_salesforce_opportunities = rail.InternalSalesforceQueryOperator(
            task_id='query_salesforce_opportunities',
            salesforce_conn_id=config.salesforce_conn_id,
            query = get_new_or_updated_account_query
        )

        # Filter and prepare opportunities for processing
        prepare_opportunities = rail.PythonOperator(
            task_id='prepare_opportunities',
            python_callable=request_payload.filter_opportunities_for_processing
        )

        # Check if there are opportunities to process
        has_opportunities = rail.IfOperator(
            task_id='has_opportunities',
            test='{{ result("prepare_opportunities") | length > 0 }}',
            yes_task='empty_task',
            no_task='no_opportunities_to_process'
        )

        # No opportunities found - log and exit
        no_opportunities_to_process = rail.PythonOperator(
            task_id='no_opportunities_to_process',
            python_callable=lambda: print('No qualifying opportunities found since last sync')
        )

        empty_task = rail.EmptyOperator(
            task_id="empty_task"
        )

        trigger_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_dags',
            trigger_dag_id=f"{config.company_key}_salesforce_to_polaris_project_sync_child_{config.instance}",
            items=lambda: rail.result("query_salesforce_opportunities").get("records"),
            batch_size=config.batch_size,
            conf=lambda item : {
                "salesforce_record": item,
            }
        )

        # Wait for all child DAG runs to complete
        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            dag_runs='{{ result("trigger_child_dags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            allowed_states=['success'],
            failed_states=['failed']
        )

        # Gather results from all child DAG runs
        gather_child_results = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_results',
            dag_runs='{{ result("trigger_child_dags") }}',
            dagrun_task_id='processing_result',
            flatten=True
        )

        # Generate summary of processing results
        generate_summary = rail.PythonOperator(
            task_id='generate_summary',
            python_callable=lambda: custom_functions.generate_processing_summary(
                rail.result('prepare_opportunities'),
                rail.result('gather_child_results')
            )
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        def get_latest_created_time():
            records = rail.result("query_salesforce_opportunities").get('records', [])

            if not records:
                raise ValueError("No records to process")

            # Filter out records without LastModifiedDate
            valid_dates = []
            for record in records:
                if 'LastModifiedDate' in record:
                    try:
                        valid_dates.append(date_parser(record['LastModifiedDate']))
                    except Exception as e:
                        # Log but don't fail for individual parse errors
                        import logging
                        logging.warning(f"Failed to parse date for record {record.get('Name')}: {e}")

            if not valid_dates:
                raise ValueError("No valid LastModifiedDate fields found in records")

            # Return the latest date
            return max(valid_dates).strftime(config.created_date_format)
        
        update_last_sync_timestamp_variable = rail.PythonOperator(
            task_id = "update_last_sync_timestamp_variable",
            python_callable=lambda : Variable.set(key= config.last_sync_time_variable, value= get_latest_created_time())
        )

        # Define DAG workflow
        query_salesforce_opportunities >> prepare_opportunities >> has_opportunities

        # Path when opportunities found
        has_opportunities  >> empty_task >> trigger_child_dags
        trigger_child_dags >> wait_for_child_dags >> gather_child_results >> generate_summary >> update_last_sync_timestamp_variable
    
        # Path when no opportunities found
        has_opportunities >> no_opportunities_to_process >> delete_this_dagrun

    return dag
rail.for_each_instance(create_master_dag)
