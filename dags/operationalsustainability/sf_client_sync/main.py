from datetime import timedelta
import rail
from airflow.models import Variable
from dateutil.parser import parse as date_parser



def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description= f'New or Updated account from Salesforce will Sync as Client in Replicon_{config.instance}',
        company_key=config.company_key,
        schedule_interval=timedelta(minutes=config.master_schedule_interval),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
    ) as dag:
        
        def get_new_or_updated_account_query():
            last_created_time_stamp = Variable.get(config.last_sync_time_variable)
            return f"""SELECT FIELDS(ALL) FROM Account WHERE (
                CreatedDate > {last_created_time_stamp} )
                LIMIT 200"""
        
        new_or_updated_account_sf = rail.SalesforceQueryOperator2(
            task_id="new_or_updated_account_sf",
            salesforce_conn_id=config.sf_conn_id,
            query=get_new_or_updated_account_query,
        )

        is_new_or_updated_account_found = rail.IfOperator(
            task_id = "is_new_or_updated_account_found",
            trigger_rule="all_done",
            test='{{ result("new_or_updated_account_sf") | is_truthy }}',
            yes_task="process_sf_account",
            no_task="delete_this_dagrun"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')
        
        process_sf_account = rail.EmptyOperator(
            task_id = "process_sf_account"
        )

        trigger_process_account = rail.trigger_parallel_dagrun(
            task_id= "trigger_process_account",
            trigger_dag_id=config.child_dag_id,
            parallel_count= config.trigger_parallel_dagrun_count,
            items=lambda: rail.result("new_or_updated_account_sf")['records'],
            execution_timeout=timedelta(hours=1)
        )

        def get_latest_created_time():
            records = rail.result("new_or_updated_account_sf").get('records', [])

            if not records:
                raise ValueError("No records to process")

            # Filter out records without CreatedDate
            valid_dates = []
            for record in records:
                if 'CreatedDate' in record:
                    try:
                        valid_dates.append(date_parser(record['CreatedDate']))
                    except Exception as e:
                        # Log but don't fail for individual parse errors
                        import logging
                        logging.warning(f"Failed to parse date for record {record.get('Id')}: {e}")

            if not valid_dates:
                raise ValueError("No valid CreatedDate fields found in records")

            # Return the latest date
            return max(valid_dates).strftime(config.created_date_format)
        
        update_last_sync_timestamp_variable = rail.PythonOperator(
            task_id = "update_last_sync_timestamp_variable",
            python_callable=lambda : Variable.set(key= config.last_sync_time_variable, value= get_latest_created_time())
        )



        new_or_updated_account_sf >> is_new_or_updated_account_found >> rail.Label("Yes") >> process_sf_account >> trigger_process_account >>\
        update_last_sync_timestamp_variable

        is_new_or_updated_account_found >> rail.Label("No") >> delete_this_dagrun


rail.for_each_instance(create_main_airflow_dag)