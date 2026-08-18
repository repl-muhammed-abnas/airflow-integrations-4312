from datetime import timedelta
from pendulum import datetime
import rail
from dkpierceassociates.project_sync.utils import custom_function, request_payload, request_query

def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='dkpierceassociates Project Sync - Master DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        def get_new_or_updated_account_query():
            return f"""SELECT
                fields(all)
            FROM
                Opportunity
            WHERE
                LastModifiedById != '{config.salesforce_integration_user_id}' and LastModifiedDate > {rail.result("get_last_sync_time")['last_synctime']}
            AND Type != 'Change'
            Limit {config.salesforce_opportunity_query_limit}"""

        get_last_sync_time = rail.PythonOperator(
            task_id = 'get_last_sync_time',
            python_callable= lambda: custom_function.last_sync_time(config.last_sync_time_variable)
        )

        new_or_updated_salesforce_object = rail.SalesforceQueryOperator2(
            task_id='new_or_updated_salesforce_object',
            salesforce_conn_id=config.salesforce_conn_id,
            query= get_new_or_updated_account_query
        )

        # Trigger child DAG for each Salesforce opportunity record
        trigger_process_opportunities = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_opportunities',
            trigger_dag_id=config.process_opportunity_dag_id,
            items=lambda: rail.result('new_or_updated_salesforce_object').get('records', []),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'opportunity_record': item
            }
        )

        # Wait for all child DAG runs to complete
        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_process_opportunities") }}'
        )

        validate_child_success = rail.PythonOperator(
            task_id='validate_child_success',
            python_callable=lambda: custom_function.validate_all_child_dags_succeeded(
                rail.result('wait_for_child_dags')
            )
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

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        ## airflow hierarchy
        get_last_sync_time >> new_or_updated_salesforce_object >> trigger_process_opportunities >> wait_for_child_dags >> validate_child_success >> update_lastsync_time >> collection_task >> log_to_sumo

    return dag
        

# Create DAG for each instance
rail.for_each_instance(create_main_dag)