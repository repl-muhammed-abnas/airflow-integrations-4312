from datetime import timedelta
import rail
from airflow.models import Variable
from refinedtechnologies.project_sync.utils import custom_function

def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Refined Technologies Project Sync - Master DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_new_or_updated_opportunity_query():

            integration_user_id = Variable.get(
                config.salesforce_integration_user_id, default_var='').strip()
            exclude_integration_user = (
                f"LastModifiedById != '{integration_user_id}' AND"
                if integration_user_id else ''
            )
            return f"""SELECT
                LastModifiedById,
                Replicon_PID_Description__c,
                RTI_PROJECT_ID__c,
                StageName,
                OwnerId,
                Start_Date__c,
                Description,
                AccountId,
                Name,
                NG_Contact_LegacyID_Updated__c,
                RTI_ACCOUNT_ID__c
            FROM
                Opportunity
            WHERE
                {exclude_integration_user}
                RTI_PROJECT_ID__c != null AND
                Replicon_PID_Description__c != null AND
                StageName IN ('Proposal', 'Uncommitted Acceptance', 'Closed Won') AND
                LastModifiedDate > {rail.result("get_last_sync_time")['last_synctime']}
            """

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: custom_function.last_sync_time(config.last_sync_time_variable)
        )

        # Batch the whole flow into one task when the toggle Variable is enabled.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_new_or_updated_opportunities'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_new_or_updated_opportunities',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_new_or_updated_opportunities = rail.SalesforceQueryOperator2(
            task_id='query_new_or_updated_opportunities',
            salesforce_conn_id=config.salesforce_conn_id,
            query=get_new_or_updated_opportunity_query,
        )

        trigger_process_opportunities = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_opportunities',
            trigger_dag_id=config.process_project_child_dag_id,
            items=lambda: rail.result('query_new_or_updated_opportunities').get('records', []),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'opportunity_record': item
            }
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_process_opportunities") }}'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        log_project_sync_success =rail.WriteLogOperator(
            task_id='log_project_sync_success',
            message="Project Sync is successfully processed",
            severity='Success',
            properties={
                'status': 'Success',
            }
        )

        update_lastsync_time = rail.PythonOperator(
            task_id='update_lastsync_time',
            python_callable=lambda: custom_function.update_last_sync(config.last_sync_time_variable)
        )

        get_last_sync_time >> can_run_batch_task
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> query_new_or_updated_opportunities

        query_new_or_updated_opportunities >> trigger_process_opportunities >> wait_for_child_dags >> update_lastsync_time >> log_to_sumo >> log_project_sync_success
    return dag
        

rail.for_each_instance(create_main_dag)