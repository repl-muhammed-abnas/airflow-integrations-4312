from datetime import timedelta
import rail
from airflow.models import Variable
from refinedtechnologies.client_import.utils import custom_function

def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Refined Technologies Inc Client Import - Master DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master
        
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_new_or_updated_account_query():
            return f"""SELECT LastModifiedById,
                    Legacy_Id__c,
                    Name,
                    Id,
                    ShippingStreet,
                    BillingStreet,
                    Description,
                    ShippingCountry,
                    BillingCountry,
                    ShippingCity,
                    ShippingState,
                    ShippingPostalCode,
                    Phone,
                    Fax,
                    Website,
                    BillingCity,
                    BillingState,
                    BillingPostalCode,
                    OwnerId
                    FROM account
                    WHERE LastModifiedById != '{config.lastModifiedById}'
                    AND Legacy_Id__c != null
                    AND LastModifiedDate > {rail.result("get_last_sync_time")['last_synctime']}
                    LIMIT {config.limit}"""

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
            no_task='query_new_or_updated_accounts'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_new_or_updated_accounts',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_new_or_updated_accounts = rail.SalesforceQueryOperator2(
            task_id='query_new_or_updated_accounts',
            salesforce_conn_id=config.salesforce_conn_id,
            query=get_new_or_updated_account_query,
        )

        extract_account_records = rail.PythonOperator(
            task_id='extract_account_records',
            python_callable=lambda: custom_function.extract_salesforce_records(rail.result("query_new_or_updated_accounts"))
        )

        check_records_exist = rail.IfOperator(
            task_id='check_records_exist',
            test=lambda: len(rail.result('extract_account_records')) > 0,
            yes_task='get_all_countries',
            no_task='update_lastsync_time'
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint="/services/InternationalizationService1.svc/GetAllCountries"
        )

        trigger_process_accounts = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_accounts',
            items=lambda: rail.result('extract_account_records'),
            trigger_dag_id=config.process_client_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'salesforce_record': item,
                'countries': rail.result('get_all_countries')
            }
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_process_accounts") }}'
        )

        update_lastsync_time = rail.PythonOperator(
            task_id='update_lastsync_time',
            python_callable=lambda: custom_function.update_last_sync(config.last_sync_time_variable),
            trigger_rule='none_failed_min_one_success'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        get_last_sync_time >> can_run_batch_task
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> query_new_or_updated_accounts

        query_new_or_updated_accounts >> extract_account_records >> check_records_exist
        check_records_exist >> rail.Label("Yes") >> get_all_countries >> trigger_process_accounts >> wait_for_child_dags >> update_lastsync_time
        check_records_exist >> rail.Label("No") >> update_lastsync_time
        update_lastsync_time >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
