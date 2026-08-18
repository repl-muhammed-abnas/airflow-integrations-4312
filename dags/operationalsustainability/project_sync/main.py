
import rail
import pendulum
from datetime import timedelta
from airflow.models import Variable
from operationalsustainability.project_sync.utils import request_payload
from operationalsustainability.project_sync.utils import custom_methods


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='New or updated opportunities in Salesforce sync as projects in Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2025, 1, 8, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        get_opportunity_lookback_timestamp = rail.PythonOperator(
            task_id="get_opportunity_lookback_timestamp",
            python_callable=lambda: custom_methods.get_opportunity_lookback_timestamp(
                config.last_modified_datetime
            )
        )

        get_current_time_in_utc_minus_1_min = rail.PythonOperator(
            task_id="get_current_time_in_utc_minus_1_min",
            python_callable=lambda: custom_methods.get_current_time_in_utc_minus_1_min(config.time_zone)
        )

        get_created_or_modified_opportunities_from_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_created_or_modified_opportunities_from_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query=lambda: request_payload.get_new_created_or_updated_opportunity_query()
        )

        set_last_modified_datetime = rail.PythonOperator(
            task_id='set_last_modified_datetime',
            python_callable=lambda: Variable.set(
                config.last_modified_datetime,
                str(rail.result("get_current_time_in_utc_minus_1_min"))
            )
        )

        process_each_created_or_updated_opportunity = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_created_or_updated_opportunity',
            retries=0,
            items=lambda: rail.result('get_created_or_modified_opportunities_from_salesforce')['records'],
            trigger_dag_id=config.process_each_opprtunity_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_each_opportunity = rail.WaitForDagRunsSensor(
            task_id='wait_process_each_opportunity',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_created_or_updated_opportunity") }}'
        )

        get_opportunity_lookback_timestamp >> get_current_time_in_utc_minus_1_min >> get_created_or_modified_opportunities_from_salesforce >> set_last_modified_datetime >> process_each_created_or_updated_opportunity
        process_each_created_or_updated_opportunity >> wait_process_each_opportunity

    return dag


rail.for_each_instance(create_main_airflow_dag)
