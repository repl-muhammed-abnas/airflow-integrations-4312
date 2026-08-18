from datetime import timedelta
from dateutil.parser import parse as date_parser
from pendulum import datetime
import rail
from airflow import DAG
from airflow.models import Variable
from rail.lib.alerts_email import send_dagrun_alert_email

def create_dag(config):
    with DAG(
        dag_id=config.master_dag_id,
        description=config.dag_description,
        schedule=config.schedule_interval,
        default_view='graph',
        start_date=datetime(2022, 1, 1),
        default_args={
            "salesforce_conn_id": config.salesforce_connection_id,
            'owner':'salesforce_auto_response',
        },
        user_defined_macros=rail.dag.get_macros(),
        user_defined_filters=rail.dag.get_filters(),
        max_active_runs=1,
        on_failure_callback=send_dagrun_alert_email,
    ) as dag:

        def get_new_case_query():
            last_created_time_stamp = Variable.get("auto_response_last_createdDate_date_value")
            return f"""SELECT FIELDS(ALL) FROM Case WHERE (
                (RecordTypeId = '0120g0000005z5dAAA') AND ( OwnerID != '00G4u000005LefJ') AND (CreatedDate > {last_created_time_stamp}) AND Status = 'New')
                LIMIT 200"""

        new_case = rail.InternalSalesforceQuerySensor(
            task_id="new_case",
            salesforce_conn_id=config.salesforce_connection_id,
            query=get_new_case_query,
            soft_fail_timeout=timedelta(minutes=1),
            poke_interval=10,
            retries=1
        )

        is_new_case_found = rail.IfOperator(
            task_id = "is_new_case_found",
            trigger_rule="all_done",
            test='{{ result("new_case") | is_truthy }}',
            yes_task="process_sf_cases",
            no_task="delete_this_dagrun"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        process_sf_cases = rail.EmptyOperator(
            task_id = "process_sf_cases"
        )

        trigger_process_case = rail.trigger_parallel_dagrun(
            task_id= "trigger_process_case",
            trigger_dag_id=config.child_dag_id,
            parallel_count=5,
            items=lambda: rail.result("new_case")['records'],
            execution_timeout=timedelta(hours=1)
        )

        def get_earliest_created_time():
            sorted_dates = sorted(list(map(lambda case: date_parser(case['CreatedDate']), rail.result("new_case")['records'])))
            return sorted_dates[-1].strftime(config.CREATED_DATE_FORMAT)

        update_timestamp_variable = rail.PythonOperator(
            task_id = "update_timestamp_variable",
            python_callable=lambda : Variable.set(key="auto_response_last_createdDate_date_value", value=get_earliest_created_time())
        )

        new_case >> is_new_case_found >> rail.Label("No") >> delete_this_dagrun
        is_new_case_found >> rail.Label("Yes") >> process_sf_cases >> trigger_process_case >> update_timestamp_variable

    return dag


rail.for_each_instance(create_dag)
