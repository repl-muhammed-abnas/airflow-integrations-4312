from datetime import timedelta
from pendulum import datetime, now
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_process_missing_replicon_ids_master_dag_id,
        description="Mammoet time export process missing replicon ids",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        get_unique_employee_ids = rail.QueryCollectionOperator(
            task_id = "get_unique_employee_ids",
            query="""SELECT DISTINCT employee_id FROM blank_replicon_ids rtd WHERE NULLIF(rtd.employee_id, '') IS NOT NULL
                        AND CAST(rtd.hours AS FLOAT) != 0.0""",
            name="unique_timesheet_periods"
        )

        get_replicon_id_oef = rail.RepliconServiceOperator(
            task_id = "get_replicon_id_oef",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:time-entry"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name', 'Replicon ID')
        )

        process_update_missing_replicon_ids = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_update_missing_replicon_ids",
            items="{{result('get_unique_employee_ids')}}",
            trigger_dag_id = config.time_export_process_timesheets_dag_id,
            conf = lambda dag_run, item :{
                "employee_id" : item['employee_id'],
                "time_export_name" : dag_run.conf['time_export_name'],
                "replicon_id_oef": rail.result("get_replicon_id_oef")
            }
        )

        wait_for_process_update_missing_replicon_ids = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_update_missing_replicon_ids",
            dag_runs="{{result('process_update_missing_replicon_ids')}}",
            retries=0,
            execution_timeout = timedelta(days=config.execution_timeout_days_for_posting)
        )

        get_unique_employee_ids >> get_replicon_id_oef >> process_update_missing_replicon_ids >> wait_for_process_update_missing_replicon_ids

    return dag


rail.for_each_instance(create_main_dag)
