from datetime import timedelta
from pendulum import datetime
import pendulum
from capgemini.france_payroll_export.utils import custom_methods, request_payload
import rail

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Capgemini France Payroll Export Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 11, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'retries': 0
        }
    ) as dag:

        rail.ViewDagRunScheduleOperator(task_id="view_dagrun_schedule")

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_valid_scheduled_run',
            end_task='create_export_for_each_daterange',
        )

        is_valid_scheduled_run = rail.IfOperator(
            task_id='is_valid_scheduled_run',
            test=lambda: pendulum.now(config.time_zone).strftime("%d/%m/%Y") in config.schedules,
            yes_task='get_france_payroll_script'
        )

        get_france_payroll_script = rail.RepliconServiceOperator(
            task_id="get_france_payroll_script",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,
                'displayText', config.payroll_export_file_format, 'uri')
        )

        is_file_format_script_present = rail.IfOperator(
            task_id='is_file_format_script_present',
            test='{{ result("get_france_payroll_script") | is_truthy }}',
            yes_task='get_allowed_location_uris'
        )

        get_allowed_location_uris = rail.RepliconServiceOperator(
            task_id='get_allowed_location_uris',
            endpoint="/services/LocationService1.svc/GetPageOfAvailableLocationsByTextSearch",
            data=request_payload.get_location_uri_payload(config.location),
            data_handler=lambda response: custom_methods.get_location_uri(
                response, config.location)
        )

        create_export_for_each_daterange = rail.TriggerDagRunForEachItemOperator(
            task_id='create_export_for_each_daterange',
            items=lambda: custom_methods.get_export_date_range_details(config.time_zone, config.filename_prefix),
            trigger_dag_id=config.create_payroll_extract_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item: {
                "exportdetails": item,
                "fileformatscripturi": rail.result("get_france_payroll_script"),
                "locationuris": rail.result("get_allowed_location_uris")
            }
        )

        batch_task >> create_export_for_each_daterange
        batch_task >> is_valid_scheduled_run
        is_valid_scheduled_run >> rail.Label("Yes") >> get_france_payroll_script >> is_file_format_script_present
        is_file_format_script_present >> rail.Label("Yes") >> get_allowed_location_uris >> create_export_for_each_daterange

    return dag
rail.for_each_instance(create_main_dag)
