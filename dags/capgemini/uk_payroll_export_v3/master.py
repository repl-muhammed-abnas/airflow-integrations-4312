from datetime import timedelta
from pendulum import datetime
from capgemini.uk_payroll_export_v3.utils import custom_methods, request_payload
import rail

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Capgemini UK Overtime Payroll Export Master {config.instance} V3",
        company_key=config.company_key,
        start_date=datetime(2025, 3, 26, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='logging_details',
            end_task='finish_payroll_export',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.time_zone]
        )

        get_uk_payroll_script = rail.RepliconServiceOperator(
            task_id="get_uk_payroll_script",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,
                'displayText', config.payroll_export_file_format, 'uri')
        )

        is_file_format_script_present = rail.IfOperator(
            task_id='is_file_format_script_present',
            test='{{ result("get_uk_payroll_script") | is_truthy }}',
            yes_task='get_uk_location_uri'
        )

        get_uk_location_uri = rail.RepliconServiceOperator(
            task_id='get_uk_location_uri',
            endpoint="/services/LocationService1.svc/GetPageOfAvailableLocationsByTextSearch",
            data=request_payload.get_location_uri_payload(config.location),
            data_handler=lambda response: custom_methods.get_uk_location_uri(
                response, config.location)
        )

        create_export_for_costcenters = rail.TriggerDagRunForEachItemOperator(
            task_id='create_export_for_costcenters',
            trigger_dag_id=config.create_export_child_dag_id,
            items=list(config.cost_center_groups_list.keys()),  # Now iterates over "Non-FS" and "FS"
            conf=lambda item: {
                "exportdetails": rail.result("logging_details"),
                "cost_center_group_name": item,  # "Non-FS" or "FS"
                "cost_center_group_code": item,  # Will be used in filename
                "cost_center_hierarchy_level": config.cost_center_groups_list[item]["hierarchy_level"],
                "cost_centers_list": config.cost_center_groups_list[item]["cost_centers"],  # List of actual cost centers
                "uk_location_uri": rail.result("get_uk_location_uri"),
                "payroll_script_uri": rail.result("get_uk_payroll_script")
            }
        )

        finish_payroll_export = rail.EmptyOperator(
            task_id='finish_payroll_export'
        )

        batch_task >> finish_payroll_export
        batch_task >> logging_details

        logging_details >> get_uk_payroll_script >> is_file_format_script_present
        is_file_format_script_present >> rail.Label(
            "Yes") >> get_uk_location_uri >> create_export_for_costcenters >> finish_payroll_export

    return dag


rail.for_each_instance(create_child_dag)
