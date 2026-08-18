from datetime import timedelta
from pendulum import datetime
import rail
from dxctechnology.lcsc_les_uk_ireland_payroll_extract_v1.utils import request_payload, response_filter

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"DXC LCSC UK Ireland Payroll Export Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2026, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:
        
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_valid_payroll_export_day',
            end_task='process_payrolldata_export',
        )

        is_valid_payroll_export_day = rail.IfOperator(
            task_id='is_valid_payroll_export_day',
            test=lambda: request_payload.is_valid_payroll_export_day(
                config.time_zone, config.lcsc_payroll_calendar, config.les_payroll_calendar),
            yes_task='logging_details'
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=request_payload.get_logging_details,
            op_args=[config.pta_weeks, config.time_zone]
        )

        get_specific_scripts = rail.RepliconServiceOperator(
            task_id="get_specific_scripts",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: response_filter.get_specific_payload(
                response, config.locations_company_codes_mapper, "file_format_name"
            )
        )

        get_specific_enabled_locations = rail.RepliconServiceOperator(
            task_id="get_specific_enabled_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            data_handler=lambda response: response_filter.get_specific_payload(
                response, config.locations_company_codes_mapper, "location"
            )
        )

        get_specific_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_specific_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=lambda response: response_filter.get_specific_payload(
                response, config.locations_company_codes_mapper, "company_code"
            )
        )

        get_specific_enabled_employee_type_groups = rail.RepliconServiceOperator(
            task_id="get_specific_enabled_employee_type_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", 'Contractor', "uri")
        )

        search_entries_companycode_mapper = rail.PythonOperator(
            task_id='search_entries_companycode_mapper',
            python_callable=lambda: request_payload.companycode_from_mapper(
                config.export, config.locations_company_codes_mapper,
                config.time_zone, config.lcsc_payroll_calendar, config.les_payroll_calendar)
        )

        process_payrolldata_export = rail.TriggerDagRunForEachItemOperator(
            task_id='process_payrolldata_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=config.process_payroll_data_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: request_payload.process_payrolldata_export_conf(
                item, config.time_zone, config.file_name_prefix)
        )

        batch_task >> process_payrolldata_export
        batch_task >> is_valid_payroll_export_day >> rail.Label("Yes") >> logging_details >> get_specific_scripts \
            >> get_specific_enabled_locations >> get_specific_enabled_divisions \
                >> get_specific_enabled_employee_type_groups >> search_entries_companycode_mapper \
                    >> process_payrolldata_export

    return dag


rail.for_each_instance(create_main_dag)
