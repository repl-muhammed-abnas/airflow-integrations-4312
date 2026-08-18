from datetime import timedelta
import rail
from dxctechnology.portugal_payroll_export.utils import request_payload, response_filter, custom_method

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_portugal_payroll_export_master_{config.instance}',
        description=f'DXCTechnology_Portugal_Payroll_Export_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        portugal_payroll_start = rail.EmptyOperator(
            task_id = 'portugal_payroll_start'
        )

        get_all_scripts= rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts"
        )

        get_enabled_companycodes= rail.RepliconServiceOperator(
            task_id='get_enabled_companycodes',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions"
        )

        get_enabled_employeetype_groups= rail.RepliconServiceOperator(
            task_id='get_enabled_employeetype_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups"
        )

        get_child_hierarchy_data= rail.RepliconServiceOperator(
            task_id='get_child_hierarchy_data',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetChildHierarchyData",
            data=request_payload.get_employeetype_child_hierarchy,
            response_filter=response_filter.convert_location_hierarchy
        )

        is_division_and_employee_type_present= rail.IfOperator(
            task_id='is_division_and_employee_type_present',
            test= lambda: custom_method.check_division_and_emptype(config.company_code),
            yes_task= 'process_portugal_payroll_child',
            no_task="finish",
        )

        process_portugal_payroll_child= rail.TriggerDagRunOperator(
            task_id='process_portugal_payroll_child',
            retries=0,
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.child_dag_conf(config)
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        portugal_payroll_start >> [get_all_scripts, get_enabled_companycodes, get_enabled_employeetype_groups, get_child_hierarchy_data] >> \
                is_division_and_employee_type_present

        is_division_and_employee_type_present >> rail.Label(
            "Yes") >> process_portugal_payroll_child >> finish

        is_division_and_employee_type_present >> rail.Label(
            "No") >> finish

        finish >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
