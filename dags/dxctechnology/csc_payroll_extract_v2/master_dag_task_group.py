import rail
from dxctechnology.csc_payroll_extract_v2 import request_payload


def get_master_dag_task_group(time, export, frequency, company_key):

    with rail.TaskGroup(group_id='master_dag_task_group', prefix_group_id=False):

        master_dag_task_group_start = rail.EmptyOperator(
            task_id='master_dag_task_group_start')
        get_all_scripts = rail.RepliconServiceOperator(
            task_id="get_all_scripts",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            data={
                "tenant": {
                    "companyKey": company_key
                }
            }
        )
        get_all_enabled_locations = rail.RepliconServiceOperator(
            task_id="get_all_enabled_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )
        get_all_enabled_employee_type_groups = rail.RepliconServiceOperator(
            task_id="get_all_enabled_employee_type_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
        )
        search_entries_companycode_mapper = rail.PythonOperator(
            task_id='search_entries_companycode_mapper',
            python_callable=lambda: request_payload.companycode_from_mapper(
                time, export, frequency)
        )

        master_dag_task_group_loaded = rail.EmptyOperator(
            task_id='master_dag_task_group_loaded')
        master_dag_task_group_start >> [get_all_scripts, get_all_enabled_locations,
                                        get_all_enabled_divisions,get_all_enabled_employee_type_groups,
                                        search_entries_companycode_mapper]
        [get_all_scripts, get_all_enabled_locations, get_all_enabled_divisions, get_all_enabled_employee_type_groups,
            search_entries_companycode_mapper]
        [get_all_scripts, get_all_enabled_locations, get_all_enabled_divisions, get_all_enabled_employee_type_groups,
            search_entries_companycode_mapper] >> master_dag_task_group_loaded
    return master_dag_task_group_start, master_dag_task_group_loaded
