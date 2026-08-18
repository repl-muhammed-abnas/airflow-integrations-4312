from datetime import timedelta
import rail
from airflow.models import Variable
from tokamakenergy.user_import_v1.utils import request_payload, response_filters
EFFECTIVE_DATE_FORMAT_BAMBOOHR = '%Y-%m-%d'
null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_user_child_dagid,
        description=f'TokamakEnergy BambooHR to Polaris User Sync Create Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_var_for_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_var_for_logs',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        declare_var_for_logs = rail.SetVariableOperator(
            task_id='declare_var_for_logs',
            append=False,
            name='user_logs',
            value=[]
        )

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: request_payload.get_create_user_payload(dag_run, config.user_permission_set)
        )

        assign_licenses_to_user = rail.RepliconServiceOperator(
            task_id='assign_licenses_to_user',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: request_payload.get_assign_licenses_to_user_payload(config.licenses)
        )

        declare_employee_type_uri_list = rail.SetVariableOperator(
            task_id='declare_employee_type_uri_list',
            append=False,
            name='employeetypeuri',
            value=[]
        )

        foreach_employeetype_group = rail.ForEachOperator(
            task_id='foreach_employeetype_group',
            items=lambda dag_run: response_filters.filtered_groups_data(dag_run, "employmenttypedata", "employmentStatus"),
            start_task='if_employeetype_present_in_replicon',
            end_task='foreach_employeetype_group_end'
        )

        if_employeetype_present_in_replicon = rail.IfOperator(
            task_id='if_employeetype_present_in_replicon',
            test='{{ result("foreach_employeetype_group").uri | is_truthy }}',
            yes_task="accumulate_employee_group_uris",
            no_task="log_employee_type_not_present_in_replicon",
        )

        log_employee_type_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_employee_type_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value='Employee Type \"{{ result("foreach_employeetype_group").employmentStatus }}\" from BambooHR is not present in Replicon'
        )

        accumulate_employee_group_uris = rail.SetVariableOperator(
            task_id='accumulate_employee_group_uris',
            name='employeetypeuri',
            append=True,
            value=lambda dag_run: {
                "employeeTypeGroup": {
                    "uri": rail.result("foreach_employeetype_group")["uri"],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null if rail.result("foreach_employeetype_group")["date"] == dag_run.conf["user_details"]["employmenttypedata"][0]["date"]
                    else rail.parse_date(rail.result("foreach_employeetype_group")["date"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
            }
        )

        foreach_employeetype_group_end = rail.EmptyOperator(
            task_id='foreach_employeetype_group_end',
        )

        get_user_employee_type_groups = rail.GetVariableOperator(
            task_id='get_user_employee_type_groups',
            name='employeetypeuri'
        )

        declare_departments_uri_list = rail.SetVariableOperator(
            task_id='declare_departments_uri_list',
            append=False,
            name='departmentsuri',
            value=[]
        )

        foreach_department_group = rail.ForEachOperator(
            task_id='foreach_department_group',
            items=lambda dag_run: response_filters.filtered_groups_data(dag_run, "departmentgroupdata", "department"),
            start_task='if_department_group_present_in_replicon',
            end_task='foreach_department_group_end'
        )

        if_department_group_present_in_replicon = rail.IfOperator(
            task_id='if_department_group_present_in_replicon',
            test='{{ result("foreach_department_group").uri | is_truthy }}',
            yes_task='accumulate_department_group_uris',
            no_task='log_department_not_present_in_replicon'
        )

        log_department_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_department_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value='Department Group \"{{ result("foreach_department_group").department }}\" from BambooHR is not present in Replicon'
        )

        accumulate_department_group_uris = rail.SetVariableOperator(
            task_id='accumulate_department_group_uris',
            name='departmentsuri',
            append=True,
            value=lambda dag_run: {
                "departmentGroup": {
                    "uri": rail.result("foreach_department_group")["uri"],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null if rail.result("foreach_department_group")["date"] == dag_run.conf["user_details"]["departmentgroupdata"][0]["date"]
                    else rail.parse_date(rail.result("foreach_department_group")["date"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
            }
        )

        foreach_department_group_end = rail.EmptyOperator(
            task_id='foreach_department_group_end',
        )

        get_user_department_groups = rail.GetVariableOperator(
            task_id='get_user_department_groups',
            name='departmentsuri'
        )

        declare_supervisors_uri_list = rail.SetVariableOperator(
            task_id='declare_supervisors_uri_list',
            append=False,
            name='supervisorsuri',
            value=[]
        )

        foreach_supervisor = rail.ForEachOperator(
            task_id='foreach_supervisor',
            items=lambda dag_run: response_filters.filtered_groups_data(dag_run, "supervisorsdata", "supervisor_empid"),
            start_task='get_user_supervisor_from_replicon',
            end_task='foreach_supervisor_end'
        )

        get_user_supervisor_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_supervisor_from_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "employeeId": '{{ result("foreach_supervisor").supervisor_empid }}',
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_supervisor_present_in_replicon = rail.IfOperator(
            task_id='is_supervisor_present_in_replicon',
            test='{{ result("get_user_supervisor_from_replicon") | is_truthy }}',
            yes_task='is_supervisor_permission_present',
            no_task='log_supervisor_not_present_in_replicon'
        )

        is_supervisor_permission_present = rail.IfOperator(
            task_id='is_supervisor_permission_present',
            test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_user_supervisor_from_replicon")["permissionSets"],
                "displayText", "Supervisor", "uri", False),
            yes_task='accumulate_supervisors_uris',
            no_task='assign_supervisor_permissions'
        )

        assign_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permissions',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=request_payload.assign_supervisor_permission
        )

        accumulate_supervisors_uris = rail.SetVariableOperator(
            task_id='accumulate_supervisors_uris',
            name='supervisorsuri',
            append=True,
            value=lambda dag_run: {
                "supervisor": {
                    "uri": rail.result("get_user_supervisor_from_replicon")["userDetails"]["uri"],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null if rail.result("foreach_supervisor")["date"] == dag_run.conf["user_details"]["supervisorsdata"][0]["date"]
                    else rail.parse_date(rail.result("foreach_supervisor")["date"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
            }
        )

        log_supervisor_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_supervisor_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value='Supervisor Employee ID \"{{ result("foreach_supervisor").supervisor_empid }}\" from BambooHR is not present in Replicon'
        )

        foreach_supervisor_end = rail.EmptyOperator(
            task_id='foreach_supervisor_end',
        )

        get_user_supervisors = rail.GetVariableOperator(
            task_id='get_user_supervisors',
            name='supervisorsuri'
        )

        apply_modifications_on_user_in_replicon = rail.RepliconServiceOperator(
            task_id='apply_modifications_on_user_in_replicon',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_apply_modifications_user_payload
        )

        if_oef_tags_not_present_in_replicon = rail.IfOperator(
            task_id='if_oef_tags_not_present_in_replicon',
            test=lambda dag_run: request_payload.get_oef_details_to_add(dag_run) and request_payload.get_oef_details_to_add(dag_run)["oef_logs"],
            yes_task='log_oef_tags_not_present_in_replicon',
            no_task='get_all_user_logs'
        )

        log_oef_tags_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_oef_tags_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value=lambda dag_run: " | ".join(request_payload.get_oef_details_to_add(dag_run)["oef_logs"])
        )

        get_all_user_logs = rail.GetVariableOperator(
            task_id='get_all_user_logs',
            name='user_logs'
        )

        log_user_create_logs = rail.WriteLogOperator(
            task_id='log_user_create_logs',
            log='{{dag_run.conf.log_artifact}}',
            severity=lambda: "Exception" if rail.result("get_all_user_logs")["value"] else "Success",
            message='User Created',
            properties=lambda dag_run: {
                "username": f'{dag_run.conf["user_details"]["firstname"]} {dag_run.conf["user_details"]["lastname"]}',
                "employee_id": dag_run.conf["user_details"]["employeenumber"],
                "action": "Add",
                "status": "Exception" if rail.result("get_all_user_logs")["value"] else "Success",
                "comments": "User created partially - " + " | ".join(rail.result("get_all_user_logs")["value"])
                    if rail.result("get_all_user_logs")["value"] else "User created succesfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{dag_run.conf.log_artifact}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "username": '{{ dag_run.conf.user_details.firstname }} {{ dag_run.conf.user_details.lastname }}',
                "employee_id": '{{ dag_run.conf.user_details.employeenumber }}',
                "action": "Add",
                "status": "Error",
                "comments": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> declare_var_for_logs

        declare_var_for_logs >> create_user_in_replicon >> assign_licenses_to_user \
            >> declare_employee_type_uri_list >> foreach_employeetype_group >> if_employeetype_present_in_replicon

        if_employeetype_present_in_replicon >> rail.Label("Yes") >> accumulate_employee_group_uris >> foreach_employeetype_group_end
        if_employeetype_present_in_replicon >> rail.Label("No") >> log_employee_type_not_present_in_replicon >> foreach_employeetype_group_end
        foreach_employeetype_group_end >> get_user_employee_type_groups >> declare_departments_uri_list
        foreach_employeetype_group >> foreach_employeetype_group_end

        declare_departments_uri_list >> foreach_department_group >> if_department_group_present_in_replicon
        if_department_group_present_in_replicon >> rail.Label("Yes") >> accumulate_department_group_uris \
            >> foreach_department_group_end
        if_department_group_present_in_replicon >> rail.Label("No") >> log_department_not_present_in_replicon >> foreach_department_group_end
        foreach_department_group >> foreach_department_group_end
        foreach_department_group_end >> get_user_department_groups >> declare_supervisors_uri_list \
            >> foreach_supervisor >> get_user_supervisor_from_replicon
        get_user_supervisor_from_replicon >> is_supervisor_present_in_replicon
        is_supervisor_present_in_replicon >> rail.Label("Yes") >> is_supervisor_permission_present
        is_supervisor_permission_present >> rail.Label("Yes") >> accumulate_supervisors_uris >> foreach_supervisor_end
        is_supervisor_permission_present >> rail.Label("No") >> assign_supervisor_permissions >> accumulate_supervisors_uris
        is_supervisor_present_in_replicon >> rail.Label("No") >> log_supervisor_not_present_in_replicon \
            >> foreach_supervisor_end >> get_user_supervisors >> apply_modifications_on_user_in_replicon >> if_oef_tags_not_present_in_replicon
        if_oef_tags_not_present_in_replicon >> rail.Label("Yes") >> log_oef_tags_not_present_in_replicon >> get_all_user_logs
        if_oef_tags_not_present_in_replicon >> rail.Label("No") >> get_all_user_logs
        get_all_user_logs >> log_user_create_logs >> catch_and_log_errors
        foreach_supervisor >> foreach_supervisor_end

    return dag


rail.for_each_instance(create_child_dag)
