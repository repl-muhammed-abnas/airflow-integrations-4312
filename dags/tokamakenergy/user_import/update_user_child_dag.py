from datetime import timedelta
import pendulum
import rail
from airflow.models import Variable
from tokamakenergy.user_import.utils import request_payload, custom_methods, response_filters
EFFECTIVE_DATE_FORMAT_BAMBOOHR = '%Y-%m-%d'
null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_child_dagid,
        description=f'TokamakEnergy BambooHR to Polaris User Sync Update Child DAG {config.instance}',
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
            name='exception_logs',
            value=[]
        )

        if_rehired_user = rail.IfOperator(
            task_id='if_rehired_user',
            test=lambda dag_run: (dag_run.conf["user_details"]["status"].lower() == "active"
                and dag_run.conf["replicon_user_details"]["userDetails"]["isEnabled"] is False),
            yes_task='update_user_loginname_and_licenses',
            no_task='updated_user_basic_details'
        )

        update_user_loginname_and_licenses = rail.RepliconServiceOperator(
            task_id="update_user_loginname_and_licenses",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.update_loginname_licenses(dag_run, config.licenses)
        )

        updated_user_basic_details = rail.PythonOperator(
            task_id='updated_user_basic_details',
            python_callable=custom_methods.get_updated_user_basic_details
        )

        if_login_name_updated = rail.IfOperator(
            task_id='if_login_name_updated',
            test=lambda dag_run: (dag_run.conf["replicon_user_details"]["securityConfiguration"]["loginName"]
                != dag_run.conf["user_details"]["workemail"]),
            yes_task='update_user_loginname_in_replicon',
            no_task='if_no_basic_details_update'
        )

        update_user_loginname_in_replicon = rail.RepliconServiceOperator(
            task_id='update_user_loginname_in_replicon',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.get_update_user_loginname_in_replicon_payload
        )

        if_no_basic_details_update = rail.IfOperator(
            task_id='if_no_basic_details_update',
            test=lambda: all(value is null for value in rail.result("updated_user_basic_details").values()),
            yes_task="get_effectiveusergroupmembership_replicon",
            no_task="update_user_basic_details_in_replicon"
        )

        update_user_basic_details_in_replicon = rail.RepliconServiceOperator(
            task_id='update_user_basic_details_in_replicon',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_update_user_basic_details_payload
        )

        get_effectiveusergroupmembership_replicon = rail.RepliconServiceOperator(
            task_id="get_effectiveusergroupmembership_replicon",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.replicon_user_details.userDetails.uri }}"
            },
            data_handler=response_filters.get_effective_user_groupmembership_filter
        )

        if_effective_employeetype_present_in_bamboohr = rail.IfOperator(
            task_id='if_effective_employeetype_present_in_bamboohr',
            test=lambda dag_run: dag_run.conf["user_details"].get("employmentstatus"),
            yes_task="if_employeetype_present_in_replicon",
            no_task="if_effective_departments_present_in_bamboohr",
        )

        if_employeetype_present_in_replicon = rail.IfOperator(
            task_id='if_employeetype_present_in_replicon',
            test=lambda dag_run: dag_run.conf["user_details"].get("employmentstatus_uri"),
            yes_task="if_effective_departments_present_in_bamboohr",
            no_task="log_employee_type_not_present_in_replicon",
        )

        log_employee_type_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_employee_type_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value='Employee Type \"{{ dag_run.conf.user_details.employmentstatus }}\"'
                + ' from BambooHR is not present in Replicon'
        )

        if_effective_departments_present_in_bamboohr = rail.IfOperator(
            task_id='if_effective_departments_present_in_bamboohr',
            test=lambda dag_run: dag_run.conf["user_details"].get("department"),
            yes_task="if_department_group_present_in_replicon",
            no_task="if_effective_supervisor_present_in_bamboohr",
        )

        if_department_group_present_in_replicon = rail.IfOperator(
            task_id='if_department_group_present_in_replicon',
            test=lambda dag_run: dag_run.conf["user_details"].get("department_uri"),
            yes_task='if_effective_supervisor_present_in_bamboohr',
            no_task='log_department_not_present_in_replicon'
        )

        log_department_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_department_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value='Department Group \"{{ dag_run.conf.user_details.department }}\" from BambooHR is not present in Replicon'
        )

        if_effective_supervisor_present_in_bamboohr = rail.IfOperator(
            task_id='if_effective_supervisor_present_in_bamboohr',
            test=lambda dag_run: dag_run.conf["user_details"].get("reportsto"),
            yes_task="is_supervisor_empid_present",
            no_task="get_update_modifications_user_payload",
        )

        is_supervisor_empid_present = rail.IfOperator(
            task_id='is_supervisor_empid_present',
            test=lambda dag_run: dag_run.conf["user_details"].get("supervisor_empid"),
            yes_task='get_user_supervisor_from_replicon',
            no_task='get_update_modifications_user_payload'
        )

        get_user_supervisor_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_supervisor_from_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "employeeId": '{{ dag_run.conf.user_details.supervisor_empid }}',
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
            yes_task='get_supervisor_assignment_details',
            no_task='assign_supervisor_permissions'
        )

        assign_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permissions',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=request_payload.assign_supervisor_permission
        )

        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_assignment_details",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf["replicon_user_details"]["userDetails"]["uri"],
                "asOfDate": custom_methods.get_today_json(config.time_zone)
            },
            data_handler=lambda response: rail.set_result(key="supervisor", val=response["supervisor"] if response else {})
        )

        log_supervisor_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_supervisor_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value='Supervisor Employee ID \"{{ dag_run.conf.user_details.supervisor_empid }}\"'
                + ' from BambooHR is not present in Replicon'
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=request_payload.is_supervisor_changed,
            yes_task='update_supervisor_for_user',
            no_task='get_update_modifications_user_payload'
        )

        update_supervisor_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf["replicon_user_details"]["userDetails"]["uri"],
                "supervisorUri": rail.result("get_user_supervisor_from_replicon")["userDetails"]["uri"],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["user_details"]["jobinfoeffectivedate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
                }
            }
        )

        get_update_modifications_user_payload = rail.PythonOperator(
            task_id='get_update_modifications_user_payload',
            python_callable=lambda dag_run: request_payload.get_update_modifications_user_payload(dag_run, config.MDY_DATE_FORMAT)
        )

        check_any_modifications = rail.IfOperator(
            task_id='check_any_modifications',
            test=custom_methods.check_any_modifications,
            yes_task='apply_modifications_on_user_in_replicon',
            no_task='get_exception_logs'
        )

        apply_modifications_on_user_in_replicon = rail.RepliconServiceOperator(
            task_id='apply_modifications_on_user_in_replicon',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda: rail.result("get_update_modifications_user_payload")
        )

        if_oef_tags_not_present_in_replicon = rail.IfOperator(
            task_id='if_oef_tags_not_present_in_replicon',
            test=lambda: request_payload.get_oef_details_to_update and request_payload.get_oef_details_to_update()["oef_logs"],
            yes_task='log_oef_tags_not_present_in_replicon',
            no_task='get_exception_logs'
        )

        log_oef_tags_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_oef_tags_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value=lambda: " | ".join(request_payload.get_oef_details_to_update()["oef_logs"])
        )

        get_exception_logs = rail.GetVariableOperator(
            task_id='get_exception_logs',
            name='{{ result("declare_var_for_logs").name }}'
        )

        log_user_update_logs = rail.WriteLogOperator(
            task_id='log_user_update_logs',
            log='{{dag_run.conf.log_artifact}}',
            severity=lambda: "Exception" if rail.result("get_exception_logs")["value"] else "Success",
            message='User Updated',
            properties=lambda dag_run: {
                "username": f'{dag_run.conf["user_details"]["firstname"]} {dag_run.conf["user_details"]["lastname"]}',
                "employee_id": dag_run.conf["user_details"]["employeenumber"],
                "action": "Update",
                "status": "Exception" if rail.result("get_exception_logs")["value"] else ("Skipped" if
                    custom_methods.no_data_updated() else "Success"),
                "comments": "User updated partially - " + " | ".join(rail.result("get_exception_logs")["value"]
                    + request_payload.get_updated_log(dag_run, config.MDY_DATE_FORMAT)) if rail.result("get_exception_logs")["value"] else (
                        "No updates found" if custom_methods.no_data_updated() else "User updated succesfully")
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
                "action": "Update",
                "status": "Error",
                "comments": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> declare_var_for_logs

        declare_var_for_logs >> if_rehired_user
        if_rehired_user >> rail.Label("Yes") >> update_user_loginname_and_licenses >> updated_user_basic_details >> if_login_name_updated
        if_login_name_updated >> rail.Label("Yes") >> update_user_loginname_in_replicon >> if_no_basic_details_update
        if_login_name_updated >> rail.Label("No") >> if_no_basic_details_update
        if_rehired_user >> rail.Label("No") >> updated_user_basic_details
        if_no_basic_details_update >> rail.Label("No") >> update_user_basic_details_in_replicon \
            >> get_effectiveusergroupmembership_replicon
        if_no_basic_details_update >> rail.Label("Yes") >> get_effectiveusergroupmembership_replicon
        get_effectiveusergroupmembership_replicon >> if_effective_employeetype_present_in_bamboohr
        if_effective_employeetype_present_in_bamboohr >> rail.Label("Yes") >> if_employeetype_present_in_replicon
        if_effective_employeetype_present_in_bamboohr >> rail.Label("No") >> if_effective_departments_present_in_bamboohr
        if_employeetype_present_in_replicon >> rail.Label("Yes") >> if_effective_departments_present_in_bamboohr
        if_employeetype_present_in_replicon >> rail.Label("No") >> log_employee_type_not_present_in_replicon \
            >> if_effective_departments_present_in_bamboohr
        if_effective_departments_present_in_bamboohr >> rail.Label("Yes") >> if_department_group_present_in_replicon
        if_effective_departments_present_in_bamboohr >> rail.Label("No") >> if_effective_supervisor_present_in_bamboohr
        if_department_group_present_in_replicon >> rail.Label("Yes") >> if_effective_supervisor_present_in_bamboohr
        if_department_group_present_in_replicon >> rail.Label("No") >> log_department_not_present_in_replicon \
            >> if_effective_supervisor_present_in_bamboohr
        if_effective_supervisor_present_in_bamboohr >> rail.Label("Yes") >> is_supervisor_empid_present
        if_effective_supervisor_present_in_bamboohr >> rail.Label("No") >> get_update_modifications_user_payload
        is_supervisor_empid_present >> rail.Label("Yes") >> get_user_supervisor_from_replicon >> is_supervisor_present_in_replicon
        is_supervisor_empid_present >> rail.Label("No") >> get_update_modifications_user_payload
        is_supervisor_present_in_replicon >> rail.Label("Yes") >> is_supervisor_permission_present
        is_supervisor_permission_present >> rail.Label("Yes") >> get_supervisor_assignment_details
        is_supervisor_permission_present >> rail.Label("No") >> assign_supervisor_permissions \
            >> get_supervisor_assignment_details >> is_supervisor_changed
        is_supervisor_changed >> rail.Label("Yes") >> update_supervisor_for_user >> get_update_modifications_user_payload
        is_supervisor_changed >> rail.Label("No") >> get_update_modifications_user_payload
        is_supervisor_present_in_replicon >> rail.Label("No") >> log_supervisor_not_present_in_replicon \
            >> get_update_modifications_user_payload >> check_any_modifications
        check_any_modifications >> rail.Label("Yes") >> apply_modifications_on_user_in_replicon >> if_oef_tags_not_present_in_replicon
        if_oef_tags_not_present_in_replicon >> rail.Label("Yes") >> log_oef_tags_not_present_in_replicon >> get_exception_logs
        if_oef_tags_not_present_in_replicon >> rail.Label("No") >> get_exception_logs
        get_exception_logs >> log_user_update_logs >> catch_and_log_errors
        check_any_modifications >> rail.Label("No") >> get_exception_logs

    return dag


rail.for_each_instance(create_child_dag)
