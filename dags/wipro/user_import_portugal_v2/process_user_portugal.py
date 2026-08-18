from datetime import timedelta
from wipro.user_import_portugal_v2.utils import request_payload, custom_methods
import rail
from airflow.models import Variable
null = None
INVALID_DATES = ["9999-12-31", "0000-00-00"]


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.valid_user_dag_id,
        description="wipro User import process record",
        company_key=config.company_key,
        max_active_runs=config.max_active_run_child,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_process_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_user_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_user_details",
            end_task="catch_and_log_errors"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["employee_id"],
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_project_supervisor_details_in_feed = rail.IfOperator(
            task_id="if_project_supervisor_details_in_feed",
            test=lambda dag_run: dag_run.conf["project_supervisor_id"],
            yes_task="get_project_supervisor_details",
            no_task="if_user_not_exists"
        )

        get_project_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_project_supervisor_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf["project_supervisor_id"],
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else null
        )

        if_project_supervisor_in_replicon = rail.IfOperator(
            task_id="if_project_supervisor_in_replicon",
            test=lambda: not rail.result("get_project_supervisor_details"),
            yes_task="if_user_not_exists",
            no_task="if_project_supervisor_with_permissions"
        )

        if_project_supervisor_with_permissions = rail.IfOperator(
            task_id="if_project_supervisor_with_permissions",
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_project_supervisor_details")[
                    "permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["l1_manager"],
                "uri") and rail.find_first_by_attr_and_get_attr(
                rail.result("get_project_supervisor_details")[
                    "permissionSets"],
                "displayText",
                config.GENERAL_MAPPER["project_manager"],
                "uri"),
            yes_task="if_user_not_exists",
            no_task="update_project_supervisor_permissions"
        )

        update_project_supervisor_permissions = rail.RepliconServiceOperator(
            task_id="update_project_supervisor_permissions",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=custom_methods.get_update_supervisor_permission_payload
        )

        if_user_not_exists = rail.IfOperator(
            task_id="if_user_not_exists",
            test=lambda: not rail.result("get_user_details"),
            yes_task="start_process_add_user",
            no_task="get_current_group_membership"
        )

        get_current_group_membership = rail.RepliconServiceOperator(
            task_id="get_current_group_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda: {
                    "userUri": rail.result('get_user_details')["userDetails"]['uri'],
                    "dateRange": null
            },
            data_handler=lambda response: {
                "existingcompanycodeuri": response.get('divisions', [{}])[0].get('division', {}).get('division', {}).get('uri'),
                "existingcompanycodename": response.get('divisions', [{}])[0].get('division', {}).get('division', {}).get('displayText'),
                "existingcountryname": response.get('serviceCenters', [{}])[0].get('serviceCenter', {}).get('serviceCenter', {}).get('displayText'),
                "existingdepartmentname":  response.get('departments', [{}])[0].get('department', {}).get('department', {}).get('displayText') if response.get('departments') else None,
            } if (response["divisions"] and response["serviceCenters"])else {}
        )

        if_country_changed = rail.IfOperator(
            task_id="if_country_changed",
            test=lambda dag_run: dag_run.conf["country"] != rail.result(
                "get_current_group_membership").get("existingcountryname", ""),
            yes_task="write_country_mismatch_log",
            no_task="if_company_code_changed"
        )

        write_country_mismatch_log = rail.WriteLogOperator(
            task_id="write_country_mismatch_log",
            log='{{dag_run.conf.lookuptable}}',
            message="User is tranferred to different country",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Exception",
                "details": "User belongs to " +
                    rail.result("get_current_group_membership").get("existingcountryname", "") +
                    "received country is" + dag_run.conf["country"]
            },

        )

        if_company_code_changed = rail.IfOperator(
            task_id="if_company_code_changed",
            test=lambda dag_run: (rail.result("get_user_details") and
                                  rail.result("get_current_group_membership").get("existingcompanycodeuri", "") !=
                                  dag_run.conf["legalentityuri"]),
            yes_task="add_end_date_to_user",
            no_task="if_reversal_date_present"
        )

        add_end_date_to_user = rail.RepliconServiceOperator(
            task_id="add_end_date_to_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.get_add_end_date_to_tranferred_user_payload
        )

        write_disable_tranferred_user_log = rail.WriteLogOperator(
            task_id="write_disable_tranferred_user_log",
            log='{{dag_run.conf.lookuptable}}',
            message="User is tranferred to different legal entity",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Success",
                "details": "Company code changed from " +
                    rail.result("get_current_group_membership").get("existingcompanycodename", "") +
                    "to" + rail.find_first_by_attr_and_get_attr(
                        dag_run.conf["legalentities"], "code", dag_run.conf["company_code"], "name", ""),
            },
        )

        if_reversal_date_present = rail.IfOperator(
            task_id="if_reversal_date_present",
            test=lambda dag_run: rail.result("get_user_details")["securityConfiguration"]["isLoginEnabled"] or
            (not rail.result("get_user_details")["securityConfiguration"]["isLoginEnabled"] and
             dag_run.conf["reversal_date"] and dag_run.conf["reversal_date"] not in INVALID_DATES),
            yes_task="process_update_user",
            no_task="write_log_user_not_processed"
        )

        write_log_user_not_processed = rail.WriteLogOperator(
            task_id="write_log_user_not_processed",
            log='{{dag_run.conf.lookuptable}}',
            message="User is disabled",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Exception",
                "details": "User is disabled and data received with no valid reversal date",
            },
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id="process_update_user",
            trigger_dag_id=config.update_user_dag_id,
            retries=0,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run:
            {
                **dag_run.conf,
                "process_project_supervisor_flag": "Y" if rail.result("get_project_supervisor_details") else "N",
                "existingdepartment": rail.result(
                "get_current_group_membership").get("existingdepartmentname", "")
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_user",
            dag_runs="{{result('process_update_user')}}",
            execution_timeout=timedelta(config.execution_timeout)
        )

        start_process_add_user = rail.EmptyOperator(
            task_id="start_process_add_user"
        )

        if_new_entity_user = rail.IfOperator(
            task_id="if_new_entity_user",
            test=lambda dag_run: dag_run.conf["new_entity_flag"],
            yes_task="process_new_entity_add_user",
            no_task="process_add_user"
        )

        process_new_entity_add_user = rail.TriggerDagRunOperator(
            task_id="process_new_entity_add_user",
            trigger_dag_id=config.add_new_entity_user_dag_id,
            retries=0,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run:
            {
                **dag_run.conf,
                "process_project_supervisor_flag": "Y" if rail.result("get_project_supervisor_details") else "N"
            }
        )

        wait_for_new_entity_add_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_new_entity_add_user",
            dag_runs="{{result('process_new_entity_add_user')}}",
            execution_timeout=timedelta(config.execution_timeout),
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id="process_add_user",
            trigger_dag_id=config.add_user_dag_id,
            retries=0,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run:
            {
                **dag_run.conf,
                "process_project_supervisor_flag": "Y" if rail.result("get_project_supervisor_details") else "N"
            }
        )

        wait_for_add_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_user",
            dag_runs="{{result('process_add_user')}}",
            execution_timeout=timedelta(config.execution_timeout),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="User not processed for the following reason/s",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "employee_first_name": dag_run.conf["employee_first_name"],
                "employee_last_name": dag_run.conf["employee_last_name"],
                "country": dag_run.conf["country"],
                "company_code": dag_run.conf["company_code"],
                "status": "Failed",
                "details": "User not processed for the following reason/s" + custom_methods.get_error_message(),


            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            get_user_details >>\
            if_project_supervisor_details_in_feed >> rail.Label(
                "No") >> if_user_not_exists
        if_project_supervisor_details_in_feed >> rail.Label("Yes") >>\
            get_project_supervisor_details >>\
            if_project_supervisor_in_replicon >> rail.Label(
                "No") >> if_user_not_exists
        if_project_supervisor_in_replicon >> rail.Label("Yes") >>\
            if_project_supervisor_with_permissions >> rail.Label("No") >>\
            update_project_supervisor_permissions >> if_user_not_exists
        if_project_supervisor_with_permissions >> rail.Label(
            "Yes") >> if_user_not_exists
        if_user_not_exists >> rail.Label(
            "Yes") >> start_process_add_user >>\
            if_new_entity_user >> rail.Label(
                "Yes") >> process_new_entity_add_user >> wait_for_new_entity_add_user >> catch_and_log_errors
        if_new_entity_user >> rail.Label(
            "No") >> process_add_user >> wait_for_add_user >> catch_and_log_errors
        if_user_not_exists >> rail.Label("No") >>\
            get_current_group_membership >>\
            if_country_changed >> rail.Label("Yes") >>\
            write_country_mismatch_log >> catch_and_log_errors
        if_country_changed >> rail.Label("No") >>\
            if_company_code_changed >> rail.Label("Yes") >> \
            add_end_date_to_user >>\
            write_disable_tranferred_user_log >> \
            if_new_entity_user
        if_company_code_changed >> rail.Label("No") >>\
            if_reversal_date_present >> rail.Label("Yes") >>\
            process_update_user >> wait_for_update_user >> catch_and_log_errors
        if_reversal_date_present >> rail.Label("No") >>\
            write_log_user_not_processed >> catch_and_log_errors
        return dag


rail.for_each_instance(create_airflow_child)
