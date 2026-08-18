from datetime import timedelta
from sigroup.user_import.utils import custom_methods

import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_valid_user_dag_id,
        description="sigroup user import valid user child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")


        bulk_get_users = rail.RepliconServiceOperator(
            task_id="bulk_get_users",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                    "users": [
                        {
                            "employeeId": '{{dag_run.conf.employeeid}}',
                            "loginName": null,
                            "parameterCorrelationId": null
                        }
                    ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response:
            {
                "status": response[0]["userDetails"]["isEnabled"],
                "uri": response[0]['userDetails']["uri"]
             } if response else null
        )

        derive_group_identfier_strings = rail.PythonOperator(
            task_id="derive_group_identfier_strings",
            python_callable=lambda dag_run:list(
            ["ALL|ALL|ALL|"+dag_run.conf["location_code"] + "|"+ dag_run.conf["employee_type"] + "|ALL|ALL",
            "ALL|ALL|" + dag_run.conf["legalemployer_code"] + "|ALL|" + dag_run.conf["employee_type"] + "|ALL|ALL",
            "ALL|ALL|ALL|"+dag_run.conf["location_code"] + "|"+ dag_run.conf["employee_type"] + "|ALL|" + dag_run.conf["istariffemployee"],
            "ALL|ALL|ALL|ALL|" + dag_run.conf["employee_type"]+ "|"+ dag_run.conf["location_state"] + "|ALL"]
            )
        )

        def project_mapper_rows(mapper_rows):
            return list(map(lambda row: {
                "Field": row["Field"],
                "Value": row["Value"],
                "check": row["Check"]
            }, custom_methods.normalize_mapper_rows(mapper_rows)))

        def get_mapping_values(dag_run):
            mapping_values = []
            if dag_run.conf["location_code"] in ["41", "106"]:
                mapping_values.extend(project_mapper_rows(
                    config.USER_IMPORT_MAPPER.get(dag_run.conf["location_code"], [])))
            else:
                for key in rail.result("derive_group_identfier_strings"):
                    mapper_rows = custom_methods.find_mapper_rows(
                        config.USER_IMPORT_MAPPER, key)
                    if mapper_rows:
                        mapping_values.extend(project_mapper_rows(mapper_rows))
            return mapping_values
        
        aggregate_mapping_values = rail.PythonOperator(
            task_id="aggregate_mapping_values",
            python_callable=get_mapping_values
        )

        create_user_config_for_mapper_values = rail.PythonOperator(
            task_id="create_user_config_for_mapper_values",
            python_callable=custom_methods.get_user_config_for_mapper_values
        )

        if_user_is_active = rail.IfOperator(
            task_id="if_user_is_active",
            test=lambda dag_run:bool(dag_run.conf[
                              "status"] and
                              dag_run.conf[
                              "status"].lower() == "active"),
            yes_task="if_location_code_present",
            no_task="if_user_terminated"
        )

        if_location_code_present = rail.IfOperator(
            task_id="if_location_code_present",
            test=lambda dag_run:bool(list(filter(lambda i:i.get("Location") == dag_run.conf["location_code"] and
                        i.get("Field") == "Shift Required" and i.get("Value") == "Yes",
                        config.USER_IMPORT_MAPPER.get("", [])))),
            yes_task="get_shift_mapper_values",
            no_task="if_useruri_present"
        )

        get_shift_mapper_values = rail.PythonOperator(
            task_id="get_shift_mapper_values",
            python_callable=lambda dag_run:list(filter(lambda i:i["Check"].lower() == "yes",
                                                       custom_methods.normalize_mapper_rows(
                                                           config.USER_IMPORT_MAPPER.get(
                                                               dag_run.conf["location_code"], []))))
        )

        if_shift_useruri_present = rail.IfOperator(
            task_id="if_shift_useruri_present",
            test=lambda:bool(rail.result(
                "create_user_config_for_mapper_values")["useruri"]),
            yes_task="process_update_shift_user",
            no_task="process_add_shift_user"
        )

        process_update_shift_user = rail.TriggerDagRunOperator(
            task_id="process_update_shift_user",
            trigger_dag_id=config.sigroup_update_user_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=custom_methods.get_shift_user_config
        )

        process_add_shift_user = rail.TriggerDagRunOperator(
            task_id="process_add_shift_user",
            trigger_dag_id=config.sigroup_add_user_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=custom_methods.get_shift_user_config
        )

        if_useruri_present = rail.IfOperator(
            task_id="if_useruri_present",
            test=lambda :bool(rail.result(
                "create_user_config_for_mapper_values")["useruri"]),
            yes_task="process_update_user",
            no_task="process_add_user"
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id="process_update_user",
            trigger_dag_id=config.sigroup_update_user_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=custom_methods.get_user_config
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id="process_add_user",
            trigger_dag_id=config.sigroup_add_user_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=custom_methods.get_user_config
        )

        if_user_terminated = rail.IfOperator(
            task_id="if_user_terminated",
            test=lambda dag_run:bool(dag_run.conf[
                              "status"].lower() == "terminated"),
            yes_task="if_user_present_in_replicon",
            no_task="write_log_user_status_is_invalid"
        )

        write_log_user_status_is_invalid = rail.WriteLogOperator(
            task_id="write_log_user_status_is_invalid",
            message="Invalid status for user",
            log='{{dag_run.conf.lookuptable}}',
            properties={
                
                "EmployeeId": '{{dag_run.conf.employeeid}}',
                "Username": '{{dag_run.conf.firstname}}' + '{{dag_run.conf.lastname}}',
                "Action": "na",
                "Status": "Ignored",
                "Details": "Invalid user status received hence not processed.",
                "childjobid": "{{dag_run_ecid()}}"
            }
        )

        if_user_present_in_replicon = rail.IfOperator(
            task_id="if_user_present_in_replicon",
            test=lambda :bool(rail.result(
                "create_user_config_for_mapper_values")["useruri"]),
            yes_task="if_user_disabled_in_replicon",
            no_task="write_log_user_not_present"
        )

        write_log_user_not_present = rail.WriteLogOperator(
            task_id="write_log_user_not_present",
            message="User status received as terminated, hence not added",
            log='{{dag_run.conf.lookuptable}}',
            properties={
                
                "EmployeeId": '{{dag_run.conf.employeeid}}',
                "Username": '{{dag_run.conf.firstname}}' + '{{dag_run.conf.lastname}}',
                "Action": "Disable",
                "Status": "Ignored",
                "Details": "User status received as terminated, hence not added",
                
            }
        )

        if_user_disabled_in_replicon = rail.IfOperator(
            task_id="if_user_disabled_in_replicon",
            test=lambda :bool(str(rail.result(
                "create_user_config_for_mapper_values")["status"]).lower() == "disabled"),
            yes_task="write_log_user_disabled_in_replicon",
            no_task="process_disable_user"
        )

        write_log_user_disabled_in_replicon = rail.WriteLogOperator(
            task_id="write_log_user_disabled_in_replicon",
            message="User is already disabled in Replicon",
            log='{{dag_run.conf.lookuptable}}',
            properties={
                
                "EmployeeId": '{{dag_run.conf.employeeid}}',
                "Username": '{{dag_run.conf.firstname}}' + '{{dag_run.conf.lastname}}',
                "Action": "Disable",
                "Status": "Ignored",
                "Details": "User is already disabled in Replicon",
                
            }
        )

        process_disable_user = rail.TriggerDagRunOperator(
            task_id="process_disable_user",
            trigger_dag_id=config.sigroup_disable_user_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=custom_methods.get_user_config
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            trigger_rule="one_failed",
            message="User is not processed",
            log='{{dag_run.conf.lookuptable}}',
            properties={
                
                "EmployeeId": '{{dag_run.conf.employeeid}}',
                "Username": '{{dag_run.conf.firstname}}' + '{{dag_run.conf.lastname}}',
                "Action": "na",
                "Status": "Error",
                "Details": "{{get_error_message()}}",
                
            }
        )

        bulk_get_users >> derive_group_identfier_strings >>\
        aggregate_mapping_values >>\
        create_user_config_for_mapper_values >>\
        if_user_is_active >> rail.Label("Yes") >> if_location_code_present
        if_user_is_active >> rail.Label("No") >> if_user_terminated
        if_location_code_present >> rail.Label("Yes") >>\
            get_shift_mapper_values >> if_shift_useruri_present
        if_location_code_present >> rail.Label("No") >>\
            if_useruri_present >> rail.Label(
                "Yes") >> process_update_user >> catch_and_log_errors
        if_useruri_present >> rail.Label(
            "No") >> process_add_user >> catch_and_log_errors
        if_shift_useruri_present >> rail.Label(
            "Yes") >> process_update_shift_user >> catch_and_log_errors
        if_shift_useruri_present >> rail.Label(
            "No") >> process_add_shift_user >> catch_and_log_errors
        if_user_terminated >> rail.Label("Yes") >>\
            if_user_present_in_replicon
        if_user_terminated >> rail.Label(
            "No") >> write_log_user_status_is_invalid >> catch_and_log_errors
        if_user_present_in_replicon >> rail.Label("Yes") >>\
            if_user_disabled_in_replicon
        if_user_present_in_replicon >> rail.Label(
            "No") >> write_log_user_not_present >> catch_and_log_errors
        if_user_disabled_in_replicon >> rail.Label(
            "Yes") >> write_log_user_disabled_in_replicon >> catch_and_log_errors
        if_user_disabled_in_replicon >> rail.Label("No") >>\
            process_disable_user >> catch_and_log_errors

        return dag

rail.for_each_instance(create_airflow_dag)
