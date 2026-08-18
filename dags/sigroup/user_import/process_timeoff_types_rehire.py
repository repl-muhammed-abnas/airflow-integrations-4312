import json
from sigroup.user_import.utils import custom_methods
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_user_import_timeoff_type_for_rehire_user,
       description="sigroup user import rehire assign timeoff types child",
        max_active_runs=config.child_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        get_all_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_types",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_timeoff_types_list_data = rail.PythonOperator(
            task_id="get_timeoff_types_list_data",
            python_callable=lambda dag_run:list(map(lambda i:i.strip(),
                list(dag_run.conf["timeofftypes"].split("|"))))
        )

        get_timeoff_types_data = rail.PythonOperator(
            task_id="get_timeoff_types_data",
            python_callable=lambda : list(filter(lambda i : i["uri"] is not None,map(lambda i: {
                "uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_timeoff_types"),
                    "displayText",
                    i,
                    "uri"),
                "name":rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_timeoff_types"),
                    "displayText",
                    i,
                    "displayText")
            }, rail.result("get_timeoff_types_list_data"))))
        )

        get_timeoff_type_policy = rail.RepliconServiceOperator(
            task_id="get_timeoff_type_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": '{{dag_run.conf.useruri}}'
            },
            data_handler=lambda response: list(filter(lambda i : i["uri"] is not None,map(lambda i:{
                "name": i["timeOffType"]["name"],
                "enabled": i["isTimeOffAllowedAgainstThisTimeOffType"],
                "uri": i["timeOffType"]["uri"],
                "policy": i["policySetSchedule"],
            }, response["policiesByTimeOffType"])))
        )

        get_timeoff_types_to_retain = rail.PythonOperator(
            task_id="get_timeoff_types_to_retain",
            python_callable=custom_methods.get_time_off_to_retain
        )

        get_new_timeoff_type_from_mapper = rail.PythonOperator(
            task_id="get_new_timeoff_type_from_mapper",
            python_callable=custom_methods.get_new_timeoff_types
        )

        if_assign_required_timeoff_types = rail.IfOperator(
            task_id="if_assign_required_timeoff_types",
            test=lambda :bool(rail.result("get_new_timeoff_type_from_mapper")),
            yes_task="assign_required_timeoff_types",
            no_task="catch_and_log_errors"
        )

        assign_required_timeoff_types = rail.RepliconServiceOperator(
            task_id="assign_required_timeoff_types",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf["useruri"],
                "timeOffTypeUris": rail.result("get_new_timeoff_type_from_mapper")
            }
        )

        get_default_timeoff_policies_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_default_timeoff_policies_for_user",
            items='{{result("get_new_timeoff_type_from_mapper")|to_json}}',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run,item:{
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": item
                }
            }
        )

        assign_default_timeoff_policies = rail.RepliconServiceCallForEachItemOperator(
            task_id="assign_default_timeoff_policies",
            items='{{result("get_new_timeoff_type_from_mapper")|to_json}}',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run,item: {
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": item
                },
                "policySetScheduleEntries": json.loads(json.dumps(rail.result('get_default_timeoff_policies_for_user')
                                                                  [rail.result("get_new_timeoff_type_from_mapper").index(item)])
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
            }
        )

        get_date_to_consider = rail.PythonOperator(
            task_id="get_date_to_consider",
            python_callable=custom_methods.get_action_date_to_consider
        )

        get_year_of_service = rail.PythonOperator(
            task_id="get_year_of_service",
            python_callable=custom_methods.get_tenure
        )

        process_policies_for_rehire = rail.ForEachOperator(
            task_id="process_policies_for_rehire",
            items=lambda:rail.result("get_timeoff_types_to_retain"),
            start_task="start_rehire_timeoff",
            end_task="end_rehire_timeoff"
        )

        start_rehire_timeoff = rail.EmptyOperator(task_id="start_rehire_timeoff")

        if_existing_timeoff = rail.IfOperator(
            task_id="if_existing_timeoff",
            test=lambda:bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_timeoff_type_policy"),
                "timeOffType.uri",
                rail.result("process_policies_for_rehire"),
                "policySetSchedule"
            )),
            yes_task="get_existing_policy_based_on_enddate",
            no_task="get_default_timeoff_policies_schedule"
        )

        get_existing_policy_based_on_enddate = rail.PythonOperator(
            task_id="get_existing_policy_based_on_enddate",
            python_callable=custom_methods.get_timeoff_policies_for_user
        )

        get_default_timeoff_policies_schedule = rail.RepliconServiceOperator(
            task_id="get_default_timeoff_policies_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda:{
                "timeOffTypeUri": rail.result("process_policies_for_rehire")
            }
        )

        get_offset_count_list = rail.PythonOperator(
            task_id="get_offset_count_list",
            python_callable=custom_methods.get_offset_count_policies
        )

        get_new_policies_to_assign = rail.PythonOperator(
            task_id="get_new_policies_to_assign",
            python_callable=custom_methods.get_all_new_policy_data
        )

        if_policies_set_schedule = rail.IfOperator(
            task_id="if_policies_set_schedule",
            test=lambda:bool (rail.result("get_new_policies_to_assign")),
            yes_task="put_policy_set_schedule",
            no_task="end_rehire_timeoff"
        )

        put_policy_set_schedule = rail.RepliconServiceOperator(
            task_id="put_policy_set_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": rail.result("process_policies_for_rehire")
                },
                "policySetScheduleEntries": json.loads(json.dumps(rail.result("get_new_policies_to_assign"))
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
                }
        )

        end_rehire_timeoff= rail.EmptyOperator(task_id="end_rehire_timeoff")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="User update failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                
                "EmployeeId": dag_run.conf.get("employeeid"),
                "Username": dag_run.conf.get("firstname") + dag_run.conf.get("lastname"),
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template('{{get_error_message()}}'),
                
            }
        )

        get_all_timeoff_types >>\
        get_timeoff_types_list_data >>\
        get_timeoff_types_data >> get_timeoff_type_policy >>\
        get_timeoff_types_to_retain>>\
        get_new_timeoff_type_from_mapper >> \
        if_assign_required_timeoff_types >> rail.Label("No") >> catch_and_log_errors
        if_assign_required_timeoff_types >> rail.Label("Yes") >>\
        assign_required_timeoff_types >>\
        get_default_timeoff_policies_for_user >> assign_default_timeoff_policies >>\
        get_date_to_consider >> get_year_of_service >>\
        process_policies_for_rehire >> end_rehire_timeoff
        process_policies_for_rehire >>\
        start_rehire_timeoff>>\
        if_existing_timeoff >> rail.Label("Yes") >>\
        get_existing_policy_based_on_enddate >> get_default_timeoff_policies_schedule
        if_existing_timeoff >> rail.Label("No") >>\
        get_default_timeoff_policies_schedule >> get_offset_count_list >>\
        get_new_policies_to_assign >>\
        if_policies_set_schedule >> rail.Label("No") >> end_rehire_timeoff
        if_policies_set_schedule >> rail.Label("Yes") >>\
        put_policy_set_schedule >> end_rehire_timeoff >>\
        catch_and_log_errors
        return dag


rail.for_each_instance(create_airflow_dag)
