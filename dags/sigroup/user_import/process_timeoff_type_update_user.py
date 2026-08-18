from datetime import datetime, timedelta
import json
from sigroup.user_import.utils import custom_methods
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_user_import_timeoff_type_for_update_user,
       description="sigroup user import update user assign timeoff types child",
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
            python_callable=lambda: list(filter(lambda i : i["uri"] is not None,map(lambda i: {
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

        get_new_timeoff_type_from_mapper = rail.PythonOperator(
            task_id="get_new_timeoff_type_from_mapper",
            python_callable=custom_methods.get_new_timeoff_types
        )

        get_timeoff_type_to_disable = rail.PythonOperator(
            task_id="get_timeoff_type_to_disable",
            python_callable=custom_methods.get_disable_timeoff_types
        )

        # Only the types that still carry a policy need a payout line. This used to be an
        # IfOperator inside a ForEachOperator, but rail's ForEachOperator runs its body
        # synchronously and does not support deferrable tasks, so the TriggerDagRunOperator
        # below could never wait for completion from inside the loop.
        get_timeoff_type_to_payout = rail.PythonOperator(
            task_id="get_timeoff_type_to_payout",
            python_callable=lambda: list(filter(
                lambda i: i["policy"], rail.result("get_timeoff_type_to_disable")))
        )

        get_balance_summary_for_account = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_balance_summary_for_account",
            items='{{result("get_timeoff_type_to_payout")|to_json}}',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda item, dag_run: {
                "account": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": item["uri"]
                },
                "asOfDate": rail.parse_date(dag_run.conf["actioneffectivedate"], "%m/%d/%Y")
                }
        )

        process_timeoff_payout = rail.trigger_parallel_dagrun(
            task_id="process_timeoff_payout",
            items='{{result("get_timeoff_type_to_payout")|to_json}}',
            trigger_dag_id=config.sigroup_user_import_timeoff_type_for_update_user_payout_user,
            parallel_count=config.time_off_policy_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "user_uri": dag_run.conf["useruri"],
                "timeoffuri": item["uri"],
                "policyset": item["policy"],
                "newschedulebalance": rail.result("get_balance_summary_for_account")[
                    rail.result("get_timeoff_type_to_payout").index(item)]["timeRemaining"],
                "enddate": datetime.strftime(datetime.strptime(
                    dag_run.conf["actioneffectivedate"], custom_methods.MMDDYYY), "%d/%m/%Y"),
                "lookuptable": dag_run.conf["lookuptable"],
                "employeeid": dag_run.conf["employeeid"],
                "firstname": dag_run.conf["firstname"],
                "lastname": dag_run.conf["lastname"],
                "startingbalancesettouri": dag_run.conf["startingbalancesettouri"],
                "preventbalanceoverdrawuri": dag_run.conf["preventbalanceoverdrawuri"],
                "loginname": dag_run.conf["loginname"],
                "enddateday":dag_run.conf["actioneffectivedate"].split("/")[1],
                "enddatemonth":dag_run.conf["actioneffectivedate"].split("/")[0],
                "enddateyear":dag_run.conf["actioneffectivedate"].split("/")[-1]
            }
        )

        assign_required_timeoff_types = rail.RepliconServiceOperator(
            task_id="assign_required_timeoff_types",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf["useruri"],
                "timeOffTypeUris": list(map(lambda i: i["uri"],
                                            rail.result("get_timeoff_types_data")))
            }
        )

        get_default_timeoff_policies_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_default_timeoff_policies_for_user",
            items='{{result("get_timeoff_types_data")|to_json}}',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": '{{dag_run.conf.useruri}}',
                    "timeOffTypeUri": '{{item.uri}}'
                }
            }
        )

        assign_default_timeoff_policies = rail.RepliconServiceCallForEachItemOperator(
            task_id="assign_default_timeoff_policies",
            items='{{result("get_timeoff_types_data")|to_json}}',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run,item: {
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": item["uri"]
                },
                "policySetScheduleEntries": json.loads(json.dumps(rail.result('get_default_timeoff_policies_for_user')
                                                                  [rail.result("get_timeoff_types_data").index(item)])
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
            }
        )

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
        get_new_timeoff_type_from_mapper >> get_timeoff_type_to_disable >>\
        get_timeoff_type_to_payout >> get_balance_summary_for_account >>\
        process_timeoff_payout >> assign_required_timeoff_types >>\
        get_default_timeoff_policies_for_user >> assign_default_timeoff_policies >> catch_and_log_errors
        return dag


rail.for_each_instance(create_airflow_dag)
