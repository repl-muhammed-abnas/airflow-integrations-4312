from datetime import timedelta
from dateutil.relativedelta import relativedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global_v2.utils import custom_methods as gbl_custom_methods  
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods \
    import get_tenure_value

def create_update_user_vacation_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_canada_users_process_canada_vacation_timeoff_type_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.global_update_user_timeoff_assignment_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_canada, default_var='true').lower() == 'true',
            no_task="get_default_policies_for_timeoff_type",
            yes_task="batch_task"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_default_policies_for_timeoff_type",
            end_task="end_timeoff_process",
            execution_timeout=timedelta(days=14)
        )
        
        get_default_policies_for_timeoff_type = rail.RepliconServiceOperator(
            task_id = "get_default_policies_for_timeoff_type",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data= {
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_type_uri }}"
            }
        )

        def prepare_payload_callable(dag_run):
            continues_service_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['json_formatted_dates']['continuous_service_date'])
            start_day = dag_run.conf['json_formatted_dates']['start_date']
            tenure = get_tenure_value(continues_service_date, gbl_custom_methods.convert_json_date_to_date(start_day))

            default_policy = rail.result("get_default_policies_for_timeoff_type")
            policy_sets = []
            policy_to_assign = []
            current_policy_assigned_found = False
            for _policy in default_policy:
                if _policy['startOffset']['offsetValue'] > tenure:
                    policy_sets.append({
                        "offset": _policy['startOffset']['offsetValue'],
                        "policy": _policy['policySet'],
                        "first": "No"
                    })

                if _policy['startOffset']['offsetValue'] == tenure:
                    current_policy_assigned_found = True
                    policy_sets.append({
                        "offset": _policy['startOffset']['offsetValue'],
                        "policy": _policy['policySet'],
                        "first": "Yes"
                    })

            _temp_policy_set_list = []
            if not current_policy_assigned_found:
                max_diff = None
                for _policy1 in default_policy:
                    if _policy1['startOffset']['offsetValue'] < tenure:
                        current_diff = float(_policy['startOffset']['offsetValue']) - tenure
                        if not max_diff:
                            max_diff = current_diff
                        _temp_policy_set_list.append(
                            {
                                "offset": _policy1['startOffset']['offsetValue'],
                                "policy": _policy1['policySet'],
                                "diff":  float(_policy1['startOffset']['offsetValue']) - tenure
                            }
                        )
                
                if max_diff:
                    for _policy2 in _temp_policy_set_list:
                        if float(_policy2['offset']) == float(max_diff):
                            current_diff = float(_policy2['offset']) - tenure
                            policy_sets.append(
                                {
                                    "offset": _policy2['offset'],
                                    "policy": _policy2['policySet'],
                                    "first": "Yes"
                                }
                            )
            for each_policy in policy_sets:
                if each_policy['first'] == "Yes":
                    policy_to_assign.append({
                        "description": f"Effective on {start_day['day']}/{start_day['month']}/{start_day['year']}",
                        "effectiveDate": start_day,
                        "policySet": each_policy['policy']
                    })
                else:
                    effective_date = continues_service_date + relativedelta(years=each_policy['offset'])
                    policy_to_assign.append({
                        "description": f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate": {
                            "day": effective_date.day,
                            "month": effective_date.month,
                            "year": effective_date.year
                        },
                        "policySet": each_policy['policy']
                    })

            return loads(dumps(policy_to_assign).replace("\"script\"", "\"scriptTarget\""))

        prepare_payload = rail.PythonOperator(
            task_id = "prepare_payload",
            python_callable= prepare_payload_callable
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda : bool(rail.result("prepare_payload")),
            yes_task="assign_policy",
            no_task="end_timeoff_process"
        )

        assign_policy = rail.RepliconServiceOperator(
            task_id = "assign_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": rail.result("prepare_payload")
            }        
        )

        end_timeoff_process = rail.EmptyOperator(
            task_id = "end_timeoff_process"
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Update",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Update',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("No") >> get_default_policies_for_timeoff_type
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> end_timeoff_process >> catch_and_log_error

        get_default_policies_for_timeoff_type >> prepare_payload >> has_any_policy_to_assign >> rail.Label(
            "Yes") >> assign_policy >> end_timeoff_process >> catch_and_log_error
        has_any_policy_to_assign >> rail.Label("No") >> end_timeoff_process >> catch_and_log_error

        return dag

rail.for_each_instance(create_update_user_vacation_timeoff_assignment_dag)
