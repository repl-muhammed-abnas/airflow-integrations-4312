from json import dumps, loads
from dateutil.relativedelta import relativedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_tenure_value
from dxctechnology.workday_user_import.user_import_global.utils import custom_methods as gbl_custom_methods
from datetime import timedelta

null = None

def create_rehire_user_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.india_rehire_user_timeoff_assignment_dag_id,
        description="DXC Workday User Import Costa Rica - Process Rehire User TimeOff Assignment",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_rehire_user_timeoff_assignement_costa_rica
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_india, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_user_tenure_for_timeoff_type"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_user_tenure_for_timeoff_type",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_user_tenure_for_timeoff_type = rail.PythonOperator(
            task_id = "get_user_tenure_for_timeoff_type",
            python_callable= lambda dag_run: get_tenure_value(
                gbl_custom_methods.convert_json_date_to_date(dag_run.conf['continuous_service_date_json_format']),
                gbl_custom_methods.convert_json_date_to_date(dag_run.conf['start_date_json_format']))
        )

        get_default_policy_for_timeoff = rail.RepliconServiceOperator(
            task_id = "get_default_policy_for_timeoff",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_type_uri }}"
            }
        )

        def get_policy_to_assign_callable(dag_run):

            start_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['start_date_json_format'])
            continue_service_date = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['continuous_service_date_json_format'])

            tenure = rail.result('get_user_tenure_for_timeoff_type')

            policy_to_assign = []

            policy_sets = []

            first_policy_found = False
            default_policy_for_timeoff = rail.result("get_default_policy_for_timeoff")

            for policy in default_policy_for_timeoff:
                if policy['startOffset']['offsetValue'] == tenure:
                    first_policy_found = True
                    policy_sets.append(
                        {
                            "offset": policy['startOffset']['offsetValue'],
                            "policy": policy['policySet'],
                            "first": "Yes"
                        }
                    )

                if policy['startOffset']['offsetValue'] > tenure:
                    policy_sets.append(
                        {
                            "offset": policy['startOffset']['offsetValue'],
                            "policy": policy['policySet'],
                            "first": "No"
                        }
                    )

            if not first_policy_found:
                off_set_list = list(filter(lambda row: row['to_be_considered']=="Yes", map(lambda _policy: {
                    "offset": _policy['startOffset']['offsetValue'],
                    "day_diff": float(_policy['startOffset']['offsetValue']) - tenure,
                    "to_be_considered" : "Yes" if float(_policy['startOffset']['offsetValue']) < tenure else "No",
                    "policy": _policy['policySet']
                }, default_policy_for_timeoff)))

                if off_set_list:
                    min_daydiff = min([offset['day_diff'] for offset in off_set_list])
                    min_offset_data = rail.find_first_by_attr_and_get_attr(off_set_list, 'day_diff', min_daydiff, default={})

                    policy_sets.append(
                        {
                            "offset": min_offset_data['offset'],
                            "policy": min_offset_data['policy'],
                            "first": "Yes"
                        }
                    )

            current_timeoff_policies = dag_run.conf['current_timeoff_policies'] if isinstance(dag_run.conf['current_timeoff_policies'], list) else loads(dag_run.conf['current_timeoff_policies'])
            for policy_line in current_timeoff_policies:
                if gbl_custom_methods.convert_json_date_to_date(policy_line['effectiveDate']) < start_date:
                    policy_to_assign.append(
                        {
                            "description": policy_line['description'],
                            "effectiveDate": policy_line['effectiveDate'],
                            "policySet": policy_line['policySet']
                        }
                    )

            for policy_set in policy_sets:
                effective_date = start_date + relativedelta(years=int(policy_set['offset']))
                policy_to_assign.append(
                    {
                        "description": f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        },
                        "policySet": policy_set['policy']
                    }
                )

            return loads(dumps(policy_to_assign).replace("\"script\"", "\"scriptTarget\"")) if policy_to_assign else null

        get_policy_to_assign = rail.PythonOperator(
            task_id = "get_policy_to_assign",
            python_callable=get_policy_to_assign_callable
        )

        has_policy_to_assign = rail.IfOperator(
            task_id = "has_policy_to_assign",
            test="{{ result('get_policy_to_assign') |is_truthy }}",
            yes_task="assign_policy",
            no_task="finish"
        )

        assign_policy = rail.RepliconServiceOperator(
            task_id = "assign_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": rail.result("get_policy_to_assign")
            }
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Update Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['emp_id'],
                "Email": dag_run.conf['email_id'],
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_user_tenure_for_timeoff_type

        get_user_tenure_for_timeoff_type >> get_default_policy_for_timeoff >> get_policy_to_assign >> has_policy_to_assign
        has_policy_to_assign >> rail.Label("No") >> finish
        has_policy_to_assign >> rail.Label("Yes") >> assign_policy >> finish >> catch_and_log_error

    return dag

rail.for_each_instance(create_rehire_user_dag)
