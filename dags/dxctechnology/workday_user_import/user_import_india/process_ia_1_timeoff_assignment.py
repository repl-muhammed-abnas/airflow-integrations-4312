
from dateutil.relativedelta import relativedelta
from json import dumps, loads
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

from dxctechnology.workday_user_import.user_import_global.utils import custom_methods as gbl_custom_methods
from datetime import timedelta

DATE_FORMAT = "%Y-%d-%m"
null = None

# pylint: disable=too-many-statements
def create_ia_1_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id = config.india_update_user_ia_1_timeoff_assignment_dag_id,
        description = "DXC Workday User Import INDIA - Process Update User IA 1 TimeOff Assignment",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.max_active_run_update_user_ia_1_timeoff_assignment_india
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_india, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_default_policy_for_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_default_policy_for_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        get_default_policy_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_policy_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoff_type_uri }}"
            }
        )

        def get_policy_to_assign_callable(dag_run):
            default_policy_for_user = rail.result('get_default_policy_for_user')
            policy_set = []
            policy_to_assign = []

            for item in default_policy_for_user:
                policy_set.append({
                    "offset": item['startOffset']['offsetValue'],
                    "policy": item['policySet']
                })

            _policy_set = dag_run.conf['policy_set'] if dag_run.conf['policy_set'] else []
            for policy in _policy_set:
                effective_date = policy['effectiveDate']
                effective_date_obj = gbl_custom_methods.convert_json_date_to_date(effective_date)
                start_date_obj = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['start_date_json_format'])
                if effective_date_obj < start_date_obj:
                    policy_to_assign.append(
                        {
                            'description' : policy['description'],
                            "effectiveDate" : effective_date,
                            "policySet": policy['policySet']

                        }
                )
                    
            for policy in policy_set:
                start_date_obj = gbl_custom_methods.convert_json_date_to_date(dag_run.conf['start_date_json_format'])
                effective_date = start_date_obj + relativedelta(years=int(policy['offset']))
                
                policy_to_assign.append(
                    {
                        'description' : f"Effective on {effective_date.day}/{effective_date.month}/{effective_date.year}",
                        "effectiveDate" : {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        },
                        "policySet": policy['policy']

                    }
                )

            rail.set_result(key="policy_set", val=policy_set)
            
            return policy_to_assign

        get_policy_to_assign = rail.PythonOperator(
            task_id = "get_policy_to_assign",
            python_callable=get_policy_to_assign_callable
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda: bool(rail.result('get_policy_to_assign')),
            yes_task="assign_policy",
            no_task="catch_and_log_error"
        )

        assign_policy = rail.RepliconServiceOperator(
            task_id = "assign_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result('get_policy_to_assign')).replace("\"script\"", "\"scriptTarget\""))
            }        
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Update",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid":  "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Update',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_default_policy_for_user

        get_default_policy_for_user >> get_policy_to_assign >> has_any_policy_to_assign
        has_any_policy_to_assign >> rail.Label('Yes') >> assign_policy >> catch_and_log_error
        has_any_policy_to_assign >> rail.Label('No') >> catch_and_log_error
        
        return dag

rail.for_each_instance(create_ia_1_timeoff_assignment_dag)
