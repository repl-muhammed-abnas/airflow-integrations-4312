from datetime import timedelta
from dateutil.relativedelta import relativedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_hungary_v1.utils.custom_methods import convert_json_date_to_date

def create_user_timeoff_assignment_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.workday_user_import_ia_one_timeoff_assignment_child_dag}{prefix}",
            description="dxctechnology workday user sync process users child",
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2023, 9, 26),
            max_active_runs=config.max_active_run_process_ia_1_timeoff_assignment
        ) as dag:

            rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id = "can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_hungary, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="get_default_policies_for_timeoff_type"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id = "batch_task",
                start_task="get_default_policies_for_timeoff_type",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            get_default_policies_for_timeoff_type = rail.RepliconServiceOperator(
                task_id = "get_default_policies_for_timeoff_type",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data= lambda dag_run : {
                    "timeOffTypeUri": dag_run.conf['secondary_timeoff_uri'] if dag_run.conf['secondary_timeoff_uri'] else dag_run.conf['timeoff_uri']
                }
            )


            def get_payload_to_process(dag_run):
                start_date = convert_json_date_to_date(dag_run.conf['json_formatted_dates']['start_date'])
                policy_sets = []
                policy_to_assign = []

                default_policies = rail.result("get_default_policies_for_timeoff_type")

                for _policy in default_policies:
                    policy_sets.append({
                        "offset": _policy['startOffset']['offsetValue'],
                        "policy": _policy['policySet'],
                        "first": "Yes"
                    })

                policies_from_caller = dag_run.conf['policy'] if dag_run.conf['policy'] else []

                for policy in policies_from_caller:
                    if convert_json_date_to_date( policy['effectiveDate']) < start_date:
                        policy_to_assign.append(
                            {
                                "description": policy['description'],
                                "effectiveDate": policy['effectiveDate'],
                                'policySet': policy['policySets']
                            }
                        )
                    
                for each_policy in policy_sets:
                    _effective_date = start_date + relativedelta(year=int(each_policy['offset'] if each_policy['offset'] else 0))
                    policy_to_assign.append(
                        {
                            "description": f"Effective on {_effective_date.day}/{_effective_date.month}/{_effective_date.year}",
                            "effectiveDate": {
                                "day": _effective_date.day,
                                "month": _effective_date.month,
                                "year": _effective_date.year
                            },
                            'policySet': each_policy['policy']
                        }
                    )

                return loads(dumps(policy_to_assign).replace("\"script\"", "\"scriptTarget\""))
        
            generate_payload_for_policy_assignment = rail.PythonOperator(
                task_id = "generate_payload_for_policy_assignment",
                python_callable=get_payload_to_process
            )

            has_any_policy_to_assign = rail.IfOperator(
                task_id = "has_any_policy_to_assign",
                test=lambda: bool(rail.result("generate_payload_for_policy_assignment")),
                yes_task="assign_policy",
                no_task="catch_and_log_error"
            )

            assign_policy = rail.RepliconServiceOperator(
                task_id = "assign_policy",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data = lambda dag_run: {
                    "timeOffAccount": {
                        "userUri": dag_run.conf['user_uri'],
                        "timeOffTypeUri": dag_run.conf['timeoff_uri']
                    },
                    "policySetScheduleEntries": rail.result("generate_payload_for_policy_assignment")
                }        
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

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
            can_run_batch_task >> rail.Label("No") >> get_default_policies_for_timeoff_type >> generate_payload_for_policy_assignment >> has_any_policy_to_assign
            has_any_policy_to_assign >> rail.Label("Yes") >> assign_policy >> catch_and_log_error
            has_any_policy_to_assign >> rail.Label("No") >> catch_and_log_error

            _dags.append(dag)

    return _dags

rail.for_each_instance(create_user_timeoff_assignment_dag)
