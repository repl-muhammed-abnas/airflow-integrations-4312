from datetime import timedelta
from json import dumps, loads
import rail
from airflow.models import Variable


null = None

def create_update_user_timeoff_assignment_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id = f"{config.workday_user_import_hungary_update_user_timeoff_assignment_dag}{prefix}",
            description = config.workday_user_import_hungary_update_user_timeoff_assignment_dag_description,
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs = config.max_active_run_update_user_timeoff_assignment_hungary
        ) as dag:

            rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id = "can_run_batch_task",
                test=lambda: Variable.get(
                config.can_run_batch_task_var_name_hungary, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="is_ia_updated"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id = "batch_task",
                start_task="is_ia_updated",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            def check_ia_updated(dag_run):
                ia_updated = dag_run.conf.get('ia_updated')
                return ia_updated in [True, 'true', 'True']

            is_ia_updated = rail.IfOperator(
                task_id = "is_ia_updated",
                test = check_ia_updated,
                yes_task = "is_ia_1",
                no_task = "get_default_timeoff_policy"
            )

            def check_is_ia_1(dag_run):
                is_ia = dag_run.conf['file_data']['is_ia']
                return is_ia in ['1', 1]

            is_ia_1 = rail.IfOperator(
                task_id = "is_ia_1",
                test = check_is_ia_1,
                yes_task = "trigger_ia_one_timeoff_assignment",
                no_task = "trigger_ia_zero_timeoff_assignment"
            )

            def get_ia_one_conf(dag_run):
                timeoff_details = dag_run.conf.get('timeoff_type_details', {})
                file_data = dag_run.conf.get('file_data', {})
                # Use IA start date
                start_date = dag_run.conf['file_data']['ia_start_date']
                json_start_date = dag_run.conf['ia_start_date']
            
                return {
                    "file_name": dag_run.conf['file_name'],
                    "login_name": dag_run.conf['loginName'],
                    "email_id": file_data['email_id'],
                    "emp_id": file_data['emp_id'],
                    "user_uri": dag_run.conf['user_uri'],
                    "user_log": dag_run.conf['user_log'],
                    "company_code": file_data['company_code'],
                    "source": file_data['parent_company'],
                    "start_date": start_date,
                    "country": file_data['country'],
                    "personnel_subarea": "",
                    "employee_group":"",
                    "employee_subgroup": "",
                    "contineous_service_date": file_data['hire_date'],
                    "timeoff_uri": dag_run.conf['timeoff_type_uri'],
                    "timeoff_name": timeoff_details['name'],
                    "secondary_timeoff_uri": None,
                    "policy": [],
                    "json_formatted_dates": {
                        "start_date": json_start_date
                    }
                }
            
            trigger_ia_one_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
                task_id = "trigger_ia_one_timeoff_assignment",
                items=[1],
                trigger_dag_id=config.workday_user_import_ia_one_timeoff_assignment_child_dag,
                conf=get_ia_one_conf,
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

            wait_for_process_timeoffs_ia_one = rail.WaitForDagRunsSensor(
                    task_id="wait_for_process_timeoffs_ia_one",
                    dag_runs="{{result('trigger_ia_one_timeoff_assignment')}}",
                    execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            def get_ia_zero_conf(dag_run):
                timeoff_details = dag_run.conf.get('timeoff_type_details', {})
                file_data = dag_run.conf.get('file_data', {})
                # For IA=0, we need to determine the start date differently
                # The IA zero flow typically uses hire_date as start_date
                
                # Use hire date (default for IA zero)
                start_date = file_data['hire_date']
                json_start_date = dag_run.conf['json_formatted_dates']['hire_date']
                
                return {
                    "file_name": dag_run.conf['file_name'],
                    "login_name": dag_run.conf['loginName'],
                    "email_id": file_data['email_id'],
                    "emp_id": file_data['emp_id'],
                    "user_uri": dag_run.conf['user_uri'],
                    "user_log": dag_run.conf['user_log'],
                    "company_code": file_data['company_code'],
                    "source": file_data['parent_company'],
                    "start_date": start_date,
                    "ia_end_date": file_data['ia_end_date'],
                    "country": file_data['country'],
                    "personnel_subarea": "",
                    "employee_group":"",
                    "employee_subgroup": "",
                    "contineous_service_date": file_data['hire_date'],
                    "timeoff_uri": dag_run.conf['timeoff_type_uri'],
                    "timeoff_name": timeoff_details['name'],
                    "secondary_timeoff_uri": None,
                    "policy": [],
                    "json_formatted_dates": {
                        "start_date": json_start_date,
                        "ia_end_date": dag_run.conf['json_formatted_dates']['ia_end_date']
                    }
                }
            
            trigger_ia_zero_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
                task_id = "trigger_ia_zero_timeoff_assignment",
                items=[1],
                trigger_dag_id=config.workday_user_import_ia_zero_timeoff_assignment_child_dag,
                conf=get_ia_zero_conf,
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

            wait_for_process_timeoffs_ia_zero = rail.WaitForDagRunsSensor(
                    task_id="wait_for_process_timeoffs_ia_zero",
                    dag_runs="{{result('trigger_ia_zero_timeoff_assignment')}}",
                    execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            get_default_timeoff_policy = rail.RepliconServiceOperator(
                task_id = "get_default_timeoff_policy",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
                data=lambda dag_run: {
                    "timeOffAccount":{
                        "userUri" : dag_run.conf['user_uri'],
                        "timeOffTypeUri": dag_run.conf['timeoff_type_uri'],
                    }
                }
            )

            def process_policy_set_to_assign():
                policy_data = rail.result("get_default_timeoff_policy")
                if not policy_data:
                    return []
                
                # Convert policy data
                processed_data = loads(dumps(policy_data)
                    .replace("null", "\"effective\"")
                    .replace("\"script\"", "\"scriptTarget\""))
                
                return processed_data
            
            policy_set_to_assign = rail.PythonOperator(
                task_id='policy_set_to_assign',
                python_callable=process_policy_set_to_assign
            )

            def has_valid_policies_to_assign():
                # Check both possible sources of policy data
                policy_data = rail.result("policy_set_to_assign", None)
                return policy_data is not None and bool(policy_data)

            has_any_policy_to_assign = rail.IfOperator(
                task_id = "has_any_policy_to_assign",
                test=has_valid_policies_to_assign,
                yes_task="put_user_timeoff_policy_set",
                no_task="catch_and_log_error"
            )

            def get_put_user_timeoff_policy_set_data(dag_run):
                return {
                    "timeOffAccount": {
                        "userUri": dag_run.conf['user_uri'],
                        "timeOffTypeUri": dag_run.conf['timeoff_type_uri'],
                    },
                    "policySetScheduleEntries": rail.result('policy_set_to_assign')
                }

            put_user_timeoff_policy_set = rail.RepliconServiceOperator(
                task_id = "put_user_timeoff_policy_set",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=get_put_user_timeoff_policy_set_data
            )

            def get_timeoff_update_error_log_properties(dag_run):
                # Check if we have valid dag_run.conf
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return {
                        "Jobid": "",
                        "Userid": "Unknown",
                        "Email": "Unknown",
                        "Action": "Update",
                        "Status": "Error",
                        "Details": "Missing dag_run.conf data"
                    }

                emp_id = dag_run.conf.get('emp_id', 'Unknown')
                email_id = dag_run.conf.get('email_id', 'Unknown')
                error_message = rail.render_template("{{get_error_message()}}")

                return {
                    "Jobid": "",
                    "Userid": emp_id,
                    "Email": email_id,
                    "Action": "Update",
                    "Status": "Error",
                    "Details": error_message
                }

            catch_and_log_error = rail.WriteLogOperator(
                task_id = "catch_and_log_error",
                log = '{{ dag_run.conf.user_log_name }}',
                trigger_rule = "one_failed",
                message="User Update",
                severity="Error",
                properties=get_timeoff_update_error_log_properties
            )
            
            # Main flow
            can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
            can_run_batch_task >> rail.Label("No") >> is_ia_updated
            
            # IA flow branches
            is_ia_updated >> rail.Label("Yes") >> is_ia_1
            is_ia_updated >> rail.Label("No") >> get_default_timeoff_policy
            
            # IA-1 and IA-0 branches
            is_ia_1 >> rail.Label("Yes") >> trigger_ia_one_timeoff_assignment >> wait_for_process_timeoffs_ia_one >> catch_and_log_error
            is_ia_1 >> rail.Label("No") >> trigger_ia_zero_timeoff_assignment >> wait_for_process_timeoffs_ia_zero >> catch_and_log_error
            
            # Non-IA policy assignment flow
            get_default_timeoff_policy >> policy_set_to_assign >> has_any_policy_to_assign
            has_any_policy_to_assign >> rail.Label("Yes") >> put_user_timeoff_policy_set >> catch_and_log_error
            has_any_policy_to_assign >> rail.Label("No") >> catch_and_log_error

            _dags.append(dag)
    return _dags

rail.for_each_instance(create_update_user_timeoff_assignment_dag)
