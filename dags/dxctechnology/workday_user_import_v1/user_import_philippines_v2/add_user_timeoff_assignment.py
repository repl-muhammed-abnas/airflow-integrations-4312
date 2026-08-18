from pendulum import datetime
from json import loads, dumps
import rail
from dxctechnology.workday_user_import_v1.user_import_philippines_v2.utils import request_payload, custom_methods
from airflow.models import Variable
from datetime import timedelta

def create_add_user_timeoff_assignment_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id= f"{config.workday_user_import_philippines_add_user_timeoff_assignment_dag}{prefix}",
            description=config.workday_user_import_philippines_add_user_timeoff_assignment_dag_description,
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2023, 9, 26),
            max_active_runs=config.max_active_run_add_user_timeoff_assignemnt_philippines
        ) as dag:

            rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id = "can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_philippines, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="check_marital_status_requirement"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id = "batch_task",
                start_task="check_marital_status_requirement",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            def check_marital_status_requirement_callable(dag_run):
                mapper_details = dag_run.conf.get('mapper_details', {})
                marital_status_required = mapper_details.get('Marital Status Required', 'No')
                
                if marital_status_required.lower() == 'yes':
                    file_data = dag_run.conf.get('file_data', {})
                    marital_status_ind = file_data.get('marital_status_ind', '')
                    return marital_status_ind.lower() == 'yes'
                
                return False

            check_marital_status_requirement = rail.IfOperator(
                task_id = "check_marital_status_requirement",
                test = check_marital_status_requirement_callable,
                yes_task = "get_default_timeoff_policy_for_type",
                no_task = "get_default_timeoff_policy"
            )

            # Get default policy for the timeoff type (not user-specific) when marital status = Yes
            get_default_timeoff_policy_for_type = rail.RepliconServiceOperator(
                task_id = "get_default_timeoff_policy_for_type",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data={
                    "timeOffTypeUri": "{{ dag_run.conf.timeoff_uri }}"
                }
            )

            # Process the timeoff type policy with marital status date
            process_marital_status_policy = rail.PythonOperator(
                task_id='process_marital_status_policy',
                python_callable=lambda dag_run: custom_methods.process_marital_status_policy_for_type(dag_run)
            )

            # Original flow for non-marital status timeoffs
            get_default_timeoff_policy = rail.RepliconServiceOperator(
                task_id = "get_default_timeoff_policy",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
                data=request_payload.get_default_timeoff_policy_payload
            )

            policy_set_to_assign = rail.PythonOperator(
                task_id='policy_set_to_assign',
                python_callable=custom_methods.get_policy_set_to_assign
            )

            def has_valid_policies():
                # Check both possible sources of policy data
                policy_data = rail.result("policy_set_to_assign", None) or rail.result("process_marital_status_policy", None)
                return policy_data is not None and bool(policy_data)

            has_any_policy_to_assign = rail.IfOperator(
                task_id = "has_any_policy_to_assign",
                test=has_valid_policies,
                yes_task="put_user_timeoff_policy_set",
                no_task= "stop"
            )

            stop = rail.EmptyOperator(
                task_id = "stop"
            )

            def get_put_user_timeoff_policy_set_data(dag_run):
                # Check if we went through marital status flow
                marital_policy = rail.result("process_marital_status_policy", None)
                if marital_policy:
                    # Need to do the script field transformation
                    transformed_policy = loads(dumps(marital_policy).replace("\"script\"", "\"scriptTarget\""))
                    return {
                        "timeOffAccount": {
                            "userUri": dag_run.conf['user_uri'],
                            "timeOffTypeUri": dag_run.conf['timeoff_uri']
                        },
                        "policySetScheduleEntries": transformed_policy
                    }
                # Otherwise use the standard flow
                return request_payload.get_put_user_timeoff_policy_set_payload(dag_run)

            put_user_timeoff_policy_set = rail.RepliconServiceOperator(
                task_id = "put_user_timeoff_policy_set",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=get_put_user_timeoff_policy_set_data
            )

            def get_timeoff_error_log_properties(dag_run):
                # Check if we have valid dag_run.conf
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return {
                        "Jobid": "",
                        "Userid": "Unknown",
                        "Email": "Unknown",
                        "Action": "Add",
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
                    "Action": "Add",
                    "Status": "Error",
                    "Details": error_message
                }

            catch_and_log_error = rail.WriteLogOperator(
                task_id = "catch_and_log_error",
                log = "{{ dag_run.conf.user_log }}",
                trigger_rule = "one_failed",
                message="User Add",
                severity="Error",
                properties=get_timeoff_error_log_properties
            )

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
            can_run_batch_task >> rail.Label("No") >> check_marital_status_requirement
            
            # Marital status flow
            check_marital_status_requirement >> rail.Label("Yes") >> get_default_timeoff_policy_for_type >> process_marital_status_policy >> has_any_policy_to_assign
            
            # Non-marital status flow
            check_marital_status_requirement >> rail.Label("No") >> get_default_timeoff_policy >> policy_set_to_assign >> has_any_policy_to_assign
            
            has_any_policy_to_assign >> rail.Label('Yes') >> put_user_timeoff_policy_set >> stop
            has_any_policy_to_assign >> rail.Label('No') >> stop >> rail.Label("On Error") >> catch_and_log_error

            _dags.append(dag)
    return _dags

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
