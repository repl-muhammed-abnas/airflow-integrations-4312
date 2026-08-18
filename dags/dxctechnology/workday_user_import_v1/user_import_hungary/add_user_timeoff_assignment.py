from json import dumps, loads
import rail
from airflow.models import Variable

from dxctechnology.workday_user_import_v1.user_import_hungary.utils import custom_methods,request_payload
from datetime import timedelta

null = None

def create_add_user_timeoff_assignment_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.workday_user_import_hungary_add_user_timeoff_assignment_dag}{prefix}",
            description=config.workday_user_import_hungary_add_user_timeoff_assignment_dag_description,
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_run_add_user_timeoff_assignemnt_hungary,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_hungary, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="get_default_timeoff_policy"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id="batch_task",
                start_task="get_default_timeoff_policy",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

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
                policy_data = rail.result("policy_set_to_assign", None)
                return policy_data is not None

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
            can_run_batch_task >> rail.Label("No") >> get_default_timeoff_policy >> policy_set_to_assign >> has_any_policy_to_assign
            
            has_any_policy_to_assign >> rail.Label('Yes') >> put_user_timeoff_policy_set >> stop
            has_any_policy_to_assign >> rail.Label('No') >> stop >> rail.Label("On Error") >> catch_and_log_error

            _dags.append(dag)

    return _dags

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
