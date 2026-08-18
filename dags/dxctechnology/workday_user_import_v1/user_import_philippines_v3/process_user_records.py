from datetime import timedelta
import random
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_philippines_v3.utils.custom_methods import get_trigger_dag_id, get_item_index
null = None

def create_dag(config):

    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.workday_user_import_philippines_process_users_child_dag}{prefix}",
            description="DXC Technology Workday User Sync Philippines Process Users Child",
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2023, 9, 26),
            max_active_runs=config.max_active_run_process_each_users_philippines
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_philippines, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="create_user_log"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id="batch_task",
                start_task="create_user_log",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            create_user_log = rail.CreateLogOperator(
                task_id="create_user_log"
            )

            mapper_values_found = rail.IfOperator(
                task_id = "mapper_values_found",
                test = lambda dag_run: dag_run.conf['mapper_data']['mapper_values_found'] in [True],
                yes_task = "get_user_details_via_emp_id",
                no_task = "log_no_mapper_values_found_exception"
            )

            log_no_mapper_values_found_exception = rail.WriteLogOperator(
                task_id="log_no_mapper_values_found_exception",
                log="{{result('create_user_log')}}",
                message="User processing Exception",
                severity="Exception",
                properties=lambda dag_run: {
                    "Jobid": "",
                    "Userid": dag_run.conf['file_data']['emp_id'],
                    "Email": dag_run.conf['file_data']['email_id'],
                    "Action": "Validation",
                    "Status": "Exception",
                    "Details": "Record processing skipped, as no mapper values found for provided combination."
                }
            )

            # Check if the user already exists in Replicon
            get_user_details_via_emp_id = rail.RepliconServiceOperator(
                task_id="get_user_details_via_emp_id",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data={
                    "users": [
                        {
                            "uri": null,
                            "loginName": null,
                            "employeeId": "{{ dag_run.conf.file_data.emp_id }}",
                            "parameterCorrelationId": null
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=lambda response: response[0] if response else {}
            )

            # Determine if we need to add a new user or update an existing one
            is_user_found = rail.IfOperator(
                task_id="is_user_found",
                test="{{ result('get_user_details_via_emp_id') | is_truthy}}",
                yes_task="trigger_update_philippines_user",
                no_task="trigger_add_philippines_user"
            )

            # Trigger the update user workflow with enhanced error handling
            def prepare_update_user_conf(dag_run):
                user_details = rail.result('get_user_details_via_emp_id')
                user_details_obj = user_details.get('userDetails', {})
                user_uri = user_details_obj.get('uri')
                user_log = rail.result('create_user_log')
                return {
                    **{
                        "user_uri": user_uri,
                        "user_log": user_log
                    },
                    **dag_run.conf
                }

            trigger_update_philippines_user = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_update_philippines_user",
                items= [1],
                trigger_dag_id=lambda dag_run: get_trigger_dag_id(
                    config.workday_user_import_philippines_update_user_dag,
                    config.DAG_BATCH_COUNT,
                    item_index=get_item_index(dag_run, config.DAG_BATCH_COUNT)
                ),
                conf=prepare_update_user_conf
            )

            wait_for_update_user_completion = rail.WaitForDagRunsSensor(
                task_id="wait_for_update_user_completion",
                dag_runs="{{result('trigger_update_philippines_user')}}",
                retries=0,
                execution_timeout=timedelta(days=1)
            )

            # Trigger the add user workflow with enhanced error handling
            def prepare_add_user_conf(dag_run):
                return {
                    **{
                        "user_log": rail.result('create_user_log')
                    },
                    **dag_run.conf
                }

            trigger_add_philippines_user = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_add_philippines_user",
                items=[1],
                trigger_dag_id=lambda dag_run: get_trigger_dag_id(
                    config.workday_user_import_philippines_add_user_dag,
                    config.DAG_BATCH_COUNT,
                    item_index=get_item_index(dag_run, config.DAG_BATCH_COUNT)
                ),
                conf=prepare_add_user_conf
            )

            wait_for_add_user_completion = rail.WaitForDagRunsSensor(
                task_id="wait_for_add_user_completion",
                dag_runs="{{result('trigger_add_philippines_user')}}",
                retries=0,
                execution_timeout=timedelta(days=1)
            )

            def get_process_user_catch_and_log_error_properties(dag_run):
                error_exception_msg = rail.render_template("{{get_error_message()}}")
                status = 'Error'

                # Get file_data with fallbacks
                file_data = dag_run.conf.get('file_data', {})
                emp_id = file_data.get('emp_id', 'Unknown')
                email = file_data.get('email_id', 'Unknown')

                if 'specified target field is ambiguous' in error_exception_msg:
                    error_exception_msg = f'''Multiple users available with employee id "{emp_id}"'''
                    status = 'Exception'

                return {
                    "Jobid": "",
                    "Userid": emp_id,
                    "Email": email,
                    "Action": "Validation",
                    "Status": status,
                    "Details": error_exception_msg
                }

            catch_and_log_error = rail.WriteLogOperator(
                task_id="catch_and_log_error",
                trigger_rule="one_failed",
                log="{{result('create_user_log')}}",
                message="User processing Error",
                severity="Error",
                properties=get_process_user_catch_and_log_error_properties
            )

            gather_logs = rail.EmptyOperator(
                task_id="gather_logs"
            )

            # Define task dependencies
            can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
            can_run_batch_task >> rail.Label("No") >> create_user_log

            create_user_log >> mapper_values_found >> rail.Label("No") >> log_no_mapper_values_found_exception >> catch_and_log_error
            mapper_values_found >> rail.Label("Yes") >> get_user_details_via_emp_id >> is_user_found
            is_user_found >> rail.Label("Yes") >> trigger_update_philippines_user >> wait_for_update_user_completion >> gather_logs
            is_user_found >> rail.Label("No") >> trigger_add_philippines_user >> wait_for_add_user_completion >> gather_logs

            gather_logs >> catch_and_log_error

            _dags.append(dag)
    
    return _dags

rail.for_each_instance(create_dag)