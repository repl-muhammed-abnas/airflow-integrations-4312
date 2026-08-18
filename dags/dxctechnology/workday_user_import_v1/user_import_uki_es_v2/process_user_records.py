from datetime import timedelta
import random
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.utils.custom_methods import get_trigger_dag_id, get_item_index
null = None

def create_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.workday_user_import_process_uki_es_user_records_child_dag}{prefix}",
            description="DXC Technology Workday User Sync UK&I CSC Process User Records Child",
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2025, 4, 1),
            max_active_runs=config.max_active_run_process_each_users_uki_es
        ) as dag:

            # View the DAG run configuration
            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            # Check if batch task execution is enabled
            can_run_batch_task = rail.IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_uki_es, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="create_user_log"
            )

            # Batch task operator for processing multiple users
            batch_task = rail.BatchTaskRunOperator(
                task_id="batch_task",
                start_task="create_user_log",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            # Create log for this user processing
            create_user_log = rail.CreateLogOperator(
                task_id="create_user_log"
            )

            # Check if mapper values are found for this user's company code
            mapper_values_found = rail.IfOperator(
                task_id="mapper_values_found",
                test=lambda dag_run: dag_run.conf.get('mapper_data', {}).get('mapper_data_found', 'no') != 'no',
                yes_task="validate_company_code",
                no_task="log_no_mapper_values_found_exception"
            )

            # Log exception if no mapper values found
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
                    "Details": f"Record processing skipped, no mapper values found"
                }
            )

            # Validate company code is valid for UK&I CSC
            validate_company_code = rail.IfOperator(
                task_id="validate_company_code",
                test=lambda dag_run: dag_run.conf['file_data'].get('company_code', '') in config.valid_company_codes,
                yes_task="get_user_details_via_emp_id",
                no_task="log_invalid_company_code"
            )

            # Log invalid company code
            log_invalid_company_code = rail.WriteLogOperator(
                task_id="log_invalid_company_code",
                log="{{result('create_user_log')}}",
                message="Invalid company code for UK&I CSC",
                severity="Error",
                properties=lambda dag_run: {
                    "Userid": dag_run.conf['file_data']['emp_id'],
                    "Email": dag_run.conf['file_data']['email_id'],
                    "Action": "Validation",
                    "Status": "Error",
                    "Details": f"Invalid company code: {dag_run.conf['file_data'].get('company_code', 'Unknown')} for UK&I ES processing"
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
                yes_task="trigger_update_uki_es_user",
                no_task="trigger_add_uki_es_user"
            )

            # Trigger the update user workflow
            def prepare_update_user_conf(dag_run):
                user_details = rail.result('get_user_details_via_emp_id')
                user_details_obj = user_details.get('userDetails', {})
                user_uri = user_details_obj.get('uri')
                user_log = rail.result('create_user_log')
                return {
                    **{
                        "user_uri": user_uri,
                        "user_log": user_log,
                        "existing_user_details": user_details_obj
                    },
                    **dag_run.conf
                }

            trigger_update_uki_es_user = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_update_uki_es_user",
                items=[1],
                trigger_dag_id=lambda dag_run: get_trigger_dag_id(
                    config.workday_user_import_uki_es_update_user_dag,
                    config.DAG_BATCH_COUNT,
                    item_index=get_item_index(dag_run, config.DAG_BATCH_COUNT)
                ),
                conf=prepare_update_user_conf
            )

            wait_for_update_user_completion = rail.WaitForDagRunsSensor(
                task_id="wait_for_update_user_completion",
                dag_runs="{{result('trigger_update_uki_es_user')}}",
                retries=0,
                execution_timeout=timedelta(days=14)
            )

            # Trigger the add user workflow
            def prepare_add_user_conf(dag_run):
                return {
                    **{
                        "user_log": rail.result('create_user_log')
                    },
                    **dag_run.conf
                }

            trigger_add_uki_es_user = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_add_uki_es_user",
                items=[1],
                trigger_dag_id=lambda dag_run: get_trigger_dag_id(
                    config.workday_user_import_uki_es_add_user_dag,
                    config.DAG_BATCH_COUNT,
                    item_index=get_item_index(dag_run, config.DAG_BATCH_COUNT)
                ),
                conf=prepare_add_user_conf
            )

            wait_for_add_user_completion = rail.WaitForDagRunsSensor(
                task_id="wait_for_add_user_completion",
                dag_runs="{{result('trigger_add_uki_es_user')}}",
                retries=0,
                execution_timeout=timedelta(hours=1)
            )

            # End task - completes the processing
            end_task = rail.EmptyOperator(
                task_id="end_task"
            )

            # Catch and log any errors
            catch_and_log_error = rail.WriteLogOperator(
                task_id="catch_and_log_error",
                log="{{result('create_user_log')}}",
                message="Process Error",
                severity="Error",
                properties=lambda dag_run: {
                    "Userid": dag_run.conf['file_data'].get('emp_id', 'Unknown'),
                    "Email": dag_run.conf['file_data'].get('email_id', 'Unknown'),
                    "Action": "Process",
                    "Status": "Error",
                    "Details": rail.render_template("{{get_error_message()}}")
                },
                trigger_rule="one_failed"
            )

            # Set up task dependencies
            can_run_batch_task >> batch_task >> catch_and_log_error
            can_run_batch_task >> create_user_log >> mapper_values_found
            
            mapper_values_found >> validate_company_code >> get_user_details_via_emp_id
            mapper_values_found >> log_no_mapper_values_found_exception
            
            validate_company_code >> log_invalid_company_code
            get_user_details_via_emp_id >> is_user_found
            
            is_user_found >> trigger_update_uki_es_user >> wait_for_update_user_completion >> end_task
            is_user_found >> trigger_add_uki_es_user >> wait_for_add_user_completion >> end_task
            
            [log_invalid_company_code, log_no_mapper_values_found_exception] >> end_task >> catch_and_log_error

        _dags.append(dag)
    return _dags

rail.for_each_instance(create_dag)