from datetime import timedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable


null = None 


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_canada_users_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.process_users_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_user_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_user_log",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )
        
        create_user_log = rail.CreateLogOperator(
            task_id = "create_user_log"
        )

        get_user_details_via_emp_id = rail.RepliconServiceOperator(
            task_id = "get_user_details_via_emp_id",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
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
            data_handler= lambda response: response[0] if response else {}
        )

        is_user_found = rail.IfOperator(
            task_id = "is_user_found",
            test= "{{ result('get_user_details_via_emp_id') | is_truthy}}",
            yes_task="trigger_update_user",
            no_task="trigger_add_user"
        )
        
        trigger_update_user = rail.TriggerDagRunOperator(
            task_id = "trigger_update_user",
            trigger_dag_id=config.workday_user_import_canada_users_update_user_child_dag,
            conf=lambda dag_run: {
                **{
                    "user_uri": rail.result('get_user_details_via_emp_id')['userDetails']['uri'],
                    "user_log": rail.result('create_user_log')
                },
                **dag_run.conf
            }
        )

        wait_for_update_user_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_update_user_completion",
            dag_runs="{{result('trigger_update_user')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        trigger_add_user = rail.TriggerDagRunOperator(
            task_id = "trigger_add_user",
            trigger_dag_id=config.workday_user_import_canada_users_add_user_child_dag,
            conf=lambda dag_run: {
                **{
                    "user_log": rail.result('create_user_log')
                },
                **dag_run.conf
            }
        )

        wait_for_add_user_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_add_user_completion",
            dag_runs="{{result('trigger_add_user')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        def get_process_user_catch_and_log_error_properties(dag_run):
            error_exception_msg = rail.render_template("{{get_error_message()}}")
            status = 'Error'
            if 'specified target field is ambiguous' in error_exception_msg:
                error_exception_msg = f'''Multiple users available with employee id "{dag_run.conf['file_data']['emp_id']}"'''
                status = 'Exception'

            return{                
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Validation",
                "Status": status,
                "Details": error_exception_msg
            }


        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{result('create_user_log')}}",
            message = "User processing Error",
            severity = "Error",
            properties = get_process_user_catch_and_log_error_properties
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_user_log

        create_user_log >> get_user_details_via_emp_id >> is_user_found
        is_user_found >> rail.Label("Yes") >> trigger_update_user >> wait_for_update_user_completion >> catch_and_log_error
        is_user_found >> rail.Label("No") >> trigger_add_user >> wait_for_add_user_completion >> catch_and_log_error

    return dag
    
rail.for_each_instance(create_dag)
