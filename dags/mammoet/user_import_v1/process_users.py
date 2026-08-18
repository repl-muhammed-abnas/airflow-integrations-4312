from datetime import timedelta
from pendulum import datetime
import rail
from airflow.models import Variable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_process_users_child_dag_id,
        description="Mammoet User Import Process Each User",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_users_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_error',
        )

        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "employeeId": "{{ dag_run.conf.employee_id }}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        is_user_found = rail.IfOperator(
            task_id="is_user_found",
            test="{{ result('get_user_details') | is_truthy }}",
            yes_task="is_user_indirect_office",
            no_task="search_user_via_loginname"
        )

        search_user_via_loginname = rail.RepliconServiceOperator(
            task_id="search_user_via_loginname",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": "{{ dag_run.conf.login_name }}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        is_user_found_via_login_name = rail.IfOperator(
            task_id="is_user_found_via_login_name",
            test="{{ result('search_user_via_loginname') | is_truthy }}",
            yes_task="is_user_indirect_office",
            no_task="can_add_user"
        )

        is_user_indirect_office = rail.IfOperator(
            task_id="is_user_indirect_office",
            test=lambda dag_run: 'indirect office' in dag_run.conf['employee_type_name'].lower(
            ),
            yes_task='log_user_is_indirect',
            no_task='can_process_update_user'
        )

        def can_process_update_user_test(dag_run):
            if dag_run.conf['employee_status'].lower() == "inactive":
                if dag_run.conf['end_date']:
                    return "process"
                return "exception"
            return "process"

        can_process_update_user = rail.IfOperator(
            task_id="can_process_update_user",
            test=lambda dag_run: can_process_update_user_test(
                dag_run) != "exception",
            yes_task="process_update_user",
            no_task="log_user_for_disable_without_enddate"
        )

        log_user_for_disable_without_enddate = rail.WriteLogOperator(
            task_id="log_user_for_disable_without_enddate",
            severity="Exception",
            message="Employee Status is inactive but end date is not provided",
            log="{{result('create_user_log')}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Exception",
                "action": "pre-check",
                "details": "Employee Status is inactive but end date is not provided"
            }
        )

        def is_rehire_user():
            # For rehire the login name will be same but the employee id will be different
            # As per this we will never get user with employeeId
            return bool(rail.result('search_user_via_loginname') and not rail.result("search_user_via_loginname")[0]['userDetails']['isEnabled'])

        def get_user_uri():
            if rail.result("get_user_details"):
                return rail.result("get_user_details")[0]['userDetails']['uri']
            return rail.result("search_user_via_loginname")[0]['userDetails']['uri']

        process_update_user = rail.TriggerDagRunOperator(
            task_id="process_update_user",
            trigger_dag_id=config.user_import_update_users_child_dag_id,
            conf=lambda dag_run: {
                **{
                    "user_uri": get_user_uri(),
                    "log": rail.result('create_user_log'),
                    "rehire": is_rehire_user()
                },
                **dag_run.conf
            },
            retries=0
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_update_user",
            dag_runs="{{result('process_update_user')}}",
            execution_timeout=timedelta(days=14),
            retries=0
        )

        can_add_user = rail.IfOperator(
            task_id="can_add_user",
            test="{{dag_run.conf.employee_status | lower() != 'inactive'}}",
            yes_task="validate_add_user_fields",
            no_task="log_new_user_status_is_inactive"
        )

        log_new_user_status_is_inactive = rail.WriteLogOperator(
            task_id='log_new_user_status_is_inactive',
            severity="Exception",
            message="Inactive status for new user",
            log="{{result('create_user_log')}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Exception",
                "action": "pre-check",
                "details": "Inactive status for new user"
            }
        )

        validate_add_user_fields = rail.IfOperator(
            task_id='validate_add_user_fields',
            test="{{dag_run.conf.start_date | is_truthy }}",
            yes_task="is_indirect_office_user",
            no_task="log_validation_exception"
        )

        log_validation_exception = rail.WriteLogOperator(
            task_id="log_validation_exception",
            severity="Exception",
            message="Start date not present for new user",
            log="{{result('create_user_log')}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Exception",
                "action": "pre-check",
                "details": "Start date not present for new user"
            }
        )

        is_indirect_office_user = rail.IfOperator(
            task_id="is_indirect_office_user",
            test=lambda dag_run: 'indirect office' in dag_run.conf['employee_type_name'].lower(
            ),
            yes_task="process_indirect_add_user",
            no_task="process_add_user"
        )

        process_indirect_add_user = rail.TriggerDagRunOperator(
            task_id="process_indirect_add_user",
            trigger_dag_id=config.user_import_indirect_employee_add_users_child_dag_id,
            conf=lambda dag_run: {
                **{
                    "log": rail.result('create_user_log')
                },
                **dag_run.conf
            },
            retries=0
        )

        wait_for_process_indirect_add_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_indirect_add_user",
            dag_runs="{{result('process_indirect_add_user')}}",
            execution_timeout=timedelta(days=14),
            retries=0
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id="process_add_user",
            trigger_dag_id=config.user_import_add_users_child_dag_id,
            conf=lambda dag_run: {
                **{
                    "log": rail.result('create_user_log')
                },
                **dag_run.conf
            },
            retries=0
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_add_user",
            dag_runs="{{result('process_add_user')}}",
            execution_timeout=timedelta(days=14),
            retries=0
        )

        log_user_is_indirect = rail.WriteLogOperator(
            task_id='log_user_is_indirect',
            severity="Exception",
            message="New user status is inactive",
            log="{{result('create_user_log')}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Exception",
                "action": "pre-check",
                "details": "New user status is inactive"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule="one_failed",
            severity="Error",
            message="{{get_error_message()}}",
            log="{{result('create_user_log')}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Error",
                "action": "pre-check",
                "details": "{{get_error_message()}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule="all_done"
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_user_log

        create_user_log >> get_user_details >> is_user_found >> rail.Label(
            "No") >> search_user_via_loginname
        search_user_via_loginname >> is_user_found_via_login_name >> rail.Label(
            "Yes") >> is_user_indirect_office
        is_user_found_via_login_name >> rail.Label(
            "No") >> can_add_user >> rail.Label("Yes") >> validate_add_user_fields
        validate_add_user_fields >> rail.Label("Require fields present") >> is_indirect_office_user >> rail.Label("No") >> process_add_user \
            >> wait_for_process_add_user >> rail.Label("On Error") >> catch_and_log_error
        validate_add_user_fields >> rail.Label(
            "Required fields missing") >> log_validation_exception >> rail.Label("On Error") >> catch_and_log_error
        is_indirect_office_user >> rail.Label("Yes") >> process_indirect_add_user \
            >> wait_for_process_indirect_add_user >> rail.Label("On Error") >> catch_and_log_error >> log_to_sumo
        can_add_user >> rail.Label("No") >> log_new_user_status_is_inactive >> rail.Label(
            "On Error") >> catch_and_log_error
        is_user_found >> rail.Label("Yes") >> is_user_indirect_office >> rail.Label("No") >> can_process_update_user \
            >> rail.Label("Yes") >> process_update_user >> wait_for_process_update_user >> rail.Label("On Error") >> catch_and_log_error
        can_process_update_user >> rail.Label(
            "No") >> log_user_for_disable_without_enddate >> rail.Label("On Error") >> catch_and_log_error
        is_user_indirect_office >> rail.Label(
            "Yes") >> log_user_is_indirect >> rail.Label("On Error") >> catch_and_log_error

    return dag


rail.for_each_instance(create_main_dag)
