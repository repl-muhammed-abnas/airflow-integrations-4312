from datetime import timedelta
import rail
from macquariegroup.user_import.utils.request_payload import get_create_user_payload
from macquariegroup.user_import.utils.custom_methods import get_log_message
from airflow.models import Variable


def create_add_user_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_user_import_add_users_child_{config.instance}',
        description=f'Macquarie User Import add user child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "create_add_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_add_log',
            end_task="catch_and_log_error",
        )

        create_add_log = rail.CreateLogOperator(
            task_id="create_add_log"
        )

        search_user_with_loginname = rail.RepliconServiceOperator(
            task_id="search_user_with_loginname",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": "{{dag_run.conf.login_name}}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )

        is_user_found = rail.IfOperator(
            task_id="is_user_found",
            test="{{result('search_user_with_loginname') | is_truthy}}",
            yes_task="log_user_found_with_same_login_name",
            no_task="create_user"
        )

        log_user_found_with_same_login_name = rail.WriteLogOperator(
            task_id="log_user_found_with_same_login_name",
            severity='Exception',
            log="{{result('create_add_log')}}",
            message="User with login name - {{dag_run.conf.login_name}} already present",
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                'employee_id': "{{dag_run.conf.emp_id}}",
                'action': 'Add',
                'status': 'Exception',
                'details': "User with login name - {{dag_run.conf.login_name}} already present"
            }
        )

        create_user = rail.RepliconServiceOperator(
            task_id="create_user",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=get_create_user_payload
        )

        send_general_email_notification = rail.TriggerDagRunOperator(
            task_id="send_general_email_notification",
            trigger_dag_id=f"macquarie_user_import_send_recovery_enabled_emails_child_{config.instance}",
            conf=lambda dag_run: {
                **dag_run.conf,
                **{
                    "log": rail.result('create_add_log'),
                    "useruri": rail.result("create_user")['uri'],
                    "action": "Add",
                    'user_name': f"{dag_run.conf['first_name']}.{dag_run.conf['last_name']}",
                }
            }
        )

        can_log_for_supervisor_processing = rail.IfOperator(
            task_id="can_log_for_supervisor_processing",
            test="{{dag_run.conf.supervisor | is_truthy}}",
            yes_task="log_for_supervisor_assignment",
            no_task="assign_default_supervisor"
        )

        assign_default_supervisor = rail.RepliconServiceOperator(
            task_id="assign_default_supervisor",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": rail.result("create_user")['uri'],
                "supervisorUri": dag_run.conf['default_supervisor_uri'],
                "dateRange":  None
            }
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            severity='Success',
            log="{{result('create_add_log')}}",
            message="User created successfully",
            properties=lambda dag_run: {
                'userloginname': dag_run.conf['login_name'],
                'user_name': dag_run.conf['first_name'] + "." + dag_run.conf['last_name'],
                'employee_id': dag_run.conf['emp_id'],
                'action': 'Add',
                'status': 'Success',
                'details': get_log_message('create_user', 'add')
            }
        )

        log_for_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_for_supervisor_assignment",
            severity="process",
            log="{{dag_run.conf.supervisor_log}}",
            message="Process Supervisor assignment",
            properties={
                "file_name": "{{dag_run.conf.file_name}}",
                'user_name': "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "action": "Add",
                "user_uri": "{{ result('create_user').uri }}",
                "supervisor": "{{ dag_run.conf.supervisor}}",
                "can_assign_default": "No",
                "default_supervisor_uri": "{{ dag_run.conf.default_supervisor_uri }}",
                "user_employee_id": "{{dag_run.conf.emp_id}}",
                "supervisor_uri": "",
                "supervisor_permission": "Gen3 Supervisor"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            severity='Error',
            log="{{result('create_add_log')}}",
            message='{{ get_error_message() }}',
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                'employee_id': "{{dag_run.conf.emp_id}}",
                'action': 'Add',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> rail.Label("On Error") >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_add_log
        create_add_log >> search_user_with_loginname >> is_user_found >> rail.Label(
            "No") >> create_user >> send_general_email_notification >> can_log_for_supervisor_processing \
            >> rail.Label("Yes") >> log_for_supervisor_assignment >> log_success >> rail.Label("On Error") >> catch_and_log_error >> log_to_sumo
        can_log_for_supervisor_processing >> rail.Label(
            "No") >> assign_default_supervisor >> log_success >> rail.Label("On Error") >> catch_and_log_error
        is_user_found >> rail.Label("Yes") >> log_user_found_with_same_login_name >> rail.Label(
            "On Error") >> catch_and_log_error

    return dag


rail.for_each_instance(create_add_user_dag)
