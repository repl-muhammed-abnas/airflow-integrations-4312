from datetime import timedelta
from airflow.models import Variable
import rail
from guidehouse.workday_user_import.utils import request_payload, custom_method
from guidehouse.workday_user_import.task.process_supervisor import process_supervisor_assignment_task_group
from guidehouse.workday_user_import.task.update_timeoff_policies import update_timeoff_policies_task_group


def create_child_dag(config):
    """
    Create child DAG for processing new user creation.

    Args:
        config: Configuration object containing DAG settings.

    Returns:
        list[DAG]: List of configured Airflow DAG objects (one per batch)
    """
    # pylint: disable=too-many-statements, line-too-long, cell-var-from-loop
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f'{config.process_new_users}_batch_{idx+1}',
            description='Guidehouse Workday User Import - Process New Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_new_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='add_new_user'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='add_new_user',
                end_task='catch_and_log_errors',
            )

            add_new_user = rail.RepliconServiceOperator(
                task_id="add_new_user",
                endpoint="/services/importservice2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_create_update_user_payload(config, dag_run, "add_user")
            )

            update_license_and_holiday_calendar = rail.RepliconServiceOperator(
                task_id='update_license_and_holiday_calendar',
                endpoint='/services/ImportService1.svc/ApplyUserModifications3',
                data=lambda dag_run: {
                    "user": {
                        "uri": rail.result('add_new_user')['user']['uri'],
                    },
                    "modifications": {
                        "holidayCalendarAssignmentsToApply": request_payload.get_updated_holiday_calendar_for_user(
                            dag_run, dag_run.conf['holiday_calander_uri'], "add_user", dag_run.conf['change_effective_date']),
                        "productAssignmentsToApply": None, # No product assignment changes for new users as of now, but can be added in future if needed
                        # {
                        #     "productUrisToUnassign": [
                        #         "urn:replicon-saas:product:time-bill-plus",
                        #         "urn:replicon-saas:product:time-intelligence",
                        #     ]
                        # }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            )

            policy_entry, policy_exit = update_timeoff_policies_task_group(config, 'add_user', user_ref='add_new_user')

            if_supervisor_id_present = rail.IfOperator(
                task_id='if_supervisor_id_present',
                test=lambda dag_run: bool(dag_run.conf['supervisor_id']),
                yes_task='if_user_is_supervisor',
                no_task='log_user_completion'
            )

            if_user_is_supervisor = rail.IfOperator(
                task_id='if_user_is_supervisor',
                test=lambda dag_run: dag_run.conf['supervisor_id'] == dag_run.conf['employee_id'],
                yes_task='log_user_supervisor_same',
                no_task='search_supervisor_in_replicon'
            )

            log_user_supervisor_same = rail.EmptyOperator(
                task_id='log_user_supervisor_same',
            )

            process_supervisor_entry, process_supervisor_exit = process_supervisor_assignment_task_group(
                'add_new_user', 'new_user', config)

            log_user_completion = rail.WriteLogOperator(
                task_id='log_user_completion',
                log='{{ dag_run.conf.user_log }}',
                message=custom_method.get_add_user_message,
                severity=custom_method.get_add_user_severity,
                properties=lambda dag_run: {
                    'lastname': dag_run.conf['last_name'],
                    'firstname': dag_run.conf['first_name'],
                    'loginname': dag_run.conf['login_name'],
                    'employeeid': dag_run.conf['employee_id'],
                    'manager': dag_run.conf['supervisor_id'],
                    "userstatus": dag_run.conf['user_status'],
                    "co_costcenter": dag_run.conf['company_description'],
                    "location": dag_run.conf['location'],
                    'action': 'Add',
                    'status': custom_method.get_add_user_severity(),
                    'details': custom_method.get_add_user_message(),
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ dag_run.conf.user_log }}',
                trigger_rule='one_failed',
                severity='Error',
                message="\
                    {%- if get_task_state('add_new_user') == 'success' -%} \
                        User Added Partially; {{ get_error_message() }}\
                    {%- else -%}\
                        User not created; {{ get_error_message() }}\
                    {%- endif -%}",
                properties={
                    'lastname': '{{dag_run.conf.last_name}}',
                    'firstname': '{{dag_run.conf.first_name}}',
                    'loginname': '{{dag_run.conf.login_name}}',
                    'employeeid': '{{dag_run.conf.employee_id}}',
                    'manager': "{{dag_run.conf.supervisor_id}}",
                    "userstatus": "{{ dag_run.conf.user_status }}",
                    "co_costcenter": "{{ dag_run.conf.company_description }}",
                    "location": "{{ dag_run.conf.location }}",
                    'action': 'Add',
                    'status': "{{'Exception' if get_task_state('add_new_user') == 'success' else 'Error' }}",
                    'details': "\
                    {%- if get_task_state('add_new_user') == 'success' -%} \
                        User Added Partially; {{ get_error_message() }}\
                    {%- else -%}\
                        User not created; {{ get_error_message() }}\
                    {%- endif -%}"
                }
            )

            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> add_new_user

            add_new_user >> update_license_and_holiday_calendar >> policy_entry
            policy_exit >> if_supervisor_id_present

            if_supervisor_id_present >> rail.Label("Yes") >> if_user_is_supervisor
            if_supervisor_id_present >> rail.Label("No") >> log_user_completion

            if_user_is_supervisor >> rail.Label("Yes") >> log_user_supervisor_same >> log_user_completion
            if_user_is_supervisor >> rail.Label("No") >> process_supervisor_entry
            process_supervisor_exit >> log_user_completion
            log_user_completion >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags


rail.for_each_instance(create_child_dag)