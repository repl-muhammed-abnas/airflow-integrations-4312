from datetime import timedelta
from airflow.models import Variable
import rail
import json

from alvarezandmarsalholdings.user_import_v2.utils import request_payload


null = None

# pylint: disable=too-many-statements


def create_child_dag(config):
    add_dags = []

    for idx in range(0, config.BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_new_users_dagid}{get_postfix}",
            description='Alvarezandmarsalholdings - User Import - Process New Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_new_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='if_worker_subtype_is_regular'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='if_worker_subtype_is_regular',
                end_task='catch_and_log_errors',
            )

            if_worker_subtype_is_regular = rail.IfOperator(
                task_id='if_worker_subtype_is_regular',
                test=lambda dag_run: dag_run.conf['employee_type'] in [
                    'Regular', 'Fixed Term', 'Intern'],
                yes_task='get_placeholder_policyset',
                no_task='if_worker_subtype_is_other'
            )

            if_worker_subtype_is_other = rail.IfOperator(
                task_id='if_worker_subtype_is_other',
                test=lambda dag_run: dag_run.conf['employee_type'] in ['Agency Temp'],
                yes_task='if_email_domain_matched',
                no_task='log_user_not_created_worker_subtype_different'
            )

            if_email_domain_matched = rail.IfOperator(
                task_id='if_email_domain_matched',
                test=lambda dag_run: dag_run.conf['email'].split(
                    '@')[1] == 'alvarezandmarsal.com',
                yes_task='get_placeholder_policyset',
                no_task='log_user_not_created_worker_subtype_email_domain_different'
            )

            get_placeholder_policyset = rail.RepliconServiceOperator(
                task_id='get_placeholder_policyset',
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data={
                    "timeOffTypeUri": "{{ dag_run.conf.placeholder_timeoffuri }}"
                },
                data_handler=lambda res: json.loads(json.dumps(res[0]['policySet'], ensure_ascii=False).replace('"null"', '"effective"').replace(
                    '"script"', '"scriptTarget"'))
            )

            create_new_user = rail.RepliconServiceOperator(
                task_id="create_new_user",
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_create_new_user_payload(
                    dag_run, config, rail.result('get_placeholder_policyset')),
            )

            log_create_user_successful = rail.WriteLogOperator(
                task_id='log_create_user_successful',
                log='{{ dag_run.conf.user_log }}',
                message="User created successfully",
                severity='Success',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "add",
                    "status": "Success",
                    'details': "User created successfully"
                }
            )

            log_user_not_created_worker_subtype_different = rail.WriteLogOperator(
                task_id='log_user_not_created_worker_subtype_different',
                log='{{ dag_run.conf.user_log }}',
                message="User not Created, Worker subtype out of scope",
                severity='Exception',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "add",
                    "status": "Exception",
                    'details': "User not Created, Worker subtype out of scope"
                }
            )

            log_user_not_created_worker_subtype_email_domain_different = rail.WriteLogOperator(
                task_id='log_user_not_created_worker_subtype_email_domain_different',
                log='{{ dag_run.conf.user_log }}',
                message="User not Created, email domain mismatch",
                severity='Exception',
                properties={
                    'employee_id': '{{dag_run.conf.employee_id}}',
                    "action": "add",
                    "status": "Exception",
                    'details': "User not Created, email domain mismatch"
                }
            )

            if_supervisor_details_in_feed = rail.IfOperator(
                task_id="if_supervisor_details_in_feed",
                test=lambda dag_run: bool(dag_run.conf["reporting_manager"]),
                yes_task="write_supervisor_pending_logs",
                no_task="if_performance_manager_details_in_feed"
            )

            write_supervisor_pending_logs = rail.WriteLogOperator(
                task_id="write_supervisor_pending_logs",
                log='{{dag_run.conf.supervisor_log}}',
                message="Supervisor",
                severity="Pending",
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf["employee_id"],
                    "reporting_manager": dag_run.conf["reporting_manager"],
                    "reporting_manager_effective_date": dag_run.conf["reporting_manager_effective_date"],
                    "Add_Update": "Add",
                    "type": "reporting_manager",
                    "useruri": rail.result("create_new_user")["user"]["uri"]
                }
            )

            if_performance_manager_details_in_feed = rail.IfOperator(
                task_id="if_performance_manager_details_in_feed",
                test=lambda dag_run: bool(dag_run.conf["performance_manager"]),
                yes_task="write_performance_manager_pending_logs",
            )

            write_performance_manager_pending_logs = rail.WriteLogOperator(
                task_id="write_performance_manager_pending_logs",
                log='{{dag_run.conf.supervisor_log}}',
                message="Supervisor",
                severity="Pending",
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf["employee_id"],
                    "reporting_manager": dag_run.conf["performance_manager"],
                    "reporting_manager_effective_date": null,
                    "Add_Update": "Add",
                    "type": "performance_manager",
                    "useruri": rail.result("create_new_user")['user']['uri']
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{dag_run.conf.user_log}}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    "employee_id": "{{dag_run.conf.employee_id}}",
                    "action": "Add",
                    'status': 'Error',
                    'details': '{{ get_error_message() }}'
                },
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label(
                'No') >> if_worker_subtype_is_regular
            if_worker_subtype_is_regular >> rail.Label(
                'Yes') >> get_placeholder_policyset
            if_worker_subtype_is_regular >> rail.Label(
                'No') >> if_worker_subtype_is_other
            if_worker_subtype_is_other >> rail.Label(
                'Yes') >> if_email_domain_matched
            if_email_domain_matched >> rail.Label(
                'Yes') >> get_placeholder_policyset >> create_new_user >> log_create_user_successful >> if_supervisor_details_in_feed
            if_supervisor_details_in_feed >> rail.Label(
                'Yes') >> write_supervisor_pending_logs >> if_performance_manager_details_in_feed
            if_supervisor_details_in_feed >> rail.Label(
                'No') >> if_performance_manager_details_in_feed
            if_performance_manager_details_in_feed >> rail.Label(
                'Yes') >> write_performance_manager_pending_logs >> catch_and_log_errors
            if_email_domain_matched >> rail.Label(
                'No') >> log_user_not_created_worker_subtype_email_domain_different >> catch_and_log_errors
            if_worker_subtype_is_other >> rail.Label(
                'No') >> log_user_not_created_worker_subtype_different >> catch_and_log_errors

        add_dags.append(dag)

    return add_dags


rail.for_each_instance(create_child_dag)
