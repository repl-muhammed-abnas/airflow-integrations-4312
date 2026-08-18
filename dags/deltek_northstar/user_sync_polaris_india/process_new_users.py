from datetime import timedelta
from airflow.models import Variable
import rail
from deltek_northstar.user_sync_polaris_india.utils import request_payload, response_filter
from deltek_northstar.user_sync_polaris_india.tasks.process_supervisor import process_supervisor_assignment_task_group

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_users,
        description='Deltek Costpoint User Import - Process New Users',
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


        is_supervisor_in_api_response = rail.IfOperator(
            task_id='is_supervisor_in_api_response',
            test=lambda dag_run: bool(dag_run.conf['mgr_empl_id']),
            yes_task='if_user_is_supervisor',
            no_task='log_user_completion'
        )

        if_user_is_supervisor = rail.IfOperator(
            task_id='if_user_is_supervisor',
            test=lambda dag_run: dag_run.conf['mgr_empl_id'] == dag_run.conf['empl_id'],
            yes_task='log_user_supervisor_same',
            no_task='search_supervisor_in_replicon'
        )

        log_user_supervisor_same = rail.EmptyOperator(
            task_id='log_user_supervisor_same'
        )

        process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
            'add_new_user', 'new_user', config)

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_add_user_message,
            severity=request_payload.get_add_user_severity,
            properties=lambda dag_run: {
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['email_id'],
                'employeeid': dag_run.conf['empl_id'],
                'useruri': rail.result('add_new_user')['user']['uri'] if rail.result('add_new_user') else '',
                'manager': dag_run.conf['mgr_empl_id'],
                'action': 'Add',
                'status': request_payload.get_add_user_severity(),
                'details': request_payload.get_add_user_message(),
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
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
                'loginname': '{{dag_run.conf.email_id}}',
                'employeeid': '{{dag_run.conf.empl_id}}',
                'useruri': "{{result('add_new_user').user.uri if result('add_new_user') | is_truthy else ''}}",
                'manager': "{{dag_run.conf.mgr_empl_id}}",
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

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> add_new_user

        add_new_user >> is_supervisor_in_api_response
        is_supervisor_in_api_response >> rail.Label('No') >> log_user_completion
        is_supervisor_in_api_response >> rail.Label('Yes') >> if_user_is_supervisor

        if_user_is_supervisor >> rail.Label('No') >> process_supervisor_entry
        if_user_is_supervisor >> rail.Label('Yes') >> log_user_supervisor_same >> log_user_completion
        process_supervisor_exit >> log_user_completion
        log_user_completion >> catch_and_log_errors

        return dag

rail.for_each_instance(create_child_dag)
