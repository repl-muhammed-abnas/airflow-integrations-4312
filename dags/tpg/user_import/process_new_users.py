from datetime import timedelta
from airflow.models import Variable
import rail
from tpg.user_import.utils import request_payload
from tpg.user_import.tasks.process_supervisor import process_supervisor_assignment_task_group

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_users,
        description='TPG User Import - Process New Users',
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
            endpoint="/services/importService1.svc/PutUser3",
            data=request_payload.get_put_user_payload
        )

        is_officeschedule_uri_present = rail.IfOperator(
            task_id='is_officeschedule_uri_present',
            test=lambda dag_run: bool(dag_run.conf['officescheduleuri']),
            yes_task='put_default_office_schedule',
            no_task='update_license_for_user'
        )

        put_default_office_schedule = rail.RepliconServiceOperator(
            task_id='put_default_office_schedule',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('add_new_user')['uri'],
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeSchedule": {
                                "officeScheduleUri": dag_run.conf['officescheduleuri']
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        update_license_for_user = rail.RepliconServiceOperator(
            task_id='update_license_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('add_new_user').uri }}",
                "productUris": [
                    "urn:replicon-saas:product:time-bill-plus"
                ]
            }
        )

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: bool(dag_run.conf['manager']),
            yes_task='if_user_is_supervisor',
            no_task='log_user_completion'
        )

        if_user_is_supervisor = rail.IfOperator(
            task_id='if_user_is_supervisor',
            test=lambda dag_run: dag_run.conf['manager'] == dag_run.conf['employeeid'],
            yes_task='log_user_supervisor_same',
            no_task='search_supervisor_in_replicon'
        )

        log_user_supervisor_same = rail.SetVariableOperator(
            task_id='log_user_supervisor_same',
            name='sameusersupervisor',
            value='User and Supervisor is same'
        )

        process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
            'add_new_user', 'new_user')

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_add_user_message,
            severity=request_payload.get_add_user_severity,
            properties=lambda dag_run: {
                'jobid': dag_run.conf['jobid'],
                'lastname': dag_run.conf['lastname'],
                'firstname': dag_run.conf['firstname'],
                'loginname':  dag_run.conf['loginname'],
                'employeeid': dag_run.conf['employeeid'],
                'useruri': rail.result('add_new_user')['uri'] if rail.result('add_new_user') else '',
                'manager': dag_run.conf['manager'],
                'action': 'Add',
                'status': request_payload.get_add_user_severity(),
                'details': request_payload.get_add_user_message(),
                'user_log': dag_run.conf['user_log']
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
                'jobid': "{{dag_run.conf.jobid}}",
                'lastname': "{{dag_run.conf.lastname}}",
                'firstname': "{{dag_run.conf.firstname}}",
                'loginname':  "{{dag_run.conf.loginname}}",
                'employeeid': "{{dag_run.conf.employeeid}}",
                'useruri': "{{result('add_new_user').uri if result('add_new_user') | is_truthy else ''}}",
                'manager': "{{dag_run.conf.manager}}",
                'action': 'Add',
                'status': "{{'Exception' if get_task_state('add_new_user') == 'success' else 'Error' }}",
                'user_log': "{{dag_run.conf.user_log}}",
                'details': "\
                {%- if get_task_state('add_new_user') == 'success' -%} \
                    User Added Partially; {{ get_error_message() }}\
                {%- else -%}\
                    User not created; {{ get_error_message() }}\
                {%- endif -%}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> add_new_user

        add_new_user >> is_officeschedule_uri_present >> rail.Label("Yes") >> put_default_office_schedule >> update_license_for_user
        is_officeschedule_uri_present >> rail.Label("Yes") >> update_license_for_user >> is_supervisor_in_feed_file
        is_supervisor_in_feed_file >> rail.Label('No') >> log_user_completion
        is_supervisor_in_feed_file >> rail.Label('Yes') >> if_user_is_supervisor

        if_user_is_supervisor >> rail.Label('No') >> process_supervisor_entry
        if_user_is_supervisor >> rail.Label('Yes') >> log_user_supervisor_same >> log_user_completion
        process_supervisor_exit >> log_user_completion
        log_user_completion >> catch_and_log_errors >> log_to_sumo

        return dag

rail.for_each_instance(create_child_dag)
