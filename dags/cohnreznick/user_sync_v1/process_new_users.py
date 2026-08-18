from datetime import timedelta
from airflow.models import Variable
import rail

from cohnreznick.user_sync_v1.utils import request_payload
from cohnreznick.user_sync_v1.utils.python_callable_methods import get_log_status_or_message

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_users,
        description='Cohnreznick User Sync - Process New Users',
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
            data=lambda dag_run: request_payload.get_put_user_payload(dag_run,config.WORKWEEK)
        )

        remove_timeoff_assignments = rail.RepliconServiceOperator(
            task_id="remove_timeoff_assignments",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_remove_timeoff_payload
        )

        add_timeentry_approval_path = rail.RepliconServiceOperator(
            task_id='add_timeentry_approval_path',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                'user': {
                    'uri': "{{ result('add_new_user').uri }}"
                },
                'modifications': {
                    'timeEntryRevisionGroupApprovalPathToApply': {
                        'name': '{{ dag_run.conf.timeentryapprovalpathuri }}'
                    }
                },
                'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
            }
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log = '{{ dag_run.conf.user_log }}',
            message=lambda: get_log_status_or_message("msg", "Added", add_new_user.task_id),
            severity='Success',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "employeenumber": dag_run.conf['employeenumber'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "action": get_log_status_or_message("action", "Added", add_new_user.task_id),
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "employeenumber": dag_run.conf['employeenumber'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "action": "Add",
                'status': 'Error',
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

        add_new_user >> remove_timeoff_assignments >> add_timeentry_approval_path
        add_timeentry_approval_path >> log_success >> catch_and_log_errors >> log_to_sumo


        return dag

rail.for_each_instance(create_child_dag)
