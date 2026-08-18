from datetime import timedelta
from pendulum import datetime
import rail
from mammoet.user_import_v4.utils import request_payload
from mammoet.user_import_v4.utils.custom_methods import get_timeoff_assignment_log_message
from airflow.models import Variable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_add_users_child_dag_id,
        description="Mammoet User Import Process Add User",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_add_user_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user',
            end_task='catch_and_log_error',
        )

        create_user = rail.RepliconServiceOperator(
            task_id="create_user",
            endpoint="/services/importService1.svc/PutUser3",
            data=request_payload.get_create_user_payload
        )

        def get_create_user_message():
            msg = f'''User created {"partially" if rail.result('create_user', 'has_exception') else 'successfully'}'''
            return msg + (f";{rail.result('create_user','log')}" if rail.result('create_user', 'log') else '')

        log_user_created = rail.WriteLogOperator(
            task_id="log_user_created",
            severity="""{{'Success' if result('create_user', 'has_exception') | is_falsy else 'Exception'}}""",
            message="User created successfully",
            log="{{dag_run.conf.log}}",
            properties=lambda dag_run: {
                "payload_id": dag_run.conf['payload_id'],
                "login_name": dag_run.conf['login_name'],
                "employee_id": dag_run.conf['employee_id'],
                "emp_record_index": dag_run.conf['emp_records_index'],
                "status": "Success",
                "action": "Add",
                "details": get_create_user_message()
            }
        )

        remove_timeoff_assignments = rail.RepliconServiceOperator(
            task_id="remove_timeoff_assignments",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "timeOffTypeUris": []
            }
        )

        add_timeentry_approval_path = rail.RepliconServiceOperator(
            task_id='add_timeentry_approval_path',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                'user': {
                    'uri': "{{ result('create_user').uri }}"
                },
                'modifications': {
                    'timeEntryRevisionGroupApprovalPathToApply': request_payload.get_default_timeentry_approval_path()
                },
                'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
            }
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id="has_any_timeoff_to_assign",
            test="{{dag_run.conf.mapper_derived.timeoff_to_assign | is_truthy}}",
            yes_task="assign_timeoff_type_to_user",
            no_task="log_no_timeoffs_to_assign"
        )

        assign_timeoff_type_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_type_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "timeOffTypeUris": list(filter(None, map(lambda to: to['uri'], dag_run.conf['mapper_derived']['timeoff_to_assign'])))
            }
        )

        log_timeoffs_assignment = rail.WriteLogOperator(
            task_id="log_timeoffs_assignment",
            severity="Success",
            message="Timeoff assigned",
            log="{{dag_run.conf.log}}",
            properties=lambda dag_run: {
                "payload_id": dag_run.conf['payload_id'],
                "login_name": dag_run.conf['login_name'],
                "employee_id": dag_run.conf['employee_id'],
                "emp_record_index": dag_run.conf['emp_records_index'],
                "status": "Success",
                "action": "Add",
                "details": get_timeoff_assignment_log_message(dag_run)
            }
        )

        log_no_timeoffs_to_assign = rail.WriteLogOperator(
            task_id="log_no_timeoffs_to_assign",
            severity="Exception",
            message="No Timeoff found to assign",
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "status": "Exception",
                "action": "Add",
                "details": "No Timeoff found to assign"
            }
        )

        log_for_supervisor_assignment = rail.WriteLogOperator(
            task_id='log_for_supervisor_assignment',
            severity="To-reprocess",
            message="To re-process",
            log="{{dag_run.conf.supervisor_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "action": "Add",
                "details": "For Reprocessing Supervisor",
                'user_uri': "{{result('create_user').uri}}",
                "supervisor_id": "{{dag_run.conf.manager_id}}",
                "user_log": "{{dag_run.conf.log}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "effective_date": "{{dag_run.conf.start_date}}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            severity="Error",
            trigger_rule='one_failed',
            message="{{get_error_message()}}",
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "status": "Error",
                "action": "Add",
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
        can_run_batch_task >> rail.Label("No") >> create_user

        create_user >> log_user_created >> remove_timeoff_assignments >> add_timeentry_approval_path >> has_any_timeoff_to_assign\
            >> rail.Label("Yes") >> assign_timeoff_type_to_user >> log_timeoffs_assignment >> log_for_supervisor_assignment
        has_any_timeoff_to_assign >> rail.Label("No") >> log_no_timeoffs_to_assign\
            >> log_for_supervisor_assignment >> rail.Label("On Error") >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
