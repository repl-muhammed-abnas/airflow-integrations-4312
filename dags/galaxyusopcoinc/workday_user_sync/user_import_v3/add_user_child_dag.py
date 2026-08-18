from datetime import timedelta
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import request_payload, response_filter
from galaxyusopcoinc.workday_user_sync.user_import_v3.tasks.update_supervisor import get_update_supervisor
from airflow.models import Variable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_add_dag_id,
        description=f'VialtoPartners_User Import_Child_User Add {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_user",
            end_task="catch_and_log_errors"
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint='/services/importservice1.svc/PutUser3',
            data=request_payload.get_create_user_data
        )

        remove_timeoff_assignment = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignment',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "timeOffTypeUris": []
            }
        )

        (update_supervisor_task, _) = get_update_supervisor(caller="add")

        allowed_for_supervisor_dag = rail.PythonOperator(
            task_id="allowed_for_supervisor_dag",
            # if the supervisor is not found then it will be true
            python_callable=lambda: not bool(
                rail.result("search_supervisor_by_employeeid"))
        )

        get_timeoff_toassign = rail.PythonOperator(
            task_id='get_timeoff_toassign',
            python_callable=response_filter.map_timeoff_data
        )

        can_update_timeentry_approval = rail.IfOperator(
            task_id="can_update_timeentry_approval",
            test="{{dag_run.conf.mapper_value_found != 'No'}}",
            yes_task="update_timeentry_approval",
            no_task="has_timeofftype"
        )

        update_timeentry_approval = rail.RepliconServiceOperator(
            task_id='update_timeentry_approval',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/UpdateApprovalPathForUser',
            data=request_payload.get_timeentry_approval_path,
        )

        has_timeofftype = rail.IfOperator(
            task_id='has_timeofftype',
            test=lambda: len(rail.result('get_timeoff_toassign')) > 0,
            yes_task='add_timeofftype',
            no_task='log_timeoff_assignment',
        )

        log_timeoff_assignment = rail.WriteLogOperator(
            task_id='log_timeoff_assignment',
            message='No time off types found for assignment',
            log="{{dag_run.conf.create_user_log}}",
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Exception',
                'action': 'Add',
                'message': 'No time off types found for assignment',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "{{result('create_user').uri}}",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        add_timeofftype = rail.RepliconServiceOperator(
            task_id='add_timeofftype',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.get_timeoff_payload
        )

        process_timeoff_policies = rail.TriggerDagRunForEachItemOperator(
            task_id="process_timeoff_policies",
            items=lambda:rail.result("get_timeoff_toassign"),
            trigger_dag_id=config.process_timeoff_dag_id,
            conf=lambda dag_run, item: {
                **{
                    "timeoff_to_process": item,
                    "action": "add"
                },
                **{
                    "user_uri": rail.result("create_user")['uri'],
                    "effective_date_to_use": request_payload.get_replicon_date(dag_run.conf['hiredate'])
                },
                **dag_run.conf
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_process_timeoff_policies = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_timeoff_policies",
            dag_runs="{{result('process_timeoff_policies')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        def do_get_exception_logs():
            logs = []
            if len(request_payload.get_conf().get('validationlog', [])) > 0:
                logs.extend(list(
                    map(lambda item: item['message'], request_payload.get_conf()['validationlog'])))

            return logs

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=do_get_exception_logs
        )

        has_exception_logs = rail.IfOperator(
            task_id='has_exception_logs',
            test=lambda: len(rail.result('get_exception_logs')) > 0,
            yes_task='write_exception_logs',
            no_task='write_success_log',
        )

        write_exception_logs = rail.WriteLogOperator(
            task_id='write_exception_logs',
            log="{{dag_run.conf.create_user_log}}",
            message='User created with exception {{ result("get_exception_logs") | join(",")}}',
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Exception',
                'action': 'Add',
                'message': 'User created with exception {{ result("get_exception_logs") | join(",")}}',
                "allowed_for_supervisor_dag": "{{result('allowed_for_supervisor_dag')}}",
                "user_uri": "{{result('create_user').uri}}",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False",
                "create_user_log": "{{dag_run.conf.create_user_log}}"
            }
        )

        write_success_log = rail.WriteLogOperator(
            task_id='write_success_log',
            message='User created successfully',
            log="{{dag_run.conf.create_user_log}}",
            severity='Success',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Success',
                'action': 'Add',
                'message': 'User created successfully',
                "allowed_for_supervisor_dag": "{{result('allowed_for_supervisor_dag')}}",
                "user_uri": "{{result('create_user').uri}}",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False",
                "create_user_log": "{{dag_run.conf.create_user_log}}",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.create_user_log}}",
            severity="Error",
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': "Error",
                'action': 'Add',
                'message': '{{ get_error_message() }}',
                "allowed_for_supervisor_dag": "{{result('allowed_for_supervisor_dag') if get_task_state('create_user') == 'success' else  False}}",
                "user_uri": "{{result('create_user').uri if get_task_state('create_user') == 'success' else ''}}",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "{{True if get_task_state('create_user') == 'success' else 'False'}}",
                "create_user_log": "{{dag_run.conf.create_user_log}}"
            },
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_user

        create_user >> remove_timeoff_assignment >> update_supervisor_task >> \
            allowed_for_supervisor_dag >> get_timeoff_toassign >> can_update_timeentry_approval \
            >> rail.Label("Yes") >> update_timeentry_approval >> has_timeofftype
        can_update_timeentry_approval >> rail.Label("No") >> has_timeofftype
        has_timeofftype >> rail.Label(
            'Yes') >> add_timeofftype >> process_timeoff_policies >> wait_for_process_timeoff_policies >> get_exception_logs
        has_timeofftype >> rail.Label(
            'No') >> log_timeoff_assignment >> get_exception_logs

        get_exception_logs >> has_exception_logs
        has_exception_logs >> rail.Label(
            'yes') >> write_exception_logs >> catch_and_log_errors
        has_exception_logs >> rail.Label(
            'no') >> write_success_log >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
