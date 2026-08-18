from datetime import timedelta
import rail
from itvdaytime.user_import.utils import request_payload, data_handler


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_user_import_process_each_user_record_{config.instance}",
        description=f"iTV DayTime User Import master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        search_user = rail.RepliconServiceOperator(
            task_id="search_user",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_payload,
            data_handler=data_handler.get_search_user_data_handler
        )

        has_any_users_found = rail.IfOperator(
            task_id="has_any_users_found",
            test=lambda: len(rail.result('search_user')) > 0,
            yes_task="has_multiple_user_found",
            no_task="search_user_with_loginname"
        )

        search_user_with_loginname = rail.RepliconServiceOperator(
            task_id="search_user_with_loginname",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": "{{dag_run.conf.first_name}}" + "." + "{{dag_run.conf.last_name}}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )
        is_user_found = rail.IfOperator(
            task_id="is_user_found",
            test="{{result('search_user_with_loginname') | is_truthy}}",
            yes_task="log_user_found_with_loginname",
            no_task="create_user"
        )
        log_user_found_with_loginname = rail.WriteLogOperator(
            task_id="log_user_found_with_loginname",
            severity="Error",
            message="User with login name - {{dag_run.conf.first_name}}" +
            '.' + "{{dag_run.conf.last_name}} already present",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}",
                "status": "Skipped",
                "action": "Validation",
                "details": "User with login name - {{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}} already present",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "",
                "allowed_for_supervisor_processing": "No"
            }
        )
        has_multiple_user_found = rail.IfOperator(
            task_id="has_multiple_user_found",
            test=lambda: len(rail.result('search_user')) != 1,
            yes_task="log_multiple_user_found",
            no_task="is_user_disabled"
        )

        is_user_disabled = rail.IfOperator(
            task_id="is_user_disabled",
            test=lambda: rail.result('search_user')[
                0]['status'].lower() == "false",
            yes_task="log_user_is_disabled",
            no_task="update_user"
        )

        log_user_is_disabled = rail.WriteLogOperator(
            task_id="log_user_is_disabled",
            severity="Exception",
            message="User already disabled",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}",
                "status": "Skipped",
                "action": "Validation",
                "details": "User already disabled",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "",
                "allowed_for_supervisor_processing": "No"
            }
        )
        log_multiple_user_found = rail.WriteLogOperator(
            task_id="log_multiple_user_found",
            severity="Exception",
            message="Multiple users found for employee ID {{dag_run.conf.employee_number}}",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}",
                "status": "Skipped",
                "action": "Validation",
                "details": "Multiple user for person number: {{dag_run.conf.employee_number}}",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "",
                "allowed_for_supervisor_processing": "No"
            }
        )

        def get_common_conf(dag_run):
            return {
                **{
                    "user_uri": "" if not rail.result('search_user') else rail.result('search_user')[0].get('user_uri'),
                },
                **{k: v if v is not None else '' for k, v in dag_run.conf.items()}
            }

        create_user = rail.TriggerDagRunForEachItemOperator(
            task_id="create_user",
            items=[1],
            trigger_dag_id=f"itvdaytime_user_import_create_user_{config.instance}",
            conf=get_common_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_create_user = rail.WaitForDagRunsSensor(
            task_id="wait_create_user",
            dag_runs="{{result('create_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        update_user = rail.TriggerDagRunForEachItemOperator(
            task_id="update_user",
            items=[1],
            trigger_dag_id=f"itvdaytime_user_import_update_user_{config.instance}",
            conf=get_common_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_update_user",
            dag_runs="{{result('update_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        # pylint: disable= line-too-long
        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}",
                "status": "Error",
                "action": "Pre-Check",
                "details": '{{ get_error_message() }}',
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "",
                "allowed_for_supervisor_processing": "No"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        search_user >> has_any_users_found >> rail.Label("Yes") >> has_multiple_user_found >> rail.Label("Yes") >> log_multiple_user_found\
            >> rail.Label("On error") >> catch_and_log_error >> log_to_sumo
        has_any_users_found >> rail.Label("No") >> search_user_with_loginname >> is_user_found >> rail.Label("No") >> create_user >> wait_create_user >> rail.Label(
            "On error") >> catch_and_log_error
        is_user_found >> rail.Label("Yes") >> log_user_found_with_loginname >> rail.Label(
            "On error") >> catch_and_log_error
        has_multiple_user_found >> rail.Label(
            "No") >> is_user_disabled >> rail.Label("No") >> update_user >> wait_update_user >> rail.Label("On error") >> catch_and_log_error
        is_user_disabled >> rail.Label("Yes") >> log_user_is_disabled >> rail.Label(
            "On error") >> catch_and_log_error
    return dag


rail.for_each_instance(create_child_dag)
