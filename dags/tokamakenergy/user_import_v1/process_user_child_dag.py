from datetime import timedelta
import rail
from airflow.models import Variable
from tokamakenergy.user_import_v1.utils import request_payload
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_child_dagid,
        description=f'TokamakEnergy BambooHR to Polaris User Sync Create Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

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
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(task_id="create_user_log")

        is_employee_number_present = rail.IfOperator(
            task_id='is_employee_number_present',
            test='{{ dag_run.conf.user_details.employeenumber | is_truthy }}',
            yes_task='get_user_details_from_replicon',
            no_task='log_missing_employee_id'
        )

        log_missing_employee_id = rail.WriteLogOperator(
            task_id='log_missing_employee_id',
            log='{{result("create_user_log")}}',
            severity='Exception',
            message='Employee ID is missing in BambooHR',
            properties=lambda dag_run: {
                "username": f'{dag_run.conf["user_details"]["firstname"]} {dag_run.conf["user_details"]["lastname"]}',
                "employee_id": dag_run.conf["user_details"]["employeenumber"],
                "action": "Process User",
                "status": "Exception",
                "comments": "Employee ID is missing in BambooHR"
            }
        )

        get_user_details_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_details_from_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_user_details_from_replicon,
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_user_exists_in_replicon = rail.IfOperator(
            task_id='is_user_exists_in_replicon',
            test='{{ result("get_user_details_from_replicon") | is_truthy }}',
            yes_task='is_disabled_in_bamboohr',
            no_task='trigger_create_user'
        )

        is_disabled_in_bamboohr = rail.IfOperator(
            task_id='is_disabled_in_bamboohr',
            test=lambda dag_run: dag_run.conf["user_details"]["status"].lower() == "inactive",
            yes_task='is_disabled_in_replicon',
            no_task='trigger_update_user'
        )

        is_disabled_in_replicon = rail.IfOperator(
            task_id='is_disabled_in_replicon',
            test=lambda: rail.result("get_user_details_from_replicon")["userDetails"]["isEnabled"] is False,
            yes_task='catch_and_log_errors',
            no_task='update_user_loginname_enddate_unassign_licenses'
        )

        update_user_loginname_enddate_unassign_licenses = rail.RepliconServiceOperator(
            task_id="update_user_loginname_enddate_unassign_licenses",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.update_loginname_enddate_licenses(dag_run, config.licenses)
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/securityService1.svc/DisableLogin",
            data=lambda: {
                "userUri": rail.result('get_user_details_from_replicon')['userDetails']['uri']
            }
        )

        log_user_disabled_in_replicon = rail.WriteLogOperator(
            task_id='log_user_disabled_in_replicon',
            log='{{result("create_user_log")}}',
            severity='Success',
            message='User Disabled',
            properties=lambda dag_run: {
                "username": f'{dag_run.conf["user_details"]["firstname"]} {dag_run.conf["user_details"]["lastname"]}',
                "employee_id": dag_run.conf["user_details"]["employeenumber"],
                "action": "Disable User",
                "status": "Success",
                "comments": 'User profile was disabled in Replicon'
            }
        )

        trigger_update_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_update_user',
            items=["one"],
            trigger_dag_id=config.update_user_child_dagid,
            conf=lambda dag_run: {
                "user_details": dag_run.conf["user_details"],
                "replicon_user_details": rail.result("get_user_details_from_replicon"),
                "last_synctime": dag_run.conf['last_synctime'],
                "supervisor_permission_sets": dag_run.conf["supervisor_permission_sets"],
                "log_artifact": rail.result("create_user_log"),
                "oef_details": dag_run.conf["oef_details"],
                "process_start_time": dag_run.conf["process_start_time"]
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_user",
            dag_runs="{{result('trigger_update_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        trigger_create_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_create_user',
            items=["one"],
            trigger_dag_id=config.create_user_child_dagid,
            conf=lambda dag_run: {
                "user_details": dag_run.conf["user_details"],
                "last_synctime": dag_run.conf["last_synctime"],
                "supervisor_permission_sets": dag_run.conf["supervisor_permission_sets"],
                "log_artifact": rail.result("create_user_log"),
                "oef_details": dag_run.conf["oef_details"],
                "process_start_time": dag_run.conf["process_start_time"]
            }
        )

        wait_for_create_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_create_user",
            dag_runs="{{result('trigger_create_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{result('create_user_log')}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "username": '{{ dag_run.conf.user_details.firstname }} {{ dag_run.conf.user_details.lastname }}',
                "employee_id": '{{ dag_run.conf.user_details.employeenumber }}',
                "action": "Process User",
                "status": "Error",
                "comments": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_user_log

        create_user_log >> is_employee_number_present >> rail.Label("Yes") >> get_user_details_from_replicon
        is_employee_number_present >> rail.Label("No") >> log_missing_employee_id >> catch_and_log_errors

        get_user_details_from_replicon >> is_user_exists_in_replicon

        is_user_exists_in_replicon >> rail.Label("Yes") >> is_disabled_in_bamboohr
        is_disabled_in_bamboohr >> rail.Label("Yes") >> is_disabled_in_replicon
        is_disabled_in_replicon >> rail.Label("Yes") >> catch_and_log_errors
        is_disabled_in_replicon >> rail.Label("No") >> update_user_loginname_enddate_unassign_licenses
        is_disabled_in_bamboohr >> rail.Label("No") >> trigger_update_user >> wait_for_update_user >> catch_and_log_errors
        is_user_exists_in_replicon >> rail.Label("No") >> trigger_create_user >> wait_for_create_user >> catch_and_log_errors

        update_user_loginname_enddate_unassign_licenses >> disable_user \
            >> log_user_disabled_in_replicon >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
