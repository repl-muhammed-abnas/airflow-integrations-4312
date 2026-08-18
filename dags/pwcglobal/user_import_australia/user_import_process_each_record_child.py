from datetime import timedelta
import rail
from pwcglobal.user_import_australia import request_payload
from pwcglobal.user_import_australia import custom_methods


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_import_process_each_record_child_{config.instance}",
        description=f"PwCGlobal User Import Australia - User import process record file {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.child_max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        is_email_correct = rail.IfOperator(
            task_id="is_email_correct",
            test="{{dag_run.conf.work_email | ends_with('pwc.com')}}",
            no_task="log_invalid_email",
            yes_task="get_users_data"
        )

        log_invalid_email = rail.WriteLogOperator(
            task_id="log_invalid_email",
            log="{{dag_run.conf.log}}",
            severity="skipped",
            message="Domain is not PWC",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "action": "validation",
                "status": "skipped",
                "details": "Domain is not PWC",
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "no"
            }
        )
        get_users_data = rail.RepliconServiceOperator(
            task_id="get_users_data",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_get_data_payload,
            response_filter=custom_methods.get_user_data
        )

        is_user_exists = rail.IfOperator(
            task_id="is_user_exists",
            test="{{result('get_users_data') | is_truthy}}",
            no_task="add_user",
            yes_task="update_user"
        )

        def get_user_add_update_conf(dag_run, action):
            return {
                "file_name": dag_run.conf['file_name'],
                "employee_id": dag_run.conf['employee_id'],
                "party_id": dag_run.conf['party_id'],
                "guid": dag_run.conf['guid'],
                "staff_code": dag_run.conf['staff_code'],
                "active_status": dag_run.conf['active_status'],
                "id": dag_run.conf['id'],
                "work_email": dag_run.conf['work_email'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "hire_date": dag_run.conf['hire_date'],
                "employee_type": dag_run.conf['employee_type'],
                "time_type": dag_run.conf['time_type'],
                "management_level": dag_run.conf['management_level'],
                "line_of_service": dag_run.conf['line_of_service'],
                "manager_id": dag_run.conf['manager_id'],
                "costcenter_id": dag_run.conf['costcenter_id'],
                "costcenter_name": dag_run.conf['costcenter_name'],
                "costcenter_level_1": dag_run.conf['costcenter_level_1'],
                "costcenter_level_2": dag_run.conf['costcenter_level_2'],
                "costcenter_level_3": dag_run.conf['costcenter_level_3'],
                "costcenter_level_4": dag_run.conf['costcenter_level_4'],
                "location_level_1": dag_run.conf['location_level_1'],
                "location_level_2": dag_run.conf['location_level_2'],
                "location_level_3": dag_run.conf['location_level_3'],
                "location_level_4": dag_run.conf['location_level_4'],
                "classification": dag_run.conf['classification'],
                "md5": dag_run.conf['md5'],
                "party_id_customfield_uri": dag_run.conf['party_id_customfield_uri'],
                "management_level_customfield_uri": dag_run.conf['management_level_customfield_uri'],
                "line_of_service_customfield_uri": dag_run.conf['line_of_service_customfield_uri'],
                "staff_code_customfield_uri": dag_run.conf['local_staff_code_customfield_uri'],
                "manager_user": "Yes" if dag_run.conf['manager_user'] else "No",
                "user_uri": rail.result('get_users_data')[0]['user_uri'] if rail.result('get_users_data') else None,
                "log": dag_run.conf['log'],
                "supervisor_log": dag_run.conf['supervisor_log'],
                "action": action
            }
        update_user = rail.TriggerDagRunForEachItemOperator(
            task_id="update_user",
            items=[0],
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_update_user_child_{config.instance}",
            conf=lambda dag_run: get_user_add_update_conf(dag_run, "update"),
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_user",
            dag_runs="{{result('update_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )
        add_user = rail.TriggerDagRunForEachItemOperator(
            task_id="add_user",
            items=['{{dag_run.conf | to_json}}'],
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_add_new_user_child_{config.instance}",
            conf=lambda dag_run: get_user_add_update_conf(dag_run, "add"),
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_add_user = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_user",
            dag_runs="{{result('add_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "action": "add/update",
                "status": "Error",
                "details": '{{ get_error_message() }}',
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "no"
            },
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        is_email_correct >> rail.Label("No") >> log_invalid_email >> rail.Label(
            "On error") >> catch_and_log_errors
        is_email_correct >> rail.Label(
            "Yes") >> get_users_data >> is_user_exists

        is_user_exists >> rail.Label("Yes") >> update_user >> wait_for_update_user >> rail.Label(
            "On error") >> catch_and_log_errors
        is_user_exists >> rail.Label("No") >> add_user >> wait_for_add_user >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag)
