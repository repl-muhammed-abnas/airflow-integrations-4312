import rail
from pwcglobal.user_import_australia import request_payload
from pwcglobal.user_import_australia import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_termination_details_child_process_each_records_{config.instance}",
        description=f"PwCGlobal User Import Australia Termination Details child process each records {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.child_max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        get_my_actual_user_identity = rail.RepliconServiceOperator(
            task_id="get_my_actual_user_identity",
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity",
        )

        is_integration_user = rail.IfOperator(
            task_id="is_integration_user",
            test=lambda: rail.result("get_my_actual_user_identity")[
                "loginName"] == "{{dag_run.conf.guid}}",
            yes_task="log_integration_account_received",
            no_task="is_termination_date_present"
        )

        log_integration_account_received = rail.WriteLogOperator(
            task_id="log_integration_account_received",
            log="{{dag_run.conf.log}}",
            message="This account is used for integration execution",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "This account is used for integration execution",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        is_termination_date_present = rail.IfOperator(
            task_id="is_termination_date_present",
            test=lambda dag_run: dag_run.conf['termination_date'],
            yes_task="get_users_data",
            no_task="log_termination_date_not_present"
        )

        log_termination_date_not_present = rail.WriteLogOperator(
            task_id="log_termination_date_not_present",
            log="{{dag_run.conf.log}}",
            message="Termination Date not present in feed file",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "Termination Date not present in feed file",
                "employeeid": "{{dag_run.conf.employee_id}}"
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
            test="{{result('get_users_data') | length > 0}}",
            yes_task="is_user_enabled",
            no_task="log_user_does_not_exists"
        )

        log_user_does_not_exists = rail.WriteLogOperator(
            task_id="log_user_does_not_exists",
            log="{{dag_run.conf.log}}",
            message="User not found",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "User not found",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        is_user_enabled = rail.IfOperator(
            task_id="is_user_enabled",
            test="{{result('get_users_data')[0].enabled}}",
            yes_task="get_user_details",
            no_task="log_user_already_disabled"
        )

        log_user_already_disabled = rail.WriteLogOperator(
            task_id="log_user_already_disabled",
            log="{{dag_run.conf.log}}",
            message="User is already disabled",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "User is already disabled",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )
        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": "{{result('get_users_data')[0].user_uri}}"
            }
        )

        is_enddate_before_startdate = rail.IfOperator(
            task_id="is_enddate_before_startdate",
            test=custom_methods.bool_enddate_before_startdate,
            yes_task="log_end_date_before_start_date",
            no_task="update_employment_date_range"
        )

        log_end_date_before_start_date = rail.WriteLogOperator(
            task_id="log_end_date_before_start_date",
            log="{{dag_run.conf.log}}",
            message="User's termination date is before the user start date. Start date: {{result('get_user_details').employmentDateRange.startDate}}\
                &  Termination date: {{dag_run.conf.termination_date}}",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "User's termination date is before the user start date. Start date: {{result('get_user_details').employmentDateRange.startDate}}\
                &  Termination date: {{dag_run.conf.termination_date}}",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        update_employment_date_range = rail.RepliconServiceOperator(
            task_id="update_employment_date_range",
            endpoint="/services/UserService1.svc//UpdateEmploymentDateRange",
            data=request_payload.get_update_employment_date_range_payload
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id="disable_user_login",
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{result('get_users_data')[0].user_uri}}"
            }

        )
        log_user_successfuly_disabled = rail.WriteLogOperator(
            task_id="log_user_successfuly_disabled",
            log="{{dag_run.conf.log}}",
            message="User disabled",
            severity="Success",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Success",
                "details": "User disabled",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
                "employeeid": "{{dag_run.conf.employee_id}}"
            },
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        get_my_actual_user_identity >> is_integration_user >> rail.Label(
            "Yes") >> log_integration_account_received >> rail.Label("On Error") >> catch_and_log_errors >> log_to_sumo
        is_integration_user >> rail.Label("No") >> is_termination_date_present >> rail.Label(
            "No") >> log_termination_date_not_present >> rail.Label("On Error") >> catch_and_log_errors
        is_termination_date_present >> rail.Label("Yes") >> get_users_data >> is_user_exists >> rail.Label(
            "No") >> log_user_does_not_exists >> rail.Label("On Error") >> catch_and_log_errors
        is_user_exists >> rail.Label("Yes") >> is_user_enabled >> rail.Label("Yes") >> get_user_details >> is_enddate_before_startdate >> rail.Label(
            "Yes") >> log_end_date_before_start_date >> rail.Label("On Error") >> catch_and_log_errors
        is_user_enabled >> rail.Label("No") >> log_user_already_disabled >> rail.Label(
            "On Error") >> catch_and_log_errors
        is_enddate_before_startdate >> rail.Label(
            "No") >> update_employment_date_range >> disable_user_login >> log_user_successfuly_disabled >> rail.Label("On Error") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
