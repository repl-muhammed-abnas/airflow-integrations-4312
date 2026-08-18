from datetime import timedelta
from airflow.models import Variable
import rail
from lendingclub.user_import.utils import request_payload

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_user_import_process_user_child_{config.instance}',
        description=f'lendingclub_user_import_process_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_eachuser_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "loginName": "{{ dag_run.conf.loginname }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {
                "user_uri" : response[0]['userDetails']['uri'],
                "user_end_date" : response[0]['userDetails']['employmentDateRange']['endDate'],
                "user_start_date" : response[0]['userDetails']['employmentDateRange']['startDate'],
                "user_status" : response[0]['userDetails']['isEnabled']
            } if response else {}
        )

        if_user_details_present = rail.IfOperator(
            task_id='if_user_details_present',
            test="{{ result('get_user_details') | is_truthy }}",
            yes_task="if_user_status_is_false",
            no_task="process_user_for_add",
        )

        if_user_status_is_false = rail.IfOperator(
            task_id='if_user_status_is_false',
            test="{{ result('get_user_details').user_status | is_falsy }}",
            yes_task="if_employeestatus_is_disabled",
            no_task="if_employeestatus_is_absent",
        )

        if_employeestatus_is_disabled = rail.IfOperator(
            task_id='if_employeestatus_is_disabled',
            test="{{ dag_run.conf.employeestatus.lower() == 'disabled' }}",
            yes_task="log_user_already_disabled",
            no_task="if_employeestatus_is_on_leave",
        )

        log_user_already_disabled = rail.WriteLogOperator(
            task_id="log_user_already_disabled",
            log = '{{ dag_run.conf.logger}}',
            message="Skipped",
            severity="Skipped",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Disable User",
                "Status": "Skipped",
                'Details': "User already disabled in Replicon"
            }
        )

        if_employeestatus_is_on_leave = rail.IfOperator(
            task_id='if_employeestatus_is_on_leave',
            test="{{ dag_run.conf.employeestatus.lower() == 'on leave' }}",
            yes_task="if_user_end_date_present",
            no_task="if_employeestatus_is_active",
        )

        if_user_end_date_present = rail.IfOperator(
            task_id='if_user_end_date_present',
            test="{{ result('get_user_details').user_end_date | is_truthy }}",
            yes_task="update_employment_daterange",
            no_task="log_user_already_on_leave",
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data = request_payload.update_emp_start_date
        )

        log_user_already_on_leave = rail.WriteLogOperator(
            task_id="log_user_already_on_leave",
            log = '{{ dag_run.conf.logger}}',
            message="Skipped",
            severity="Skipped",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Disable User",
                "Status": "Skipped",
                'Details': "User already on leave in Replicon"
            }
        )

        if_employeestatus_is_active = rail.IfOperator(
            task_id='if_employeestatus_is_active',
            test="{{ dag_run.conf.employeestatus.lower() == 'active' }}",
            yes_task="process_user_for_update",
            no_task="catch_and_log_error",
        )

        if_employeestatus_is_absent = rail.IfOperator(
            task_id='if_employeestatus_is_absent',
            test="{{ dag_run.conf.employeestatus | is_falsy }}",
            yes_task="log_invalid_employeestatus",
            no_task="if_employee_status_is_disabled",
        )

        log_invalid_employeestatus = rail.WriteLogOperator(
            task_id="log_invalid_employeestatus",
            log = '{{ dag_run.conf.logger}}',
            message="Ignored",
            severity="Skipped",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Disable User",
                "Status": "Ignored",
                'Details': "Record not process as the employee staus is invalid",
            }
        )

        if_employee_status_is_disabled = rail.IfOperator(
            task_id='if_employee_status_is_disabled',
            test="{{ dag_run.conf.employeestatus.lower() == 'disabled' }}",
            yes_task="process_user_for_disable",
            no_task="if_employee_status_is_on_leave",
        )

        if_employee_status_is_on_leave = rail.IfOperator(
            task_id='if_employee_status_is_on_leave',
            test="{{ dag_run.conf.employeestatus.lower() == 'on leave' }}",
            yes_task="process_user_for_disable",
            no_task="if_employee_status_is_active",
        )

        process_user_for_disable = rail.TriggerDagRunOperator(
            task_id='process_user_for_disable',
            trigger_dag_id=f'lendingclub_user_import_disable_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                'loginid': dag_run.conf['loginname'],
                'employeestatus': dag_run.conf['employeestatus'],
                'empid': dag_run.conf['empid'],
                'useruri': rail.result('get_user_details')['user_uri'],
                'logger' : dag_run.conf["logger"],
                'user_start_date' : rail.result('get_user_details')['user_start_date']
            }
        )

        if_employee_status_is_active = rail.IfOperator(
            task_id='if_employee_status_is_active',
            test="{{ dag_run.conf.employeestatus.lower() == 'active' }}",
            yes_task="process_user_for_update",
            no_task="catch_and_log_error",
        )

        process_user_for_update = rail.TriggerDagRunOperator(
            task_id='process_user_for_update',
            trigger_dag_id=f'lendingclub_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_user_update
        )

        wait_for_process_user_for_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_user_for_update',
            dag_runs='{{ result("process_user_for_update") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_user_for_add = rail.TriggerDagRunOperator(
            task_id='process_user_for_add',
            trigger_dag_id=f'lendingclub_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_user_add
        )

        wait_for_process_user_for_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_user_for_add',
            dag_runs='{{ result("process_user_for_add") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "UserID": "{{ dag_run.conf.loginname }}" + "|" + "{{ dag_run.conf.empid }}",
                "Action": "Process Each User",
                "Status": "Error",
                "Details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_user_details

        get_user_details >> if_user_details_present

        if_user_details_present >> rail.Label('Yes') >> if_user_status_is_false
        if_user_details_present >> rail.Label('No') >> process_user_for_add >> wait_for_process_user_for_add >> catch_and_log_error

        if_user_status_is_false >> rail.Label('Yes') >> if_employeestatus_is_disabled
        if_user_status_is_false >> rail.Label('No') >> if_employeestatus_is_absent

        if_employeestatus_is_disabled >> rail.Label('Yes') >> log_user_already_disabled >> catch_and_log_error
        if_employeestatus_is_disabled >> rail.Label('No') >> if_employeestatus_is_on_leave

        if_employeestatus_is_on_leave >> rail.Label('Yes') >> if_user_end_date_present
        if_employeestatus_is_on_leave >> rail.Label('No') >> if_employeestatus_is_active

        if_user_end_date_present >> rail.Label('Yes') >> update_employment_daterange >> log_user_already_on_leave >> catch_and_log_error
        if_user_end_date_present >> rail.Label('No') >> log_user_already_on_leave >> catch_and_log_error

        if_employeestatus_is_active >> rail.Label('Yes') >> process_user_for_update >> wait_for_process_user_for_update >> catch_and_log_error
        if_employeestatus_is_active >> rail.Label('No') >> catch_and_log_error

        if_employeestatus_is_absent >> rail.Label('Yes') >> log_invalid_employeestatus >> catch_and_log_error
        if_employeestatus_is_absent >> rail.Label('No') >> if_employee_status_is_disabled

        if_employee_status_is_disabled >> rail.Label('Yes') >> process_user_for_disable >> catch_and_log_error
        if_employee_status_is_disabled >> rail.Label('No') >> if_employee_status_is_on_leave

        if_employee_status_is_on_leave >> rail.Label('Yes') >> process_user_for_disable >> catch_and_log_error
        if_employee_status_is_on_leave >> rail.Label('No') >> if_employee_status_is_active

        if_employee_status_is_active >> rail.Label('Yes') >> process_user_for_update >> wait_for_process_user_for_update >> catch_and_log_error
        if_employee_status_is_active >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
