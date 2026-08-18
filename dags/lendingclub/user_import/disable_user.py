import rail
from lendingclub.user_import.utils import request_payload

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_user_import_disable_user_child_{config.instance}',
        description=f'lendingclub_user_import_disable_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disbale_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        is_loginid_is_admin = rail.IfOperator(
            task_id='is_loginid_is_admin',
            test="{{ dag_run.conf.loginid == 'admin' }}",
            yes_task="catch_and_log_error",
            no_task="disable_user",
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/securityService1.svc/DisableLogin",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}"
                }
        )

        if_employee_status_is_on_leave = rail.IfOperator(
            task_id='if_employee_status_is_on_leave',
            test="{{ dag_run.conf.employeestatus.lower() == 'on leave' }}",
            yes_task="log_user_diabled_without_enddate",
            no_task="if_startdate_is_present",
        )

        log_user_diabled_without_enddate = rail.WriteLogOperator(
            task_id="log_user_diabled_without_enddate",
            log = '{{ dag_run.conf.logger}}',
            message="Success",
            severity="Success",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginid'] + "|" + dag_run.conf['empid'],
                "Action": "Disable user",
                "Status": "Success",
                'Details': "User disabled and no end date added (on leave)"
            }
        )

        if_startdate_is_present = rail.IfOperator(
            task_id='if_startdate_is_present',
            test="{{ dag_run.conf.user_start_date | is_truthy }}",
            yes_task="updateemployment_daterange",
            no_task="catch_and_log_error",
        )

        updateemployment_daterange = rail.RepliconServiceOperator(
            task_id='updateemployment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data = request_payload.update_emp_daterange
        )

        log_user_disabled_with_enddate = rail.WriteLogOperator(
            task_id="log_user_disabled_with_enddate",
            log = '{{ dag_run.conf.logger}}',
            message="Success",
            severity="Success",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginid'] + "|" + dag_run.conf['empid'],
                "Action": "Disable user",
                "Status": "Success",
                'Details': "User disabled with end date"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "UserID": "{{ dag_run.conf.loginname }}" + "|" + "{{ dag_run.conf.empid }}",
                "Action": "Disable user",
                "Status": "Error",
                "Details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        is_loginid_is_admin >> rail.Label('Yes') >> catch_and_log_error
        is_loginid_is_admin >> rail.Label('No') >> disable_user >> if_employee_status_is_on_leave

        if_employee_status_is_on_leave >> rail.Label('Yes') >> log_user_diabled_without_enddate >> catch_and_log_error
        if_employee_status_is_on_leave >> rail.Label('No') >> if_startdate_is_present

        if_startdate_is_present >> rail.Label('Yes') >> updateemployment_daterange >> log_user_disabled_with_enddate >> catch_and_log_error
        if_startdate_is_present >> rail.Label('No') >> catch_and_log_error


        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
