import rail
from fujifilmdimatixg3.pto_timeoff_balance_update.utils import python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdimatixg3_pto_timeoff_balance_update_child_{config.instance}',
        description=f'fujifilmdimatixg3_pto_timeoff_balance_update_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        schedule_interval = None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_as_config_child",extra_config=config)

        get_user_data_from_empid = rail.RepliconServiceOperator(
            task_id = "get_user_data_from_empid",
            endpoint = "services/UserListService1.svc/GetData",
            data = python_callable.check_user_for_emp_id,
            data_handler = python_callable.get_user_data_from_empid_data_handler
        )

        is_user_present = rail.IfOperator(
            task_id = "is_user_present",
            test = "{{ result('get_user_data_from_empid') | is_truthy }}",
            yes_task = "get_user_time_off_policy",
            no_task = "log_no_user"
        )

        get_user_time_off_policy = rail.RepliconServiceOperator(
            task_id = "get_user_time_off_policy",
            endpoint = "services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=lambda: {
                "userUri": rail.result('get_user_data_from_empid')[0]['uri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['pto_uri'], 'policySetSchedule', '')
        )

        update_pto = rail.RepliconServiceOperator(
            task_id = "update_pto",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=python_callable.pto_update
        )

        log_success = rail.WriteLogOperator(
            task_id = "log_success",
            log = "{{ dag_run.conf.logger }}",
            message= "update success",
            severity= "Success",
            properties= {
                "EmployeeID": "{{dag_run.conf.employeeid}}",
                "status": "Success"
            }
        )

        log_no_user = rail.WriteLogOperator(
            task_id = "log_no_user",
            log = "{{ dag_run.conf.logger }}",
            message = "No user",
            severity= "Exception",
            properties= {
                "EmployeeID": "{{dag_run.conf.employeeid}}",
                "status": "Failed - No User"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = "{{ dag_run.conf.logger }}",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "EmployeeID": "{{dag_run.conf.employeeid}}",
                "status": "Error",
                "action": "Update",
                "details": '{{ get_error_message() }}'
            }
        )


        get_user_data_from_empid >> is_user_present >> rail.Label("No") >> log_no_user >> rail.Label("On Error") >> catch_and_log_error
        is_user_present >> rail.Label("Yes") >> get_user_time_off_policy >> update_pto >> log_success >> rail.Label("On Error") >> catch_and_log_error

        update_pto >> log_success >> rail.Label("On Error") >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)
