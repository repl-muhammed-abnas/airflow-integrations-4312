from datetime import timedelta
import rail
from valleychildrens.disable_user.utils import request_payload, response_filter


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"valleychildrens_disable_users_child_{config.instance}",
        description=f"Valletchildrens Disable Users Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        disable_user = rail.RepliconServiceOperator(
            task_id="disable_user",
            endpoint="services/securityService1.svc/DisableLogin",
            data=request_payload.get_user_payload,
        )

        update_timesheet_period = rail.RepliconServiceOperator(
            task_id="update_timesheet_period",
            endpoint="services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_timesheet_period,
        )

        update_employee_type = rail.RepliconServiceOperator(
            task_id="update_employee_type",
            endpoint="services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_employee_type,
        )

        get_user_timeoff_type_policy = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_type_policy",
            endpoint="services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_payload,
            response_filter=response_filter.get_filtered_policy
        )

        process_policy_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_policy_records",
            items="{{result('get_user_timeoff_type_policy') | to_json}}",
            trigger_dag_id=f"valleychildrens_put_blank_policy_child_{config.instance}",
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_policy_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_policy_records",
            dag_runs="{{result('process_policy_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_disable_errors_failures = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disable_errors_failures',
            dag_runs="{{ result('process_policy_records') }}",
            dagrun_task_id='catch_and_log_error',
            flatten=True
        )

        has_any_failures = rail.IfOperator(
            task_id='has_any_failures',
            test="{{ result('gather_disable_errors_failures') | is_truthy }}",
            yes_task='fail_disable_user_error',
            no_task='dagrun_log_to_sumo'
        )

        fail_disable_user_error = rail.FailOperator(
            task_id='fail_disable_user_error',
            message='Errors while disabling the users'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'user': '{{ dag_run.conf.item.User_Name }}',
                'useruri': '{{ dag_run.conf.item.UserUri }}',
                'enddate': '{{ dag_run.conf.item.User_End_Date }}',
                'error': '{{ get_error_message() }}'
            }
        )

        disable_user >> update_timesheet_period >> update_employee_type\
            >> get_user_timeoff_type_policy >> process_policy_records >> wait_process_policy_records >> gather_disable_errors_failures\
            >> has_any_failures >> rail.Label("Yes") >> fail_disable_user_error
        has_any_failures >> rail.Label(
            'On Error') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
