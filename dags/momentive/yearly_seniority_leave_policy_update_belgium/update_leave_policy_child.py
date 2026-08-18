from momentive.yearly_seniority_leave_policy_update_belgium.custom_methods import get_time_off_policies
from momentive.yearly_seniority_leave_policy_update_belgium.request_payload import request_payload_put_timeoff_policy
import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"momentive_yearly_seniority_leave_policy_update_belgium_child_{config.instance}",
        description="momentive yearly seniority leave policy update belgium child",
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        company_key=config.company_key
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        get_user_timeoff_type_policy = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_type_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri":'{{ dag_run.conf.useruri }}'
            }
        )

        get_default_timeoff_policy_for_timeoff_type = rail.RepliconServiceOperator(
            task_id="get_default_timeoff_policy_for_timeoff_type",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": '{{dag_run.conf.timeoffuri}}'
            }
        )

        get_user_timeoff_policy_set = rail.PythonOperator(
            task_id="get_user_timeoff_policy_set",
            python_callable=get_time_off_policies
        )

        if_time_off_schedules_present = rail.IfOperator(
            task_id = "if_time_off_schedules_present",
            test='{{result("get_user_timeoff_policy_set") | length >0}}',
            yes_task="update_user_timeoff_policy",
            no_task="log_to_sumo"
        )

        update_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id="update_user_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload_put_timeoff_policy
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{get_error_message()}}"
        )

        get_user_timeoff_type_policy >> get_default_timeoff_policy_for_timeoff_type >>\
        get_user_timeoff_policy_set >> \
        if_time_off_schedules_present >> rail.Label("Yes") >> update_user_timeoff_policy >> log_to_sumo >> can_fail_dag >> fail_dagrun
        if_time_off_schedules_present >> rail.Label("No") >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_child_dag)
