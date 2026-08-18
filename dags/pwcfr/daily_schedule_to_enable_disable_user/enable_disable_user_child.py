from pwcfr.daily_schedule_to_enable_disable_user.custom_methods import get_replicon_date, get_user_schedule_policy_list, create_schedule_entries_and_policy
import rail

def create_child_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcfr_daily_schedule_to_execute_enable_disable_user_child_{config.instance}",
        description="enable or disable users child",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        check_action_to_complete = rail.IfOperator(
            task_id="check_action_to_complete",
            test='{{dag_run.conf.action == "enable"}}',
            yes_task="enable_user",
            no_task="if_disable_user_schedule_frof"
        )

        enable_user = rail.RepliconServiceOperator(
            task_id="enable_user",
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                    "userUri": '{{dag_run.conf.useruri}}'
                }
        )

        write_enable_disable_user_log = rail.WriteLogOperator(
            task_id="write_enable_disable_user_log",
            log='{{dag_run.conf.lookup_table}}',
            message="na",
            severity="success",
            properties={
                "username":"{{dag_run.conf.username}}",
                "action":"{{dag_run.conf.action}}",
                "enddate":"{{dag_run.conf.enddate}}",
                "status":"success",
                "details":"NA",
                "jobid":"{{dag_run.conf.parent_ecid}}",
                "childjobid":"{{dag_run_ecid()}}",
                "schedulename":"{{dag_run.conf.schedulename}}"
            }
        )

        if_disable_user_schedule_frof = rail.IfOperator(
            task_id="if_disable_user_schedule_frof",
            test="{{dag_run.conf.schedulename == 'FROF'}}",
            yes_task="disable_user_with_any_schedule",
            no_task="get_enddate"
        )

        disable_user_with_any_schedule = rail.RepliconServiceOperator(
            task_id="disable_user_with_any_schedule",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                    "userUri": '{{dag_run.conf.useruri}}'
                }

        )

        get_enddate = rail.PythonOperator(
            task_id="get_enddate",
            python_callable=get_replicon_date,
            op_args=['{{dag_run.conf.enddate}}']
        )

        get_schedule_policy_for_user = rail.RepliconServiceOperator(
            task_id="get_schedule_policy_for_user",
            endpoint="services/SchedulingService2.svc/GetSchedulePolicyScheduleForUser",
            data={
                    "userUri": '{{dag_run.conf.useruri}}'
                },
            data_handler=get_user_schedule_policy_list,
        )

        check_if_display_text_in_user_schedules = rail.IfOperator(
            task_id="check_if_display_text_in_user_schedules",
            test=lambda: bool(rail.result("get_schedule_policy_for_user")),
            yes_task="check_if_current_user_schedule_is_empty",
            no_task="assign_schedule_policy_to_user"
        )

        check_if_current_user_schedule_is_empty = rail.IfOperator(
            task_id="check_if_current_user_schedule_is_empty",
            test=lambda:list(filter(
                lambda schedule:schedule["effectiveDate"] == rail.result("get_enddate") and schedule["schedule"] == "EMPTY",
                rail.result("get_schedule_policy_for_user"))),
            yes_task="disable_user_with_any_schedule",
            no_task="check_if_any_assigned_schedule_for_user"
        )

        check_if_any_assigned_schedule_for_user = rail.IfOperator(
            task_id="check_if_any_assigned_schedule_for_user",
            test=lambda:list(filter(
                lambda schedule:schedule["effectiveDate"]
                   == rail.result("get_enddate"),
                rail.result("get_schedule_policy_for_user"))),
            yes_task="disable_user_with_any_schedule",
            no_task="assign_schedule_policy_to_user"
        )

        assign_schedule_policy_to_user = rail.RepliconServiceOperator(
            task_id="assign_schedule_policy_to_user",
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=create_schedule_entries_and_policy
        )

        write_enable_disable_user_failure_log = rail.WriteLogOperator(
            task_id="write_enable_disable_user_failure_log",
            log='{{dag_run.conf.lookup_table}}',
            message="na",
            severity="Error",
            properties={
                "username":"{{dag_run.conf.username}}",
                "action":"{{dag_run.conf.action}}",
                "enddate":"{{dag_run.conf.enddate}}",
                "status":"Error",
                "details":'{{get_error_message()}}',
                "jobid":"{{dag_run.conf.parent_ecid}}",
                "childjobid":"{{dag_run_ecid()}}",
                "schedulename":"{{dag_run.conf.schedulename}}"
            },
            trigger_rule="one_failed"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        check_action_to_complete >> rail.Label("enable") >> enable_user >> write_enable_disable_user_log >> log_to_sumo
        check_action_to_complete >> rail.Label("disable") >> \
        if_disable_user_schedule_frof >> rail.Label("Yes") >> disable_user_with_any_schedule >> write_enable_disable_user_log >> log_to_sumo
        if_disable_user_schedule_frof >> rail.Label("No") >> get_enddate >> get_schedule_policy_for_user >> \
        check_if_display_text_in_user_schedules >> rail.Label("Yes") >> \
        check_if_current_user_schedule_is_empty >> rail.Label("Yes") >> disable_user_with_any_schedule >> write_enable_disable_user_log >> log_to_sumo
        check_if_current_user_schedule_is_empty >> rail.Label("No") >>\
        check_if_any_assigned_schedule_for_user >> rail.Label("Yes") >> disable_user_with_any_schedule >> write_enable_disable_user_log >> log_to_sumo
        check_if_any_assigned_schedule_for_user >> rail.Label("No") >> assign_schedule_policy_to_user >>\
        disable_user_with_any_schedule >> write_enable_disable_user_failure_log
        check_if_display_text_in_user_schedules >> rail.Label("No") >> assign_schedule_policy_to_user >>\
        disable_user_with_any_schedule >> write_enable_disable_user_failure_log >> log_to_sumo
    return dag
rail.for_each_instance(create_child_airflow_dag)
