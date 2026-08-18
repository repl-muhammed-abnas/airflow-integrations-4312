import rail
from itvdaytime.user_import.utils.custom_methods import get_put_policy_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_user_import_process_each_timeoff_{config.instance}",
        description=f"iTV DayTime User Import process each timeoff {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_update_policies = rail.IfOperator(
            task_id="can_update_policies",
            test="{{dag_run.conf.effective_date | is_truthy and dag_run.conf.balance | is_truthy}}",
            yes_task="get_specific_timeoff_policy_for_user"
        )
        get_specific_timeoff_policy_for_user = rail.RepliconServiceOperator(
            task_id="get_specific_timeoff_policy_for_user",
            endpoint="services/TimeOffService1.svc/GetTimeOffPolicyForTimeOffTypeForUser",
            data={
                "userUri": "{{dag_run.conf.user_uri}}",
                "timeOffTypeUri": "{{dag_run.conf.timeoff_uri}}"
            }
        )

        generate_put_policy_payload = rail.PythonOperator(
            task_id="generate_put_policy_payload",
            python_callable=get_put_policy_payload
        )

        update_timeoff_policies_for_user = rail.RepliconServiceOperator(
            task_id="update_timeoff_policies_for_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffPolicyForUser",
            data="{{result('generate_put_policy_payload') | to_json}}"
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='User partially updated; {{ get_error_message() }}',
            properties=lambda dag_run: {
                "employee_number": dag_run.conf['employee_number'],
                "loginname": dag_run.conf['loginname'],
                "status": "Error",
                "action": "update",
                "details": 'User partially updated; {{ get_error_message() }}',
                "line_manager": dag_run.conf['line_manager'],
                "user_uri": dag_run.conf['user_uri'],
                "allowed_for_supervisor_processing": "No"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_update_policies >> rail.Label("Yes") >> get_specific_timeoff_policy_for_user >> generate_put_policy_payload \
            >> update_timeoff_policies_for_user >> rail.Label("On Error") >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
