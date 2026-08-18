import json
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_user_sync_timeoff_add_user,
        description=f'impervainc user sync timeoff add user child dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
            },
            data_handler=lambda response:json.loads(json.dumps(
                    response, ensure_ascii=False).replace('null', '"effective"').replace(
                    '"script"', '"scriptTarget"'))
        )

        if_log_policyto_assign_present = rail.IfOperator(
            task_id='if_log_policyto_assign_present',
            test='''{{ result('get_default_time_off_type_policy_schedule_for_user') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule",
            no_task="log_to_sumo",
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_default_time_off_type_policy_schedule_for_user')
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_default_time_off_type_policy_schedule_for_user >> if_log_policyto_assign_present >> rail.Label(
            "Yes") >> put_user_time_off_account_policy_set_schedule >> log_to_sumo
        if_log_policyto_assign_present >> rail.Label(
            "No") >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
