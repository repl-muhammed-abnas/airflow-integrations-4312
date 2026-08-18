
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_czech_time_off_policy_update_cz_compensation_time_{config.instance}',
        description=f'GE Czech Time Off policy update - CZ_Compensation Time {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='getassignedpolicyforthetimeofftype_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='getassignedpolicyforthetimeofftype_3',
            end_task='add_timeoff_policy_logs_13',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        getassignedpolicyforthetimeofftype_3 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_3',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        if_to_s_not_contains_urn_5 = rail.IfOperator(
            task_id='if_to_s_not_contains_urn_5',
            test="{{ result('getassignedpolicyforthetimeofftype_3') | first_or_default(default='') | \
                is_truthy and result('getassignedpolicyforthetimeofftype_3') | first_or_default(default='') | \
                    attr_or_default('description') | is_truthy }}",
            yes_task="get_default_time_off_type_policy_schedule_for_user_7",
            no_task="add_timeoff_policy_logs_13",
        )

        get_default_time_off_type_policy_schedule_for_user_7 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_7',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
            }
        )

        log_global_policy_9 = rail.PythonOperator(
            task_id='log_global_policy_9',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_7'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
        )

        if_to_s_contains_urn_10 = rail.IfOperator(
            task_id='if_to_s_contains_urn_10',
            test='''{{ result('log_global_policy_9') | is_truthy }}''',
            yes_task="put_time_offpolicy_11",
            no_task="add_timeoff_policy_logs_13",
        )

        put_time_offpolicy_11 = rail.RepliconServiceOperator(
            task_id='put_time_offpolicy_11',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_global_policy_9')
            }
        )

        add_timeoff_policy_logs_13 = rail.WriteLogOperator(
            task_id='add_timeoff_policy_logs_13',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "Add/update",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_timeoff_policy_logs_13
        can_run_batch_task >> rail.Label(
            'No') >> getassignedpolicyforthetimeofftype_3
        getassignedpolicyforthetimeofftype_3 >> if_to_s_not_contains_urn_5
        if_to_s_not_contains_urn_5 >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user_7 >> \
            log_global_policy_9 >> if_to_s_contains_urn_10
        if_to_s_contains_urn_10 >> rail.Label(
            'Yes') >> put_time_offpolicy_11 >> add_timeoff_policy_logs_13
        if_to_s_not_contains_urn_5 >> rail.Label(
            'No') >> add_timeoff_policy_logs_13 >> log_to_sumo
        if_to_s_contains_urn_10 >> rail.Label(
            'No') >> add_timeoff_policy_logs_13

    return dag


rail.for_each_instance(create_dag)
