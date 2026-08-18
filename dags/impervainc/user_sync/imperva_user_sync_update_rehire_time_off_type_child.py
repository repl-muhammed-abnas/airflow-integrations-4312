import json
from datetime import datetime
from pendulum import now
import rail
from impervainc.user_sync.utils import python_callable

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_user_sync_update_rehire_time_off_type_child,
        description=f'impervainc user sync update rehire timeoff type child dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_user_time_off_type_policy_assigned = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_assigned',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: json.loads(json.dumps(
                response, ensure_ascii=False).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        get_default_timeoff_policyset = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_policyset',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=lambda response: json.loads(json.dumps(
                response, ensure_ascii=False).replace('null', '"effective"').replace(
                '"script"', '"scriptTarget"'))
        )

        existing_policies = rail.PythonOperator(
            task_id='existing_policies',
            python_callable=lambda dag_run: python_callable.existing_policies(
                dag_run.conf['timeoffuri'],
                rail.result('get_user_time_off_type_policy_assigned')['policiesByTimeOffType']
            )
        )

        effective_date = rail.PythonOperator(
            task_id='effective_date',
            python_callable=lambda dag_run: dag_run.conf['Hire_Date'].split("T")[0] \
                if dag_run.conf['rehire_update'] == 'rehire' else datetime.strftime(now(), '%Y-%m-%d')
        )

        if_existing_policies_present = rail.IfOperator(
            task_id='if_existing_policies_present',
            test="{{result('existing_policies') | is_truthy}}",
            yes_task="create_pto_policy_list",
            no_task="update_pto_policy_list_and_create_policy_list"
        )

        create_pto_policy_list = rail.PythonOperator(
            task_id='create_pto_policy_list',
            python_callable=lambda: python_callable.create_pto_policy_list(
                rail.result('existing_policies'),
                rail.result('effective_date')
            )
        )

        update_pto_policy_list_and_create_policy_list = rail.PythonOperator(
            task_id='update_pto_policy_list_and_create_policy_list',
            python_callable=lambda dag_run: python_callable.update_pto_policy_list_and_create_policy_list(
                dag_run, rail.result('effective_date'),
                rail.result('get_default_timeoff_policyset'), rail.result('create_pto_policy_list')
            )
        )

        if_final_policyset_present = rail.IfOperator(
            task_id='if_final_policyset_present',
            test="{{result('update_pto_policy_list_and_create_policy_list').pto_policy_list | is_truthy}}",
            yes_task="put_user_timeoff_account_policysetschedule",
            no_task="log_to_sumo"
        )

        put_user_timeoff_account_policysetschedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policysetschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('update_pto_policy_list_and_create_policy_list')['pto_policy_list']
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_user_time_off_type_policy_assigned >> get_default_timeoff_policyset >> existing_policies >> effective_date >> \
        if_existing_policies_present >> rail.Label("Yes") >> create_pto_policy_list >> update_pto_policy_list_and_create_policy_list
        if_existing_policies_present >> rail.Label("No") >> update_pto_policy_list_and_create_policy_list >> if_final_policyset_present
        if_final_policyset_present >> rail.Label("Yes") >> put_user_timeoff_account_policysetschedule >> log_to_sumo
        if_final_policyset_present >> rail.Label("No") >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
