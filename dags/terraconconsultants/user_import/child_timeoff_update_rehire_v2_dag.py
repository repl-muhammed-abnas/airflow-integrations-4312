from datetime import timedelta
import json
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils import python_callable_method
from terraconconsultants.user_import.utils import request_payload


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_update_timeoff_user(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_timeoff_update_rehire_v2_{config.instance}',
        description=f'TerraconConsultants User Sync Child Time Off Update/Rehire V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_timeoff_update_rehire'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_timeoff_update_rehire',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_timeoff_update_rehire = rail.EmptyOperator(
            task_id='process_timeoff_update_rehire'
        )

        should_process_timeoff_update_rehire = rail.IfOperator(
            task_id='should_process_timeoff_update_rehire',
            test=request_payload.process_timeoff_update_rehire_check,
            yes_task='get_defaultpolicy_from_global_level',
            no_task='dagrun_log_to_sumo'
        )

        get_defaultpolicy_from_global_level = rail.RepliconServiceOperator(
            task_id='get_defaultpolicy_from_global_level',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=lambda response: {
                'policy_sets': json.loads(json.dumps(
                    [x['policySet'] for x in response], ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                    '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
                    [x['policySet'] for x in response]) else '',
                'response': response if response else ''
            }
        )

        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_timeoff_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        is_first_description_present = rail.IfOperator(
            task_id='is_first_description_present',
            test="{{ result('get_user_timeoff_policy_summary') | first_or_default | \
                attr_or_default('description') | is_truthy }}",
            yes_task="past_policyset_schedule",
            no_task="is_timeofftypename_not_equals_paidtimeoff",
        )

        past_policyset_schedule = rail.PythonOperator(
            task_id='past_policyset_schedule',
            python_callable=python_callable_method.construct_policyschedule_list
        )

        is_timeofftypename_not_equals_paidtimeoff = rail.IfOperator(
            task_id='is_timeofftypename_not_equals_paidtimeoff',
            test="{{ dag_run.conf.timeofftypename != 'Paid Time Off' \
                and dag_run.conf.timeofftypename != 'Floating Holiday' }}",
            yes_task="is_globallevel_defaultpolicy",
            no_task="is_timeofftypename_equals_floatingholiday",
        )

        is_globallevel_defaultpolicy = rail.IfOperator(
            task_id='is_globallevel_defaultpolicy',
            test="{{ result('get_defaultpolicy_from_global_level').policy_sets | is_truthy }}",
            yes_task="assign_timeoff_policy",
            no_task="is_timeofftypename_equals_floatingholiday",
        )

        assign_timeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_assign_timeoff_policy_request
        )

        is_timeofftypename_equals_floatingholiday = rail.IfOperator(
            task_id='is_timeofftypename_equals_floatingholiday',
            test="{{ dag_run.conf.timeofftypename == 'Floating Holiday' }}",
            yes_task="is_globaldefault_policy_present_2",
            no_task="is_timeofftypename_equals_paidtimeoff",
        )

        is_globaldefault_policy_present_2 = rail.IfOperator(
            task_id='is_globaldefault_policy_present_2',
            test="{{ result('get_defaultpolicy_from_global_level').policy_sets | is_truthy }}",
            yes_task="final_policy_to_assign",
            no_task="is_timeofftypename_equals_paidtimeoff",
        )

        final_policy_to_assign = rail.PythonOperator(
            task_id='final_policy_to_assign',
            python_callable=python_callable_method.final_policy_to_assign_update
        )

        is_finalpolicy_to_assign = rail.IfOperator(
            task_id='is_finalpolicy_to_assign',
            test="{{ result('final_policy_to_assign') | is_truthy }}",
            yes_task="assign_timeoff_policy2",
            no_task="is_timeofftypename_equals_paidtimeoff",
        )

        assign_timeoff_policy2 = rail.RepliconServiceOperator(
            task_id='assign_timeoff_policy2',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_assign_timeoff_policy_request2
        )

        is_timeofftypename_equals_paidtimeoff = rail.IfOperator(
            task_id='is_timeofftypename_equals_paidtimeoff',
            test="{{ dag_run.conf.timeofftypename == 'Paid Time Off' }}",
            yes_task="get_final_paidtimeoff_policysets",
            no_task="dagrun_log_to_sumo",
        )

        get_final_paidtimeoff_policysets = rail.PythonOperator(
            task_id='get_final_paidtimeoff_policysets',
            python_callable=python_callable_method.get_final_paidtimeoff_policysets_updateuser
        )

        is_finalpolicy_to_assign_paidtimeoff = rail.IfOperator(
            task_id='is_finalpolicy_to_assign_paidtimeoff',
            test="{{ result('get_final_paidtimeoff_policysets') | is_truthy }}",
            yes_task="assign_timeoff_policy3",
            no_task="dagrun_log_to_sumo",
        )

        assign_timeoff_policy3 = rail.RepliconServiceOperator(
            task_id='assign_timeoff_policy3',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_assign_timeoff_policy_request3
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> process_timeoff_update_rehire
        process_timeoff_update_rehire >> should_process_timeoff_update_rehire
        should_process_timeoff_update_rehire >> rail.Label(
            'Yes') >> get_defaultpolicy_from_global_level
        get_defaultpolicy_from_global_level >> get_user_timeoff_policy_summary >> is_first_description_present
        is_first_description_present >> rail.Label(
            'Yes') >> past_policyset_schedule >> is_timeofftypename_not_equals_paidtimeoff
        is_first_description_present >> rail.Label(
            'No') >> is_timeofftypename_not_equals_paidtimeoff
        is_timeofftypename_not_equals_paidtimeoff >> rail.Label(
            'Yes') >> is_globallevel_defaultpolicy
        is_globallevel_defaultpolicy >> rail.Label(
            'Yes') >> assign_timeoff_policy >> is_timeofftypename_equals_floatingholiday
        is_globallevel_defaultpolicy >> rail.Label(
            'No') >> is_timeofftypename_equals_floatingholiday
        is_timeofftypename_not_equals_paidtimeoff >> rail.Label(
            'No') >> is_timeofftypename_equals_floatingholiday
        is_timeofftypename_equals_floatingholiday >> rail.Label(
            'Yes') >> is_globaldefault_policy_present_2
        is_globaldefault_policy_present_2 >> rail.Label(
            'Yes') >> final_policy_to_assign >> is_finalpolicy_to_assign
        is_finalpolicy_to_assign >> rail.Label(
            'Yes') >> assign_timeoff_policy2 >> is_timeofftypename_equals_paidtimeoff
        is_finalpolicy_to_assign >> rail.Label(
            'No') >> is_timeofftypename_equals_paidtimeoff
        is_globaldefault_policy_present_2 >> rail.Label(
            'No') >> is_timeofftypename_equals_paidtimeoff
        is_timeofftypename_equals_floatingholiday >> rail.Label(
            'No') >> is_timeofftypename_equals_paidtimeoff
        is_timeofftypename_equals_paidtimeoff >> rail.Label(
            'Yes') >> get_final_paidtimeoff_policysets >> is_finalpolicy_to_assign_paidtimeoff
        is_finalpolicy_to_assign_paidtimeoff >> rail.Label(
            'Yes') >> assign_timeoff_policy3 >> dagrun_log_to_sumo
        is_finalpolicy_to_assign_paidtimeoff >> rail.Label(
            'Yes') >> dagrun_log_to_sumo
        is_timeofftypename_equals_paidtimeoff >> rail.Label(
            'No') >> dagrun_log_to_sumo
        should_process_timeoff_update_rehire >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_update_timeoff_user)
