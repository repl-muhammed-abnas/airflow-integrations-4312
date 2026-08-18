from datetime import timedelta
import json
from airflow.models import Variable
import rail
from adtalem.user_import.utils import python_callable_method
from adtalem.user_import.utils.request_payload import get_assign_pto_policy, get_assign_sicktimeoff_policy, get_assign_sicktimeoff_policy2
from adtalem.user_import.utils.response_filter import get_policyschedule_entries


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_timeoff_adduser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_timeoff_add_new_user_crv4.0_{config.instance}',
        description=f'Adtalem_Timeoff_add_new_user CRV4.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_mapper_lookup'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_mapper_lookup',
            end_task='dagrun_log_to_sumo',
        )

        def get_mapper_lookup_value():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            if dag_run_conf['ususer'] != 'yes':
                if dag_run_conf['regulartemp'] == 'R' and dag_run_conf['fullparttime'] == 'F':
                    return f"{dag_run_conf['mapperlookup']}/RF"
            return dag_run_conf['mapperlookup']
        get_mapper_lookup = rail.PythonOperator(
            task_id='get_mapper_lookup',
            python_callable=get_mapper_lookup_value
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_mapperfile,
            op_args=["{{ result('get_mapper_lookup') }}"]
        )

        get_timeofftypes_from_mapper = rail.PythonOperator(
            task_id='get_timeofftypes_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Time Off Types']
        )

        is_timeofftypes_present = rail.IfOperator(
            task_id='is_timeofftypes_present',
            test="{{ result('get_timeofftypes_from_mapper') | is_truthy }}",
            yes_task="get_alltimeoff_types",
            no_task="dagrun_log_to_sumo",
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_timeofftype_uris_to_assign = rail.PythonOperator(
            task_id='get_timeofftype_uris_to_assign',
            python_callable=python_callable_method.get_timeofftype_uris
        )

        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_timeofftype_uris_to_assign')
            }
        )

        get_default_timeoff_types_policy_schedule_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_default_timeoff_types_policy_schedule_for_user',
            items=lambda: rail.result(
                'get_timeofftype_uris_to_assign'),
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser',
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ item }}"
                }
            },
            flatten=True,
            data_handler=get_policyschedule_entries
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            items=lambda: [x for x in rail.result(
                'get_default_timeoff_types_policy_schedule_for_user') if x],
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run, item: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": item['timeOffTypeUri']
                },
                "policySetScheduleEntries": item['policySetScheduleEntries']
            }
        )

        should_assign_sicktimeoff_policy_ususer = rail.IfOperator(
            task_id='should_assign_sicktimeoff_policy_ususer',
            test="{{ result('get_timeofftype_uris_to_assign', 'sick_timeoff_name') | sn | is_truthy and \
                dag_run.conf.ususer == 'yes' }}",
            yes_task="get_policy_schedule",
            no_task="is_not_ususer",
        )

        get_policy_schedule = rail.PythonOperator(
            task_id='get_policy_schedule',
            python_callable=python_callable_method.get_policy_schedule_entries,
            op_args=[config.adtalem_sicktime_timeoffpolicy_schedule_mapper_old]
        )

        is_sickleave_matches = rail.IfOperator(
            task_id='is_sickleave_matches',
            test="{{ result('get_timeofftype_uris_to_assign', 'sick_timeoff_name') == 'Sick Leave - MA' or \
                result('get_timeofftype_uris_to_assign', 'sick_timeoff_name') == 'Sick Leave' or \
                    result('get_timeofftype_uris_to_assign', 'sick_timeoff_name') == 'Sick Leave - OR' }}",
            yes_task="assign_sicktimeoff_policy",
            no_task="assign_sicktimeoff_policy2",
        )

        assign_sicktimeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_sicktimeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_assign_sicktimeoff_policy
        )

        assign_sicktimeoff_policy2 = rail.RepliconServiceOperator(
            task_id='assign_sicktimeoff_policy2',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_assign_sicktimeoff_policy2
        )

        is_not_ususer = rail.IfOperator(
            task_id='is_not_ususer',
            test="{{ dag_run.conf.ususer == 'no' }}",
            yes_task="is_paygroup_equals_to",
            no_task="check_if_vacationtimeoff_to_assign",
        )

        is_paygroup_equals_to = rail.IfOperator(
            task_id='is_paygroup_equals_to',
            test=lambda dag_run: dag_run.conf['paygroup'] in (
                'ACAUK', 'ACAFR', 'ACADE', 'ACAAU'),
            yes_task="get_pto_timeofftype_uri",
            no_task="check_if_vacationtimeoff_to_assign",
        )

        get_pto_timeofftype_uri = rail.PythonOperator(
            task_id='get_pto_timeofftype_uri',
            python_callable=python_callable_method.get_ptotimeofftype_uri
        )

        get_pto_policyset = rail.RepliconServiceOperator(
            task_id='get_pto_policyset',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_pto_timeofftype_uri') }}"
            },
            data_handler=lambda response: json.loads(json.dumps(
                response['policySet'], ensure_ascii=False).replace('"script"', '"scriptTarget"'))
        )

        assign_pto_timeoffpolicy = rail.RepliconServiceOperator(
            task_id='assign_pto_timeoffpolicy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_assign_pto_policy
        )

        check_if_vacationtimeoff_to_assign = rail.PythonOperator(
            task_id='check_if_vacationtimeoff_to_assign',
            python_callable=python_callable_method.get_vacationtimeoff_to_assign
        )

        is_vacationtimeoff_present = rail.IfOperator(
            task_id='is_vacationtimeoff_present',
            test="{{ result('check_if_vacationtimeoff_to_assign') | sn | is_truthy }}",
            yes_task="get_required_timeoff_jobcode_mapper",
            no_task="dagrun_log_to_sumo",
        )

        get_required_timeoff_jobcode_mapper = rail.PythonOperator(
            task_id='get_required_timeoff_jobcode_mapper',
            python_callable=python_callable_method.get_jobcode_timeoff_jobcode_mapper,
            op_args=['{{ dag_run.conf.jobcode }}']
        )

        final_policy_mapper_vacation = rail.PythonOperator(
            task_id='final_policy_mapper_vacation',
            python_callable=python_callable_method.get_final_policy_mapper
        )

        is_final_policy_mapper_vacation_present = rail.IfOperator(
            task_id='is_final_policy_mapper_vacation_present',
            test="{{ result('final_policy_mapper_vacation') | is_truthy }}",
            yes_task="trigger_vacation_policy_newuser",
            no_task="dagrun_log_to_sumo",
        )

        trigger_vacation_policy_newuser = rail.TriggerDagRunOperator(
            task_id='trigger_vacation_policy_newuser',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_assign_vacation_policy_new_users_crv2.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "policyname": "{{ result('final_policy_mapper_vacation') }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "servicedate": "{{ dag_run.conf.servicedate }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_mapper_lookup

        get_mapper_lookup >> get_mapper_entries >> get_timeofftypes_from_mapper >> is_timeofftypes_present

        is_timeofftypes_present >> rail.Label(
            'Yes') >> get_alltimeoff_types >> get_timeofftype_uris_to_assign >> \
            assign_required_timeofftypes >> get_default_timeoff_types_policy_schedule_for_user >> \
            put_user_time_off_account_policy_set_schedule >> should_assign_sicktimeoff_policy_ususer

        should_assign_sicktimeoff_policy_ususer >> rail.Label(
            'Yes') >> get_policy_schedule >> is_sickleave_matches

        is_sickleave_matches >> rail.Label(
            'Yes') >> assign_sicktimeoff_policy >> is_not_ususer
        is_sickleave_matches >> rail.Label(
            'No') >> assign_sicktimeoff_policy2 >> is_not_ususer

        should_assign_sicktimeoff_policy_ususer >> rail.Label(
            'No') >> is_not_ususer
        is_not_ususer >> rail.Label(
            'Yes') >> is_paygroup_equals_to

        is_paygroup_equals_to >> rail.Label(
            'Yes') >> get_pto_timeofftype_uri >> get_pto_policyset >> assign_pto_timeoffpolicy >> \
            check_if_vacationtimeoff_to_assign
        is_paygroup_equals_to >> rail.Label(
            'No') >> check_if_vacationtimeoff_to_assign

        is_not_ususer >> rail.Label(
            'No') >> check_if_vacationtimeoff_to_assign
        check_if_vacationtimeoff_to_assign >> is_vacationtimeoff_present

        is_vacationtimeoff_present >> rail.Label(
            'Yes') >> get_required_timeoff_jobcode_mapper >> final_policy_mapper_vacation >> \
            is_final_policy_mapper_vacation_present

        is_final_policy_mapper_vacation_present >> rail.Label(
            'Yes') >> trigger_vacation_policy_newuser >> dagrun_log_to_sumo
        is_final_policy_mapper_vacation_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_vacationtimeoff_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_timeofftypes_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_timeoff_adduser_child_dag)
