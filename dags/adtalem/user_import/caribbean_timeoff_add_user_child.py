from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils import python_callable_method
from adtalem.user_import.utils.response_filter import get_policyschedule_entries


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_timeoff_adduser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_caribbean_child_timeoff_add_new_user_{config.instance}',
        description=f'Adtalem Carribean_Timeoff_add_new_user_Prod {config.instance}',
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
            no_task='get_salary'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_salary',
            end_task='dagrun_log_to_sumo',
        )

        get_salary = rail.PythonOperator(
            task_id='get_salary',
            python_callable=python_callable_method.get_salary_timeoff
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_caribbean_mapperfile,
            op_args=["{{ dag_run.conf.paygroup }}",
                     "{{ dag_run.conf.jobcode }}"]
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
            python_callable=python_callable_method.get_timeofftype_uris_caribbean
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
            items=lambda: rail.result('get_timeofftype_uris_to_assign'),
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser',
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ item }}"
                }
            },
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

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_salary

        get_salary >> get_mapper_entries >> get_timeofftypes_from_mapper >> is_timeofftypes_present

        is_timeofftypes_present >> rail.Label(
            'Yes') >> get_alltimeoff_types >> get_timeofftype_uris_to_assign >> \
            assign_required_timeofftypes >> get_default_timeoff_types_policy_schedule_for_user >> \
            put_user_time_off_account_policy_set_schedule >> dagrun_log_to_sumo

        is_timeofftypes_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_timeoff_adduser_child_dag)
