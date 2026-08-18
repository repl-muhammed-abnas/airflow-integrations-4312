from datetime import timedelta
from airflow.models import Variable
import rail
from technicolorg3.user_import.utils.python_callable_method import get_final_timeoff_types_update, get_timeoff_types_to_assign


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


def create_timeoff_assignment_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_timeoff_assignment_{config.instance}',
        description=f'Technicolor_User Sync_Child_Timeoff assignment {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_timeoff_assignment_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_timeofftypes'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_all_timeofftypes',
            end_task='log_dagrun_to_sumo',
        )

        get_all_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_all_timeofftypes',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes'
        )

        get_timeoff_from_mapper = rail.PythonOperator(
            task_id='get_timeoff_from_mapper',
            python_callable=lambda dag_run: [x for x in config.user_master_mapper if x['type'] == 'Timeoff'
                                             and x['country'] == dag_run.conf['country'] and
                                             x['identifier1(worklocation)'] == dag_run.conf['businessunitname'] and
                                             x['identifier2(employeetype_businessunit_type)'] == dag_run.conf['jobcategory']]
        )

        is_timeoff_not_exists = rail.IfOperator(
            task_id='is_timeoff_not_exists',
            test=lambda: not bool(rail.result('get_timeoff_from_mapper')),
            yes_task='log_dagrun_to_sumo',
            no_task='timeoff_types_to_assign'
        )

        timeoff_types_to_assign = rail.PythonOperator(
            task_id='timeoff_types_to_assign',
            python_callable=get_timeoff_types_to_assign,
            op_args=['get_all_timeofftypes', 'get_timeoff_from_mapper']
        )

        is_timeoff_types_to_assign = rail.IfOperator(
            task_id='is_timeoff_types_to_assign',
            test=lambda: bool(rail.result('timeoff_types_to_assign')),
            yes_task='process_timeoffs',
            no_task='log_dagrun_to_sumo'
        )

        process_timeoffs = rail.EmptyOperator(
            task_id='process_timeoffs'
        )

        is_action_add = rail.IfOperator(
            task_id='is_action_add',
            test="{{ dag_run.conf.action == 'add' }}",
            yes_task='assign_timeoff_assignments',
            no_task='get_user_timeoff_policy_summary'
        )

        get_user_timeoff_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_timeoff_policy_summary',
            endpoint='/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            },
            data_handler=lambda response: list(filter(lambda y: y['enabled'] == 'true', map(lambda x: {
                'name': x['timeOffType']['name'],
                'enabled': x['isTimeOffAllowedAgainstThisTimeOffType'],
                'uri': x['timeOffType']['uri'],
                'policy': x['policySetSchedule']['effectiveDate']['day'] if x['policySetSchedule'] and x[
                    'policySetSchedule'].get('effectiveDate') else None
            }, response['policiesByTimeOffType']))) if response['policiesByTimeOffType'] else []
        )

        get_final_timeoff_types_assign = rail.PythonOperator(
            task_id='get_final_timeoff_types_assign',
            python_callable=get_final_timeoff_types_update
        )

        is_timeoff_types_to_update = rail.IfOperator(
            task_id='is_timeoff_types_to_update',
            test="{{ result('get_final_timeoff_types_assign') | length > 0 }}",
            yes_task='assign_timeoff_assignments',
            no_task='log_dagrun_to_sumo'
        )

        assign_timeoff_assignments = rail.RepliconServiceOperator(
            task_id='assign_timeoff_assignments',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'timeOffTypeUris': rail.result('timeoff_types_to_assign')
            }
        )

        trigger_timeoff_policy_assignments = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_policy_assignments',
            retries=0,
            items=lambda: rail.result('timeoff_types_to_assign'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_timeoff_policy_assignment_{config.instance}',
            conf={
                'useruri': '{{ dag_run.conf.useruri }}',
                'timeoff_type_uri': '{{ item }}'
            }
        )

        wait_for_trigger_timeoff_policy_assignments = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_timeoff_policy_assignments',
            dag_runs='{{ result("trigger_timeoff_policy_assignments") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_all_timeofftypes

        get_all_timeofftypes >> get_timeoff_from_mapper >> is_timeoff_not_exists

        is_timeoff_not_exists >> rail.Label(
            'Yes') >> log_dagrun_to_sumo

        is_timeoff_not_exists >> rail.Label(
            'No') >> timeoff_types_to_assign >> is_timeoff_types_to_assign

        is_timeoff_types_to_assign >> rail.Label(
            'Yes') >> process_timeoffs

        process_timeoffs >> is_action_add

        is_action_add >> rail.Label(
            'Yes') >> assign_timeoff_assignments

        is_action_add >> rail.Label(
            'No') >> get_user_timeoff_policy_summary >> get_final_timeoff_types_assign >> is_timeoff_types_to_update

        is_timeoff_types_to_update >> rail.Label(
            'Yes') >> assign_timeoff_assignments

        is_timeoff_types_to_update >> rail.Label(
            'No') >> log_dagrun_to_sumo

        assign_timeoff_assignments >> trigger_timeoff_policy_assignments >> wait_for_trigger_timeoff_policy_assignments >> \
            log_dagrun_to_sumo

        is_timeoff_types_to_assign >> rail.Label(
            'No') >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_timeoff_assignment_child_dag)
