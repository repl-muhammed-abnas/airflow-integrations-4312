from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_new_user_timeoff_type_proration_assignment_keenan_non_ca_h_keenan_non_ca_ex_dag_id,
        description=f'Assured Partners User Import new user Timeoff Type proration assignment Keenan Non-CA H| Keenan Non-CA EX Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='combined_initial_tasks'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='combined_initial_tasks',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        combined_initial_tasks = rail.PythonOperator(
            task_id='combined_initial_tasks',
            python_callable=lambda dag_run: python_callable.timeoff_proration_assignment_initial_tasks(
                dag_run, config)
        )

        log_hoursday_5 = rail.PythonOperator(
            task_id='log_hoursday_5',
            python_callable=lambda dag_run:  float(dag_run.conf['weekly_scheduled_hours']) / float(
                rail.result('combined_initial_tasks')['number_of_working_days_in_week'])
        )

        get_defaultpolicyfromgloballevel_11 = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel_11',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        get_policies_to_be_assigned_and_max_min_offsets = rail.PythonOperator(
            task_id='get_policies_to_be_assigned_and_max_min_offsets',
            python_callable=lambda dag_run: python_callable.policies_to_be_assigned(
                rail.result('get_defaultpolicyfromgloballevel_11'), dag_run)
        )

        policy_set_list_part_1 = rail.PythonOperator(
            task_id='policy_set_list_part_1',
            python_callable=lambda dag_run: python_callable.get_policy_set_list_1(rail.result(
                "combined_initial_tasks")['time_off_policy_mapper_search_entries'], rail.result(
                "log_hoursday_5"), rail.result('get_policies_to_be_assigned_and_max_min_offsets'), rail.result('get_defaultpolicyfromgloballevel_11'), dag_run)
        )

        final_policy_set_list = rail.PythonOperator(
            task_id='final_policy_set_list',
            python_callable=lambda dag_run: python_callable.get_final_policyset_list(rail.result("policy_set_list_part_1"), rail.result(
                "combined_initial_tasks")['time_off_policy_mapper_search_entries'], rail.result(
                "log_hoursday_5"), rail.result('get_policies_to_be_assigned_and_max_min_offsets'), rail.result('get_defaultpolicyfromgloballevel_11'), dag_run)
        )

        assign_time_offpolicy_96 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_96',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('final_policy_set_list')
            }
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            name='response_from_dag',
            append=False,
            value="Error in Timeoff Assignment - Keenan Non-CA H| Keenan Non-CA EX : {{get_error_message()}}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.get_dag_run_var(
                "response_from_dag") if rail.result('catch_and_log_error') else ""
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label(
            'No') >> combined_initial_tasks

        combined_initial_tasks >> log_hoursday_5 >> get_defaultpolicyfromgloballevel_11 >> get_policies_to_be_assigned_and_max_min_offsets \
            >> policy_set_list_part_1 >> final_policy_set_list >> assign_time_offpolicy_96 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
