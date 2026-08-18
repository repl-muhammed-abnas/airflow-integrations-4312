from datetime import timedelta
from airflow.models import Variable
import rail
from momentive.user_import_south_korea.utils import python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_policy_assignment_rehire_update_days_child_{config.instance}',
        description=f'momentive_userimport_policy_assignment_rehire_update_days_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.policy_assignment_rehire_update_days_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_existingpolicy_schedule_for_timeoff'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existingpolicy_schedule_for_timeoff',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_existingpolicy_schedule_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_existingpolicy_schedule_for_timeoff',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        create_policyset=rail.SetVariableOperator(
            task_id='create_policyset',
            append=False,
            name='policyset',
            value=[]
        )

        is_existingpolicy_schedule = rail.IfOperator(
            task_id='is_existingpolicy_schedule',
            test="{{ result('get_existingpolicy_schedule_for_timeoff') | is_truthy }}",
            yes_task="past_policyset_schedule",
            no_task="get_final_policysets_schedule",
        )

        past_policyset_schedule = rail.PythonOperator(
            task_id='past_policyset_schedule',
            python_callable=python_callable.construct_policyschedule,
            op_args=['{{ dag_run.conf.startdate }}']
        )

        update_policyset = rail.SetVariableOperator(
            task_id='update_policyset',
            append=False,
            name='{{ result("create_policyset").name }}',
            value=lambda: rail.result('past_policyset_schedule')
        )

        get_final_policysets_schedule = rail.RepliconServiceOperator(
            task_id='get_final_policysets_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            },
            data_handler=python_callable.get_final_policysets
        )

        put_user_timeoff_account_policysetschedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policysetschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('get_final_policysets_schedule')
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "details":"{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_existingpolicy_schedule_for_timeoff

        get_existingpolicy_schedule_for_timeoff >> create_policyset >> is_existingpolicy_schedule

        is_existingpolicy_schedule >> rail.Label('Yes') >> past_policyset_schedule >> update_policyset >> get_final_policysets_schedule
        is_existingpolicy_schedule >> rail.Label('No') >> get_final_policysets_schedule

        get_final_policysets_schedule >> put_user_timeoff_account_policysetschedule >> catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
