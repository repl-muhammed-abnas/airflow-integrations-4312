import rail
from strayeruniversity.user_sync_v4.utils.request_payload import get_put_timeoffpolicywithinitialbalance
from strayeruniversity.user_sync_v4.utils.python_callable import construct_policyschedule


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_assign_0_balance_timeoff_dag_id,
        description=f'strayeruniversity_usersync_assign_0_balance_timeoff_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.assign_balance_timeoff_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_existingpolicy_schedule_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_existingpolicy_schedule_for_timeoff',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri']['uri'], 'policySetSchedule', '')
        )

        is_first_description_present = rail.IfOperator(
            task_id='is_first_description_present',
            test="{{ result('get_existingpolicy_schedule_for_timeoff') | first_or_default(default='') | \
                is_truthy and result('get_existingpolicy_schedule_for_timeoff') | first_or_default | \
                attr_or_default('description') | is_truthy }}",
            yes_task="past_policyset_schedule",
            no_task="catch_and_log_error",
        )

        past_policyset_schedule = rail.PythonOperator(
            task_id='past_policyset_schedule',
            python_callable=construct_policyschedule
        )

        put_timeoffpolicy_with_initial_balance_as_0 = rail.RepliconServiceOperator(
            task_id='put_timeoffpolicy_with_initial_balance_as_0',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_timeoffpolicywithinitialbalance
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Assign Balance Timeoff",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        get_existingpolicy_schedule_for_timeoff >> is_first_description_present

        is_first_description_present >> rail.Label(
            'Yes') >> past_policyset_schedule >> put_timeoffpolicy_with_initial_balance_as_0 >> catch_and_log_error
        is_first_description_present >> rail.Label('No') >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
