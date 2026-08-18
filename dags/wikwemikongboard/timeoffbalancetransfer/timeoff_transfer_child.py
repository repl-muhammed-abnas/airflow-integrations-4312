import rail

from wikwemikongboard.timeoffbalancetransfer.utils import request_payload,response_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_child_dag_id,
        description=f"Timeoff Transfer Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_batch_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_log_artifact = rail.CreateLogOperator(
            task_id='create_log_artifact'
        )

        is_timeoff_combination_present = rail.IfOperator(
            task_id='is_timeoff_combination_present',
            test=request_payload.get_timeoff_combination,
            yes_task='get_user_timeoff_types',
            no_task='timeoff_combination_not_present'
        )

        get_user_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_user_timeoff_types",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_timeoff_types,
            data_handler=response_payload.filter_timeoff_types_sick
        )

        is_policy_not_present = rail.IfOperator(
            task_id='is_policy_not_present',
            test=lambda: request_payload.get_policy_present(
                rail.result('get_user_timeoff_types')),
            yes_task='get_timeoff_payload',
            no_task='log_policy_already_present'
        )

        get_timeoff_payload = rail.PythonOperator(
            task_id="get_timeoff_payload",
            python_callable=lambda dag_run: request_payload.get_final_payload(
                rail.result('get_user_timeoff_types'), dag_run)
        )

        put_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_user_timeoff_policy
        )

        timeoff_transfered_success = rail.WriteLogOperator(
            task_id='timeoff_transfered_success',
            log="{{ result('create_log_artifact') }}",
            message="success",
            severity='success',
            properties={
                'loginname': "{{dag_run.conf.loginName}}",
                'assignetimeoffs': "{{dag_run.conf.Assignedtimeoffs}}",
                'personalbalance': "{{ dag_run.conf.personalleavebalance[0] if dag_run.conf.personalleavebalance else '' }}",
                'sickleaveannualbalance': "{{ dag_run.conf.sickleaveannualbalance[0] if dag_run.conf.sickleaveannualbalance else '' }}",
                'sickleavebankedbalance': "{{ dag_run.conf.sickleavebankedbalance[0] if dag_run.conf.sickleavebankedbalance else '' }}",
                'status': 'success',
                'details': "Timeoff transfered successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log_artifact') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'loginname': "{{dag_run.conf.loginName}}",
                'assignetimeoffs': "{{dag_run.conf.Assignedtimeoffs}}",
                'personalbalance': "{{ dag_run.conf.personalleavebalance[0] if dag_run.conf.personalleavebalance else '' }}",
                'sickleaveannualbalance': "{{ dag_run.conf.sickleaveannualbalance[0] if dag_run.conf.sickleaveannualbalance else '' }}",
                'sickleavebankedbalance': "{{ dag_run.conf.sickleavebankedbalance[0] if dag_run.conf.sickleavebankedbalance else '' }}",
                'status': 'error',
                'details' : "{{ get_error_message() }}"
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        timeoff_combination_not_present = rail.WriteLogOperator(
            task_id='timeoff_combination_not_present',
            log="{{ result('create_log_artifact') }}",
            message="The assigned timeoff types combination is not valid",
            severity='ignored',
            properties={
                'loginname': "{{dag_run.conf.loginName}}",
                'assignetimeoffs': "{{dag_run.conf.Assignedtimeoffs}}",
                'personalbalance': "{{ dag_run.conf.personalleavebalance[0] if dag_run.conf.personalleavebalance else '' }}",
                'sickleaveannualbalance': "{{ dag_run.conf.sickleaveannualbalance[0] if dag_run.conf.sickleaveannualbalance else '' }}",
                'sickleavebankedbalance': "{{ dag_run.conf.sickleavebankedbalance[0] if dag_run.conf.sickleavebankedbalance else '' }}",
                'status': 'ignored',
                'details': "The assigned timeoff types combination is not valid."
            }
        )

        log_policy_already_present = rail.WriteLogOperator(
            task_id='log_policy_already_present',
            log="{{ result('create_log_artifact') }}",
            message="Time Off type already has a policy line for the current year",
            severity='ignored',
            properties={
                'loginname': "{{dag_run.conf.loginName}}",
                'assignetimeoffs': "{{dag_run.conf.Assignedtimeoffs}}",
                'personalbalance': "{{ dag_run.conf.personalleavebalance[0] if dag_run.conf.personalleavebalance else '' }}",
                'sickleaveannualbalance': "{{ dag_run.conf.sickleaveannualbalance[0] if dag_run.conf.sickleaveannualbalance else '' }}",
                'sickleavebankedbalance': "{{ dag_run.conf.sickleavebankedbalance[0] if dag_run.conf.sickleavebankedbalance else '' }}",
                'status': 'ignored',
                'details': "Time Off type already has a policy line for the current year"
            }
        )

        create_log_artifact >> is_timeoff_combination_present

        is_timeoff_combination_present >> rail.Label(
            "Yes") >> get_user_timeoff_types >> is_policy_not_present >> rail.Label("Yes") >> get_timeoff_payload >> put_user_timeoff_policy\
                  >> timeoff_transfered_success >> catch_and_log_errors >> log_to_sumo

        is_policy_not_present >> rail.Label(
            "No") >> log_policy_already_present >> catch_and_log_errors >> log_to_sumo

        is_timeoff_combination_present >> rail.Label(
            "No") >> timeoff_combination_not_present >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
