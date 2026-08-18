
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'xencor_send_timeoffbalance_email_and_push_notification_processbyuser_child_{config.instance}',
        description=f'Xencor send timeoffbalance email and push notification_processbyuser - child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
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
            no_task='get_user_time_off_type_policy_summary_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_time_off_type_policy_summary_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_time_off_type_policy_summary_3 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_3',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.userrecords[0].useruri }}"
            }
        )

        declare_list_dag_runs_3 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_3',
            name='user_process_dag_runs',
            value=[]
        )

        foreach_request_6 = rail.ForEachOperator(
            task_id='foreach_request_6',
            items="{{ dag_run.conf.userrecords | to_json}}",
            start_task='foreach_d_7',
            end_task='foreach_request_6_end'
        )

        foreach_d_7 = rail.ForEachOperator(
            task_id='foreach_d_7',
            items="{{ result('get_user_time_off_type_policy_summary_3').policiesByTimeOffType | to_json }}",
            start_task='if_timeofftype_uri_equals_to_dataforeachforeach_request_6timeofftypeuri_8',
            end_task='foreach_d_7_end'
        )

        if_timeofftype_uri_equals_to_dataforeachforeach_request_6timeofftypeuri_8 = rail.IfOperator(
            task_id='if_timeofftype_uri_equals_to_dataforeachforeach_request_6timeofftypeuri_8',
            test='''{{ result('foreach_d_7').timeOffType.uri == result('foreach_request_6').timeofftypeuri }}''',
            yes_task="trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9",
            no_task="foreach_d_7_end",
        )

        trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9',
            retries=0,
            items=[-1],
            trigger_dag_id=f'xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userrecords": [{
                    "username": "{{ result('foreach_request_6').username }}",
                    "timeofftype": "{{ result('foreach_request_6').timeofftype }}",
                    "units": "{{ result('foreach_request_6').units }}",
                    "timeoffbalance": "{{ result('foreach_request_6').timeoffbalance }}",
                    "usersupervisorname": "{{ result('foreach_request_6').usersupervisorname }}",
                    "useruri": "{{ result('foreach_request_6').useruri }}",
                    "supervisoruri": "{{ result('foreach_request_6').supervisoruri }}",
                    "timeofftypeuri": "{{ result('foreach_request_6').timeofftypeuri }}",
                    "useremail": "{{ result('foreach_request_6').useremail }}",
                    "supervisoremail": "{{ result('foreach_request_6').supervisoremail }}"
                }],
                "policySetSchedule": "{{ result('foreach_d_7').policySetSchedule | to_json }}"
            }
        )

        insert_to_user_dag_run_list_9 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_9',
            append=True,
            name='{{ result("declare_list_dag_runs_3").name }}',
            value='{{(result("trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9"))[0]}}'
        )

        foreach_d_7_end = rail.EmptyOperator(
            task_id='foreach_d_7_end',
        )

        foreach_request_6_end = rail.EmptyOperator(
            task_id='foreach_request_6_end',
        )

        if_trigger_dag_run_available_9 = rail.IfOperator(
            task_id='if_trigger_dag_run_available_9',
            test='''{{ result('insert_to_user_dag_run_list_9') | is_truthy}}''',
            yes_task="wait_for_completion_trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9",
            no_task="stop_10",
        )

        wait_for_completion_trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_9").value | to_json }}'
        )

        stop_10 = rail.EmptyOperator(
            task_id='stop_10',

        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_user_time_off_type_policy_summary_3
        get_user_time_off_type_policy_summary_3 >> declare_list_dag_runs_3 >> foreach_request_6 >> foreach_d_7 >> \
            if_timeofftype_uri_equals_to_dataforeachforeach_request_6timeofftypeuri_8
        if_timeofftype_uri_equals_to_dataforeachforeach_request_6timeofftypeuri_8 >> rail.Label(
            'Yes') >> trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9 >> \
            insert_to_user_dag_run_list_9 >> foreach_d_7_end
        if_timeofftype_uri_equals_to_dataforeachforeach_request_6timeofftypeuri_8 >> rail.Label(
            'No') >> foreach_d_7_end
        foreach_d_7 >> foreach_d_7_end >> foreach_request_6_end
        foreach_request_6 >> foreach_request_6_end >> if_trigger_dag_run_available_9
        if_trigger_dag_run_available_9 >> rail.Label('Yes') >>\
            wait_for_completion_trigger_dag_run_xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_childasync_9 >> stop_10 >> log_to_sumo
        if_trigger_dag_run_available_9 >> rail.Label('No') >> stop_10

    return dag


rail.for_each_instance(create_dag)
