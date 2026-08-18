from datetime import timedelta
import json
from airflow.models import Variable
import rail

from crl.user_import_usa_v7.utils import request_payload, python_callable_methods, response_filter

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_type_special_accrual_dagid,
        description='CRL User Import USA- Process TIme Off Type Special Accrual',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_timeoff_type_no_accrual,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_contingent_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_contingent_user',
            end_task='catch_and_log_errors',
        )

        is_contingent_user = rail.IfOperator(
            task_id = "is_contingent_user",
            test=lambda dag_run: dag_run.conf['is_contingent'] == 'Y',
            yes_task="is_logging_required",
            no_task="get_all_time_off_types"
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_filtered_time_off_types
        )

        get_user_time_off_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_time_off_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler= response_filter.assigned_timeoffs_types_to_user
        )

        is_special_timeoff_type_assigned_to_user = rail.IfOperator(
            task_id = "is_special_timeoff_type_assigned_to_user",
            test=lambda dag_run: bool(list(filter(lambda x: x['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TO_TYPES,
                rail.result('get_user_time_off_policy_summary')))),
            yes_task="is_status_updaid_or_paid_leave",
            no_task="is_logging_required"
        )

        is_status_updaid_or_paid_leave = rail.IfOperator(
            task_id = "is_status_updaid_or_paid_leave",
            test=lambda dag_run: dag_run.conf['emp_status']=="Unpaid Leave" or dag_run.conf['emp_status']=="Paid Leave",
            yes_task="is_reason_available_in_mapper_and_code_10_in_payload",
            no_task="is_logging_required"
        )

        is_reason_available_in_mapper_and_code_10_in_payload = rail.IfOperator(
            task_id = "is_reason_available_in_mapper_and_code_10_in_payload",
            test=lambda dag_run:dag_run.conf['event_reason_code']=='10' and
                list(filter(lambda x: x['event']==dag_run.conf['event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)),
            yes_task="is_assigned_event_and_event_reason_code_empty",
            no_task="is_logging_required"
        )

        is_assigned_event_and_event_reason_code_empty = rail.IfOperator(
            task_id = "is_assigned_event_and_event_reason_code_empty",
            test=lambda dag_run:not dag_run.conf['assigned_event'] and not dag_run.conf['assigned_event_reason_code'],
            yes_task="time_off_type_to_be_updated",
            no_task="check_update_required"
        )

        check_update_required = rail.IfOperator(
            task_id = "check_update_required",
            test=lambda dag_run: python_callable_methods.check_special_timeoff_update_required(dag_run,config.SPECIAL_TIMEOFF_TYPES_ACCRUALS),
            yes_task="time_off_type_to_be_updated",
            no_task="is_logging_required"
        )

        time_off_type_to_be_updated = rail.PythonOperator(
            task_id='time_off_type_to_be_updated',
            python_callable=lambda dag_run:python_callable_methods.time_off_type_to_be_updated(dag_run, config)
        )

        is_time_off_type_to_be_updated = rail.IfOperator(
            task_id = "is_time_off_type_to_be_updated",
            test=lambda: bool(rail.result('time_off_type_to_be_updated')),
            yes_task="for_each_time_off_type_policy",
            no_task="is_logging_required"
        )

        for_each_time_off_type_policy = rail.ForEachOperator(
            task_id="for_each_time_off_type_policy",
            items=lambda: rail.result('time_off_type_to_be_updated'),
            start_task='get_balance_summary_for_user',
            end_task='for_each_time_off_policy_end'
        )

        get_balance_summary_for_user = rail.RepliconServiceOperator(
            task_id="get_balance_summary_for_user",
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run:{
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('for_each_time_off_type_policy')['time_off_type_uri']
                },
                "asOfDate": request_payload.get_replicon_date(dag_run.conf['change_effective_date'])
                }
        )

        get_historical_policy_to_assign_special_accrual = rail.PythonOperator(
            task_id='get_historical_policy_to_assign_special_accrual',
            python_callable=lambda dag_run: python_callable_methods.get_historical_policy_to_assign_special_accrual_list(
                dag_run,rail.result('for_each_time_off_type_policy')['time_off_type_uri'], config)
        )

        get_custom_policy_line = rail.PythonOperator(
            task_id='get_custom_policy_line',
            python_callable= lambda dag_run:python_callable_methods.get_custom_policy_line(dag_run, config)
        )

        get_all_policy_to_assign = rail.PythonOperator(
            task_id='get_all_policy_to_assign',
            python_callable=python_callable_methods.get_all_policy_to_assign_for_special_accrual
        )

        put_user_timeoff_policy_schedule_blank_policy = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy_schedule_blank_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
            "timeOffAccount": {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUri": rail.result('for_each_time_off_type_policy')['time_off_type_uri']
            },
            "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign'))
        }
        )

        for_each_time_off_policy_end = rail.EmptyOperator(
                task_id='for_each_time_off_policy_end'
            )

        is_logging_required = rail.IfOperator(
            task_id = "is_logging_required",
            test=lambda dag_run: dag_run.conf['emp_status']=="Unpaid Leave",
            yes_task="log_unpaid_leave_complete",
            no_task="catch_and_log_errors"
        )

        log_unpaid_leave_complete = rail.WriteLogOperator(
            task_id='log_unpaid_leave_complete',
            log="{{ dag_run.conf.user_log }}",
            severity='Success',
            message="User Updated",
            properties={
                'employee_id': '{{dag_run.conf.emp_id}}',
                'first_name': '{{dag_run.conf.first_name}}',
                'last_name': '{{dag_run.conf.last_name}}',
                'action': 'Update',
                'status': 'Success',
                'details': "User Updated",
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.user_log }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employee_id': '{{dag_run.conf.emp_id}}',
                'first_name': '{{dag_run.conf.first_name}}',
                'last_name': '{{dag_run.conf.last_name}}',
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_contingent_user

        is_contingent_user >> rail.Label('Yes') >> is_logging_required
        is_contingent_user >> rail.Label('No') >> get_all_time_off_types >> get_user_time_off_policy_summary >> is_special_timeoff_type_assigned_to_user
        is_special_timeoff_type_assigned_to_user >> rail.Label('No') >> is_logging_required
        is_special_timeoff_type_assigned_to_user >> rail.Label('Yes') >> is_status_updaid_or_paid_leave >> rail.Label('No') >> is_logging_required
        is_status_updaid_or_paid_leave >> rail.Label('Yes') >> is_reason_available_in_mapper_and_code_10_in_payload
        is_reason_available_in_mapper_and_code_10_in_payload >> rail.Label('Yes') >> is_assigned_event_and_event_reason_code_empty
        is_reason_available_in_mapper_and_code_10_in_payload >> rail.Label('No') >> is_logging_required

        is_assigned_event_and_event_reason_code_empty >> rail.Label('Yes') >> time_off_type_to_be_updated
        is_assigned_event_and_event_reason_code_empty >> rail.Label('No') >> check_update_required
        check_update_required >> rail.Label('Yes') >> time_off_type_to_be_updated >> is_time_off_type_to_be_updated
        check_update_required >> rail.Label('No') >> is_logging_required

        is_time_off_type_to_be_updated >> rail.Label('No') >> is_logging_required
        is_time_off_type_to_be_updated >> rail.Label('Yes') >> for_each_time_off_type_policy
        for_each_time_off_type_policy >> for_each_time_off_policy_end

        for_each_time_off_type_policy >> get_balance_summary_for_user >> get_historical_policy_to_assign_special_accrual
        get_historical_policy_to_assign_special_accrual >> get_custom_policy_line >> get_all_policy_to_assign >> put_user_timeoff_policy_schedule_blank_policy
        put_user_timeoff_policy_schedule_blank_policy >> for_each_time_off_policy_end >> is_logging_required

        is_logging_required >> rail.Label("Yes") >> log_unpaid_leave_complete >> catch_and_log_errors
        is_logging_required >> rail.Label("No") >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
