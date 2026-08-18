from datetime import timedelta
from airflow.models import Variable
import rail

from incyte_biosciences_international_sarl.user_import_v1.utils import request_payload, python_callable_methods
from incyte_biosciences_international_sarl.user_import_v1.utils import response_filter

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_type_assignment_update_rehire_user_dagid,
        description='IBIS - User Import - Process TIme Off Type Assignment- Update/Rehire',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_type_assignment_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_time_off_types'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_all_time_off_types',
            end_task='catch_and_log_errors',
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_filtered_time_off_types
        )

        get_enabled_time_off_types = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetEnabledTimeOffTypes',
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

        is_rehire_user = rail.IfOperator(
            task_id="is_rehire_user",
            test=lambda dag_run: bool(dag_run.conf['action']=='rehire'),
            yes_task="time_off_types_to_be_assigned",
            no_task="is_country_changed"
        )

        is_country_changed = rail.IfOperator(
            task_id="is_country_changed",
            test=lambda dag_run: dag_run.conf['assigned_country_uri']!=dag_run.conf['new_country_uri'],
            yes_task="assigned_time_offs_types",
            no_task="catch_and_log_errors"
        )

        assigned_time_offs_types = rail.PythonOperator(
            task_id='assigned_time_offs_types',
            python_callable=python_callable_methods.assigned_time_offs_types
        )

        is_compensation_day_timeoff_type_assigned = rail.IfOperator(
            task_id="is_compensation_day_timeoff_type_assigned",
            test=lambda : bool(list(filter(lambda x: x['timeoff_type_name']=="Compensation Day", rail.result("assigned_time_offs_types")))),
            yes_task="get_historical_policy_compensation_day_update",
            no_task="put_timeoff_assignment_for_update"
        )

        put_timeoff_assignment_for_update = rail.RepliconServiceOperator(
            task_id="put_timeoff_assignment_for_update",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_assignment_for_user_update
        )

        get_historical_policy_compensation_day_update = rail.PythonOperator(
            task_id='get_historical_policy_compensation_day_update',
            python_callable=python_callable_methods.get_historical_policy_compensation_day_update
        )

        get_time_off_policy_schedule_compensation_day_update= rail.RepliconServiceOperator(
            task_id="get_time_off_policy_schedule_compensation_day_update",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run:request_payload.get_timeoff_policy_schedule_update_payload(dag_run,config),
            data_handler=response_filter.get_policy_to_assign
        )

        get_all_policy_to_assign_compensation_day_update = rail.PythonOperator(
            task_id='get_all_policy_to_assign_compensation_day_update',
            python_callable=python_callable_methods.get_all_policy_to_assign_compensation_day_update
        )

        is_policy_to_assign_present = rail.IfOperator(
            task_id='is_policy_to_assign_present',
            test=lambda: bool(rail.result('get_all_policy_to_assign_compensation_day_update')),
            yes_task='put_user_timeoff_policy_update',
            no_task='no_timeoff_policy_line_available'
        )

        put_user_timeoff_policy_update = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy_update",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_update_user_timeoff_policy_payload
        )

        no_timeoff_policy_line_available = rail.EmptyOperator(
            task_id='no_timeoff_policy_line_available'
        )

        time_off_types_to_be_assigned = rail.PythonOperator(
            task_id='time_off_types_to_be_assigned',
            python_callable=python_callable_methods.time_off_types_to_be_assigned
        )

        put_timeoff_assignment_for_rehire = rail.RepliconServiceOperator(
            task_id="put_timeoff_assignment_for_rehire",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_assignment_for_user
        )

        for_each_time_off_type_policy = rail.ForEachOperator(
            task_id="for_each_time_off_type_policy",
            items=lambda: rail.result('time_off_types_to_be_assigned'),
            start_task='is_compensation_day_timeoff_type',
            end_task='for_each_time_off_type_policy_end'
        )

        is_compensation_day_timeoff_type = rail.IfOperator(
            task_id='is_compensation_day_timeoff_type',
            test=lambda : rail.result('for_each_time_off_type_policy')['timeoff_type_name'] == "Compensation Day",
            yes_task='get_historical_policy_compensation_day_rehire',
            no_task='get_default_time_off_policy_schedule'
        )

        get_historical_policy_compensation_day_rehire = rail.PythonOperator(
            task_id='get_historical_policy_compensation_day_rehire',
            python_callable=python_callable_methods.get_historical_policy_to_assign_list
        )

        get_custom_time_off_policy_schedule_compensation_day_rehire= rail.RepliconServiceOperator(
            task_id="get_custom_time_off_policy_schedule_compensation_day_rehire",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run:request_payload.get_custom_timeoff_policy_schedule_payload(dag_run,config),
            data_handler=response_filter.get_policy_to_assign
        )

        get_all_policy_to_assign_compensation_day_rehire = rail.PythonOperator(
            task_id='get_all_policy_to_assign_compensation_day_rehire',
            python_callable=python_callable_methods.get_all_policy_to_assign_compensation_day_rehire
        )

        get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
            task_id="get_default_time_off_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run:request_payload.get_default_timeoff_policy_schedule_payload(dag_run,'for_each_time_off_type_policy'),
            data_handler=response_filter.get_policy_to_assign
        )

        put_user_timeoff_policy_rehire = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy_rehire",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_user_timeoff_policy_rehire
        )

        for_each_time_off_type_policy_end = rail.EmptyOperator(
            task_id='for_each_time_off_type_policy_end'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.user_log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'login_name': '{{dag_run.conf.login_name}}',
                'first_name': '{{dag_run.conf.first_name}}',
                'last_name': '{{dag_run.conf.last_name}}',
                'action': '{{"Update" if dag_run.conf.action != "rehire" else "Rehire" }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_all_time_off_types

        get_all_time_off_types >> get_enabled_time_off_types >> get_user_time_off_policy_summary >> is_rehire_user

        is_rehire_user >> rail.Label('No') >> is_country_changed >> rail.Label('No') >> catch_and_log_errors
        is_country_changed >> rail.Label('Yes') >> assigned_time_offs_types >> is_compensation_day_timeoff_type_assigned

        is_compensation_day_timeoff_type_assigned >> rail.Label('Yes') >> get_historical_policy_compensation_day_update
        is_compensation_day_timeoff_type_assigned >> rail.Label('No') >> put_timeoff_assignment_for_update
        put_timeoff_assignment_for_update >> get_historical_policy_compensation_day_update
        get_historical_policy_compensation_day_update >> get_time_off_policy_schedule_compensation_day_update
        get_time_off_policy_schedule_compensation_day_update >> get_all_policy_to_assign_compensation_day_update >> is_policy_to_assign_present
        is_policy_to_assign_present >> rail.Label('No') >> no_timeoff_policy_line_available >> catch_and_log_errors
        is_policy_to_assign_present >> rail.Label('Yes') >> put_user_timeoff_policy_update >> catch_and_log_errors

        is_rehire_user >> rail.Label('Yes') >> time_off_types_to_be_assigned >> put_timeoff_assignment_for_rehire >> for_each_time_off_type_policy
        for_each_time_off_type_policy >> for_each_time_off_type_policy_end
        for_each_time_off_type_policy >> is_compensation_day_timeoff_type >> rail.Label("No") >> get_default_time_off_policy_schedule
        get_default_time_off_policy_schedule >> put_user_timeoff_policy_rehire
        is_compensation_day_timeoff_type >> rail.Label("Yes") >> get_historical_policy_compensation_day_rehire
        get_historical_policy_compensation_day_rehire >> get_custom_time_off_policy_schedule_compensation_day_rehire
        get_custom_time_off_policy_schedule_compensation_day_rehire >> get_all_policy_to_assign_compensation_day_rehire
        get_all_policy_to_assign_compensation_day_rehire >> put_user_timeoff_policy_rehire >> for_each_time_off_type_policy_end

        for_each_time_off_type_policy_end >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
