import rail

from crl.user_import_canada_v5.utils import request_payload, python_callable_methods, response_filter

null = None

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_future_enddate_user_child_dagid,
        description='CRL User Import Disable User Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disable_user_child_dag_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            }
        )

        is_status_ignore_zero_accrual = rail.IfOperator(
            task_id = "is_status_ignore_zero_accrual",
            test=lambda dag_run: dag_run.conf['emp_status'] in config.IGNORE_STATUS_ZERO_ACCRUAL,
            yes_task="catch_disable_user_error",
            no_task="is_contingent_or_non_live_user"
        )

        is_contingent_or_non_live_user = rail.IfOperator(
            task_id = "is_contingent_or_non_live_user",
            test=lambda dag_run: dag_run.conf['employee_type']=="Contingent Worker" or
                dag_run.conf['location'] not in ['USA','CAN'],
            yes_task="catch_disable_user_error",
            no_task="get_all_time_off_types"
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_filtered_time_off_types
        )

        get_user_time_off_policy_summary= rail.RepliconServiceOperator(
            task_id="get_user_time_off_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler= lambda response,dag_run: response_filter.assigned_time_offs_types_to_user(response,dag_run,config.MANNUAL_TIMEOFF_TYPES)
        )

        for_each_time_off_type_no_accural = rail.ForEachOperator(
            task_id="for_each_time_off_type_no_accural",
            items=lambda: rail.result('get_user_time_off_policy_summary'),
            start_task='get_balance_summary_for_user',
            end_task='for_each_time_off_type_no_accural_end'
        )

        get_balance_summary_for_user = rail.RepliconServiceOperator(
            task_id="get_balance_summary_for_user",
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run:{
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('for_each_time_off_type_no_accural')['timeoff_type_uri']
                },
                "asOfDate": request_payload.get_replicon_date(dag_run.conf['end_date'])
                }
        )

        is_sick_timeoff_type = rail.IfOperator(
            task_id='is_sick_timeoff_type',
            test=lambda: rail.result("for_each_time_off_type_no_accural")['timeoff_type_name'] == "[USA] Sick",
            yes_task='get_termination_policy_for_sick_timeoff_Type',
            no_task='get_historical_policy_to_assign_list_disable_user'
        )

        get_termination_policy_for_sick_timeoff_Type = rail.RepliconServiceOperator(
            task_id="get_termination_policy_for_sick_timeoff_Type",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda:{
                "timeOffTypeUri":rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
                    'timeoff_type_name',"[USA] Sick Termination","timeoff_type_uri")
            },
            data_handler=lambda response, dag_run: response_filter.get_termination_policyset_sick_timeoff_type(
                response,dag_run,"disable")
        )

        get_historical_policy_to_assign_list_disable_user = rail.PythonOperator(
            task_id='get_historical_policy_to_assign_list_disable_user',
            python_callable=lambda dag_run: python_callable_methods.get_historical_policy_to_assign_list(
                dag_run,'disable','for_each_time_off_type_no_accural', config)
        )

        get_no_accrual_policy_line = rail.PythonOperator(
            task_id='get_no_accrual_policy_line',
            python_callable=lambda dag_run: python_callable_methods.get_no_accrual_policy_line(dag_run, 'disable')
        )

        get_all_policy_to_assign_for_disable_user = rail.PythonOperator(
            task_id='get_all_policy_to_assign_for_disable_user',
            python_callable=python_callable_methods.get_all_policy_to_assign_for_disable_user
        )

        put_user_timeoff_policy_schedule_blank_policy = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy_schedule_blank_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.put_user_timeoff_policy_schedule_blank_policy
        )

        for_each_time_off_type_no_accural_end = rail.EmptyOperator(
            task_id='for_each_time_off_type_no_accural_end'
        )

        catch_disable_user_error = rail.PythonOperator(
            task_id='catch_disable_user_error',
            trigger_rule='one_failed',
            python_callable=lambda dag_run: {
                'user': dag_run.conf['user'],
                'useruri': dag_run.conf['uri'],
                'enddate': dag_run.conf['enddate'],
                'error': rail.result('disable_user', key='error')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'user': '{{ dag_run.conf.user }}',
                'useruri': '{{ dag_run.conf.uri }}',
                'enddate': '{{ dag_run.conf.enddate }}',
                'error': '{{ get_error_message() }}'
            }
        )

        disable_user >> is_status_ignore_zero_accrual >> rail.Label('Yes') >> catch_disable_user_error
        is_status_ignore_zero_accrual >> rail.Label('No') >> is_contingent_or_non_live_user
        is_contingent_or_non_live_user >> rail.Label('Yes') >> catch_disable_user_error
        is_contingent_or_non_live_user >> rail.Label('No') >> get_all_time_off_types >> get_user_time_off_policy_summary
        get_user_time_off_policy_summary >> for_each_time_off_type_no_accural
        for_each_time_off_type_no_accural >> get_balance_summary_for_user >> is_sick_timeoff_type
        is_sick_timeoff_type >>  rail.Label("Yes") >> get_termination_policy_for_sick_timeoff_Type >> get_historical_policy_to_assign_list_disable_user
        is_sick_timeoff_type >> rail.Label("No") >> get_historical_policy_to_assign_list_disable_user
        get_historical_policy_to_assign_list_disable_user >> get_no_accrual_policy_line >> get_all_policy_to_assign_for_disable_user
        get_all_policy_to_assign_for_disable_user >> put_user_timeoff_policy_schedule_blank_policy >> for_each_time_off_type_no_accural_end
        for_each_time_off_type_no_accural_end >> catch_disable_user_error

        for_each_time_off_type_no_accural >> for_each_time_off_type_no_accural_end

        catch_disable_user_error >> dagrun_log_to_sumo



    return dag


rail.for_each_instance(create_child_dag_wbs)
