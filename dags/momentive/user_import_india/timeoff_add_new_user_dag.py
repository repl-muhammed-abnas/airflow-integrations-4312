from datetime import timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_india.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_india_user_sync_child_add_timeoff_new_user_dag_id,
        description=f'Momentive_user_sync_Timeoff_add_new_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
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
            no_task='get_dates_and_accruals'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_dates_and_accruals',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_dates_and_accruals = rail.PythonOperator(
            task_id='get_dates_and_accruals',
            python_callable=python_callable.initial_date_tasks
        )

        get_enabled_timeoff_types_11 = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types_11',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        def get_timeoff_types_names_list(dag_run):
            return [item.strip() for item in (dag_run.conf['timeofftypes'].split("|"))]

        declare_list_12 = rail.SetVariableOperator(
            task_id='declare_list_12',
            append=False,
            name='timeofftypenameslist',
            value=get_timeoff_types_names_list
        )

        def get_timeoff_type_uri_list():
            return [{
                "name": item,
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_timeoff_types_11'), 'displayText', item, 'uri')
            }for item in rail.get_dag_run_var('timeofftypenameslist')] if rail.get_dag_run_var('timeofftypenameslist') else null

        declare_list_13 = rail.SetVariableOperator(
            task_id='declare_list_13',
            append=False,
            name='timeofftypeuri',
            value=get_timeoff_type_uri_list
        )

        final_list_of_timeoff_uris_to_be_assigned = rail.PythonOperator(
            task_id='final_list_of_timeoff_uris_to_be_assigned',
            python_callable=lambda: [item['uri']
                                     for item in rail.get_dag_run_var('timeofftypeuri')]
        )

        assignrequired_timeofftypes_22 = rail.RepliconServiceOperator(
            task_id='assignrequired_timeofftypes_22',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('final_list_of_timeoff_uris_to_be_assigned')
            }
        )

        foreach_declare_list_13_23 = rail.ForEachOperator(
            task_id='foreach_declare_list_13_23',
            items=lambda: rail.get_dag_run_var('timeofftypeuri'),
            start_task='if_foreach_list_13_23_name_equals_to_9ind_optionalholiday_24',
            end_task='foreach_declare_list_13_23_end'
        )

        if_foreach_list_13_23_name_equals_to_9ind_optionalholiday_24 = rail.IfOperator(
            task_id='if_foreach_list_13_23_name_equals_to_9ind_optionalholiday_24',
            test='''{{ result('foreach_declare_list_13_23').name == '9. IND_Optional Holiday' }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_26",
            no_task="if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33",
        )

        get_default_time_off_type_policy_schedule_for_user_26 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_26',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_13_23').uri }}"
                }
            }
        )

        log_defaultpolicy_27 = rail.PythonOperator(
            task_id='log_defaultpolicy_27',
            python_callable=lambda: rail.result(
                'get_default_time_off_type_policy_schedule_for_user_26') or null
        )

        if_log_defaultpolicy_27_present_28 = rail.IfOperator(
            task_id='if_log_defaultpolicy_27_present_28',
            test='''{{ result('log_defaultpolicy_27') | is_truthy }}''',
            yes_task="log_policytobeassigned_29",
            no_task="if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33",
        )

        log_policytobeassigned_29 = rail.PythonOperator(
            task_id='log_policytobeassigned_29',
            python_callable=lambda:  json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_26')).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"'))
        )

        put_user_time_off_account_policy_set_schedule_30 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_30',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_declare_list_13_23')['uri']
                },
                "policySetScheduleEntries": rail.result('log_policytobeassigned_29')
            }
        )

        if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33 = rail.IfOperator(
            task_id='if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33',
            test='''{{ result('foreach_declare_list_13_23').name == '1. IND_Privilege leave' }}''',
            yes_task="log_daystobeaccruedstartingbalance_35",
            no_task="if_foreach_list_13_23_equals_to_2ind_casualleave_79",
        )

        log_daystobeaccruedstartingbalance_35 = rail.PythonOperator(
            task_id='log_daystobeaccruedstartingbalance_35',
            python_callable=lambda:  round(float(rail.result(
                'get_dates_and_accruals')['log_1_i_n_d_privilege_leaveaccrualcalculation_8'] * rail.result(
                    'get_dates_and_accruals')['log_numberofdaystobeconsideredforaccrual_7']), 2)
        )

        log_decimalpointvalue_split_37 = rail.PythonOperator(
            task_id='log_decimalpointvalue_split_37',
            python_callable=lambda:  python_callable.decimal_number_split(
                rail.result('log_daystobeaccruedstartingbalance_35'))
        )

        def final_value_after_considering_decimal_count(decimal_value):
            if decimal_value['count_of_decimal'] == 1:
                return str(decimal_value['decimal_part']) + "0"
            return str(decimal_value['decimal_part'])

        log_finalvalueafterdecimaltobeconsidered_44 = rail.PythonOperator(
            task_id='log_finalvalueafterdecimaltobeconsidered_44',
            python_callable=lambda: final_value_after_considering_decimal_count(
                rail.result('log_decimalpointvalue_split_37'))
        )

        final_calculated_starting_balance_amount_to_put_54 = rail.PythonOperator(
            task_id='final_calculated_starting_balance_amount_to_put_54',
            python_callable=lambda: python_callable.get_variable_accrual_amount(
                rail.result('log_finalvalueafterdecimaltobeconsidered_44'), rail.result("log_decimalpointvalue_split_37"))
        )

        get_default_time_off_type_policy_schedule_for_user_55 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_55',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_13_23').uri }}"
                }
            }
        )

        if_effectivedate_day_present_58 = rail.IfOperator(
            task_id='if_effectivedate_day_present_58',
            test='''{{ result('get_default_time_off_type_policy_schedule_for_user_55') | is_truthy }}''',
            yes_task="log_finalpolicyset_schedule_74",
            no_task="if_foreach_list_13_23_equals_to_2ind_casualleave_79",
        )

        log_finalpolicyset_schedule_74 = rail.PythonOperator(
            task_id='log_finalpolicyset_schedule_74',
            python_callable=lambda:  python_callable.get_modified_policyset_schedule(rail.result(
                "get_default_time_off_type_policy_schedule_for_user_55"), rail.result("final_calculated_starting_balance_amount_to_put_54"), rail.result(
                    'get_dates_and_accruals')['start_date_split'])
        )

        put_user_time_off_account_policy_set_schedule_76 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_76',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_declare_list_13_23')['uri']
                },
                "policySetScheduleEntries": rail.result('log_finalpolicyset_schedule_74')
            }
        )

        if_foreach_list_13_23_equals_to_2ind_casualleave_79 = rail.IfOperator(
            task_id='if_foreach_list_13_23_equals_to_2ind_casualleave_79',
            test='''{{ result('foreach_declare_list_13_23').name == '2. IND_Casual Leave' }}''',
            yes_task="log_daystobeaccruedstartingbalance_81",
            no_task="foreach_declare_list_13_23_end",
        )

        log_daystobeaccruedstartingbalance_81 = rail.PythonOperator(
            task_id='log_daystobeaccruedstartingbalance_81',
            python_callable=lambda:  str(round(float(rail.result(
                'get_dates_and_accruals')['log_2_ind_casual_leave_accrual_calculation_9'] * rail.result(
                    'get_dates_and_accruals')['log_numberofdaystobeconsideredforaccrual_7']), 2))
        )

        log_decimalnumber_values_83_85 = rail.PythonOperator(
            task_id='log_decimalnumber_values_83_85',
            python_callable=lambda: python_callable.decimal_number_split(
                rail.result('log_daystobeaccruedstartingbalance_81'))
        )

        def final_value_after_considering_decimal_count(decimal_value):
            if decimal_value['count_of_decimal'] == 1:
                return str(decimal_value['decimal_part']) + "0"
            return str(decimal_value['decimal_part'])

        log_finalvalueafterdecimaltobeconsidered_90 = rail.PythonOperator(
            task_id='log_finalvalueafterdecimaltobeconsidered_90',
            python_callable=lambda: final_value_after_considering_decimal_count(
                rail.result('log_decimalnumber_values_83_85'))
        )

        final_calculated_starting_balance_casual_leave_92 = rail.PythonOperator(
            task_id='final_calculated_starting_balance_casual_leave_92',
            python_callable=lambda: python_callable.get_variable_accrual_amount(
                rail.result('log_finalvalueafterdecimaltobeconsidered_90'), rail.result("log_decimalnumber_values_83_85"))
        )

        get_default_time_off_type_policy_schedule_for_user_101 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_101',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_13_23').uri }}"
                }
            }
        )

        if_effectivedate_day_present_104 = rail.IfOperator(
            task_id='if_effectivedate_day_present_104',
            test='''{{ result('get_default_time_off_type_policy_schedule_for_user_101') | is_truthy }}''',
            yes_task="log_finalpolicy_120",
            no_task="foreach_declare_list_13_23_end",
        )

        log_finalpolicy_120 = rail.PythonOperator(
            task_id='log_finalpolicy_120',
            python_callable=lambda:  python_callable.get_modified_policyset_schedule(rail.result(
                "get_default_time_off_type_policy_schedule_for_user_101"), rail.result("final_calculated_starting_balance_casual_leave_92"), rail.result(
                    'get_dates_and_accruals')['start_date_split'])
        )

        put_user_time_off_account_policy_set_schedule_122 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_122',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_declare_list_13_23')['uri']
                },
                "policySetScheduleEntries": rail.result('log_finalpolicy_120')
            }
        )

        foreach_declare_list_13_23_end = rail.EmptyOperator(
            task_id='foreach_declare_list_13_23_end',
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in timeoff add for new user ; {{get_error_message()}}")
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> get_dates_and_accruals

        get_dates_and_accruals >> get_enabled_timeoff_types_11 >> declare_list_12 >> declare_list_13 \
            >> final_list_of_timeoff_uris_to_be_assigned >> assignrequired_timeofftypes_22 >> foreach_declare_list_13_23

        foreach_declare_list_13_23 >> if_foreach_list_13_23_name_equals_to_9ind_optionalholiday_24

        if_foreach_list_13_23_name_equals_to_9ind_optionalholiday_24 >> rail.Label(
            'No') >> if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33
        if_foreach_list_13_23_name_equals_to_9ind_optionalholiday_24 >> rail.Label('Yes') >> get_default_time_off_type_policy_schedule_for_user_26 \
            >> log_defaultpolicy_27 >> if_log_defaultpolicy_27_present_28

        if_log_defaultpolicy_27_present_28 >> rail.Label('Yes') >> log_policytobeassigned_29 >> put_user_time_off_account_policy_set_schedule_30 \
            >> if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33
        if_log_defaultpolicy_27_present_28 >> rail.Label(
            'No') >> if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33

        if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33 >> rail.Label('Yes') >> log_daystobeaccruedstartingbalance_35 \
            >> log_decimalpointvalue_split_37 >> log_finalvalueafterdecimaltobeconsidered_44

        log_finalvalueafterdecimaltobeconsidered_44 >> final_calculated_starting_balance_amount_to_put_54 \
            >> get_default_time_off_type_policy_schedule_for_user_55 >> if_effectivedate_day_present_58

        if_effectivedate_day_present_58 >> rail.Label('Yes') >> log_finalpolicyset_schedule_74 >> put_user_time_off_account_policy_set_schedule_76 \
            >> if_foreach_list_13_23_equals_to_2ind_casualleave_79

        if_effectivedate_day_present_58 >> rail.Label(
            'No') >> if_foreach_list_13_23_equals_to_2ind_casualleave_79

        if_foreach_declare_list_13_23_name_equals_to_1ind_privilegeleave_33 >> rail.Label(
            'No') >> if_foreach_list_13_23_equals_to_2ind_casualleave_79

        if_foreach_list_13_23_equals_to_2ind_casualleave_79 >> rail.Label(
            'No') >> foreach_declare_list_13_23_end

        if_foreach_list_13_23_equals_to_2ind_casualleave_79 >> rail.Label(
            'Yes') >> log_daystobeaccruedstartingbalance_81 >> log_decimalnumber_values_83_85

        log_decimalnumber_values_83_85 >> log_finalvalueafterdecimaltobeconsidered_90 >> final_calculated_starting_balance_casual_leave_92 \
            >> get_default_time_off_type_policy_schedule_for_user_101 >> if_effectivedate_day_present_104

        if_effectivedate_day_present_104 >> rail.Label(
            'No') >> foreach_declare_list_13_23_end

        if_effectivedate_day_present_104 >> rail.Label(
            'Yes') >> log_finalpolicy_120 >> put_user_time_off_account_policy_set_schedule_122 >> foreach_declare_list_13_23_end

        foreach_declare_list_13_23 >> foreach_declare_list_13_23_end

        foreach_declare_list_13_23_end >> catch_error

    return dag


rail.for_each_instance(create_dag)
