import rail
from guidehouse.workday_user_import.utils import custom_method


def update_timeoff_policies_task_group(config, action, user_ref=None):
    """
    Task group for assigning time-off policies (Holiday, Floating Holiday, Sick).

    For 'update_user', the entry task is get_timeoff_balance_summary which fetches
    current taken balances before computing adjusted starting balances.
    For 'add_user', the entry task is if_holiday_eligible.

    Args:
        config: Configuration object
        action (str): 'add_user' or 'update_user'
        user_ref (str): For 'add_user', the task_id whose result contains the new user
                        URI as result['user']['uri'] (e.g. 'add_new_user').
                        For 'update_user', pass None (URI taken from dag_run.conf['useruri']).

    Returns:
        tuple: (entry_task, exit_task)
    """
    with rail.TaskGroup(group_id='update_timeoff_policies', prefix_group_id=False):

        if action == 'update_user':
            get_timeoff_balance_summary = rail.RepliconServiceOperator(
                task_id='get_timeoff_balance_summary',
                endpoint="/services/TimeOffService2.svc/BulkGetBalanceSummaryForAccounts",
                data=lambda dag_run: {
                    "userUris": [dag_run.conf['useruri']],
                    "timeOffTypeUris": [
                        uri for uri in [
                            dag_run.conf['holiday_uri'],
                            dag_run.conf['floating_holiday_uri'],
                            dag_run.conf['sick_uri'],
                            dag_run.conf['can_floating_holiday_uri'],
                            dag_run.conf['gbr_floating_holiday_uri'],
                            dag_run.conf['can_sick_uri'],
                        ] if uri
                    ],
                    "asOfDate": rail.parse_date(
                        dag_run.conf.get('change_effective_date') or dag_run.conf.get('start_date'),
                        custom_method.DATE_FORMAT
                    )
                },
                data_handler=lambda res: {
                    'holiday_taken': next(
                        (item['balanceSummary']['timeTakenForPeriod'] for item in res
                         if (item.get('balanceSummary') or {}).get('account', {}).get('timeOffType', {}).get('displayText') == 'Holiday'),
                        0
                    ),
                    'floating_holiday_taken': next(
                        (item['balanceSummary']['timeTakenForPeriod'] for item in res
                         if (item.get('balanceSummary') or {}).get('account', {}).get('timeOffType', {}).get('displayText') == '[USA] Floating Holiday'),
                        0
                    ),
                    'sick_taken': next(
                        (item['balanceSummary']['timeTakenForPeriod'] for item in res
                         if (item.get('balanceSummary') or {}).get('account', {}).get('timeOffType', {}).get('displayText') == '[USA] Sick'),
                        0
                    ),
                    'can_sick_taken': next(
                        (item['balanceSummary']['timeTakenForPeriod'] for item in res
                         if (item.get('balanceSummary') or {}).get('account', {}).get('timeOffType', {}).get('displayText') == '[CAN] Sick'),
                        0
                    ),
                }
            )

        if_holiday_eligible = rail.IfOperator(
            task_id='if_holiday_eligible',
            test=lambda dag_run: custom_method.is_timeoff_recalculation_needed(config, dag_run, timeoff_type_name='Holiday', action=action),
            yes_task='get_holiday_final_policyset',
            no_task='dummy_after_holiday'
        )

        get_holiday_final_policyset = rail.PythonOperator(
            task_id='get_holiday_final_policyset',
            python_callable=lambda dag_run: custom_method.get_final_policyset(
                config,
                dag_run,
                custom_method._get_all_records(dag_run.conf['default_policyline_holiday']),
                [] if action == 'add_user' else custom_method.get_existing_policy_schedule(
                    rail.result('get_user_data')[0],
                    dag_run.conf['holiday_uri'],
                    dag_run.conf['change_effective_date']
                ),
                balance=custom_method._get_holiday_entitlement(config, dag_run),
                timeoff_type_name='Holiday',
                action=action
            )
        )

        assign_holiday_policy = rail.RepliconServiceOperator(
            task_id='assign_holiday_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result(user_ref)['user']['uri'] if user_ref else dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['holiday_uri']
                },
                "policySetScheduleEntries": rail.result('get_holiday_final_policyset')
            }
        )

        dummy_after_holiday = rail.EmptyOperator(task_id='dummy_after_holiday')

        if_floating_holiday_eligible = rail.IfOperator(
            task_id='if_floating_holiday_eligible',
            test=lambda dag_run: custom_method.is_timeoff_recalculation_needed(config, dag_run, timeoff_type_name='[USA] Floating Holiday', action=action),
            yes_task='get_floating_holiday_final_policyset',
            no_task='if_sick_eligible'
        )

        get_floating_holiday_final_policyset = rail.PythonOperator(
            task_id='get_floating_holiday_final_policyset',
            python_callable=lambda dag_run: custom_method.get_final_policyset(
                config,
                dag_run,
                custom_method._get_all_records(dag_run.conf['default_policyline_floating_holiday']),
                [] if action == 'add_user' else custom_method.get_existing_policy_schedule(
                    rail.result('get_user_data')[0],
                    dag_run.conf['floating_holiday_uri'],
                    dag_run.conf['change_effective_date']
                ),
                balance=(
                    custom_method._get_floating_holiday_starting_balance(dag_run)
                    if action == 'add_user'
                    else custom_method.get_adjusted_balance(
                        rail.result('get_timeoff_balance_summary')['floating_holiday_taken'],
                        custom_method._get_floating_holiday_schedule_change_entitlement(dag_run)
                    )
                ),
                timeoff_type_name='[USA] Floating Holiday',
                action=action
            )
        )

        assign_floating_holiday_policy = rail.RepliconServiceOperator(
            task_id='assign_floating_holiday_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result(user_ref)['user']['uri'] if user_ref else dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['floating_holiday_uri']
                },
                "policySetScheduleEntries": rail.result('get_floating_holiday_final_policyset')
            }
        )

        if_sick_eligible = rail.IfOperator(
            task_id='if_sick_eligible',
            test=lambda dag_run: custom_method.is_timeoff_recalculation_needed(config, dag_run, timeoff_type_name='[USA] Sick', action=action),
            yes_task='get_sick_final_policyset',
            no_task='dummy_after_sick'
        )

        get_sick_final_policyset = rail.PythonOperator(
            task_id='get_sick_final_policyset',
            python_callable=lambda dag_run: custom_method.get_final_policyset(
                config,
                dag_run,
                custom_method._get_all_records(dag_run.conf['default_policyline_sick']),
                [] if action == 'add_user' else custom_method.get_existing_policy_schedule(
                    rail.result('get_user_data')[0],
                    dag_run.conf['sick_uri'],
                    dag_run.conf['change_effective_date']
                ),
                balance=(
                    custom_method._get_sick_leave_starting_balance(dag_run)
                    if custom_method.should_assign_timeoff_type(config, dag_run, timeoff_type_name='[USA] Sick', action=action)
                    else custom_method.get_adjusted_balance(
                        rail.result('get_timeoff_balance_summary')['sick_taken'],
                        custom_method._get_sick_leave_schedule_change_entitlement(dag_run)
                    )
                ),
                timeoff_type_name='[USA] Sick',
                action=action
            )
        )

        assign_sick_policy = rail.RepliconServiceOperator(
            task_id='assign_sick_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result(user_ref)['user']['uri'] if user_ref else dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['sick_uri']
                },
                "policySetScheduleEntries": rail.result('get_sick_final_policyset')
            }
        )

        dummy_after_sick = rail.EmptyOperator(task_id='dummy_after_sick')

        if_can_floating_holiday_eligible = rail.IfOperator(
            task_id='if_can_floating_holiday_eligible',
            test=lambda dag_run: custom_method.is_timeoff_recalculation_needed(config, dag_run, timeoff_type_name='[CAN] Floating Holiday', action=action),
            yes_task='get_can_floating_holiday_final_policyset',
            no_task='dummy_after_can_floating_holiday'
        )

        get_can_floating_holiday_final_policyset = rail.PythonOperator(
            task_id='get_can_floating_holiday_final_policyset',
            python_callable=lambda dag_run: custom_method.get_final_policyset(
                config,
                dag_run,
                custom_method._get_all_records(dag_run.conf['default_policyline_can_floating_holiday']),
                [] if action == 'add_user' else custom_method.get_existing_policy_schedule(
                    rail.result('get_user_data')[0],
                    dag_run.conf['can_floating_holiday_uri'],
                    dag_run.conf['change_effective_date']
                ),
                balance=custom_method._get_intl_floating_holiday_balance(dag_run),
                timeoff_type_name='[CAN] Floating Holiday',
                action=action
            )
        )

        assign_can_floating_holiday_policy = rail.RepliconServiceOperator(
            task_id='assign_can_floating_holiday_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result(user_ref)['user']['uri'] if user_ref else dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['can_floating_holiday_uri']
                },
                "policySetScheduleEntries": rail.result('get_can_floating_holiday_final_policyset')
            }
        )

        dummy_after_can_floating_holiday = rail.EmptyOperator(task_id='dummy_after_can_floating_holiday')

        if_gbr_floating_holiday_eligible = rail.IfOperator(
            task_id='if_gbr_floating_holiday_eligible',
            test=lambda dag_run: custom_method.is_timeoff_recalculation_needed(config, dag_run, timeoff_type_name='[GBR] Floating Holiday', action=action),
            yes_task='get_gbr_floating_holiday_final_policyset',
            no_task='dummy_after_gbr_floating_holiday'
        )

        get_gbr_floating_holiday_final_policyset = rail.PythonOperator(
            task_id='get_gbr_floating_holiday_final_policyset',
            python_callable=lambda dag_run: custom_method.get_final_policyset(
                config,
                dag_run,
                custom_method._get_all_records(dag_run.conf['default_policyline_gbr_floating_holiday']),
                [] if action == 'add_user' else custom_method.get_existing_policy_schedule(
                    rail.result('get_user_data')[0],
                    dag_run.conf['gbr_floating_holiday_uri'],
                    dag_run.conf['change_effective_date']
                ),
                balance=custom_method._get_intl_floating_holiday_balance(dag_run),
                timeoff_type_name='[GBR] Floating Holiday',
                action=action
            )
        )

        assign_gbr_floating_holiday_policy = rail.RepliconServiceOperator(
            task_id='assign_gbr_floating_holiday_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result(user_ref)['user']['uri'] if user_ref else dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['gbr_floating_holiday_uri']
                },
                "policySetScheduleEntries": rail.result('get_gbr_floating_holiday_final_policyset')
            }
        )

        dummy_after_gbr_floating_holiday = rail.EmptyOperator(task_id='dummy_after_gbr_floating_holiday')

        if_can_sick_eligible = rail.IfOperator(
            task_id='if_can_sick_eligible',
            test=lambda dag_run: custom_method.is_timeoff_recalculation_needed(config, dag_run, timeoff_type_name='[CAN] Sick', action=action),
            yes_task='get_can_sick_final_policyset',
            no_task='dummy_after_can_sick'
        )

        get_can_sick_final_policyset = rail.PythonOperator(
            task_id='get_can_sick_final_policyset',
            python_callable=lambda dag_run: custom_method.get_final_policyset(
                config,
                dag_run,
                custom_method._get_all_records(dag_run.conf['default_policyline_can_sick']),
                [] if action == 'add_user' else custom_method.get_existing_policy_schedule(
                    rail.result('get_user_data')[0],
                    dag_run.conf['can_sick_uri'],
                    dag_run.conf['change_effective_date']
                ),
                balance=(
                    custom_method._get_can_sick_leave_starting_balance(dag_run)
                    if custom_method.should_assign_timeoff_type(config, dag_run, timeoff_type_name='[CAN] Sick', action=action)
                    else custom_method.get_adjusted_balance(
                        rail.result('get_timeoff_balance_summary')['can_sick_taken'],
                        custom_method._get_can_sick_leave_schedule_change_entitlement(dag_run)
                    )
                ),
                timeoff_type_name='[CAN] Sick',
                action=action
            )
        )

        assign_can_sick_policy = rail.RepliconServiceOperator(
            task_id='assign_can_sick_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result(user_ref)['user']['uri'] if user_ref else dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['can_sick_uri']
                },
                "policySetScheduleEntries": rail.result('get_can_sick_final_policyset')
            }
        )

        dummy_after_can_sick = rail.EmptyOperator(task_id='dummy_after_can_sick')

        # Wiring
        if action == 'update_user':
            get_timeoff_balance_summary >> if_holiday_eligible

        if_holiday_eligible >> rail.Label("Yes") >> get_holiday_final_policyset >> assign_holiday_policy >> dummy_after_holiday
        if_holiday_eligible >> rail.Label("No") >> dummy_after_holiday
        dummy_after_holiday >> if_floating_holiday_eligible
        if_floating_holiday_eligible >> rail.Label("Yes") >> get_floating_holiday_final_policyset >> assign_floating_holiday_policy >> if_sick_eligible
        if_floating_holiday_eligible >> rail.Label("No") >> if_sick_eligible
        if_sick_eligible >> rail.Label("Yes") >> get_sick_final_policyset >> assign_sick_policy >> dummy_after_sick
        if_sick_eligible >> rail.Label("No") >> dummy_after_sick
        dummy_after_sick >> if_can_floating_holiday_eligible
        if_can_floating_holiday_eligible >> rail.Label("Yes") >> get_can_floating_holiday_final_policyset >> assign_can_floating_holiday_policy >> dummy_after_can_floating_holiday
        if_can_floating_holiday_eligible >> rail.Label("No") >> dummy_after_can_floating_holiday
        dummy_after_can_floating_holiday >> if_gbr_floating_holiday_eligible
        if_gbr_floating_holiday_eligible >> rail.Label("Yes") >> get_gbr_floating_holiday_final_policyset >> assign_gbr_floating_holiday_policy >> dummy_after_gbr_floating_holiday
        if_gbr_floating_holiday_eligible >> rail.Label("No") >> dummy_after_gbr_floating_holiday
        dummy_after_gbr_floating_holiday >> if_can_sick_eligible
        if_can_sick_eligible >> rail.Label("Yes") >> get_can_sick_final_policyset >> assign_can_sick_policy >> dummy_after_can_sick
        if_can_sick_eligible >> rail.Label("No") >> dummy_after_can_sick

    entry = get_timeoff_balance_summary if action == 'update_user' else if_holiday_eligible
    return entry, dummy_after_can_sick