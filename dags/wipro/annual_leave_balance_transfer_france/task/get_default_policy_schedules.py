import rail

null = None

def get_default_policyschedules_task_group(config):

    with rail.TaskGroup(group_id='get_default_policyschedules', prefix_group_id=False) as get_default_policyschedules:

        start_default_policyschedules = rail.EmptyOperator(
            task_id="start_default_policyschedules"
        )

        get_annual_leave_default_policy = rail.RepliconServiceOperator(
            task_id='get_annual_leave_default_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_required_timeoff_type_uris').into.timeoff_annual_leave_uri }}"
            },
            target='artifact'
        )

        get_annual_leave_carried_over_default_policy = rail.RepliconServiceOperator(
            task_id='get_annual_leave_carried_over_default_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_required_timeoff_type_uris').into.timeoff_annual_leave_carried_over_uri }}"
            },
            target='artifact'
        )

        get_annual_leave_seniority_days_carried_over_default_policy = rail.RepliconServiceOperator(
            task_id='get_annual_leave_seniority_days_carried_over_default_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_required_timeoff_type_uris').into.timeoff_annual_leave_seniority_days_carried_over_uri }}"
            },
            target='artifact'
        )

        get_annual_leave_rtt_carried_over_default_policy = rail.RepliconServiceOperator(
            task_id='get_annual_leave_rtt_carried_over_default_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_required_timeoff_type_uris').into.timeoff_annual_leave_rtt_carried_over_uri }}"
            },
            target='artifact'
        )

        get_annual_leave_rtt_for_forfait_jours_carried_over_default_policy = rail.RepliconServiceOperator(
            task_id='get_annual_leave_rtt_for_forfait_jours_carried_over_default_policy',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_required_timeoff_type_uris').into.timeoff_annual_leave_rtt_for_forfait_jours_carried_over_uri }}"
            },
            target='artifact'
        )

        end_default_policyschedules = rail.EmptyOperator(
            task_id="end_default_policyschedules"
        )

        start_default_policyschedules >> [get_annual_leave_default_policy, get_annual_leave_carried_over_default_policy,
        get_annual_leave_seniority_days_carried_over_default_policy, get_annual_leave_rtt_carried_over_default_policy,
        get_annual_leave_rtt_for_forfait_jours_carried_over_default_policy] >> end_default_policyschedules

    return start_default_policyschedules, get_default_policyschedules
