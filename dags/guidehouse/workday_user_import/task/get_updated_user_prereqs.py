import rail
from guidehouse.workday_user_import.utils import request_payload, response_filters, custom_method

null = None


def get_updated_user_prereqs_task_group(config):
    """
    Create task group for refreshing key user processing prerequisites.

    Args:
        config: Configuration object

    Returns:
        tuple: (entry_task, task_group)
    """

    with rail.TaskGroup(group_id='get_updated_user_prereqs', prefix_group_id=False) as get_updated_user_prereqs:

        dummy_get_updated_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_updated_user_prereqs"
        )

        get_updated_all_office_schedule = rail.RepliconServiceOperator(
            task_id='get_updated_all_office_schedule',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_selected_timeoff_uris = rail.PythonOperator(
            task_id='get_selected_timeoff_uris',
            python_callable=custom_method.get_selected_timeoff_uris
        )

        get_default_policyline_holiday = rail.RepliconServiceOperator(
            task_id='get_default_policyline_holiday',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('get_selected_timeoff_uris')['holiday_uri']
            }
        )

        get_default_policyline_floating_holiday = rail.RepliconServiceOperator(
            task_id='get_default_policyline_floating_holiday',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('get_selected_timeoff_uris')['floating_holiday_uri']
            }
        )

        get_default_policyline_sick = rail.RepliconServiceOperator(
            task_id='get_default_policyline_sick',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('get_selected_timeoff_uris')['sick_uri']
            }
        )

        get_default_policyline_can_floating_holiday = rail.RepliconServiceOperator(
            task_id='get_default_policyline_can_floating_holiday',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('get_selected_timeoff_uris')['can_floating_holiday_uri']
            }
        )

        get_default_policyline_gbr_floating_holiday = rail.RepliconServiceOperator(
            task_id='get_default_policyline_gbr_floating_holiday',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('get_selected_timeoff_uris')['gbr_floating_holiday_uri']
            }
        )

        get_default_policyline_can_sick = rail.RepliconServiceOperator(
            task_id='get_default_policyline_can_sick',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda: {
                "timeOffTypeUri": rail.result('get_selected_timeoff_uris')['can_sick_uri']
            }
        )

        get_all_scripts_time_off_balance_event_script = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_script',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: {
                'starting_balance_set_to': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Starting Balance Set To', 'uri', ''),
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        dummy_get_updated_user_prereqs >> get_updated_all_office_schedule >> finish
        dummy_get_updated_user_prereqs >> get_enabled_timeoff_types >> get_selected_timeoff_uris >> [
            get_default_policyline_holiday,
            get_default_policyline_floating_holiday,
            get_default_policyline_sick,
            get_default_policyline_can_floating_holiday,
            get_default_policyline_gbr_floating_holiday,
            get_default_policyline_can_sick,
            get_all_scripts_time_off_balance_event_script,
        ]
        get_default_policyline_holiday >> finish
        get_default_policyline_floating_holiday >> finish
        get_default_policyline_sick >> finish
        get_default_policyline_can_floating_holiday >> finish
        get_default_policyline_gbr_floating_holiday >> finish
        get_default_policyline_can_sick >> finish
        get_all_scripts_time_off_balance_event_script >> finish
    return dummy_get_updated_user_prereqs, get_updated_user_prereqs
