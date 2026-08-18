import rail

from crl.user_import_canada_v7.utils import request_payload, response_filter

null = None

def get_user_prereqs_task_group():

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        get_updated_location_grps = rail.RepliconServiceOperator(
            task_id="get_updated_location_grps",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:name",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_full_path_data
        )

        get_updated_buisness_unit_grps = rail.RepliconServiceOperator(
            task_id="get_updated_buisness_unit_grps",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:division-list-column:name",
                    "urn:replicon:division-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_full_path_data
        )

        get_updated_company_code = rail.RepliconServiceOperator(
            task_id="get_updated_company_code",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:name",
                    "urn:replicon:service-center-list-column:code",
                    "urn:replicon:service-center-list-column:service-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_group_data
        )

        get_updated_cost_center_grps = rail.RepliconServiceOperator(
            task_id="get_updated_cost_center_grps",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:name",
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
                },
            data_handler=response_filter.filter_full_path_data
        )

        get_all_employee_types_grp = rail.RepliconServiceOperator(
            task_id="get_all_employee_types_grp",
            endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_all_employee_grp_payload,
            data_handler=response_filter.filter_employee_grp_data
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_all_holiday_calenders = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calenders',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
        )

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_filtered_time_off_types
        )

        get_place_details = rail.RepliconServiceOperator(
            task_id='get_place_details',
            endpoint='/services/PlaceService1.svc/GetPageOfPlaceDetails',
            data={
                "page": "1",
                "pageSize": "100000",
                "searchParameter": {
                    "isEnabled": True
                }
            },
            data_handler=response_filter.get_filtered_place_details
        )

        get_timeoff_balance_event_script_uri = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance_event_script_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts",
            data_handler=lambda response: {
                "starting_balance_script_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Starting Balance Set To', 'uri')
            }
        )

        get_timeoff_balance_validation_script = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance_validation_script',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response:{
                 "prevent_balance_overdraw_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Prevent balance overdraw', 'uri')
            }
        )

        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'title_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Title', 'uri'),
                'functional_segment_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Functional Segment', 'uri'),
                'std_hrs_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Standard Hours', 'uri'),
                'adjusted_hiredate_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Adjusted Hire Date', 'uri'),
                'adjusted_hiredate_accrual_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Adjusted Hire Date for Accrual', 'uri'),
                'job_code_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Job Code', 'uri'),
                'pay_grp_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Pay Group', 'uri'),
                'us_flsa_status_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'US FLSA Status', 'uri'),
                'profit_center_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Profit Center', 'uri'),
                'project_user_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Project User', 'uri'),
                'us_vacation_exception_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Vacation Exception', 'uri'),
                'us_veterans_day_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'US Veterans Day', 'uri'),
                'term_exported_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Term Exported', 'uri'),
                'sick_payout_eligible_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Sick Payout Eligible', 'uri'),
                'banked_ot_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Banked Overtime Payout Eligible', 'uri'),
                'emp_status_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Employee Status', 'uri'),
                'buisness_segment_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Business Segment', 'uri'),
                'buisness_unit_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Business Unit', 'uri'),
                'reg_temp_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Reg/Temp', 'uri'),
                'full_part_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Full/Part', 'uri'),
                'is_hrbp_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'is HRBP', 'uri'),
                'pay_type_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Pay Type', 'uri'),
                'remote_worker_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Remote Worker', 'uri'),
                'change_effective_date_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Change Effective Date', 'uri'),
                'event_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Event', 'uri'),
                'event_reason_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Event Reason', 'uri'),
                # V2.7 - written by the integration on emp_status transitions to/from "Unpaid Leave".
                # Returns None on tenants where the UDF is not yet provisioned; write helpers guard on this.
                'leave_start_date_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Leave Start Date', 'uri'),
                'leave_end_date_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Leave End Date', 'uri'),
                'default_activity_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Default Activity', 'uri'),
                'cost_center_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Cost Center', 'uri'),
            },
        )

        get_us_flsa_status_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_us_flsa_status_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['us_flsa_status_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        get_project_user_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_project_user_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['project_user_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        get_us_veterans_day_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_us_veterans_day_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['us_veterans_day_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        get_term_exported_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_term_exported_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['term_exported_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        get_sick_payout_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_sick_payout_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['sick_payout_eligible_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        get_banked_ot_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_banked_ot_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['banked_ot_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        dummy_get_user_prereqs >> [get_updated_location_grps, get_updated_company_code, get_updated_cost_center_grps,
            get_updated_buisness_unit_grps, get_all_office_schedules,get_all_employee_types_grp, get_all_holiday_calenders, get_timesheet_approval_paths,
            get_all_permission_set, get_all_payrule_scripts, get_all_policy_sets,get_timeoff_balance_event_script_uri, get_all_timezones,
             get_all_time_off_types, get_place_details, get_timeoff_balance_validation_script, get_user_udfs]

        get_user_udfs >> get_us_flsa_status_dropdown_values >> get_project_user_dropdown_values >> get_us_veterans_day_dropdown_values >>\
            get_term_exported_dropdown_values >> get_sick_payout_dropdown_values >> get_banked_ot_dropdown_values

    return dummy_get_user_prereqs, get_user_prereqs
