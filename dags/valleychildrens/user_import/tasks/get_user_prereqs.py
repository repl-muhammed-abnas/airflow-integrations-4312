import rail

def get_user_prereqs_task_group():
    dummy = rail.EmptyOperator(task_id='dummy_get_user_prereqs')
    get_all_custom_fields = rail.RepliconServiceOperator(
        task_id='get_all_custom_fields',
        endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
        data={'objectUri': 'urn:replicon:object-type:user'},
    )
    get_all_permission_set = rail.RepliconServiceOperator(
        task_id='get_all_permission_set',
        endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
        data={},
    )
    get_enabled_activities = rail.RepliconServiceOperator(
        task_id='get_enabled_activities',
        endpoint='/services/ActivityService1.svc/GetEnabledActivities',
        data={},
    )
    get_timeoff_validation_scripts = rail.RepliconServiceOperator(
        task_id='get_timeoff_validation_scripts',
        endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
        data={},
    )
    get_timeoff_balance_event_scripts = rail.RepliconServiceOperator(
        task_id='get_timeoff_balance_event_scripts',
        endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
        data={},
    )
    get_all_timezones = rail.RepliconServiceOperator(
        task_id='get_all_timezones',
        endpoint='/services/InternationalizationService1.svc/GetAllTimeZones',
        data={},
    )
    get_all_office_schedules = rail.RepliconServiceOperator(
        task_id='get_all_office_schedules',
        endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        data={},
    )
    get_timesheet_approval_paths = rail.RepliconServiceOperator(
        task_id='get_timesheet_approval_paths',
        endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        data={},
    )
    get_timeoff_approval_paths = rail.RepliconServiceOperator(
        task_id='get_timeoff_approval_paths',
        endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
        data={},
    )
    get_all_policy_sets = rail.RepliconServiceOperator(
        task_id='get_all_policy_sets',
        endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
        data={},
    )
    get_all_holiday_calendars = rail.RepliconServiceOperator(
        task_id='get_all_holiday_calendars',
        endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
        data={},
    )
    get_timesheet_periods = rail.RepliconServiceOperator(
        task_id='get_timesheet_periods',
        endpoint='/services/TimesheetPeriodListService1.svc/GetData',
        data={
            'page': '1',
            'pagesize': '1000',
            'columnUris': [
                'urn:replicon:timesheet-period-list-column:timesheet-period',
            ],
            'sort': [],
            'filterExpression': None,
        },
    )
    get_employee_type_groups = rail.RepliconServiceOperator(
        task_id='get_employee_type_groups',
        endpoint='/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups',
        data={
            'page': '1',
            'pagesize': '10000',
            'columnUris': [
                'urn:replicon:employee-type-group-list-column:code',
                'urn:replicon:employee-type-group-list-column:employee-type-group',
            ],
            'sort': [],
            'filterExpression': None,
        },
    )
    get_department_groups = rail.RepliconServiceOperator(
        task_id='get_department_groups',
        endpoint='/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups',
        data={},
    )
    get_service_centers = rail.RepliconServiceOperator(
        task_id='get_service_centers',
        endpoint='/services/ServiceCenterService1.svc/GetEnabledServiceCenters',
        data={},
    )
    get_cme_entitlement_options = rail.RepliconServiceOperator(
        task_id='get_cme_entitlement_options',
        endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
        data=lambda: {
            'customFieldUri':  rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_custom_fields'),
                    'displayText', 'CME Entitlement', 'uri',
                ),
            }
    )
    get_employee_classification_options = rail.RepliconServiceOperator(
        task_id='get_employee_classification_options',
        endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
        data=lambda: {
            'customFieldUri':  rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_custom_fields'),
                    'displayText', 'Employee Classification', 'uri',
                ),
            }
    )
    end = rail.EmptyOperator(task_id='get_user_prereqs')
    parallel = [
        get_all_custom_fields,
        get_all_permission_set,
        get_enabled_activities,
        get_timeoff_validation_scripts,
        get_timeoff_balance_event_scripts,
        get_all_timezones,
        get_all_office_schedules,
        get_timesheet_approval_paths,
        get_timeoff_approval_paths,
        get_all_policy_sets,
        get_all_holiday_calendars,
        get_timesheet_periods,
        get_employee_type_groups,
        get_department_groups,
        get_service_centers,
    ]
    dummy >> parallel
    get_all_custom_fields >> [get_cme_entitlement_options, get_employee_classification_options]
    parallel + [get_cme_entitlement_options, get_employee_classification_options] >> end
    return dummy, end

