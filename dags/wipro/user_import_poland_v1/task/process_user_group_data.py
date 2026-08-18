from wipro.user_import_poland_v1.utils import custom_methods, request_payload
import rail
null = None


def create_prerequisite_data(config):
    with rail.TaskGroup(
        group_id="create_all_prerequisite_data",
        prefix_group_id=False
    ) as prerequisite_data:

        start_groups = rail.EmptyOperator(task_id="start_groups")

        get_poland_parent_location_details = rail.RepliconServiceOperator(
            task_id="get_poland_parent_location_details",
            endpoint="/services/LocationService1.svc/GetPageOfAvailableLocationsByTextSearch",
            data=request_payload.get_parent_location_payload,
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                        list(filter(lambda i:
                        i["hierarchyLevel"] == 0 and i["isEffectivelyEnabled"], response)),
                        "location.displayText",
                        "Poland",
                        "location.uri")
        )

        get_all_location_with_hierarchy_details = rail.RepliconServiceOperator(
            task_id="get_all_location_with_hierarchy_details",
            endpoint="/services/LocationListService1.svc/GetChildHierarchyData",
            data=request_payload.get_location_hierarchy_payload,
            data_handler=custom_methods.get_location_hierarchy_data
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                    "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=custom_methods.get_all_custom_fields_data
        )

        get_all_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_all_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=custom_methods.get_all_object_extension_fields_data
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id="get_all_time_off_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id="get_all_countries",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters"
        )

        get_all_legal_entities = rail.RepliconServiceOperator(
            task_id="get_all_legal_entities",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_legal_entity_payload,
            data_handler=custom_methods.get_all_legal_entities_data
        )

        get_all_time_zones = rail.RepliconServiceOperator(
            task_id="get_all_time_zones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        get_all_overtime_approval_paths = rail.RepliconServiceOperator(
            task_id="get_all_overtime_approval_paths",
            endpoint="/services/WorkAuthorizationApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id="get_all_timesheet_approval_path",
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_timeoff_approval_path = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_approval_path",
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_all_payrules = rail.RepliconServiceOperator(
            task_id="get_all_payrules",
            endpoint="/services/PayRuleScriptListService1.svc/GetData",
            data=request_payload.get_payrule_payload,
            data_handler=custom_methods.get_payrule_data
        )

        get_all_timesheet_periods = rail.RepliconServiceOperator(
            task_id="get_all_timesheet_periods",
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data=request_payload.get_timesheet_period_payload,
            data_handler=custom_methods.get_timesheet_period_data
        )

        get_all_employee_types = rail.RepliconServiceOperator(
            task_id="get_all_employee_types",
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        get_all_timeoff_event_scripts = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_event_scripts",
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts"
        )

        get_all_timeoff_validation_scripts = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_validation_scripts",
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetActiveScripts"
        )

        get_poland_schedule_policy = rail.RepliconServiceOperator(
            task_id="get_poland_schedule_policy",
            endpoint="/services/PolicySetService1.svc/GetAllEnabledPolicySetsForPolicy",
            data={
            "policyUri": "urn:replicon:policy:shift-schedule"
            },
            data_handler=lambda response:rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                config.GENERAL_MAPPER["schedule_policy"],
                "uri"
            )
        )

        get_all_departments = rail.RepliconServiceOperator(
            task_id="get_all_departments",
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments"
        )

        end_groups = rail.EmptyOperator(task_id="end_groups")

        start_groups >> get_poland_parent_location_details >>\
            [

                get_all_location_with_hierarchy_details,
                get_all_custom_fields,
                get_all_object_extension_fields,
                get_all_employee_types,
                get_all_time_off_types,
                get_all_permission_sets,
                get_all_policy_sets,
                get_all_countries,
                get_all_legal_entities,
                get_all_time_zones,
                get_all_holiday_calendars,
                get_all_overtime_approval_paths,
                get_all_timesheet_approval_path,
                get_all_timeoff_approval_path,
                get_all_office_schedules,
                get_all_payrules,
                get_all_timesheet_periods,
                get_all_timeoff_event_scripts,
                get_all_timeoff_validation_scripts,
                get_poland_schedule_policy,
                get_all_departments
            ] >> end_groups
        return prerequisite_data
