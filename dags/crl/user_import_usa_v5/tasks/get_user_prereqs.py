from uuid import uuid4
import rail

from crl.user_import_usa_v5.utils import request_payload, response_filter, python_callable_methods

null = None

def get_user_prereqs_task_group(config):

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

        get_updated_department_grps = rail.RepliconServiceOperator(
            task_id="get_updated_department_grps",
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
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

        get_hidden_to_oef_values = rail.RepliconServiceOperator(
            task_id='get_hidden_to_oef_values',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:object-extension-tag-definition-list-column:name",
                    "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response: response_filter.get_hidden_oef_value(response, config.TO_PLACEHOLDER_HIDDEN_OEF_NAMES)
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
                'pay_grp_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'US Pay Group', 'uri'),
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
                'default_activity_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Default Activity', 'uri'),
                'holiday_calendar_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Holiday Calendar Code', 'uri'),
                'cost_center_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Cost Center', 'uri'),
                'home_location_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Home Location', 'uri'),
                'reg_to_temp_bal_export_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'USA Reg to Temp Bal. Export', 'uri'),
                'reg_to_temp_bal_export_eff_date_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'USA Reg to Temp Update Effective Date', 'uri'),
            },
        )

        get_pay_grp_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_pay_grp_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['pay_grp_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        create_pay_grp_collection_replicon = rail.CreateCollectionOperator(
            task_id="create_pay_grp_collection_replicon",
            columns=['name', 'uri'],
            name="replicon_pay_grps",
            source="{{ result('get_pay_grp_dropdown_values') | to_json }}"
        )

        query_pay_grp_udf_values_add = rail.QueryCollectionOperator(
            task_id="query_pay_grp_udf_values_add",
            query="""SELECT DISTINCT pay_grp FROM valid_record WHERE NULLIF(pay_grp, '') IS NOT NULL and LOWER(pay_grp) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_pay_grps)""",
            name='new_pay_grps'
        )

        has_any_pay_grps_values_to_add = rail.IfOperator(
            task_id="has_any_pay_grps_values_to_add",
            test="{{result('query_pay_grp_udf_values_add', 'length') > 0}}",
            yes_task="create_pay_grps_add_payload",
            no_task="get_updated_pay_grps_udf_dropdown_values"
        )

        create_pay_grps_add_payload = rail.PythonOperator(
            task_id="create_pay_grps_add_payload",
            python_callable=python_callable_methods.create_pay_grps_add_payload
        )

        put_pay_grps_dropdown_values = rail.RepliconServiceOperator(
            task_id="put_pay_grps_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_user_udfs')['pay_grp_def_uri'],
                "customFieldDropDownOptionUris": rail.result('create_pay_grps_add_payload')
            }
        )

        get_updated_pay_grps_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_updated_pay_grps_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['pay_grp_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
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

        get_reg_to_temp_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_reg_to_temp_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['reg_to_temp_bal_export_def_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        get_all_activity = rail.RepliconServiceOperator(
            task_id='get_all_activity',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
        )

        create_activity_collection = rail.CreateCollectionOperator(
            task_id="create_activity_collection",
            columns=["code","description",'displayText',"isEnabled",'name', 'uri'],
            source="{{ result ('get_all_activity') | to_json }}",
            name="replicon_activities"
        )

        query_distinct_activities_in_payload = rail.QueryCollectionOperator(
            task_id="query_distinct_activities_in_payload",
            name='disable_user_records',
            query="""SELECT Distinct activity_type FROM valid_record WHERE NULLIF(activity_type, '') IS NOT NULL"""
        )

        query_activity_to_create = rail.QueryCollectionOperator(
            task_id='query_activity_to_create',
            query="""SELECT DISTINCT activity_type FROM valid_record where NULLIF(activity_type, '') IS NOT NULL
                    and LOWER(activity_type) NOT IN (SELECT DISTINCT LOWER(displayText) FROM replicon_activities)"""
        )

        create_new_activities = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_new_activities',
            items=lambda: rail.result('query_activity_to_create'),
            endpoint='/services/ActivityService1.svc/CreateActivityOrApplyModifications',
            data=lambda item :{
                "target": null,
                "modifications": {
                    "name": item['activity_type'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": null,
                    "userAssignmentsToApply": null
                },
                "activityModificationOptionUri": "urn:replicon:activity-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        get_all_updated_activity_uris = rail.RepliconServiceOperator(
            task_id='get_all_updated_activity_uris',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=lambda response: list(map(lambda x: {"uri":x['uri']}, response)),
            target="artifact"
        )

        dummy_get_user_prereqs >> [get_updated_location_grps,get_updated_department_grps, get_updated_company_code, get_updated_cost_center_grps,
            get_updated_buisness_unit_grps, get_all_office_schedules,get_all_employee_types_grp, get_all_holiday_calenders, get_timesheet_approval_paths,
            get_all_permission_set, get_all_payrule_scripts, get_all_policy_sets,get_timeoff_balance_event_script_uri, get_all_timezones,
             get_all_time_off_types, get_place_details, get_timeoff_balance_validation_script, get_hidden_to_oef_values, get_user_udfs]

        get_user_udfs >> get_us_flsa_status_dropdown_values >> get_project_user_dropdown_values >> get_us_veterans_day_dropdown_values
        get_us_veterans_day_dropdown_values >> get_pay_grp_dropdown_values
        get_pay_grp_dropdown_values >> get_term_exported_dropdown_values >> get_sick_payout_dropdown_values >> get_reg_to_temp_dropdown_values >> get_all_activity
        get_all_activity >> create_activity_collection >> query_distinct_activities_in_payload >> query_activity_to_create
        query_activity_to_create >> create_new_activities >> get_all_updated_activity_uris

        get_pay_grp_dropdown_values >> create_pay_grp_collection_replicon >> query_pay_grp_udf_values_add >> has_any_pay_grps_values_to_add
        has_any_pay_grps_values_to_add >> rail.Label('Yes') >> create_pay_grps_add_payload >> put_pay_grps_dropdown_values
        put_pay_grps_dropdown_values >> get_updated_pay_grps_udf_dropdown_values
        has_any_pay_grps_values_to_add >> rail.Label('No') >> get_updated_pay_grps_udf_dropdown_values

    return dummy_get_user_prereqs, get_user_prereqs
