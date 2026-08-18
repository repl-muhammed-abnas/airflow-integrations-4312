import rail
from deltek_northstar.user_sync_polaris_india.utils import request_payload, python_callable, response_filter

null = None

def get_user_prereqs_task_group():

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        get_updated_locations = rail.RepliconServiceOperator(
            task_id='get_updated_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=request_payload.get_location_payload,
            data_handler= response_filter.filter_group_data
        )

        get_updated_departments = rail.RepliconServiceOperator(
            task_id="get_updated_departments",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:name",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.groups_filter
        )

        get_updated_employee_types = rail.RepliconServiceOperator(
            task_id="get_updated_employee_types",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:name",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.groups_filter
        )

        get_products_groups = rail.RepliconServiceOperator(
            task_id="get_products_groups",
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
            data_handler=response_filter.groups_filter
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        # get_expense_approval_paths = rail.RepliconServiceOperator(
        #     task_id='get_expense_approval_paths',
        #     endpoint='/services/ExpenseApprovalService1.svc/GetAllApprovalPaths',
        # )

        get_expense_approval_paths = rail.EmptyOperator(
            task_id='get_expense_approval_paths'
        )

        get_timeoff_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timeoff_approval_paths',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_default_office_schedule = rail.RepliconServiceOperator(
            task_id = 'get_default_office_schedule',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_taxable_entities_data = rail.RepliconServiceOperator(
            task_id="get_taxable_entities_data",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters"
        )

        get_pay_groups_data = rail.RepliconServiceOperator(
            task_id="get_pay_groups_data",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters"
        )

        get_all_holiday_calendars=rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        def filter_timesheet_period_list(response):
            return list(map(lambda row:
                {
                    "uri": row["cells"][0]["uri"],
                    "name": row["cells"][1].get('textValue')
                }, response["rows"]))

        get_all_timesheet_period_list = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_period_list',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period",
                    "urn:replicon:timesheet-period-list-column:name"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=filter_timesheet_period_list
        )

        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'reim_currency_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Reimbursement Currency', 'uri'),
                'glc_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'GLC', 'uri'),
                'emp_status_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Employee Status', 'uri'),
                'personal_action_code_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Personnel Action Code', 'uri'),
                'past_hire_date_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Past Hire Date', 'uri'),
                'job_title_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Detail Job Title', 'uri'),
                'polaris_roles_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Polaris Roles', 'uri'),
                'line_of_business_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Line of Business', 'uri'),
                'work_schedule_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Work Schedule', 'uri'),
                'pay_period_code_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Pay Period Code', 'uri'),
                'shift_schedule_name_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Shift schedule Name', 'uri'),
                'oncall_allowance_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'On call Allowance', 'uri'),
            },
        )

        def project_role_list_input(response):
            rows = response.json()['d']['rows']
            return list(map(lambda row: {
                "name": row['cells'][0].get('textValue'),
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][2].get('uri')
            }, rows)) if rows else []

        get_all_roles = rail.RepliconServiceOperator(
            task_id='get_all_roles',
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris":  [
                    "urn:replicon:project-role-list-column:name",
                    "urn:replicon:project-role-list-column:description",
                    "urn:replicon:project-role-list-column:project-role"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=project_role_list_input
        )

        get_all_pay_rules = rail.RepliconServiceOperator(
            task_id='get_all_pay_rules',
            endpoint='/services/PayRuleScriptService1.svc/GetAllPayRuleScripts'
        )

        dummy_udfs_dropdown_values = rail.EmptyOperator(
            task_id='dummy_udfs_dropdown_values'
        )

        get_reimburement_currency_values = rail.RepliconServiceOperator(
            task_id="get_reimburement_currency_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').reim_currency_uri }}"
            }
        )

        get_glc_values = rail.RepliconServiceOperator(
            task_id="get_glc_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').glc_uri }}"
            }
        )

        get_emp_status_values = rail.RepliconServiceOperator(
            task_id="get_emp_status_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').emp_status_uri }}"
            }
        )

        get_action_code_values = rail.RepliconServiceOperator(
            task_id="get_action_code_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').personal_action_code_uri }}"
            }
        )

        get_job_title_values = rail.RepliconServiceOperator(
            task_id="get_job_title_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').job_title_uri }}"
            }
        )

        get_polaris_roles_values = rail.RepliconServiceOperator(
            task_id="get_polaris_roles_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').polaris_roles_uri }}"
            }
        )

        get_line_of_business_values = rail.RepliconServiceOperator(
            task_id="get_line_of_business_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').line_of_business_uri }}"
            }
        )

        get_work_schedule_values = rail.RepliconServiceOperator(
            task_id="get_work_schedule_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').work_schedule_uri }}"
            }
        )

        get_pay_period_code_values = rail.RepliconServiceOperator(
            task_id="get_pay_period_code_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').pay_period_code_uri }}"
            }
        )

        get_oncall_allowance_values = rail.RepliconServiceOperator(
            task_id="get_oncall_allowance_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_user_udfs').oncall_allowance_uri }}"
            }
        )

        dummy_get_user_prereqs >> [get_updated_locations, get_updated_departments, get_updated_employee_types, get_products_groups,
            get_all_permission_set,get_all_policy_sets,get_timesheet_approval_paths, get_expense_approval_paths, get_timeoff_approval_paths,
            get_all_timezones, get_default_office_schedule,get_taxable_entities_data,get_pay_groups_data, get_all_holiday_calendars,
            get_all_timesheet_period_list,get_user_udfs, get_all_roles, get_all_pay_rules] >> dummy_udfs_dropdown_values >> [get_reimburement_currency_values,
            get_glc_values, get_emp_status_values, get_action_code_values, get_job_title_values, get_polaris_roles_values,
            get_line_of_business_values,get_work_schedule_values,get_pay_period_code_values,get_oncall_allowance_values]

    return dummy_get_user_prereqs, get_user_prereqs
