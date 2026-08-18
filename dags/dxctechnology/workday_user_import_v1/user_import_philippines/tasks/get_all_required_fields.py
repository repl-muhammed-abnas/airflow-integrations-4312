import itertools
import rail

from dxctechnology.workday_user_import_v1.user_import_philippines.utils.response_filter import (
    get_location_response_filter,
    get_employeegroup_response_filter,
    get_companycode_response_filter,
    get_all_user_custom_fields_data_handler,
    get_prevent_balance_overdraw_script_data_handler,
    get_starting_balance_script_data_handler
)
def get_all_required_fields(_group_id, config):
    with rail.TaskGroup(group_id=_group_id, prefix_group_id=False):

        start = rail.EmptyOperator(
            task_id="start"
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id="get_all_activities",
            endpoint="/services/ActivityService1.svc/GetAllActivities"
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id = "get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        def get_all_locations_payload():
            return {
                "page": "1",
                "pagesize": "100000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "bool": "true",
                        }
                    }
                }
            }


        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationListService1.svc/GetData",
            data=get_all_locations_payload,
            data_handler=get_location_response_filter
        )

        
        def get_all_employeegroup_payload():
            return {
                "page": "1",
                "pagesize": "100000000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "bool": "true",
                        }
                    }
                }
            }


        get_all_employeegroup_data = rail.RepliconServiceOperator(
            task_id= "get_all_employeegroup_data",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=get_all_employeegroup_payload,
            data_handler=get_employeegroup_response_filter
        )

        def get_all_companycode_payload():
            return {
                "page": "1",
                "pagesize": "100000000",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "bool": "true"
                        }
                    }
                }
            }

        get_all_companycode_data = rail.RepliconServiceOperator(
            task_id= "get_all_companycode_data",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=get_all_companycode_payload,
            data_handler=get_companycode_response_filter
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            
            return list(map(lambda row: {
                'name': row['cells'][1]['textValue'],
                'uri': row['cells'][1]['uri'],
                'slug': row['cells'][1]['slug'],
                'policy_uri': row['cells'][0]['uri'],
                'policy_name': row['cells'][0]['textValue'],
            }, flaten_rows))


        get_all_policy_sets = rail.RepliconServicePageOperator(
            task_id = "get_all_policy_sets",
            endpoint="/services/PolicySetListService1.svc/GetData",
            data = {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:policy-set-list-column:policy",
                    "urn:replicon:policy-set-list-column:policy-set"
                ],
                "sort": [],
                "filterExpression": None
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler,
            flatten=True
        )
        get_all_holiday_calendar = rail.RepliconServiceOperator(
            task_id = "get_all_holiday_calendar",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )
        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id = "get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        def all_result_data_handler_cost_center_departments(result, caller="departments"):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            res = []
            for item in flaten_rows:
                res.append(
                    {
                        "displayText": item['cells'][0].get('textValue', ''),
                        "uri": item['cells'][0].get('uri', ''),
                        "full_path": "***||***".join([path['textValue'] for path in item['cells'][1]['cellCollection']]),
                        "enable": item['cells'][2].get('textValue', ''),
                        "parent": {
                            **{"parent_available": "yes"},
                            **item['cells'][1]['cellCollection'][-2]
                        } if len(item['cells'][1]['cellCollection']) > 1 else {"parent_available": "no"}
                    }
                )

            if caller == "departments":
                return [val for val in res if val['enable'].lower() == "true"]

            return res

        get_all_cost_centers = rail.RepliconServicePageOperator(
            task_id = "get_all_cost_centers",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data = {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path",
                    "urn:replicon:cost-center-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": None
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler_cost_center_departments(result, "cost_center"),
            flatten=True
        )

        get_all_enabled_departments = rail.RepliconServicePageOperator(
            task_id = "get_all_enabled_departments",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data = {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": None
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler_cost_center_departments,
            flatten=True
        )

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id = "get_all_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: get_all_user_custom_fields_data_handler(config, response)
        )

        get_starting_balance_script = rail.RepliconServiceOperator(
            task_id="get_starting_balance_script",
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=get_starting_balance_script_data_handler
        )

        get_prevent_balance_overdraw_script = rail.RepliconServiceOperator(
            task_id="get_prevent_balance_overdraw_script",
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=get_prevent_balance_overdraw_script_data_handler
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        start >> [ get_all_activities, get_all_permission_sets, get_all_locations,
                  get_all_employeegroup_data, get_all_companycode_data, get_all_policy_sets, get_all_holiday_calendar,
                  get_all_office_schedules, get_all_cost_centers, get_all_enabled_departments, get_all_user_custom_fields,
            get_starting_balance_script,get_prevent_balance_overdraw_script] >> finish
        return start, finish
