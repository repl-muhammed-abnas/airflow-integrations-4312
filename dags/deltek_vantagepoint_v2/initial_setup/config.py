region = 'us-east-1'
environment = 'pre-production'
date_format_for_file_format = "MMM' 'dd', 'yyyy"

execution_timeout_days = 14
max_active_runs = 1

provider = 'vantagepoint'
workflow = 'initial_setup'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'

webhook_event_id_var = 'vp_employee_webhook_event_id'
user_sync_filter_pending_var = 'vp_user_sync_filter_update_pending'

default_group_configs = {
        "department": {
            "type": "department",
            "renameendpoint": "/services/DepartmentGroupService1.svc/PutDepartmentGroupsSystemSettings",
            "optionsendpoint": "/services/DepartmentGroupListService1.svc/GetData",
            "getdataservicevariable": "department-group",
            "grouptypevariable": "departmentGroup",
            "disableendpoint": "/services/DepartmentGroupService1.svc/Disable",
            "creategroupendpoint": "/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            "columnuri": "urn:replicon:user-list-column:department-group"
        },
        "employeetype": {
            "type": "employeetype",
            "renameendpoint": "/services/EmployeeTypeGroupService1.svc/PutEmployeeTypeGroupsSystemSettings",
            "optionsendpoint": "/services/EmployeeTypeGroupListService1.svc/GetData",
            "getdataservicevariable": "employee-type-group",
            "grouptypevariable": "employeeTypeGroup",
            "disableendpoint": "/services/EmployeeTypeGroupService1.svc/Disable",
            "creategroupendpoint": "/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            "columnuri": "urn:replicon:user-list-column:employee-type-group",
        },
        "location": {
            "type": "location",
            "renameendpoint": "/services/LocationService1.svc/PutLocationsSystemSettings",
            "optionsendpoint": "/services/LocationListService1.svc/GetData",
            "getdataservicevariable": "location",
            "grouptypevariable": "location",
            "disableendpoint": "/services/LocationService1.svc/Disable",
            "creategroupendpoint": "/services/LocationService1.svc/CreateLocationOrApplyModification",
            "columnuri": "urn:replicon:user-list-column:location",
        },
        "division": {
            "type": "division",
            "renameendpoint": "/services/DivisionService1.svc/PutDivisionsSystemSettings",
            "optionsendpoint": "/services/DivisionListService1.svc/GetData",
            "getdataservicevariable": "division",
            "grouptypevariable": "division",
            "disableendpoint": "/services/DivisionService1.svc/Disable",
            "creategroupendpoint": "/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            "columnuri": "urn:replicon:user-list-column:division",
        },
        "servicecenter": {
            "type": "servicecenter",
            "renameendpoint": "/services/ServiceCenterService1.svc/PutServiceCentersSystemSettings",
            "optionsendpoint": "/services/ServiceCenterListService1.svc/GetData",
            "getdataservicevariable": "service-center",
            "grouptypevariable": "serviceCenter",
            "disableendpoint": "/services/ServiceCenterService1.svc/Disable",
            "creategroupendpoint": "/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            "columnuri": "urn:replicon:user-list-column:service-center",
        },
        "costcenter": {
            "type": "costcenter",
            "renameendpoint": "/services/CostCenterService1.svc/PutCostCentersSystemSettings",
            "optionsendpoint": "/services/CostCenterListService1.svc/GetData",
            "getdataservicevariable": "cost-center",
            "grouptypevariable": "costCenter",
            "disableendpoint": "/services/CostCenterService1.svc/Disable",
            "creategroupendpoint": "/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            "columnuri": "urn:replicon:user-list-column:cost-center",
        }
    }

groups = [
    {
        "id": "homecompany",
        "name": "Home Company",
        "plural": "Home Companies",
        "input": "HomeCompany",
        "assignby": "code",
        **default_group_configs.get('department')
    },
    {
        "id": "paytype",
        "name": "Employee Type",
        "plural": "Employee Types",
        "input": "PayType",
        "assignby": "name",
        **default_group_configs.get('employeetype')
    }
]

workflow_event_payload_insert = {
    'ApplicationName': 'EmployeeICBO',
    'EventType': 'Insert',
    'Active': 'Y',
    'Description': 'Please don\'t modify the OOB Rep-Vp'
}

workflow_event_payload_change = {
    'ApplicationName': 'EmployeeICBO',
    'EventType': 'Change',
    'Active': 'Y',
    'Description': 'Please don\'t modify the OOB Rep-Vp'
}

workflow_event_payload_project_insert = {
    'ApplicationName': 'ProjectICBO',
    'EventType': 'Insert',
    'Active': 'Y',
    'PRLevel': 0,
    'Description': 'Please don\'t modify the OOB Rep-Vp'
}

workflow_event_payload_project_change = {
    'ApplicationName': 'ProjectICBO',
    'EventType': 'Change',
    'Active': 'Y',
    'PRLevel': 0,
    'Description': 'Please don\'t modify the OOB Rep-Vp'
}

webhook_username = 'rep_vp_webhook_user'
webhook_password = 'Deltek@123'

project_webhook_args = [
    {'ArgName': 'WBS1', 'SQLExpression': "'[:PR.WBS1]'", 'ArgOrder': 1},
    {'ArgName': 'Name', 'SQLExpression': "'[:PR.Name]'", 'ArgOrder': 2},
    {'ArgName': 'WBS2', 'SQLExpression': "'[:PR.WBS2]'", 'ArgOrder': 3},
    {'ArgName': 'WBS3', 'SQLExpression': "'[:PR.WBS3]'", 'ArgOrder': 4},
    {'ArgName': 'StartDate', 'SQLExpression': "'[:PR.StartDate]'", 'ArgOrder': 5},
    {'ArgName': 'EndDate', 'SQLExpression': "'[:PR.EndDate]'", 'ArgOrder': 6},
    {'ArgName': 'Status', 'SQLExpression': "'[:PR.Status]'", 'ArgOrder': 7},
    {'ArgName': 'Action', 'SQLExpression': 'event_type_action', 'ArgOrder': 8},
    {'ArgName': 'ReadyForProcessing', 'SQLExpression': "'[:PR.ReadyForProcessing]'", 'ArgOrder': 9},
    {'ArgName': 'ChargeType', 'SQLExpression': "'[:PR.ChargeType]'", 'ArgOrder': 10}
]
