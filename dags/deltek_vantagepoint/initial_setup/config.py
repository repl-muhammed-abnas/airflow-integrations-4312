region = 'us-east-1'
environment = 'pre-production'
date_format_for_file_format = "MMM' 'dd', 'yyyy"
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
