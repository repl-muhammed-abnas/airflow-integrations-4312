# Company Information
region = "us-east-1"
environment = "pre-production"
version = "v1"

max_active_run_master = 1
max_active_runs_process_department_groups = 1

execution_timeout_days = 14

trigger_parallel_dagrun_count_process_department_groups = 2

file_sensor_timeout = 10

# Service Endpoints
get_hierarchy_data_endpoint = "/services/DepartmentGroupListService1.svc/GetHierarchyData"
create_or_apply_modification_department_endpoint = "/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification"