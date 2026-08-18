region = 'us-east-1'
environment = 'pre-production'
company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
sftp_conn_id = "repliconsftp"


user_report_name = "***User report***"
location_dag_max_active_runs = 1
costcenter_dag_max_active_runs = 1
department_dag_max_active_runs = 1
max_active_run_groups_child = 1
max_active_run_zero_line_policy_child = 5
dag_max_active_tasks = 128
master_dag_max_active_runs = 1
max_active_runs_process_time_off_assignment_update_user = 1
max_active_runs_process_time_off_policy_new_user = 1
max_active_runs_disable_user_child = 3

disable_schedule = '@daily'
user_dag_max_active_runs = 10
master_dag_interval = 30
file_sensor_timeout = 10
delimiter = '|'

execution_timeout_hours = 12
execution_timeout_days = 14
parallel_trigger_run_count = 10

child_dag_process_user_schedule_runs = 128

pgp_conn_id = "pgp_vialto_partners"

mandatory_columns_worker = ['EmployeeID', 'HireDate', 'WorkEmail',
                            'Company', 'CompanyCode', 'Country',
                            'CostCenterName', 'CostCenterID', 'JobCategory',
                            'WorkerType', 'EmployeeType', 'PositionID',
                            'ManagementLevel', 'LegalFirstName', 'LegalLastName']
mandatory_columns_employee = ['EmployeeID', 'HireDate', 'WorkEmail',
                              'Company', 'CompanyCode', 'Country',
                              'CostCenterName', 'CostCenterID', 'JobCategory',
                              'WorkerType', 'EmployeeType', 'PositionID',
                              'ManagementLevel', 'LegalFirstName', 'LegalLastName', 'CompensationGrade']

can_run_batch_task_var_name = "can_run_batch_task_var_name"
