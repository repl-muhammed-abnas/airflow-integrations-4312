region = 'us-east-2'
environment = 'pre-production'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
input_filepath = '/Test/Inbound/COMPASSWBSMaster/NT1/Input'
archive_filepath = '/Test/Inbound/COMPASSWBSMaster/NT1/Archive'
log_filepath = '/Test/Inbound/COMPASSWBSMaster/NT1/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
debug = False
instance = 'NT1_sandbox'
program_dag_max_active_runs = 200
client_dag_max_active_runs = 200
dag_max_active_tasks = 10000
execution_timeout_days = 14
max_active_runs=16

child_max_active_runs = 200

trigger_parallel_dagrun_count = 10

if debug:
    replicon_conn_id = 'dxctrial01'
    input_filepath = 'import'
    archive_filepath = 'archive/CompassWBS'
    log_filepath = 'logs/CompassWBS'
disabled = True
