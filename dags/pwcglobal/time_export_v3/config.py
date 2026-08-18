region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14

pacific_timezone = 'America/Los_Angeles'

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

dag_max_active_tasks = 128
master_dag_max_active_runs = 1
child_user_batch_max_active_runs = 8
child_current_past_period_dag_max_active_runs = 8
post_batch_size = 800
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'

# copy the below lines to the respective instance config if secondary sftp needs to be set up
secondary_sftp = False  # set this to True to enable secondary_sftp
api_failed_upload_filepath = "/PwCGBL_RepliconGlobal_Internal/TimeData/APIInternal"

if secondary_sftp:
    secondary_sftp_conn_id = 'pwcinternal-ftp'
    secondary_upload_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time"
    secondary_log_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Outbound/Time"

# for any custom max active runs for any location, add the location name in below list
location_configured_for_custom_max_active_runs = ["deu"]

# define custom max active run in below
custom_max_active_run_child_each_location = {
    "deu": 16
}

default_max_active_run_other_locations = 10
