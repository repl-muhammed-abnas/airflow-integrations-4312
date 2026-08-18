# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.time_export_v1.config import *

instance = "prod"

environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"

# connection name will be updated once the Conn. details are provided
sftp_conn_id = "sftp_mammoet_550793"
http_conn_id = f'mammoet_timedata_export_http_conn_{instance}'

tenant_email = 'RepliconNotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

timeexport_upload_input_filepath = "/Production/Time Data Export to S4/Input"
timeexport_upload_backup_filepath = "/Production/Time Data Export to S4/Backup"

time_export_process_missing_replicon_ids_master_dag_id = f"mammoet_time_export_process_missing_replicon_ids_master_dag_id_{instance}_v1"
time_export_process_export_dag_id = f"mammoet_time_export_master_{instance}_v1"
time_export_process_timesheets_dag_id = f"mammoet_time_export_process_timesheets_child_{instance}_v1"
time_export_process_timesheets_time_entries_dag_id = f"mammoet_time_export_process_timesheets_time_entries_child_{instance}_v1"
time_export_post_export_dag_id = f"mammoet_time_export_process_post_data_to_endpoint_{instance}_v1"


client_id_secret_variable_name = f"mammoet_client_id_secret_variable_{instance}"

"""
To have the export in inline with the Client side integration, updating the schedule
After the update the schedule will look like this 	 	 	 	 
 	Replicon to SAP	 |	 ...    | 3:00 AM | 7:00 AM | 11:00 AM | 3:00 PM | 7:00 PM | 11:00 PM
 	SAP to Replicon	 |	1:00 AM | 5:00 AM | 9:00 AM | 01:00 PM | 5:00 PM | 9:00 PM | ... 
"""
daily_run_schedule_interval = "0 3,7,11,15,19,23 * * *"
