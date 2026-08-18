region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'

cut_off_date = "2023-07-01"

parallel_trigger_dagrun_count = 50
secondary_sftp_conn_id = 'sftp_internal'
secondary_output_filepath = '/DXC/C1WBS/logs/'
# pylint: disable=line-too-long
error_template = '{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") | attr_or_default(["response.body", "exc_message", ""], default="Unknown error occurred") }}'
disabled = True
