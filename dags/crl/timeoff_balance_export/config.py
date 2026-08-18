region = 'us-east-1'
environment = 'pre-production'

process_child_dag_max_active_runs = 100
parallel_trigger_dagrun_count = 50
secondary_sftp_conn_id = 'rsftp-useast2_for_testing'
secondary_output_filepath = '/CRLTrial/interfaces/PQ3/GVE220/put'

export_location = "CAN"

timeoff_report_name = "TimeOff Balance-Sick and Banked"

# pylint: disable=line-too-long
error_template = '{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") | attr_or_default(["response.body", "exc_message", ""], default="Unknown error occurred") }}'
