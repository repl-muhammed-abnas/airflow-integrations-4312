region = 'us-east-1'
environment = 'pre-production'
company_key = 'seaspanshipyardsafmig'

pacific_timezone = 'PST8PDT'

replicon_conn_id = 'seaspanshipyardsafmig_replicon_admin'
seaspanshipyards_sftp_conn_id = 'seaspanshipyardsafmig_sftp_admin'

reference_filepath = '/Shiftautomation/Newusershiftassignment/Prod/referencefile/Prod_referenceusers.csv'

user_shift_report_name = "**Shift Assignment Report**"

schedule_interval = "0 19 * * *"
execution_timeout_days = 14
dag_max_active_runs = 5
dag_max_active_tasks = 128

master_dag_active_runs = 1
