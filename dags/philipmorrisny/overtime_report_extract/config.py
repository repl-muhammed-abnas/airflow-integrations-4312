region = 'us-east-1'
environment = 'production'
company_key = 'philipmorrisny'

replicon_conn_id= 'philipmorrisny-replicon-achauhan'

overtime_report_name = 'Overtime Report'

overtime_log_report_name = 'Overtime Log Report'

sftp_conn_id = 'philipmorrisny-sftp-509694'

schedule_interval = '30 7 * * MON'
mountain_timezone = 'America/Denver'

master_dag_max_active_runs = 1

alert_email = '{{ var.value.dagrun_internal_log_email }}'

tenant_email = 'DLUSHRServices-Payroll@pmi.com,COSSDPMIPayrollTeam@ADP.com,Emilia.Melis@pmi.com,Lucia.Riolffi@pmi.com,Sol.Velasco@pmi.com'
