region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'replicon-dxctechnology-ftp'
sftp_conn_id = 'dxctechnology-ftp'
gsap_report_projectfilter_name = 'ProjectFilter'
max_concurrent_wbs_imports = 5
max_concurrent_billingkey_task_imports = 16
max_concurrent_gsap_task_imports = 16
# dxcintegrationlogsreplicon@deltek.com
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'
debug = False
gsap_report_name = 'Replicon_Integration_GSAPbillingKeytask_basereport'
disabled = True
