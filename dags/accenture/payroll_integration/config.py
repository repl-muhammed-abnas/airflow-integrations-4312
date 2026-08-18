region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Vantagepoint hub and table names for ADP GV Payroll File
vantagepoint_hub = 'UDIC/UDIC_ADPGVPayrollFile'
file_header_table = 'UDIC_ADPGVPayrollFile_CustFileHeader'
file_details_table = 'UDIC_ADPGVPayrollFile_CustFileDetails'
file_footer_table = 'UDIC_ADPGVPayrollFile_CustFileFooter'

# SFTP details
sftp_remote_dir = '/put'

# Output template
payroll_template_file = 'schema/g2_payroll_export.txt'
