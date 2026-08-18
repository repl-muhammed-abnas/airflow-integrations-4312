region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'

secondary_sftp_conn_id = 'dxctechnology-ftp'
secondary_output_filepath ='/DXC/USCSC_Payrollexport/'
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'

can_upload_to_tertiary_sftp = False

tertiary_encrypted_filepath =''
tertiary_log_filepath = ''
tertiary_sftp_conn_id =''
tertiary_pgp_conn_id =''
