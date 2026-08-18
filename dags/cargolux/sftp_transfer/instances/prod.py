
from datetime import timedelta
from cargolux.sftp_transfer.config import * 

company_key = "Cargolux"
instance = "production"
environment = "production"

source_sftp_conn_id = f"sftp_cargolux_655611"
dest_sftp_conn_id = f"sftp_cargolux_car21041"

master_dag_id = f'Cargolux_sftp_transfer_master_{instance}'
child_dag_id = f'Cargolux_sftp_transfer_child_{instance}'

source_sftp_input_path = "/PAYROLL"  
source_sftp_log_path = "/replicon/payroll/logs/"  

dest_sftp_output_path = "/HORIZON/car21041/to_tmf/"  
file_pattern = "*" 
