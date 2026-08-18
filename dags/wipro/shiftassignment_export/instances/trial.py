# pylint: disable=wildcard-import unused-wildcard-import
from wipro.shiftassignment_export.config import *

instance = "trial"
environment = "pre-production"
company_key = "Wiprosandbox2"

replicon_conn_id = "Wiprosandbox2_replicon_repliconint"

sftp_conn_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

shift_data_upload_path = "/wipro/shiftassignment_export/trial"

countries_to_process = ['Portugal', 'Poland', 'Netherlands',
                        'Saudi Arabia', 'Romania', 'United Kingdom', 'Ireland', 'Spain']

shift_assignment_export_master = f"wipro_shift_assignment_export_master_{instance}"
shift_assignment_export_child = f"wipro_shift_assignment_export_child_{instance}"
can_run_batch_task = "can_run_batch_task_for_wipro_shift_assignment_export"
