from mercury_systems_inc.gl_labor_cost_export.config import *

instance = "trial"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercury_systems_inc_replicoint"

sftp_conn_id = "sftp_useast2"

sftp_export_file_path = "/MercurySytemsInc/TimeExportGLLaborCost/Test/input/"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

master_dag_id = f'mercury_systems_inc_gl_labor_cost_export_{instance}'

