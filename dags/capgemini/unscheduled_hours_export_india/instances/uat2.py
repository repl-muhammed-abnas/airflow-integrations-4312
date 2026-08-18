# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.unscheduled_hours_export_india.config import *

instance = 'uat2'
environment = 'pre-production'

company_key = 'capgeminiuat2'

replicon_conn_id = 'capgeminiuat2_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgemini_uhr_india_sa_export'

upload_filepath = "/Outbound/Unschedule_HoursUAT2/Input"

FILENAME_PREFIX = "UNSCHEDULED_UAT2"

tenant_email = 'gtmclusterleads.cor@capgemini.com,dctechnicalteam.in@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

master_dag_id = f'capgemini_unscheduled_hours_india_export_master_{instance}'
