# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.unscheduled_hours_export_india.config import *

instance = 'dev'
environment = 'pre-production'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_capgeminidev_uhr_india_sa_export'

upload_filepath = "/Outbound/Unschedule_Hours/Input"

FILENAME_PREFIX = "UNSCHEDULED_DEV"

tenant_email = 'gtmclusterleads.cor@capgemini.com,dctechnicalteam.in@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

master_dag_id = f'capgemini_unscheduled_hours_india_export_master_{instance}'

