# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.timesheet_auto_submission_v1.config import *

instance = 'sandbox'
environment = 'pre-production'

company_key = 'seaspanvslsb'

replicon_conn_id = 'seaspanvslsb_replicon_repliconint'

tenant_email = "keerthanahr@deltek.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

automatic_timesheet_submission_child_dagid = f"victoriashipyards_automatic_timesheets_submission_child_dag_{instance}_v1"
recalculate_timesheet_child_dagid = f"victoriashipyards_recalculate_timesheets_child_dag_{instance}_v1"
shift_change_master_dagid = f"victoriashipyards_timesheet_auto_submission_shift_change_master_{instance}_v1"
time_punch_master_dagid = f"victoriashipyards_timesheet_auto_submission_time_punch_master_dag_{instance}_v1"
time_punch_master_6_35_am_dagid = f"victoriashipyards_timesheet_auto_submission_time_punch_master_6_35_am_dag_{instance}_v1"
