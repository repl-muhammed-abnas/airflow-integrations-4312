# pylint: disable=wildcard-import unused-wildcard-import
# pylint: disable=line-too-long
from adtalem.user_import.config import *
from adtalem.user_import.mappers.trial.adtalem_sicktime_timeoffpolicy_schedule_trial_old import adtalem_sicktime_timeoffpolicy_schedule_trial_old
from adtalem.user_import.mappers.trial.adtalem_vacation_timeoffpolicy_schedule_trial_old import adtalem_vacation_timeoffpolicy_schedule_trial_old
from adtalem.user_import.mappers.trial.adtalem_sicktime_policies_existing_users_trial_old import adtalem_sicktime_policies_existing_users_trial_old
from adtalem.user_import.mappers.trial.adtalem_vacation_timeoffpolicy_schedule_existingusers_trial_old import adtalem_vacation_timeoffpolicy_schedule_existingusers_trial_old

instance = 'trial'
environment = 'pre-production'
company_key = 'adtalemafmig'

replicon_conn_id = 'adtalemafmig-replicon-integration.user'
sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
hr_email = '{{ var.value.dagrun_internal_testing_email }}'

input_filepath = '/Adtalem/Trial'

archive_filepath = '/Adtalem/Trial/Archive'
caribbean_archive_filepath = '/Adtalem/Trial/Carribeanarchive'

reference_filepath = '/Adtalem/Trial/Reference'
caribbean_reference_filepath = '/Adtalem/Trial/Carribeanref'

log_filepath = '/Adtalem/Trial/Logs'

adtalem_sicktime_timeoffpolicy_schedule_mapper_old = adtalem_sicktime_timeoffpolicy_schedule_trial_old
adtalem_vacation_timeoffpolicy_schedule_mapper_old = adtalem_vacation_timeoffpolicy_schedule_trial_old
adtalem_sicktime_policies_existing_users_mapper_old = adtalem_sicktime_policies_existing_users_trial_old
adtalem_vacation_timeoffpolicy_schedule_existingusers_old = adtalem_vacation_timeoffpolicy_schedule_existingusers_trial_old
first_timeofftype_in_instance = 'urn:replicon-tenant:665e77c532a243058451c82fb11b3452:time-off-type:1'
policy_set_uri = 'urn:replicon-tenant:665e77c532a243058451c82fb11b3452:policy-set:9bc1c1de-f36f-4b3b-ad71-54455ff2434a'
user_report_uri = 'urn:replicon-tenant:665e77c532a243058451c82fb11b3452:report:59b61b6b-929d-491a-bb2d-f9c2453b6193'
user_report_filter_uri = 'urn:replicon-tenant:665e77c532a243058451c82fb11b3452:report-filter:23202b56aef8436bb8c5d612119a6ef7;userfilter'


can_run_batch_task_var_name = f'adtalem_user_import_{instance}_can_run_batch_task'

can_process_us_canada_import = f'adtalem_user_import_{instance}_can_process_us_canada'

disabled=True
