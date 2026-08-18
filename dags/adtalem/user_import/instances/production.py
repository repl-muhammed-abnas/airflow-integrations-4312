# pylint: disable=wildcard-import unused-wildcard-import
# pylint: disable=line-too-long
from adtalem.user_import.config import *
from adtalem.user_import.mappers.production.adtalem_sicktime_timeoffpolicy_schedule_prod_old import adtalem_sicktime_timeoffpolicy_schedule_prod_old
from adtalem.user_import.mappers.production.adtalem_vacation_timeoffpolicy_schedule_prod_old import adtalem_vacation_timeoffpolicy_schedule_prod_old
from adtalem.user_import.mappers.production.adtalem_sicktime_policies_existing_users_prod_old import adtalem_sicktime_policies_existing_users_prod_old
from adtalem.user_import.mappers.production.adtalem_vacation_timeoffpolicy_schedule_existingusers_prod_old import adtalem_vacation_timeoffpolicy_schedule_existingusers_prod_old

instance = 'production'
environment = 'production'
company_key = 'adtalem'

replicon_conn_id = 'adtalem-replicon-integration.user'
sftp_conn_id = 'sftp_Integration_useast_prod'

tenant_email = 'JoAnna.DelaPaz@adtalem.com,jill.okolita@adtalem.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
hr_email = '{{ var.value.dagrun_internal_log_email }}'

input_filepath = '/Adtalem/userimport/caribbean/input'

archive_filepath = '/Adtalem/userimport/caribbean/archive'
caribbean_archive_filepath = '/Adtalem/userimport/caribbean/archive'

reference_filepath = '/Adtalem/Production/Reference'
caribbean_reference_filepath = '/Adtalem/userimport/caribbean/reference'

log_filepath = '/Adtalem/userimport/caribbean/logs'

adtalem_sicktime_timeoffpolicy_schedule_mapper_old = adtalem_sicktime_timeoffpolicy_schedule_prod_old
adtalem_vacation_timeoffpolicy_schedule_mapper_old = adtalem_vacation_timeoffpolicy_schedule_prod_old
adtalem_sicktime_policies_existing_users_mapper_old = adtalem_sicktime_policies_existing_users_prod_old
adtalem_vacation_timeoffpolicy_schedule_existingusers_old = adtalem_vacation_timeoffpolicy_schedule_existingusers_prod_old
first_timeofftype_in_instance = 'urn:replicon-tenant:a2049fb8760a405bbd15567e98063910:time-off-type:1'
policy_set_uri = 'urn:replicon-tenant:a2049fb8760a405bbd15567e98063910:policy-set:9bc1c1de-f36f-4b3b-ad71-54455ff2434a'
user_report_uri = 'urn:replicon-tenant:a2049fb8760a405bbd15567e98063910:report:59b61b6b-929d-491a-bb2d-f9c2453b6193'
user_report_filter_uri = 'urn:replicon-tenant:a2049fb8760a405bbd15567e98063910:report-filter:23202b56aef8436bb8c5d612119a6ef7;userfilter'

can_run_batch_task_var_name = f'adtalem_user_import_{instance}_can_run_batch_task'

can_process_us_canada_import = f'adtalem_user_import_{instance}_can_process_us_canada'
