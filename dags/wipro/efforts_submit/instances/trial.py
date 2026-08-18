# pylint: disable=wildcard-import unused-wildcard-import
from wipro.efforts_submit.config import *
from wipro.efforts_submit.country_mapper.country_mapper_list import country_list_trial
instance = "trial"
environment = "pre-production"
company_key = "Wiprosandbox2"
replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"
wipro_efforts_submission_bearer_token_variable = "wipro_efforts_submission_bearer_token_variable_trial"
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'
time_export_for_country = [ k  for k,v in country_list_trial.items() ]
time_export_for_country_code = country_list_trial
disabled=True

