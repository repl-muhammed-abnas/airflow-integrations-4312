# pylint: disable=wildcard-import unused-wildcard-import
from wipro.efforts_submit.config import *
from wipro.efforts_submit.country_mapper.country_mapper_list import country_list_prod
instance = "production"
environment = "production"
company_key = "WiproLimited"
replicon_conn_id = "wiprolimited_replicon_repliconint"
wipro_efforts_submission_bearer_token_variable = "wipro_efforts_submission_bearer_token_variable_production"
alert_mail = "replicon.logs.ext@wipro.com"
time_export_for_country = [ k  for k,v in country_list_prod.items() ]
time_export_for_country_code = country_list_prod
