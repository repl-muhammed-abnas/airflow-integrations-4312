# pylint: disable=wildcard-import unused-wildcard-import
from repliconinc.timeoff_bookings_notifications.config import *

instance = "deltekps"
environment = "production"

company_key = "deltekps"
replicon_conn_id = "deltekps_replicon_replicon.integration"

FROM_EMAIL_ADDR = "TCoE-APAC-Team@deltek.com"
TO_EMAIL_ADDR = "TCoE-APAC-Team@deltek.com,50eff829.deltekO365.onmicrosoft.com@amer.teams.ms"
CC_EMAIL_ADDR = "RaghuKandaswamy@deltek.com"

main_dag_id = f"replicon_timeoff_bookings_notifications_{instance}"

company_identifier = "Deltek Polaris"

timeoff_bookings_report_uri = "urn:replicon-tenant:d374ced1e022452981b5b3cc295e1651:report:1b495c96-bab5-4189-9880-0760fbfbec14"
date_range_filter_uri = "urn:replicon-tenant:d374ced1e022452981b5b3cc295e1651:report-filter:9be6ba3ae10e4f01a0f250f4f06c6ddb;daterangefilter"
