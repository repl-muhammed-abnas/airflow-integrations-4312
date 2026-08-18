# pylint: disable=unused-import
# DEPRECATED: This file is being phased out in favor of dynamic configuration
# from the Integration Platform API. Connection IDs and configuration are now
# fetched dynamically from the API instead of being hardcoded.
#
# This file is kept for backward compatibility only.
from vp_ukgpro_integration.payroll_sync.config import (
    max_active_runs,
    execution_timeout_days,
    max_retries,
    retry_delay_minutes,
    batch_size,
    validate_employees_in_ukgpro,
    ukgpro_source,
    hours_code_mapping,
    airflow_connector_ui_connid
)

region = 'us-east-1'
environment = 'pre-production'
instance = "dev"
company_key = 'dev'

# DEPRECATED: These connection IDs are now fetched from Integration Platform API
# Kept here for reference only
vp_conn_id = 'vp_integrationtest_vp_conn'
ukgpro_conn_id = 'ukgpro_integrationtest_conn'
