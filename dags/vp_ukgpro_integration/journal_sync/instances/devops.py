"""
Devops instance configuration for VP UKG Pro Journal Sync.
Defines instance-specific settings for the Devops environment.
"""
# pylint: disable=line-too-long,unused-import,invalid-name,import-error
from vp_ukgpro_integration.journal_sync.config import (
    max_active_runs,
    execution_timeout_days,
    initial_sync_time
)
instance = "devops"
region = 'us-west-2'
environment = 'devops'
company_key = f"airflowdevops{region.replace('-', '')}"
notification_email = 'MPTeamReplicon@deltek.com'
middleware_conn_id = f'middleware_conn_{instance}'
