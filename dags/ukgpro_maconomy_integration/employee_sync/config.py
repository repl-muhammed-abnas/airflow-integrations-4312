"""
Shared configuration constants for UKG Pro → Maconomy Employee Sync integration.
"""
# pylint: disable=invalid-name

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1
initial_sync_time = '2025-01-01T00:00:00.000Z'

tenant_email = 'MPTeamReplicon@deltek.com'

# Instance defaults — overridden in each instance file
ukgpro_conn_id = ''
maconomy_conn_id = ''
schedule_interval = '*/30 * * * *'
disabled = False

# Employee create defaults — override per instance if customer policy differs
employee_salesemployee = True
employee_accountmanager = True
employee_mustusetimesheets = True
employee_maxworkingtimeperday = 24
employee_standardbillingprice = 0
