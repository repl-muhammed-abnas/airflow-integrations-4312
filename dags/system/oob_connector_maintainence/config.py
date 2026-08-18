region = ['us-east-1', 'eu-central-1', 'us-west-2']
environment = ['pre-production', 'production', 'devops']

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'

alert_email = 'MPTeamReplicon@deltek.com, {{ var.value.dagrun_failure_alert_email }}'

# Update this based on connector addition
prefix_name_mapping = {
    'bh': 'bamboohr',
    'jira': 'jira',
    'nam': 'namely',
    'qbo': 'quickbooks',
    'replicon': 'replicon',
    'sagefl': 'sagefinancials',
    'si': 'sageintacct',
    'sf': 'salesforce',
    'sn': 'servicenow',
    'xero': 'xero',
    'zd': 'zendesk',
    'ce': 'computerease'
}

filter_global_urls = ['https://global.replicon.com',
                      'https://global-so.replicon.com']

execution_timeout_minutes = 60
