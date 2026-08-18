region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = f"airflow{region.replace('-', '')}"
replicon_conn_id = 'airflow-replicon-admin'
webhook_secret = 'airflow_connector_ui_hmac_secret'
internal_emails = 'MPTeamReplicon@deltek.com,integrationalerts@replicon.com'

max_active_runs = 10
