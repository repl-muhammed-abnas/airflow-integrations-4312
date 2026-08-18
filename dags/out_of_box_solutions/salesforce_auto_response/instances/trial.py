instance = "trial"

region = 'us-east-1'
environment = 'pre-production'

company_key = 'airflow'

replicon_conn_id = 'airflow-replicon-admin'
# Uses Salesforce Connection Type
salesforce_connection_id = "workatointegration@replicon.com_salesforce_connection"

master_dag_id = f"salesforce_integration_queue_auto_response_master_{instance}"
child_dag_id = f"salesforce_integration_queue_auto_response_process_each_case_{instance}"
dag_description = 'Salesforce Auto Response - Integration Queue'

CREATED_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
FROM_ADDR = "integrations@replicon.com"

schedule_interval = "*/5 * * * *"

disable=True

disabled=True
