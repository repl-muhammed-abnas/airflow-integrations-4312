from pagero.vantagepoint_onesource_einvoicing_integration.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'Pagero' 

vantagepoint_conn_id = "pagero_vantagepoint_conn_id" 
http_conn_id = "pagero_onesource_http_conn_id"
replicon_conn_id = "pagero_replicon_conn_id"

client_id_secret_variable_name = "client_id_secret_variable_name"

onesource_company_name = "Deltek Vantagepoint Buyer TEST - UK"  # Company name to match in OneSource

master_dag_id = f'pagero_vantagepoint_onesource_einvoicing_integration_master_{instance}'
master_flow2_dag_id = f'pagero_vantagepoint_onesource_einvoicing_integration_flow_2_master_{instance}'
child_flow2_dag_id = f'pagero_vantagepoint_onesource_einvoicing_integration_flow_2_child_{instance}'
setup_dag_dagid = f'pagero_vantagepoint_onesource_einvoicing_integration_setup_dag_{instance}'

invoice_details_sp = 'OneSourceGetInvoice'

airflow_master_dag_trigger_url = f"pagero_vantagepoint_onesource_einvoicing_webhook_url_{instance}"

basic_auth_username_pagero = f"pagero_vantagepoint_onesource_einvoicing_webhook_username_{instance}"
basic_auth_pass_pagero = f"pagero_vantagepoint_onesource_einvoicing_webhook_pass_{instance}"

last_sync_time_var = f'pagero_vantagepoint_last_sync_time_{instance}'
