
region = 'all'
environment = 'all'

TO_EMAIL_ADDR = '{{ var.value.dagrun_failure_alert_email }}'
CC_EMAIL_ADDR = "sammedkawade@deltek.com,raghukandaswamy@deltek.com"
FROM_EMAIL_ADDR = "dice.alerts@deltek.com"

replicon_conn_id = "airflow-replicon-admin"
activate_deactivate_automation_var_name = "airflow_activate_deactivate_dags_automation_dag_details_list_variable"


deactivate_all_dags_dag_id = "airflow_pause_all_active_dags"
reactivate_all_dags_dag_id = "airflow_unpause_all_previously_paused_dags"
