
region = 'all'
environment = 'all'

TO_EMAIL_ADDR = '{{ var.value.dagrun_failure_alert_email }}'
CC_EMAIL_ADDR = "sammedkawade@deltek.com,raghukandaswamy@deltek.com"
FROM_EMAIL_ADDR = "dice.alerts@deltek.com"

replicon_conn_id = "airflow-replicon-admin"


main_dag_id = "airflow_get_dag_id_and_current_max_active_runs"
