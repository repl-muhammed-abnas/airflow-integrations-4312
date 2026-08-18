region = 'all'
environment = 'all'
dag_config_var_name = 'system_check_twb_draft_status_replicon_conn_ids'
draft_status_alert_child_dag_id = 'system_timeworkbench_draft_status_monitor_child'
max_active_runs_child_dag = 5
tenant_email = '{{ var.value.dagrun_failure_alert_email }}'
schedule_interval = "0 */3 * * *"
