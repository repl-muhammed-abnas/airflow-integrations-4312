# Shared configuration for resource_planner_api_health_check DAG
region = 'us-east-1'
environment = 'production'

max_active_runs = 1
schedule_interval = None  # enable per-instance

# Health endpoint — no DB dependency, returns {"status": "ok"} immediately
health_check_endpoint = "/health"

# Alert recipients (override per-instance)
email_alert_recipients = []
