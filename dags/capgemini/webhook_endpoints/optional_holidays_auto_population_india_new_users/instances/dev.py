instance = 'dev'
region = 'eu-central-1'
environment = 'pre-production'
company_key = 'capgeminidev'
replicon_conn_id = 'capgeminidev_replicon_optional_holiday_admin'
max_active_runs_new_users = 10
webhook_shared_secret = f'capgemini_optional_holiday_booking_new_user_webhook_secret_{instance}_v0'
tenant_wide_log = f'{company_key}_new_users_log'
old_tenant_wide_log = f'capgemini_auto_population_of_optional_holidays_new_users_tenant_wide_log_{instance}_v0'
tenant_log = f"artifact:CapgeminiDev:log:{tenant_wide_log}"
old_tenant_log = f"artifact:CapgeminiDev:log:{old_tenant_wide_log}"
tenant_wide_log_list = [old_tenant_log,
                        f"{tenant_log}_0",
                        f"{tenant_log}_1",
                        f"{tenant_log}_2",
                        f"{tenant_log}_3",
                        f"{tenant_log}_4"
                    ]
webhook_dagid = f'capgemini_auto_population_of_optional_holidays_india_new_users_webhook_master_{instance}_v0'
webhook_logging_child_dagid = f'capgemini_auto_population_of_optional_holidays_india_new_users_webhook_logging_child_{instance}'
log_creation_setup_dagid = f'capgemini_auto_population_of_optional_holidays_india_new_users_log_creation_setup_{instance}_v0'
