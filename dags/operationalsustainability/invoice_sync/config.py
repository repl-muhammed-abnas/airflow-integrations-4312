region = 'us-east-1'
environment = 'pre-production'

master_schedule_interval = 5
execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child = 5

time_zone = 'UTC'

create_customer_in_quickbooks_if_missing = True

# Invoice sync configuration
invoice_sync_status_to_process = 'Queued for Synced'
invoice_status_filter = 'In Draft'
expense_processing_fee_item_name = 'Expense Processing Fee'
invoice_customer_message = 'Thank you for your business and have a great day!'
initial_sync_lookback_minutes = 5