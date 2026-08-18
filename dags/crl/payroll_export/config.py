region = "us-east-1"
environment = "pre-production"

process_child_dag_max_active_runs = 100



export_location = "CAN"

max_active_runs_batch_child = 1

# pylint: disable=line-too-long
error_template = '{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") | attr_or_default(["response.body", "exc_message", ""], default="Unknown error occurred") }}'

thread_pool_size_write_csv = 50
