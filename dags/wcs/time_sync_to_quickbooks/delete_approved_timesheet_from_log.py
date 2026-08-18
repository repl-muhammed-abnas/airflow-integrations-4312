from datetime import timedelta
from pendulum import datetime
import rail
from wcs.time_sync_to_quickbooks.utils import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.delete_approved_timesheet_from_log_id,
        description=f"WCS Delete approved timesheet entries older than 90 days from log - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 4, 1, tz=config.time_zone),
        schedule_interval=config.clean_up_older_log_entries_schedule_interval,
        default_args={
            "execution_timeout": timedelta(hours=1),
        },
    ) as dag:

        search_old_entries = rail.FilterLogEntriesOperator(
            task_id="search_old_entries",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            filter_callable=custom_methods.is_entry_older_than_90_days,
        )

        has_old_entries = rail.IfOperator(
            task_id="has_old_entries",
            test=lambda: rail.result("search_old_entries", "length") > 0,
            yes_task="delete_old_entries",
            no_task="stop_no_old_entries",
        )

        delete_old_entries = rail.FilterLogEntriesOperator(
            task_id="delete_old_entries",
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            filter_callable=custom_methods.is_entry_older_than_90_days,
            remove_filtered_entries=True,
        )

        stop_no_old_entries = rail.EmptyOperator(
            task_id="stop_no_old_entries"
        )

        search_old_entries >> has_old_entries
        has_old_entries >> rail.Label("Yes") >> delete_old_entries
        has_old_entries >> rail.Label("No") >> stop_no_old_entries

    return dag


rail.for_each_instance(create_dag)
