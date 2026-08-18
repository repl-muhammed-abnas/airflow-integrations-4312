"""
Unisys Fieldglass Time Export Integration - Child DAG for Processing Entries
Handles individual timesheet period processing triggered by master DAG

This child DAG processes specific timesheet periods in parallel batches to:
1. Query timesheet entries for the assigned period
2. Generate export data in Fieldglass format
3. Log processing results for master DAG consolidation

Based on design document: Replicon to Fieldglass Integration - Technical Specification V1.1
"""
import rail
from unisys.time_export_v1.utils.custom_methods import get_export_rows


def create_child_dag(config):
    """
    Create child DAG for processing individual timesheet periods

    Args:
        config: Configuration object containing DAG and connection settings

    Returns:
        Configured child DAG for timesheet entry processing
    """

    with rail.create_airflow_dag(
        dag_id=config.process_entries,
        description=f'Unisys Fieldglass Time Export - Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_run_child,
    ) as dag:

        # Task 1: Display DAG run configuration for debugging
        # Shows the configuration passed from master DAG for troubleshooting
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Task 2: Initialize child process logging
        # Creates a log entry to track this specific batch processing
        create_entries_log = rail.CreateLogOperator(
            task_id='create_entries_log'
        )

        # Task 3: Query timesheet entries for assigned period
        # Retrieves timesheet data that needs to be processed for this batch
        # Uses collection data passed from master DAG via DAG run configuration
        query_ts_entries = rail.QueryCollectionOperator(
            task_id='query_ts_entries',
            query='''SELECT * FROM employee_pay_data WHERE timesheet_period_uri=:ts_period_uri AND NULLIF(timesheet_period_uri,'') IS NOT NULL''',
            name='ts_entries',
            query_params={
                'ts_period_uri': '{{ dag_run.conf.item.timesheet_period_uri }}'
            }
        )

        # Task 4: Log processed timesheet entries
        # Converts queried timesheet data to Fieldglass export format
        # Records the formatted data in log for master DAG to collect
        log_ts_entreis = rail.WriteLogOperator(
            task_id='log_ts_entreis',
            log="{{ result('create_entries_log') }}",
            items="{{ result('query_ts_entries') }}",
            message="Log timesheet entries",
            severity='Success',
            properties=lambda item: get_export_rows(item)
        )

        # ============================================================================
        # CHILD DAG TASK DEPENDENCIES (3 Sequential Tasks)
        # ============================================================================
        # Task 2 → Task 3 → Task 4
        # Sequential flow: Initialize → Query → Log Results
        create_entries_log >> query_ts_entries >> log_ts_entreis

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_child_dag)
