"""
Unisys Cost Center Import Integration - Process Cost Centers Child DAG

This module defines the child DAG that processes individual cost center operations.
It handles creating new cost centers, updating existing ones, and disabling cost centers
based on the action specified in the configuration.

The DAG workflow:
    1. Receives cost center data and action from master DAG
    2. Creates log for tracking
    3. Routes to appropriate operation (add/update/disable)
    4. Calls Replicon Division Service API
    5. Validates response and logs result
    6. Returns processing status to master DAG

Key Features:
    - Action-based routing (add, update, disable)
    - Comprehensive error handling with detailed logging
    - Response validation
    - Optional batch task wrapping for error isolation
    - Success and failure tracking

Design Reference:
    Based on cost_center_design.txt API specifications for Division Service

Functions:
    create_child_dag(config): Creates and configures the child Airflow DAG
"""

from datetime import timedelta
import rail
from airflow.models import Variable
from unisys.cost_center_import.utils import request_payload


def create_child_dag(config):
    """
    Create the child DAG for processing individual cost center operations.

    This function configures and returns a child DAG that is triggered by the master DAG
    to process individual cost center records (add/update/disable operations).

    Args:
        config: Configuration object containing instance-specific settings including:
            - process_cost_centers_child (str): Child DAG identifier
            - instance (str): Environment instance name
            - company_key (str): Replicon company identifier
            - replicon_conn_id (str): Airflow connection ID for Replicon API
            - max_active_runs_process_cost_centers (int): Max concurrent runs

    Returns:
        airflow.DAG: Configured Airflow DAG object

    Example:
        >>> from unisys.cost_center_import.instances import development
        >>> dag = create_child_dag(development)
    """
    with rail.create_airflow_dag(
        dag_id=config.process_cost_centers_child_dag_id,
        description=f"Unisys Cost Center Import - Process Cost Centers Child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # Always None for child DAGs (triggered by master)
        schedule_interval=None,
        max_active_runs=config.max_active_runs_process_cost_centers,
        default_args={"execution_timeout": timedelta(hours=1), "retries": 0},
    ) as dag:

        # ============================================================================
        # PHASE 1: INITIALIZATION
        # ============================================================================

        # Step 1: View incoming configuration from master DAG
        view_dagrun_conf = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_conf")

        # Step 2: Optional batch task wrapper for error isolation
        can_use_batch = rail.IfOperator(
            task_id="can_use_batch",
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var="false"
            ).lower()
            == "true",
            yes_task="batch_wrapper",
            no_task="start_process",
        )

        batch_wrapper = rail.BatchTaskRunOperator(
            task_id="batch_wrapper",
            start_task="start_process",
            end_task="catch_and_log_error",
        )

        start_process = rail.EmptyOperator(task_id="start_process")
        # Step 4: Check action type
        check_action = rail.IfOperator(
            task_id="check_action",
            test='{{ dag_run.conf.action == "add" }}',
            yes_task="add_cost_center",
            no_task="update_cost_center",
        )
        # ============================================================================
        # PHASE 3: COST CENTER OPERATIONS
        # ============================================================================

        # Step 5: Add new cost center
        add_cost_center = rail.RepliconServiceOperator(
            task_id="add_cost_center",
            endpoint=config.division_service_endpoint,
            data=lambda dag_run: request_payload.create_cost_center_payload(
                company_code=dag_run.conf["company"],
                cost_center_code=dag_run.conf["cost_center"],
                cost_center_name=dag_run.conf["cost_center_name"],
                status=dag_run.conf["status"]
            ),
        )

        # Step 6: Update existing cost center
        update_cost_center = rail.RepliconServiceOperator(
            task_id="update_cost_center",
            endpoint=config.division_service_endpoint,
            data=lambda dag_run: request_payload.update_cost_center_payload(
                company_code=dag_run.conf["company"],
                cost_center_code=dag_run.conf["cost_center"],
                cost_center_name=dag_run.conf["cost_center_name"],
                status=dag_run.conf["status"]
            ),
        )

        # Step 14: Log successful add
        log_add_success = rail.WriteLogOperator(
            task_id="log_add_success",
            log="{{ dag_run.conf.processing_log }}",
            severity="success",
            message="Cost center added successfully",
            properties=lambda dag_run: {
                "company": dag_run.conf["company"],
                "cost_center": dag_run.conf["cost_center"],
                "cost_center_name": dag_run.conf["cost_center_name"],
                "action": "add",
                "status": "Success",
                "details": "Cost center added successfully",
            },
        )

        # Step 15: Log successful update
        log_update_success = rail.WriteLogOperator(
            task_id="log_update_success",
            log="{{ dag_run.conf.processing_log }}",
            severity="success",
            message="Cost center updated successfully",
            properties=lambda dag_run: {
                "company": dag_run.conf["company"],
                "cost_center": dag_run.conf["cost_center"],
                "cost_center_name": dag_run.conf["cost_center_name"],
                "action": "update" if dag_run.conf["status"].lower() == "enabled" else "disable",
                "status": "Success",
                "details": "Cost center updated successfully",
            },
        )

        # ============================================================================
        # PHASE 8: EXCEPTION HANDLING
        # ============================================================================

        # Step 20: Catch any unexpected errors
        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule="one_failed",
            log="{{ dag_run.conf.processing_log }}",
            severity="Error",
            message="Unexpected error processing cost center",
            properties=lambda dag_run: {
                "company": dag_run.conf.get("company", "Unknown"),
                "cost_center": dag_run.conf.get("cost_center", "Unknown"),
                "action": dag_run.conf.get("action", "Unknown"),
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}"),
            },
        )

        # Phase 1: Initialization
        view_dagrun_conf
        can_use_batch >> rail.Label(
            "Yes") >> batch_wrapper >> catch_and_log_error
        can_use_batch >> rail.Label("No") >> start_process >>\
        check_action >> rail.Label(
            "Add") >> add_cost_center >> log_add_success >> catch_and_log_error
        check_action >> rail.Label("Other") >> update_cost_center >>\
        log_update_success >> catch_and_log_error

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_child_dag)
