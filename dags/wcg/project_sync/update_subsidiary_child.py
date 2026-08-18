"""
WCG Project Sync v2 - Update Subsidiary Child DAG
Converted from Workato Integration - January 2026

Original Workato Recipe: live_wcg_update_subsidiary_value_on_project.recipe.json

This child DAG updates the subsidiary dropdown value on a project:
1. Gets the custom field URI for Project Subsidiary
2. Gets all dropdown options for the subsidiary field
3. Checks if the subsidiary option exists
4. If exists: Updates the project with the dropdown value
5. If not exists: Creates the option via PutDropDownOptions, then updates project
"""

from datetime import timedelta
from airflow.models import Variable
import rail
from wcg.project_sync.utils import custom_methods

null = None


def create_child_dag(config):
    """
    Child DAG for updating subsidiary dropdown on a project.
    Triggered asynchronously from process_project_child.py Step 62/99/131/173.

    Receives conf:
        - projecturi: URI of the project to update
        - subsidiaryvalue: The subsidiary name to set
    """
    with rail.create_airflow_dag(
        dag_id=config.update_subsidiary_dag_id,
        description=f"WCG Project Sync v2 - Update Subsidiary Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        # ============================================================================
        # BATCH TASK CONTROL
        # ============================================================================

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="get_project_subsidiary_field",
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task="get_project_subsidiary_field",
            end_task="finish",
        )

        # ============================================================================
        # PHASE 1: GET CUSTOM FIELD DEFINITION
        # Matches Workato Steps 2-5: Get tenant details and custom field URI
        # ============================================================================

        # Get Project Subsidiary custom field URI
        get_project_subsidiary_field = rail.RepliconServiceOperator(
            task_id="get_project_subsidiary_field",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:project"},
            data_handler=lambda udfs: rail.find_first_by_attr_and_get_attr(
                udfs, 'displayText', 'Project Subsidiary', 'uri'
            ),
        )

        # ============================================================================
        # PHASE 2: GET DROPDOWN OPTIONS
        # Matches Workato Step 6: GetEnabledCustomFieldDropDownOptions
        # ============================================================================

        # Get all dropdown options for the subsidiary field
        get_subsidiary_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_subsidiary_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_project_subsidiary_field")
            },
        )

        # Matches Workato Step 7: Extract URI where displayText matches subsidiaryvalue
        check_subsidiary_exists = rail.PythonOperator(
            task_id="check_subsidiary_exists",
            python_callable=lambda dag_run: custom_methods.find_subsidiary_uri_from_options(
                rail.result("get_subsidiary_dropdown_options"),
                dag_run.conf.get("subsidiaryvalue", "")
            ),
        )

        # ============================================================================
        # PHASE 3: CONDITIONAL - OPTION EXISTS OR NOT
        # Matches Workato Step 8: IF subsidiary URI is present
        # ============================================================================

        if_subsidiary_exists = rail.IfOperator(
            task_id="if_subsidiary_exists",
            test=lambda: bool(rail.result("check_subsidiary_exists")),
            yes_task="update_subsidiary_on_project",
            no_task="create_subsidiary_option",
        )

        # ============================================================================
        # PHASE 4A: OPTION EXISTS - UPDATE PROJECT
        # Matches Workato Step 9: UpdateDropdownValue (IF Yes branch)
        # ============================================================================

        update_subsidiary_on_project = rail.RepliconServiceOperator(
            task_id="update_subsidiary_on_project",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf.get("projecturi"),
                "customFieldUri": rail.result("get_project_subsidiary_field"),
                "customFieldDropDownOptionUri": rail.result("check_subsidiary_exists"),
            },
        )

        # ============================================================================
        # PHASE 4B: OPTION NOT EXISTS - CREATE OPTION, THEN UPDATE PROJECT
        # Matches Workato Steps 11-20: Create list, add new option, PutDropDownOptions
        # ============================================================================

        # Create new subsidiary option at system level
        # Matches Workato Step 16: PutDropDownOptions
        create_subsidiary_option = rail.RepliconServiceOperator(
            task_id="create_subsidiary_option",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: custom_methods.build_create_dropdown_option_request(
                dag_run.conf.get("subsidiaryvalue", ""),
                rail.result("get_subsidiary_dropdown_options") or [],
                rail.result("get_project_subsidiary_field"),
            ),
        )

        # Matches Workato Step 17: GetEnabledCustomFieldDropDownOptions (after creation)
        get_updated_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_updated_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_project_subsidiary_field")
            },
        )

        # Matches Workato Step 18: Extract URI of newly created option
        find_new_subsidiary_uri = rail.PythonOperator(
            task_id="find_new_subsidiary_uri",
            python_callable=lambda dag_run: custom_methods.find_subsidiary_uri_from_options(
                rail.result("get_updated_dropdown_options"),
                dag_run.conf.get("subsidiaryvalue", "")
            ),
        )

        # Matches Workato Step 20: UpdateDropdownValue (after creation)
        update_subsidiary_after_create = rail.RepliconServiceOperator(
            task_id="update_subsidiary_after_create",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf.get("projecturi"),
                "customFieldUri": rail.result("get_project_subsidiary_field"),
                "customFieldDropDownOptionUri": rail.result("find_new_subsidiary_uri"),
            },
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        # ============================================================================
        # TASK DEPENDENCIES
        # ============================================================================

        # Batch task control flow
        can_run_batch_task
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> get_project_subsidiary_field

        # Main processing flow
        (
            get_project_subsidiary_field
            >> get_subsidiary_dropdown_options
            >> check_subsidiary_exists
            >> if_subsidiary_exists
        )

        # YES branch: Option exists - update project directly
        (
            if_subsidiary_exists
            >> rail.Label("Yes")
            >> update_subsidiary_on_project
            >> finish
        )

        # NO branch: Option not exists - create option, then update project
        (
            if_subsidiary_exists
            >> rail.Label("No")
            >> create_subsidiary_option
            >> get_updated_dropdown_options
            >> find_new_subsidiary_uri
            >> update_subsidiary_after_create
            >> finish
        )

        return dag


rail.for_each_instance(create_child_dag)
