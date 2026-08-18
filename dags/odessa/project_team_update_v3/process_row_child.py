from datetime import timedelta
import rail

from odessa.project_team_update_v3.utils import custom_method
from odessa.project_team_update_v3.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_row_child_dag_id,
        description=f"odessa_project_team_update_process_row_child_v3_{config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        set_default_status = rail.SetVariableOperator(
            task_id="set_default_status",
            append=False,
            name="rowstatus",
            value="Error - Processing failed",
        )

        has_projectname = rail.IfOperator(
            task_id="has_projectname",
            test="{{ dag_run.conf.projectname | is_truthy }}",
            yes_task="has_useruri",
            no_task="status_project_missing",
        )

        status_project_missing = rail.SetVariableOperator(
            task_id="status_project_missing",
            append=False,
            name="rowstatus",
            value="Ignored - Project value is missing in the file",
        )

        has_useruri = rail.IfOperator(
            task_id="has_useruri",
            test=lambda dag_run: custom_method.is_meaningful(dag_run.conf.get("useruri")),
            yes_task="search_project",
            no_task="status_user_missing",
        )

        status_user_missing = rail.SetVariableOperator(
            task_id="status_user_missing",
            append=False,
            name="rowstatus",
            value="Ignored - User is not present in Replicon",
        )

        search_project = rail.RepliconServiceOperator(
            task_id="search_project",
            endpoint="/services/ProjectListService1.svc/GetData",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda dag_run: request_payload.search_project_by_name(
                dag_run.conf["projectname"]),
        )

        resolve_project_uri = rail.PythonOperator(
            task_id="resolve_project_uri",
            python_callable=custom_method.resolve_project_uri,
        )

        is_project_found = rail.IfOperator(
            task_id="is_project_found",
            test="{{ result('resolve_project_uri') | is_truthy }}",
            yes_task="get_project_details",
            no_task="status_project_not_found",
        )

        status_project_not_found = rail.SetVariableOperator(
            task_id="status_project_not_found",
            append=False,
            name="rowstatus",
            value="Ignored - Project is not present in Replicon",
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/GetProjectDetails",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda: {"projectUri": rail.result("resolve_project_uri")},
        )

        is_archived = rail.IfOperator(
            task_id="is_archived",
            test="{{ 'archived' in (result('get_project_details').status.name | default('') | lower) }}",
            yes_task="status_archived",
            no_task="is_add_action",
        )

        status_archived = rail.SetVariableOperator(
            task_id="status_archived",
            append=False,
            name="rowstatus",
            value="Ignored - Project in Archived Status",
        )

        is_add_action = rail.IfOperator(
            task_id="is_add_action",
            test="{{ 'add' in (dag_run.conf.action | default('') | lower) }}",
            yes_task="assign_member",
            no_task="is_remove_action",
        )

        is_remove_action = rail.IfOperator(
            task_id="is_remove_action",
            test="{{ 'remove' in (dag_run.conf.action | default('') | lower) }}",
            yes_task="unassign_member",
            no_task="is_action_present",
        )

        is_action_present = rail.IfOperator(
            task_id="is_action_present",
            test="{{ dag_run.conf.action | is_truthy }}",
            yes_task="status_invalid_action",
            no_task="status_no_action",
        )

        status_no_action = rail.SetVariableOperator(
            task_id="status_no_action",
            append=False,
            name="rowstatus",
            value="Ignored - Action provided is blank",
        )

        status_invalid_action = rail.SetVariableOperator(
            task_id="status_invalid_action",
            append=False,
            name="rowstatus",
            value="Ignored - Invalid action defined",
        )

        assign_member = rail.RepliconServiceOperator(
            task_id="assign_member",
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda dag_run: request_payload.assign_member(
                rail.result("resolve_project_uri"), dag_run.conf["useruri"]),
        )

        get_children_tasks = rail.RepliconServiceOperator(
            task_id="get_children_tasks",
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda: request_payload.get_children_tasks(
                rail.result("resolve_project_uri")),
        )

        put_task_assignments = rail.RepliconServiceOperator(
            task_id="put_task_assignments",
            endpoint="/services/ProjectService1.svc/PutTaskAssignmentsForResource",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda dag_run: request_payload.put_task_assignments(
                rail.result("resolve_project_uri"),
                dag_run.conf["useruri"],
                custom_method.open_task_uris()),
        )

        has_customer_role = rail.IfOperator(
            task_id="has_customer_role",
            test=lambda dag_run: custom_method.is_meaningful(dag_run.conf.get("customerrole")),
            yes_task="is_time_and_materials",
            no_task="status_user_added",
        )

        is_time_and_materials = rail.IfOperator(
            task_id="is_time_and_materials",
            test="{{ 'Time & Materials' in (result('get_project_details').billingType.displayText if result('get_project_details').billingType else '') }}",
            yes_task="trigger_billing_rate_child",
            no_task="is_fixed_bid",
        )

        trigger_billing_rate_child = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_billing_rate_child",
            retries=0,
            items=lambda: [rail.result("resolve_project_uri")],
            trigger_dag_id=config.assign_billing_rate_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "clienturi": "{{ result('get_project_details').client.uri if result('get_project_details').client else '' }}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "projecturi": "{{ result('resolve_project_uri') }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "customerrole": "{{ dag_run.conf.customerrole }}",
                "billingrateuri": "{{ dag_run.conf.billingrateuri }}",
            },
        )

        wait_for_billing_rate_child = rail.WaitForDagRunsSensor(
            task_id="wait_for_billing_rate_child",
            dag_runs="{{ result('trigger_billing_rate_child') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_fixed_bid = rail.IfOperator(
            task_id="is_fixed_bid",
            test="{{ 'Fixed Bid' in (result('get_project_details').billingType.displayText if result('get_project_details').billingType else '') }}",
            yes_task="status_fixed_bid",
            no_task="is_non_billable",
        )

        status_fixed_bid = rail.SetVariableOperator(
            task_id="status_fixed_bid",
            append=False,
            name="rowstatus",
            value="Ignored - Fixed bid Project",
        )

        is_non_billable = rail.IfOperator(
            task_id="is_non_billable",
            test="{{ 'Non-Billable' in (result('get_project_details').billingType.displayText if result('get_project_details').billingType else '') }}",
            yes_task="status_non_billable",
            no_task="status_user_added",
        )

        status_non_billable = rail.SetVariableOperator(
            task_id="status_non_billable",
            append=False,
            name="rowstatus",
            value="Ignored - Non-Billable Project",
        )

        is_billing_rate_found = rail.IfOperator(
            task_id="is_billing_rate_found",
            test=lambda dag_run: (dag_run.conf.get("billingratefound") or "").lower() != "no",
            yes_task="status_user_added",
            no_task="status_user_added_default_rate",
        )

        status_user_added = rail.SetVariableOperator(
            task_id="status_user_added",
            append=False,
            trigger_rule="none_failed_min_one_success",
            name="rowstatus",
            value="Success - User added",
        )

        status_user_added_default_rate = rail.SetVariableOperator(
            task_id="status_user_added_default_rate",
            append=False,
            name="rowstatus",
            value="Success - User added (default billing rate)",
        )

        unassign_member = rail.RepliconServiceOperator(
            task_id="unassign_member",
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda dag_run: request_payload.unassign_member(
                rail.result("resolve_project_uri"), dag_run.conf["useruri"]),
        )

        status_user_removed = rail.SetVariableOperator(
            task_id="status_user_removed",
            append=False,
            name="rowstatus",
            value="Success - User removed from project",
        )

        get_row_status = rail.GetVariableOperator(
            task_id="get_row_status",
            trigger_rule="all_done",
            name="rowstatus",
        )

        row_result = rail.PythonOperator(
            task_id="row_result",
            trigger_rule="all_done",
            python_callable=custom_method.build_row_result,
        )


        set_default_status >> has_projectname
        has_projectname >> rail.Label("Yes") >> has_useruri
        has_projectname >> rail.Label("No") >> status_project_missing

        has_useruri >> rail.Label("Yes") >> search_project
        has_useruri >> rail.Label("No") >> status_user_missing

        search_project >> resolve_project_uri >> is_project_found
        is_project_found >> rail.Label("Yes") >> get_project_details
        is_project_found >> rail.Label("No") >> status_project_not_found

        get_project_details >> is_archived
        is_archived >> rail.Label("Yes") >> status_archived
        is_archived >> rail.Label("No") >> is_add_action

        is_add_action >> rail.Label("Yes") >> assign_member
        is_add_action >> rail.Label("No") >> is_remove_action

        is_remove_action >> rail.Label("Yes") >> unassign_member
        is_remove_action >> rail.Label("No") >> is_action_present

        is_action_present >> rail.Label("Yes") >> status_invalid_action
        is_action_present >> rail.Label("No") >> status_no_action

        assign_member >> get_children_tasks >> put_task_assignments >> has_customer_role
        has_customer_role >> rail.Label("Yes") >> is_time_and_materials
        has_customer_role >> rail.Label("No") >> status_user_added

        is_time_and_materials >> rail.Label("Yes") >> trigger_billing_rate_child \
            >> wait_for_billing_rate_child >> is_billing_rate_found
        is_time_and_materials >> rail.Label("No") >> is_fixed_bid

        is_billing_rate_found >> rail.Label("Yes") >> status_user_added
        is_billing_rate_found >> rail.Label("No") >> status_user_added_default_rate

        is_fixed_bid >> rail.Label("Yes") >> status_fixed_bid
        is_fixed_bid >> rail.Label("No") >> is_non_billable

        is_non_billable >> rail.Label("Yes") >> status_non_billable
        is_non_billable >> rail.Label("No") >> status_user_added

        unassign_member >> status_user_removed

        [
            status_project_missing,
            status_user_missing,
            status_project_not_found,
            status_archived,
            status_no_action,
            status_invalid_action,
            status_user_added,
            status_user_added_default_rate,
            status_fixed_bid,
            status_non_billable,
            status_user_removed,
        ] >> get_row_status >> row_result

    return dag


rail.for_each_instance(create_child_dag)
