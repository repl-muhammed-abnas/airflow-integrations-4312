from datetime import timedelta
from uuid import uuid4
import rail
from airflow.models import Variable
from galaxyusopcoinc.time_data_import.utils import request_payload, response_filter, custom_methods

def _create_single_child_dag(config, postfix="", replicon_conn='replicon_conn_id'):

    with rail.create_airflow_dag(
        dag_id=f"{config.timedata_child_dag_id}{postfix}",
        description="Galaxy US Opco Inc Time Entry Sync - Child Processor",
        company_key=config.company_key,
        replicon_conn_id=replicon_conn,
        max_active_runs=config.child_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_main_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_main_log',
            end_task='finish',
        )

        create_main_log = rail.CreateLogOperator(
            task_id="create_main_log"
        )

        validate_initial_payload = rail.PythonOperator(
            task_id="validate_initial_payload",
            python_callable=lambda dag_run: custom_methods.validate_required_fields_and_date(dag_run, config)
        )

        initial_validation_passed = rail.IfOperator(
            task_id="initial_validation_passed",
            test="{{ result('validate_initial_payload') == True }}",
            yes_task="get_user_report",
            no_task="prepare_log_for_customer"
        )

        get_user_report = rail.RepliconServiceOperator(
            task_id="get_user_report",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_by_employee_id_payload,
            data_handler=response_filter.validate_and_extract_user_details
        )

        user_validation_passed = rail.IfOperator(
            task_id="user_validation_passed",
            test="{{ result('get_user_report')['user_uri'] | is_truthy }}",
            yes_task="validate_oef_by_template",
            no_task="prepare_log_for_customer"
        )

        validate_oef_by_template = rail.PythonOperator(
            task_id="validate_oef_by_template",
            python_callable=lambda dag_run: custom_methods.validate_oef_fields_by_template(dag_run, config)
        )

        oef_template_validation_passed = rail.IfOperator(
            task_id="oef_template_validation_passed",
            test="{{ result('validate_oef_by_template') == True }}",
            yes_task="get_or_create_timesheet",
            no_task="prepare_log_for_customer"
        )

        get_or_create_timesheet = rail.RepliconServiceOperator(
            task_id="get_or_create_timesheet",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": rail.result("get_user_report")["user_uri"],
                "date": rail.parse_date(dag_run.conf['time_entry_data']['entrydate'], '%Y-%m-%d'),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=lambda response, dag_run: response_filter.validate_and_extract_timesheet_details(
                response, dag_run, config
            )
        )

        timesheet_validation_passed = rail.IfOperator(
            task_id="timesheet_validation_passed",
            test="{{ result('get_or_create_timesheet')['timesheet_uri'] | is_truthy }}",
            yes_task="get_project_with_tasks",
            no_task="prepare_log_for_customer"
        )

        get_project_with_tasks = rail.RepliconServiceOperator(
            task_id="get_project_with_tasks",
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            data=request_payload.get_project_with_tasks_payload,
            data_handler=response_filter.validate_and_extract_project_team_tasks
        )

        project_team_task_validation_passed = rail.IfOperator(
            task_id="project_team_task_validation_passed",
            test="{{ result('get_project_with_tasks')['all_valid'] | is_truthy }}",
            yes_task="get_oef_definition_types",
            no_task="prepare_log_for_customer"
        )

        get_oef_definition_types = rail.RepliconServiceOperator(
            task_id="get_oef_definition_types",
            endpoint="/services/ObjectExtensionDefinitionListService1.svc/GetData",
            data=request_payload.get_oef_definition_types_payload,
            data_handler=response_filter.extract_oef_types_and_filter_by_payload
        )

        has_skipped_oefs = rail.IfOperator(
            task_id="has_skipped_oefs",
            test=lambda: len(rail.result("get_oef_definition_types", "skipped_oefs")) > 0,
            yes_task="prepare_log_for_customer",
            no_task="has_dropdown_oefs"
        )

        has_dropdown_oefs = rail.IfOperator(
            task_id="has_dropdown_oefs",
            test=lambda: len(rail.result("get_oef_definition_types", "dropdown_oefs")) > 0,
            yes_task="get_dropdown_oef_values",
            no_task="get_project_level_assignee_tags"
        )

        get_dropdown_oef_values = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_dropdown_oef_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            items=lambda: rail.result("get_oef_definition_types", "dropdown_oefs"),
            data=lambda item: {"objectExtensionTagDefinitionUri": item['oef_uri']},
            data_handler=response_filter.extract_dropdown_values_and_validate
        )

        has_invalid_dropdown_values = rail.IfOperator(
            task_id="has_invalid_dropdown_values",
            test=lambda: any(not item.get('dropdown_found') for item in rail.result("get_dropdown_oef_values")),
            yes_task="prepare_log_for_customer",
            no_task="get_project_level_assignee_tags"
        )

        get_project_level_assignee_tags = rail.RepliconServiceOperator(
            task_id="get_project_level_assignee_tags",
            endpoint="/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfEnabledProjectDependentTimeEntryObjectExtensionTags",
            data=request_payload.get_project_level_assignee_tags_payload,
            data_handler=response_filter.check_assignee_exists_in_project
        )

        assignee_exists_in_project = rail.IfOperator(
            task_id="assignee_exists_in_project",
            test="{{ result('get_project_level_assignee_tags', 'assignee_tag_uri') | is_truthy }}",
            yes_task="is_inout_template",
            no_task="check_assignee_in_global"
        )

        check_assignee_in_global = rail.RepliconServiceOperator(
            task_id="check_assignee_in_global",
            endpoint="/services/ObjectExtensionTagListService1.svc/GetData",
            data=request_payload.get_global_assignee_tag_payload,
            data_handler=response_filter.extract_assignee_tag_from_global
        )

        assignee_exists_in_global = rail.IfOperator(
            task_id="assignee_exists_in_global",
            test="{{ result('check_assignee_in_global', 'assignee_tag_uri') | is_truthy }}",
            yes_task="assign_assignee_to_project",
            no_task="prepare_log_for_customer"
        )

        assign_assignee_to_project = rail.RepliconServiceOperator(
            task_id="assign_assignee_to_project",
            endpoint="/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.apply_assignee_to_project_payload
        )

        is_inout_template = rail.IfOperator(
            task_id="is_inout_template",
            test=lambda: rail.result("get_or_create_timesheet")["template_type"] == 'In/Out',
            yes_task="get_existing_time_entries",
            no_task="put_time_entry"
        )

        get_existing_time_entries = rail.RepliconServiceOperator(
            task_id="get_existing_time_entries",
            endpoint="/services/TimeEntryRevisionGroupListService1.svc/GetData",
            data=request_payload.get_existing_time_entries_payload,
            data_handler=response_filter.extract_existing_time_entries
        )

        validate_no_inout_overlap = rail.PythonOperator(
            task_id="validate_no_inout_overlap",
            python_callable=custom_methods.validate_no_inout_overlap
        )

        inout_overlap_check_passed = rail.IfOperator(
            task_id="inout_overlap_check_passed",
            test="{{ result('validate_no_inout_overlap') == True }}",
            yes_task="put_time_entry",
            no_task="prepare_log_for_customer"
        )

        put_time_entry = rail.RepliconServiceOperator(
            task_id="put_time_entry",
            endpoint="{{ '/services/TimePunchService1.svc/BulkPutTimePunch4' if result('get_or_create_timesheet')['template_type'] == 'Punch' else '/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup' }}",
            data=request_payload.build_time_entry_or_punch_payload
        )

        needs_approval = rail.IfOperator(
            task_id="needs_approval",
            test=lambda: rail.result("get_or_create_timesheet")["template_type"] != 'Punch',
            yes_task="force_approve_entry",
            no_task="prepare_log_for_customer"
        )

        force_approve_entry = rail.RepliconServiceOperator(
            task_id="force_approve_entry",
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/ForceApprove",
            data=lambda: {
                "timeEntryRevisionGroupUri": rail.result("put_time_entry")['uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Force approved by Galaxy US Opco Inc Time Entry Sync Integration"
            }
        )

        prepare_log_for_customer = rail.PythonOperator(
            task_id="prepare_log_for_customer",
            python_callable=custom_methods.build_customer_log_for_result
        )

        post_log_to_customer = rail.SimpleHttpOperator(
            task_id="post_log_to_customer",
            http_conn_id=config.customer_log_endpoint_conn_id,
            method="POST",
            data="{{ result('prepare_log_for_customer') }}",
            headers={"Content-Type": "application/json"},
            retries = 0
        )

        prepare_error_log_for_customer = rail.PythonOperator(
            task_id="prepare_error_log_for_customer",
            trigger_rule="one_failed",
            python_callable=custom_methods.get_error_details
        )

        post_error_to_customer = rail.SimpleHttpOperator(
            task_id="post_error_to_customer",
            http_conn_id=config.customer_log_endpoint_conn_id,
            method="POST",
            data="{{ result('prepare_error_log_for_customer') }}",
            headers={"Content-Type": "application/json"}
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="Failing the DAG run due to an exception. Check the logs for more details."
        )

        finish = rail.EmptyOperator(task_id="finish")

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> create_main_log

        create_main_log >> validate_initial_payload >> initial_validation_passed

        initial_validation_passed >> rail.Label("Yes") >> get_user_report >> user_validation_passed
        initial_validation_passed >> rail.Label("No") >> prepare_log_for_customer

        user_validation_passed >> rail.Label("Yes") >> validate_oef_by_template >> oef_template_validation_passed
        user_validation_passed >> rail.Label("No") >> prepare_log_for_customer

        oef_template_validation_passed >> rail.Label("Yes") >> get_or_create_timesheet >> timesheet_validation_passed
        oef_template_validation_passed >> rail.Label("No") >> prepare_log_for_customer

        timesheet_validation_passed >> rail.Label("Yes") >> get_project_with_tasks >> project_team_task_validation_passed
        timesheet_validation_passed >> rail.Label("No") >> prepare_log_for_customer

        project_team_task_validation_passed >> rail.Label("Yes") >> get_oef_definition_types >> has_skipped_oefs
        project_team_task_validation_passed >> rail.Label("No") >> prepare_log_for_customer

        has_skipped_oefs >> rail.Label("Yes") >> prepare_log_for_customer
        has_skipped_oefs >> rail.Label("No") >> has_dropdown_oefs

        has_dropdown_oefs >> rail.Label("Yes") >> get_dropdown_oef_values >> has_invalid_dropdown_values
        has_invalid_dropdown_values >> rail.Label("Yes") >> prepare_log_for_customer
        has_invalid_dropdown_values >> rail.Label("No") >> get_project_level_assignee_tags

        has_dropdown_oefs >> rail.Label("No") >> get_project_level_assignee_tags

        get_project_level_assignee_tags >> assignee_exists_in_project

        assignee_exists_in_project >> rail.Label("Yes") >> is_inout_template
        assignee_exists_in_project >> rail.Label("No") >> check_assignee_in_global >> assignee_exists_in_global

        assignee_exists_in_global >> rail.Label("Yes") >> assign_assignee_to_project >> is_inout_template
        assignee_exists_in_global >> rail.Label("No") >> prepare_log_for_customer

        is_inout_template >> rail.Label("Yes") >> get_existing_time_entries >> validate_no_inout_overlap >> inout_overlap_check_passed
        is_inout_template >> rail.Label("No") >> put_time_entry
        inout_overlap_check_passed >> rail.Label("Yes") >> put_time_entry
        inout_overlap_check_passed >> rail.Label("No") >> prepare_log_for_customer

        put_time_entry >> needs_approval
        needs_approval >> rail.Label("Yes") >> force_approve_entry >> prepare_log_for_customer
        needs_approval >> rail.Label("No") >> prepare_log_for_customer

        prepare_log_for_customer >> post_log_to_customer >> prepare_error_log_for_customer >> post_error_to_customer >> fail_dag >> finish

    return dag


def create_child_dag(config):
    add_dags = []
    for batch_idx in range(0, config.TOTAL_BATCHES):
        postfix = "" if batch_idx == 0 else f'_batch_{batch_idx}'
        user_idx = batch_idx // config.BATCHES_PER_USER
        replicon_conn = config.replicon_conn_id if user_idx == 0 else f"{config.replicon_conn_id}_{user_idx}"
        add_dags.append(_create_single_child_dag(config, postfix, replicon_conn))
    return add_dags

rail.for_each_instance(create_child_dag)
