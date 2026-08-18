from datetime import timedelta
from airflow.models import Variable
import rail
from airflow.exceptions import AirflowException
from dxctechnology.wf39_psa_resource_assignment_v4.utils import python_callable_method
from dxctechnology.wf39_psa_resource_assignment_v4.utils import response_filter
from dxctechnology.wf39_psa_resource_assignment_v4.utils import request_payload


# pylint: disable=too-many-statements
null= None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dagid,
        description=f'DXC_WF39 Resource Assignment Automation Child - B1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs,
    ) as dag:

        project_name = "{{ dag_run.conf.wbs }}"
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        query_billing_rates_for_wbs = rail.QueryCollectionOperator(
            task_id="query_billing_rates_for_wbs",
            query="""SELECT DISTINCT wbs, employeeid FROM valid_feed_file_records WHERE wbs=:wbs""",
            query_params={
                "wbs": project_name
            }
        )

        query_all_records_for_wbs = rail.QueryCollectionOperator(
            task_id="query_all_records_for_wbs",
            query="""SELECT * FROM valid_feed_file_records WHERE wbs=:wbs""",
            query_params={
                "wbs": project_name
            },
            name="all_records_for_wbs"
        )

        query_distinct_labor_types_for_project = rail.QueryCollectionOperator(
            task_id="query_distinct_labor_types_for_project",
            query="""SELECT DISTINCT role FROM labourtypesdata WHERE wbs=:wbs""",
            query_params={
                "wbs": project_name
            },
            name='distinct_labor_types_for_project'
        )

        get_project_info_from_project_service = rail.RepliconServiceOperator(
            task_id='get_project_info_from_project_service',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_payload,
            response_filter=response_filter.map_project_response
        )

        def validate_project_and_division():
            if rail.result('get_project_info_from_project_service'):
                if rail.result('get_project_info_from_project_service')['division']:
                    return True
            return False

        is_project_and_division_exist = rail.IfOperator(
            task_id="is_project_and_division_exist",
            test=validate_project_and_division,
            yes_task="assignement_dates_validation",
            no_task="log_project_division_doesnt_exist",
        )

        assignement_dates_validation = rail.PythonOperator(
            task_id = "assignement_dates_validation",
            python_callable=lambda dag_run: python_callable_method.get_assignement_dates_validation(dag_run)
        )

        log_user_date_range_exception = rail.WriteLogOperator(
            task_id='log_user_date_range_exception',
            log='{{ result("create_log") }}',
            items=lambda: rail.result("assignement_dates_validation"),
            message=lambda item: item['log_message'],
            properties=lambda item : item
        )

        has_any_valid_record_to_process = rail.IfOperator(
            task_id="has_any_valid_record_to_process",
            test=lambda: len(rail.result("assignement_dates_validation","records_to_process"))> 0,
            yes_task="get_division_detail",
            no_task="catch_and_log_errors",
        )

        def get_project_division_check_message():
            if not rail.result('get_project_info_from_project_service'): 
                return 'WBS Element is not present in Replicon'
            if not rail.result('get_project_info_from_project_service')['division']:
                return 'WBS Element does not have division associated in Replicon'
            raise AirflowException('Record went for invalid even though all the mandatory field are present')


        log_project_division_doesnt_exist = rail.WriteLogOperator(
            task_id="log_project_division_doesnt_exist",
            log='{{ result("create_log") }}',
            message= get_project_division_check_message,
            items=lambda: python_callable_method.expand_per_role(
                list(rail.load_all_records(rail.result('query_all_records_for_wbs')))
            ),
            severity='Exception',
            properties=lambda item: {
                'wbs': item['wbs'],
                'role': item['roles'],
                'billingrate': '',
                'status': 'Exception',
                'action': 'Validation',
                'employeeid': item['employeeid']
            }
        )

        get_division_detail = rail.RepliconServiceOperator(
            task_id='get_division_detail',
            endpoint='/services/DivisionService1.svc/GetDivisionDetails',
            data=request_payload.get_division_detail
        )

        is_project_pass_validation_checks = rail.IfOperator(
            task_id="is_project_pass_validation_checks",
            test=python_callable_method.validate_project_checks,
            yes_task="get_all_assigned_labor_types_to_project",
            no_task="log_project_validation_exceptions",
        )

        log_project_validation_exceptions = rail.WriteLogOperator(
            task_id="log_project_validation_exceptions",
            log='{{ result("create_log") }}',
            message=lambda item: python_callable_method.get_log_message_project_validations(item),
            items=lambda: python_callable_method.expand_per_role(
                rail.result("assignement_dates_validation", "records_to_process")
            ),
            severity='Exception',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': item['wbs'],
                'role': item['roles'],
                'billingrate': '',
                'status': 'Exception',
                'action': 'Validation'
            }
        )

        get_all_assigned_labor_types_to_project = rail.RepliconServiceOperator(
            task_id='get_all_assigned_labor_types_to_project',
            endpoint='services/ImportService1.svc/BulkGetProjects2',
            data=lambda:{
                "projects": [
                    {
                    "uri": rail.result('get_project_info_from_project_service')['uri'],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                    }
                ]
                },
            data_handler=response_filter.get_assigned_labor_types
        )

        # Idempotency gate: only fetch the current team-member assignment state
        # when the gate is enabled, so a disabled gate adds NO extra API call and
        # is byte-identical to the original v4 behaviour.
        should_detect_changes = rail.IfOperator(
            task_id='should_detect_changes',
            test=lambda: python_callable_method.is_idempotency_gate_enabled(
                config.idempotency_gate_var_name),
            yes_task='get_current_team_assignments',
            no_task='validate_labor_types_in_project',
        )

        get_current_team_assignments = rail.RepliconServiceOperator(
            task_id='get_current_team_assignments',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2',
            data=lambda: {
                "projectUri": rail.result('get_project_info_from_project_service')['uri'],
                "asOfDate": null
            },
            response_filter=response_filter.map_current_team_assignments
        )

        # NEW: Enhanced validation for labour types present in feed vs project
        validate_labor_types_in_project = rail.PythonOperator(
            task_id='validate_labor_types_in_project',
            python_callable=lambda dag_run: python_callable_method.validate_and_filter_labor_types(
                dag_run, config.idempotency_gate_var_name)
        )

        # Log records that were already in sync in Replicon and intentionally
        # skipped by the idempotency gate. Logged as Success / NoChange so PSA
        # retains a row for every input record (full traceability, no silent drops).
        # When the gate is disabled, unchanged_records is empty and this is a no-op.
        log_unchanged_records = rail.WriteLogOperator(
            task_id='log_unchanged_records',
            log='{{ result("create_log") }}',
            items=lambda: rail.result("validate_labor_types_in_project", "unchanged_records"),
            message="No change - record already in sync",
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': project_name,
                'role': item.get('roles', ''),
                'status': 'Success',
                'action': 'No Change',
            }
        )

        # NEW: Log labour types that are not associated with the project
        log_invalid_labor_types = rail.WriteLogOperator(
            task_id='log_invalid_labor_types',
            log='{{ result("create_log") }}',
            items=lambda: rail.result("validate_labor_types_in_project", "records_with_labor_type_not_present_in_project"),
            message=lambda item: f"Labour Type is Not Associated to the WBS '{project_name}'",
            severity='Exception',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': project_name,
                'role': item['roles'],
                'billingrate': '',
                'status': 'Exception',
                'action': 'Validation'
            }
        )

        # Check if we have valid labour types to process
        has_valid_labor_types_to_process = rail.IfOperator(
            task_id="has_valid_labor_types_to_process",
            test=lambda: len(rail.result("validate_labor_types_in_project", "valid_records_with_labor_type_present_in_project")) > 0,
            yes_task="create_assignment_batches_with_labor_types",
            no_task="has_records_without_labor_types"
        )

        # Create batches of resources WITH labor types (300 per batch) for assignment
        create_assignment_batches_with_labor_types = rail.PythonOperator(
            task_id='create_assignment_batches_with_labor_types',
            python_callable=python_callable_method.batch_resources_for_assignment_with_labor_types
        )

        # Bulk assign resources WITH labor types in batches of 300
        assign_resources_to_project = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_resources_to_project',
            endpoint='services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            items=lambda: rail.result('create_assignment_batches_with_labor_types'),
            data=lambda item: python_callable_method.build_bulk_assignment_payload(item)
        )

        # Create batches of resources WITH labor types (300 per batch) for modification
        create_modification_batches_with_labor_types = rail.PythonOperator(
            task_id='create_modification_batches_with_labor_types',
            python_callable=python_callable_method.batch_resources_for_modification_with_labor_types
        )

        # Bulk add billing rates and date ranges for resources WITH labor types in batches of 300
        add_project_resource_billing_rate_assignment_date = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_project_resource_billing_rate_assignment_date",
            endpoint="services/ProjectService1.svc/CreateProjectOrApplyModifications",
            items=lambda: rail.result('create_modification_batches_with_labor_types'),
            data=lambda item: python_callable_method.build_project_modification_payload_for_batch(item, rail.get_current_context()['dag_run'])
        )

        # Check if there are records without labour types to process
        has_records_without_labor_types = rail.IfOperator(
            task_id="has_records_without_labor_types",
            test=lambda: len(rail.result("validate_labor_types_in_project", "valid_records_without_labor_type")) > 0,
            yes_task="create_assignment_batches_without_labor_types",
            no_task="catch_and_log_errors"
        )

        # Create batches of resources WITHOUT labor types (300 per batch) for assignment
        create_assignment_batches_without_labor_types = rail.PythonOperator(
            task_id='create_assignment_batches_without_labor_types',
            python_callable=python_callable_method.batch_resources_for_assignment_without_labor_types
        )

        # Bulk assign resources WITHOUT labor types in batches of 300
        assign_resources_without_labor_types = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_resources_without_labor_types',
            endpoint='services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            items=lambda: rail.result('create_assignment_batches_without_labor_types'),
            data=lambda item: python_callable_method.build_bulk_assignment_payload(item)
        )

        # Create batches of resources WITHOUT labor types (300 per batch) for modification
        create_modification_batches_without_labor_types = rail.PythonOperator(
            task_id='create_modification_batches_without_labor_types',
            python_callable=python_callable_method.batch_resources_for_modification_without_labor_types
        )

        # Bulk add date ranges for resources WITHOUT labor types in batches of 300
        add_project_resource_assignment_date = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_project_resource_assignment_date",
            endpoint="services/ProjectService1.svc/CreateProjectOrApplyModifications",
            items=lambda: rail.result('create_modification_batches_without_labor_types'),
            data=lambda item: python_callable_method.build_project_modification_payload_for_batch(item, rail.get_current_context()['dag_run'])
        )

        # NEW: Log success for resources without labour types
        log_success_without_labor_types = rail.WriteLogOperator(
            task_id='log_success_without_labor_types',
            log='{{ result("create_log") }}',
            items=lambda: rail.result("validate_labor_types_in_project", "valid_records_without_labor_type"),
            message="Completed Successfully",
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': project_name,
                'role': '',
                'status': 'Success',
                'action': 'Add',
            }
        )

        # Log success for resources with valid labour types — one entry per role for 1:1 feed mapping
        log_success_with_labor_types = rail.WriteLogOperator(
            task_id='log_success_with_labor_types',
            log='{{ result("create_log") }}',
            items=lambda: rail.result("validate_labor_types_in_project", "log_valid_records_with_labor_type"),
            message="Completed Successfully",
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': item['wbs'],
                'role': item['roles'],
                'status': 'Success',
                'action': 'Add',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            items=lambda: python_callable_method.expand_per_role(
                list(rail.load_all_records(rail.result("query_all_records_for_wbs")))
                if not rail.result("assignement_dates_validation") else
                rail.result("assignement_dates_validation", "records_to_process")
            ),
            message='{{ get_error_message() }}',
            properties=lambda item: {
                'wbs': item['wbs'],
                'role': item['roles'],
                'billingrate': '',
                'status': 'Error',
                'action': 'Error',
                'employeeid': item['employeeid']
            })
        
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> query_billing_rates_for_wbs >> query_all_records_for_wbs >> query_distinct_labor_types_for_project >> get_project_info_from_project_service
        get_project_info_from_project_service >> is_project_and_division_exist
        is_project_and_division_exist >> rail.Label('Yes') >> assignement_dates_validation >> log_user_date_range_exception >> has_any_valid_record_to_process
        is_project_and_division_exist >> rail.Label('No') >> log_project_division_doesnt_exist >> catch_and_log_errors
        has_any_valid_record_to_process >> rail.Label("Yes") >> get_division_detail >> is_project_pass_validation_checks
        has_any_valid_record_to_process >> rail.Label("No") >> catch_and_log_errors
        is_project_pass_validation_checks >> rail.Label('Yes') >> get_all_assigned_labor_types_to_project >> should_detect_changes
        should_detect_changes >> rail.Label('Yes') >> get_current_team_assignments >> validate_labor_types_in_project
        should_detect_changes >> rail.Label('No') >> validate_labor_types_in_project
        is_project_pass_validation_checks >> rail.Label('No') >>  log_project_validation_exceptions >> catch_and_log_errors
        validate_labor_types_in_project >> log_invalid_labor_types >> log_unchanged_records >> has_valid_labor_types_to_process
        has_valid_labor_types_to_process >> rail.Label('Yes') >> create_assignment_batches_with_labor_types >> assign_resources_to_project >> create_modification_batches_with_labor_types >> add_project_resource_billing_rate_assignment_date
        has_valid_labor_types_to_process >> rail.Label('No') >> has_records_without_labor_types >> rail.Label('Yes') >> create_assignment_batches_without_labor_types >> assign_resources_without_labor_types
        has_records_without_labor_types >> rail.Label('No') >> catch_and_log_errors
        add_project_resource_billing_rate_assignment_date >> log_success_with_labor_types >> has_records_without_labor_types
        assign_resources_without_labor_types >> create_modification_batches_without_labor_types >> add_project_resource_assignment_date >> log_success_without_labor_types >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
