from datetime import timedelta
from airflow.models import Variable
import rail

from dxctechnology.wf39_psa_resource_assignment_compass_v3.utils import python_callable_method
from dxctechnology.wf39_psa_resource_assignment_compass_v3.utils import response_filter
from dxctechnology.wf39_psa_resource_assignment_compass_v3.utils import request_payload

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.distinct_wbs_child_dagid,
        description=f'DXC_WF39 Resource Assignment Automation Child V3 - B1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.distinct_wbs_max_active_runs,
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

        is_project_and_division_exist = rail.IfOperator(
            task_id="is_project_and_division_exist",
            test=python_callable_method.validate_project_and_division,
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

        log_project_division_doesnt_exist = rail.WriteLogOperator(
            task_id="log_project_division_doesnt_exist",
            log='{{ result("create_log") }}',
            message=python_callable_method.get_project_division_check_message,
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
                'role': '{{ item.roles }}',
                'billingrate': '',
                'status': 'Exception',
                'action': 'Validation',
                'employeeid': '{{ item.employeeid }}'
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
            message=lambda item:python_callable_method.get_log_message_project_validations(item),
            items=lambda: rail.result("assignement_dates_validation","records_to_process"),
            severity='Exception',
            properties={
                'employeeid': '{{ item.employeeid }}',
                'wbs': project_name,
                'role': '{{ item.roles }}',
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

        add_new_labor_type_to_projects = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_new_labor_type_to_projects",
            endpoint="services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            items=request_payload.get_billing_rate_list_to_assign_to_project,
            data=lambda item: {
                "projectUri": rail.result('get_project_info_from_project_service')['uri'],
                "billingRateUri": item,
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
                }
        )

        # Create batches of resources (300 per batch) for assignment
        create_assignment_batches = rail.PythonOperator(
            task_id='create_assignment_batches',
            python_callable=python_callable_method.batch_resources_for_assignment
        )

        # Bulk assign resources to project in batches of 300
        assign_resources_to_project = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_resources_to_project',
            endpoint='services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            items=lambda: rail.result('create_assignment_batches'),
            data=lambda item: python_callable_method.build_bulk_assignment_payload(item)
        )

        # Create batches of resources (300 per batch) for modification
        create_modification_batches = rail.PythonOperator(
            task_id='create_modification_batches',
            python_callable=python_callable_method.batch_resources_for_modification
        )

        # Bulk add billing rates and date ranges in batches of 300
        add_project_resource_billing_rate_assignment_date = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_project_resource_billing_rate_assignment_date",
            endpoint="services/ProjectService1.svc/CreateProjectOrApplyModifications",
            items=lambda: rail.result('create_modification_batches'),
            data=lambda item: python_callable_method.build_project_modification_payload_for_batch(item)
        )

        log_success_bilingtype = rail.WriteLogOperator(
            task_id='log_success_bilingtype',
            log='{{ result("create_log") }}',
            items = lambda: rail.result("assignement_dates_validation","records_to_process"),
            message="Completed Successfully",
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': project_name,
                'role': item['roles'],
                'status': 'Success',
                'action': 'Add',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            items=lambda:rail.result("query_all_records_for_wbs") if not rail.result("assignement_dates_validation") else
                rail.result("assignement_dates_validation","records_to_process"),
            message='{{ get_error_message() }}',
            properties={
                'wbs': project_name,
                'role': '{{ item.roles }}',
                'billingrate': '',
                'status': 'Error',
                'action': 'Error',
                'employeeid': '{{ item.employeeid }}'
            })
        
        can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> query_billing_rates_for_wbs >>query_all_records_for_wbs >> query_distinct_labor_types_for_project
        query_distinct_labor_types_for_project >> get_project_info_from_project_service
        get_project_info_from_project_service >> is_project_and_division_exist
        is_project_and_division_exist >> rail.Label('Yes') >> assignement_dates_validation >> log_user_date_range_exception >> has_any_valid_record_to_process
        has_any_valid_record_to_process >> rail.Label('Yes') >> get_division_detail
        has_any_valid_record_to_process >> rail.Label('No') >> catch_and_log_errors
        is_project_and_division_exist >> rail.Label('No') >> log_project_division_doesnt_exist >> catch_and_log_errors
        get_division_detail >> is_project_pass_validation_checks
        is_project_pass_validation_checks >> rail.Label("Yes") >> get_all_assigned_labor_types_to_project >> add_new_labor_type_to_projects
        add_new_labor_type_to_projects >> create_assignment_batches >> assign_resources_to_project >> create_modification_batches >> add_project_resource_billing_rate_assignment_date >> log_success_bilingtype >> catch_and_log_errors

        is_project_pass_validation_checks >> rail.Label("No") >> log_project_validation_exceptions >> catch_and_log_errors


        return dag


rail.for_each_instance(create_child_dag)
