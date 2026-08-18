from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.psa_resource_assignment_v2.utils import request_payload
from dxctechnology.psa_resource_assignment_v2.utils import python_callable_method
from dxctechnology.psa_resource_assignment_v2.utils import response_filter

null = None

# pylint: disable=too-many-statements


def create_attribute_1_process_wbs_bulk_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_each_wbs_dagid,
        description=f'DXC PSA Resource Bulk Child - Process WBS with all users V2.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

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

        # Validate all user assignments for date formats
        validate_bulk_assignments = rail.PythonOperator(
            task_id="validate_bulk_assignments",
            python_callable=python_callable_method.validate_bulk_assignment_dates,
        )

        # Separate valid and invalid users
        separate_valid_invalid_users = rail.PythonOperator(
            task_id='separate_valid_invalid_users',
            python_callable=python_callable_method.separate_users_by_validity
        )

        # Log all invalid users
        log_invalid_users = rail.WriteLogOperator(
            task_id='log_invalid_users',
            log='{{ result("create_log") }}',
            items=lambda: rail.result('separate_valid_invalid_users')['invalid_users'],
            message='Gsap PSA Resource Assignment Sync Skipped',
            severity='Exception',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['wbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': item['reason']
            }
        )

        # Check if there are any valid users to process
        has_valid_users = rail.IfOperator(
            task_id='has_valid_users',
            test=lambda: len(rail.result('separate_valid_invalid_users')['valid_users']) > 0,
            yes_task='get_project_details_based_on_wbs',
            no_task='no_valid_users'
        )

        no_valid_users = rail.EmptyOperator(
            task_id='no_valid_users'
        )

        get_project_details_based_on_wbs = rail.RepliconServiceOperator(
            task_id='get_project_details_based_on_wbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": '{{ dag_run.conf.wbs }}',
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        check_wbs_exists = rail.IfOperator(
            task_id='check_wbs_exists',
            test=lambda: bool(rail.result('get_project_details_based_on_wbs') and
                              rail.result(
                'get_project_details_based_on_wbs')['uri']),
            yes_task='check_wbs_is_archived',
            no_task='log_wbs_not_available'
        )

        log_wbs_not_available = rail.WriteLogOperator(
            task_id='log_wbs_not_available',
            log='{{ result("create_log") }}',
            items=lambda: rail.result('separate_valid_invalid_users')['valid_users'],
            message='Failed to sync, since WBS not available in Replicon',
            severity='Exception',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['wbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync - since WBS not available in Replicon'
            }
        )

        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived',
            yes_task='log_wbs_is_archived',
            no_task='get_all_project_team_assignments',
        )

        log_wbs_is_archived = rail.WriteLogOperator(
            task_id='log_wbs_is_archived',
            log='{{ result("create_log") }}',
            items=lambda: rail.result('separate_valid_invalid_users')['valid_users'],
            message='Gsap PSA Resource Assignment Sync skipped, since this WBS is in Archive status.',
            severity='Exception',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['wbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync skipped, since this WBS is in Archive status.'
            }
        )

        # Get current project team assignments
        get_all_project_team_assignments = rail.RepliconServiceOperator(
            task_id="get_all_project_team_assignments",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2",
            data={
                "projectUri": "{{ result('get_project_details_based_on_wbs')['uri'] }}",
                "asOfDate": null
            },
            response_filter=response_filter.map_all_resource_assignments
        )

        # Separate users into new assignments and updates
        categorize_bulk_users = rail.PythonOperator(
            task_id='categorize_bulk_users',
            python_callable=python_callable_method.categorize_users_for_bulk_operation
        )

        # Check if there are users to add
        has_users_to_add = rail.IfOperator(
            task_id='has_users_to_add',
            test=lambda: len(rail.result('categorize_bulk_users')['users_to_add']) > 0,
            yes_task='create_assignment_batches',
            no_task='has_users_for_date_range_update'
        )

        # Create batches of users (50 per batch) for assignment
        create_assignment_batches = rail.PythonOperator(
            task_id='create_assignment_batches',
            python_callable=python_callable_method.batch_users_for_assignment
        )

        # Trigger child DAG for each batch to avoid timeout
        # Each batch is processed in a separate DAG run
        trigger_assignment_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_assignment_child_dags',
            trigger_dag_id=config.process_assignment_child_dagid,
            items=lambda: rail.result('create_assignment_batches'),
            conf=lambda item, dag_run: {
                'project_uri': rail.result('get_project_details_based_on_wbs')['uri'],
                'batch': item
            }
        )

        # Wait for all triggered child DAG runs to complete
        wait_for_assignment_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_assignment_child_dags',
            dag_runs='{{ result("trigger_assignment_child_dags") }}'
        )

        log_bulk_add_success = rail.WriteLogOperator(
            task_id='log_bulk_add_success',
            log='{{ result("create_log") }}',
            items=lambda: rail.result('categorize_bulk_users')['users_to_add'],
            message='Gsap PSA Resource Assignment Sync Successful - Added in bulk',
            severity='Success',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['wbs'],
                'empid': item['empid'],
                'action': 'Add',
                'status': 'Success',
                'details': 'Gsap PSA Resource Assignment Sync Successful for this User'
            }
        )

        # Check if there are users needing date range updates
        has_users_for_date_range_update = rail.IfOperator(
            task_id='has_users_for_date_range_update',
            test=lambda: len(rail.result('categorize_bulk_users')['users_for_date_range_update']) > 0,
            yes_task='create_date_range_batches',
            no_task='log_bulk_update_success'
        )

        # Create batches of users (50 per batch) for date range updates
        create_date_range_batches = rail.PythonOperator(
            task_id='create_date_range_batches',
            python_callable=python_callable_method.batch_users_for_date_range_update
        )

        # Trigger child DAG for each batch to avoid timeout
        # Each batch is processed in a separate DAG run
        trigger_date_range_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_date_range_child_dags',
            trigger_dag_id=config.process_date_range_child_dagid,
            items=lambda: rail.result('create_date_range_batches'),
            conf=lambda item, dag_run: {
                'project_uri': rail.result('get_project_details_based_on_wbs')['uri'],
                'batch': item
            }
        )

        # Wait for all triggered child DAG runs to complete
        wait_for_date_range_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_date_range_child_dags',
            dag_runs='{{ result("trigger_date_range_child_dags") }}'
        )

        log_bulk_update_success = rail.WriteLogOperator(
            task_id='log_bulk_update_success',
            log='{{ result("create_log") }}',
            items=lambda: rail.result('categorize_bulk_users')['users_to_update'],
            message='Gsap PSA Resource Assignment Sync Successful - Updated',
            severity='Success',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['wbs'],
                'empid': item['empid'],
                'action': 'Update',
                'status': 'Success',
                'details': 'Gsap PSA Resource Assignment Sync Successful for this User'
            }
        )

        # Check for child WBS processing
        check_needs_child_wbs = rail.IfOperator(
            task_id='check_needs_child_wbs',
            test=lambda: rail.result('categorize_bulk_users')['users_needing_child_wbs'],
            yes_task='get_all_filter_defination',
            no_task='skip_child_wbs'
        )

        skip_child_wbs = rail.EmptyOperator(
            task_id='skip_child_wbs'
        )

        get_all_filter_defination = rail.RepliconServiceOperator(
            task_id="get_all_filter_defination",
            endpoint="services/ProjectListService1.svc/GetAllFilterDefinitions",
            data={},
            response_filter=response_filter.map_parent_wbs_oef_uri
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="services/ProjectListService1.svc/GetAllColumns",
            data={},
            response_filter=response_filter.map_parent_column_uri
        )

        get_all_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_all_child_wbs_details",
            endpoint="services/ProjectListService1.svc/GetData",
            data=request_payload.get_child_wbs_payload,
            response_filter=response_filter.map_child_wbs
        )

        check_child_wbs_exist = rail.IfOperator(
            task_id='check_child_wbs_exist',
            test=lambda: rail.result('get_all_child_wbs_details'),
            yes_task='process_child_wbs_bulk',
            no_task='log_no_child_wbs_exception',
        )

        log_no_child_wbs_exception = rail.WriteLogOperator(
            task_id='log_no_child_wbs_exception',
            log='{{ result("create_log") }}',
            items=lambda: rail.result('categorize_bulk_users')['users_needing_child_wbs'],
            message='Gsap PSA Resource Assignment Sync Skipped',
            severity='Exception',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['wbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': "Gsap PSA Resource Assignment Sync Skipped for this User as no child WBS present in the parent"
            }
        )

        process_child_wbs_bulk = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child_wbs_bulk',
            retries=0,
            items=lambda: rail.result('get_all_child_wbs_details'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_child_wbs_dagid,
            conf=request_payload.get_process_child_wbs_bulk
        )

        wait_for_process_child_wbs_bulk = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child_wbs_bulk',
            dag_runs='{{ result("process_child_wbs_bulk") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            items=lambda dag_run: dag_run.conf['users'],
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['wbs'],
                'empid': item['empid'],
                'action': 'Sync',
                'status': 'Error',
                'details': rail.render_template('{{ get_error_message() }}')
            },
        )

        # Task flow
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_log >> validate_bulk_assignments >> separate_valid_invalid_users

        separate_valid_invalid_users >> log_invalid_users >> has_valid_users

        has_valid_users >> rail.Label("Yes") >> get_project_details_based_on_wbs >> check_wbs_exists
        has_valid_users >> rail.Label("No") >> no_valid_users >> catch_and_log_errors

        check_wbs_exists >> rail.Label("Yes") >> check_wbs_is_archived
        check_wbs_exists >> rail.Label("No") >> log_wbs_not_available >> catch_and_log_errors

        check_wbs_is_archived >> rail.Label("Yes") >> log_wbs_is_archived >> catch_and_log_errors
        check_wbs_is_archived >> rail.Label("No") >> get_all_project_team_assignments >> categorize_bulk_users

        categorize_bulk_users >> has_users_to_add
        has_users_to_add >> rail.Label("Yes") >> create_assignment_batches >> trigger_assignment_child_dags >> wait_for_assignment_child_dags >> log_bulk_add_success >> has_users_for_date_range_update
        has_users_to_add >> rail.Label("No") >> has_users_for_date_range_update
        has_users_for_date_range_update >> rail.Label("Yes") >> create_date_range_batches >> trigger_date_range_child_dags >> wait_for_date_range_child_dags >> log_bulk_update_success >> check_needs_child_wbs
        has_users_for_date_range_update >> rail.Label("No") >> log_bulk_update_success >> check_needs_child_wbs

        check_needs_child_wbs >> rail.Label("Yes") >> get_all_filter_defination >> get_all_columns >> get_all_child_wbs_details >> check_child_wbs_exist
        check_needs_child_wbs >> rail.Label("No") >> skip_child_wbs >> catch_and_log_errors

        check_child_wbs_exist >> rail.Label("Yes") >> process_child_wbs_bulk >> wait_for_process_child_wbs_bulk >> catch_and_log_errors
        check_child_wbs_exist >> rail.Label("No") >> log_no_child_wbs_exception >> catch_and_log_errors


    return dag


rail.for_each_instance(create_attribute_1_process_wbs_bulk_child_dag)