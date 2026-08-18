from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.psa_resource_assignment_v2.utils import request_payload
from dxctechnology.psa_resource_assignment_v2.utils import python_callable_method
from dxctechnology.psa_resource_assignment_v2.utils import response_filter

null = None


def create_attribute_1_process_child_wbs_bulk_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_child_wbs_dagid,
        description=f'DXC PSA Resource Bulk Child - Process Child WBS with users V2.0 {config.dag_id_postfix}',
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
            no_task='get_child_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_child_project_details',
            end_task='catch_and_log_errors',
        )

        get_child_project_details = rail.RepliconServiceOperator(
            task_id='get_child_project_details',
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

        get_project_wbs_type = rail.PythonOperator(
            task_id='get_project_wbs_type',
            python_callable=python_callable_method.project_wbs_type
        )

        check_child_wbs_type_diwo = rail.IfOperator(
            task_id='check_child_wbs_type_diwo',
            test=lambda: rail.result('get_project_wbs_type') == "DIWO",
            yes_task='check_wbs_exists',
            no_task='log_invalid_wbs_type',
        )

        check_wbs_exists = rail.IfOperator(
            task_id='check_wbs_exists',
            test=lambda: bool(rail.result('get_child_project_details') and
                              rail.result('get_child_project_details')['uri']),
            yes_task='check_wbs_is_archived',
            no_task='log_wbs_not_available'
        )

        log_wbs_not_available = rail.WriteLogOperator(
            task_id='log_wbs_not_available',
            log ='{{ dag_run.conf.wbs_log }}',
            items=lambda dag_run: dag_run.conf['users'],
            message='Failed to sync, since Child WBS {{dag_run.conf.wbs}} not available in Replicon',
            severity='Exception',
            properties=lambda item, dag_run: {
                'wbs': dag_run.conf['parentWbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': f'Gsap PSA Resource Assignment Sync Skipped - Child WBS {dag_run.conf["wbs"]} not available in Replicon'
            }
        )

        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_child_project_details')['status']['name'] == 'Archived',
            yes_task='log_wbs_is_archived',
            no_task='validate_users_for_child_wbs',
        )

        log_wbs_is_archived = rail.WriteLogOperator(
            task_id='log_wbs_is_archived',
            log ='{{ dag_run.conf.wbs_log }}',
            items=lambda dag_run: dag_run.conf['users'],
            message=lambda dag_run:f'Gsap PSA Resource Assignment Sync skipped, since Child WBS {dag_run.conf["wbs"]} is in Archive status.',
            severity='Exception',
            properties=lambda item, dag_run: {
                'wbs': dag_run.conf['parentWbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': f'Gsap PSA Resource Assignment Sync skipped - Child WBS {dag_run.conf["wbs"]} is in Archive status.'
            }
        )

        # Validate all users for division matching with child WBS
        validate_users_for_child_wbs = rail.PythonOperator(
            task_id='validate_users_for_child_wbs',
            python_callable=python_callable_method.validate_users_for_child_wbs
        )

        separate_valid_invalid_division = rail.PythonOperator(
            task_id='separate_valid_invalid_division',
            python_callable=python_callable_method.separate_users_by_division_match
        )

        # Log users with division mismatch
        has_invalid_division_users = rail.IfOperator(
            task_id='has_invalid_division_users',
            test=lambda: len(rail.result('separate_valid_invalid_division')['invalid_division']) > 0,
            yes_task='log_division_mismatch',
            no_task='skip_invalid_division_logging'
        )

        log_division_mismatch = rail.WriteLogOperator(
            task_id='log_division_mismatch',
            log ='{{ dag_run.conf.wbs_log }}',
            items=lambda: rail.result('separate_valid_invalid_division')['invalid_division'],
            message='Company code of Child WBS does not match with User company code',
            severity='Exception',
            properties=lambda item,dag_run:{
                'wbs': dag_run.conf['parentWbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': f'Company code of Child WBS {dag_run.conf["wbs"]} does not match with User company code'
            }
        )

        skip_invalid_division_logging = rail.EmptyOperator(
            task_id='skip_invalid_division_logging'
        )

        # Check if there are valid users to process
        has_valid_division_users = rail.IfOperator(
            task_id='has_valid_division_users',
            test=lambda: len(rail.result('separate_valid_invalid_division')['valid_division']) > 0,
            yes_task='get_child_project_team_assignment',
            no_task='no_valid_users_for_child'
        )

        no_valid_users_for_child = rail.EmptyOperator(
            task_id='no_valid_users_for_child'
        )

        get_child_project_team_assignment = rail.RepliconServiceOperator(
            task_id="get_child_project_team_assignment",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2",
            data={
                "projectUri": "{{ result('get_child_project_details')['uri'] }}",
                "asOfDate": null
            },
            response_filter=response_filter.map_all_resource_assignments
        )

        # Categorize users for bulk operations
        categorize_child_wbs_users = rail.PythonOperator(
            task_id='categorize_child_wbs_users',
            python_callable=python_callable_method.categorize_users_for_child_wbs_bulk
        )

        # Process new users with bulk assignment
        has_users_to_add = rail.IfOperator(
            task_id='has_users_to_add',
            test=lambda: len(rail.result('categorize_child_wbs_users')['users_to_add']) > 0,
            yes_task='create_child_assignment_batches',
            no_task='has_users_for_date_range_update'
        )

        # Create batches of users (50 per batch) for assignment
        create_child_assignment_batches = rail.PythonOperator(
            task_id='create_child_assignment_batches',
            python_callable=python_callable_method.batch_users_for_child_assignment
        )

        # Trigger child DAG for each batch to avoid timeout
        # Each batch is processed in a separate DAG run
        trigger_assignment_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_assignment_child_dags',
            trigger_dag_id=config.process_assignment_child_dagid,
            items=lambda: rail.result('create_child_assignment_batches'),
            conf=lambda item, dag_run: {
                'project_uri': rail.result('get_child_project_details')['uri'],
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
            log ='{{ dag_run.conf.wbs_log }}',
            items=lambda: rail.result('categorize_child_wbs_users')['users_to_add'],
            message='Gsap PSA Resource Assignment Sync Successful',
            severity='Success',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['parentWbs'],
                'empid': item['empid'],
                'action': 'Add',
                'status': 'Success',
                'details': f'Gsap PSA Resource Assignment Sync Successful for User on Child WBS {dag_run.conf["wbs"]}'
            }
        )

        # Check if there are users needing date range updates
        has_users_for_date_range_update = rail.IfOperator(
            task_id='has_users_for_date_range_update',
            test=lambda: len(rail.result('categorize_child_wbs_users')['users_for_date_range_update']) > 0,
            yes_task='create_child_date_range_batches',
            no_task='log_bulk_update_success'
        )

        # Create batches of users (50 per batch) for date range updates
        create_child_date_range_batches = rail.PythonOperator(
            task_id='create_child_date_range_batches',
            python_callable=python_callable_method.batch_users_for_child_date_range_update
        )

        # Trigger child DAG for each batch to avoid timeout
        # Each batch is processed in a separate DAG run
        trigger_date_range_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_date_range_child_dags',
            trigger_dag_id=config.process_date_range_child_dagid,
            items=lambda: rail.result('create_child_date_range_batches'),
            conf=lambda item, dag_run: {
                'project_uri': rail.result('get_child_project_details')['uri'],
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
            log ='{{ dag_run.conf.wbs_log }}',
            items=lambda: rail.result('categorize_child_wbs_users')['users_to_update'],
            message='Gsap PSA Resource Assignment Sync Successful',
            severity='Success',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['parentWbs'],
                'empid': item['empid'],
                'action': 'Update',
                'status': 'Success',
                'details': f'Gsap PSA Resource Assignment Sync Successful for User on Child WBS {dag_run.conf["wbs"]}'
            }
        )

        log_invalid_wbs_type = rail.WriteLogOperator(
            task_id='log_invalid_wbs_type',
            log ='{{ dag_run.conf.wbs_log }}',
            items=lambda dag_run: dag_run.conf['users'],
            message='Gsap PSA Resource Assignment Sync Skipped',
            severity='Exception',
            properties=lambda item,dag_run: {
                'wbs': dag_run.conf['parentWbs'],
                'empid': item['empid'],
                'action': 'Validation',
                'status': 'Exception',
                'details': f"Gsap PSA Resource Assignment Sync Skipped - Child WBS {dag_run.conf['wbs']} Type is not DIWO"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log ='{{ dag_run.conf.wbs_log }}',
            trigger_rule='one_failed',
            items=lambda dag_run: dag_run.conf['users'],
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda item,dag_run:{
                'wbs': dag_run.conf['parentWbs'],
                'empid': item['empid'],
                'action': 'Sync',
                'status': 'Error',
                'details': rail.render_template('{{ get_error_message() }}'),
            },
        )


        # Task flow
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_child_project_details >> get_project_wbs_type >> check_child_wbs_type_diwo

        check_child_wbs_type_diwo >> rail.Label("Yes") >> check_wbs_exists
        check_child_wbs_type_diwo >> rail.Label("No") >> log_invalid_wbs_type >> catch_and_log_errors

        check_wbs_exists >> rail.Label("Yes") >> check_wbs_is_archived
        check_wbs_exists >> rail.Label("No") >> log_wbs_not_available >> catch_and_log_errors

        check_wbs_is_archived >> rail.Label("Yes") >> log_wbs_is_archived >> catch_and_log_errors
        check_wbs_is_archived >> rail.Label("No") >> validate_users_for_child_wbs >> separate_valid_invalid_division

        separate_valid_invalid_division >> has_invalid_division_users
        has_invalid_division_users >> rail.Label("Yes") >> log_division_mismatch >> has_valid_division_users
        has_invalid_division_users >> rail.Label("No") >> skip_invalid_division_logging >> has_valid_division_users

        has_valid_division_users >> rail.Label("Yes") >> get_child_project_team_assignment >> categorize_child_wbs_users
        has_valid_division_users >> rail.Label("No") >> no_valid_users_for_child >> catch_and_log_errors

        categorize_child_wbs_users >> has_users_to_add
        has_users_to_add >> rail.Label("Yes") >> create_child_assignment_batches >> trigger_assignment_child_dags >> wait_for_assignment_child_dags >> log_bulk_add_success >> has_users_for_date_range_update
        has_users_to_add >> rail.Label("No") >> has_users_for_date_range_update
        has_users_for_date_range_update >> rail.Label("Yes") >> create_child_date_range_batches >> trigger_date_range_child_dags >> wait_for_date_range_child_dags >> log_bulk_update_success >> catch_and_log_errors
        has_users_for_date_range_update >> rail.Label("No") >> log_bulk_update_success >> catch_and_log_errors

    return dag


rail.for_each_instance(create_attribute_1_process_child_wbs_bulk_dag)