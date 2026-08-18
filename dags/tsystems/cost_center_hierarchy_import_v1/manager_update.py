# manager_update.py
import rail
from datetime import timedelta

# Import utilities
from tsystems.cost_center_hierarchy_import_v1.utils import request_payload, response_filter, custom_methods
from tsystems.cost_center_hierarchy_import_v1 import config

def create_manager_update_dag(config):
    """
    Creates the DAG for updating cost center manager permissions in T-Systems Cost Center Hierarchy Import.
    This DAG assigns 'Cost Manager' permissions to users and links them to their cost centers.
    
    :param config: Configuration module with settings for the instance
    :return: The created DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.manager_cost_center_restriction_update_dag_id,
        description=f'T-Systems Cost Center Hierarchy Import - Manager Permission DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,  # This DAG is only triggered by the master DAG
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:
        
        # View incoming parameters for debugging
        rail.ViewDagRunConfOperator(task_id="view_conf")
        
        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: custom_methods.can_run_batch_task_test(config.batch_task_var_name),
            yes_task='batch_task',
            no_task='create_process_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task= 'create_process_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
        )

        create_process_log = rail.CreateLogOperator(
            task_id = "create_process_log"
        )

        # Get department data from incoming parameter
        query_cost_center_data = rail.QueryCollectionOperator(
            task_id='query_cost_center_data',
           query="""SELECT * FROM valid_cost_centers WHERE Cost_Center_Manager == '{{dag_run.conf.managers.manager_id}}'"""
        )

        def get_cost_center_uris_callable(dag_run):
            replicon_depts = rail.load_json_artifact(dag_run.conf['replicon_departments'])
            cost_centers_for_manager = rail.load_all_records(rail.result('query_cost_center_data'))
            cost_center_details = []
            not_found_cost_center_details = []
            for cost_center in cost_centers_for_manager:
                rpl_cost_center = rail.find_first_by_attr_and_get_attr(
                            replicon_depts,
                            'Code',
                            cost_center['Code'],
                            default={}
                        )
                cost_center_details.append({
                    **cost_center,
                    **{
                        "replicon_details": rpl_cost_center
                    }}
                )
                if not rpl_cost_center:
                    not_found_cost_center_details.append(cost_center['Code'])
            if not_found_cost_center_details:
                rail.set_result(key="not_found_cost_centers", val=not_found_cost_center_details)
            # As we are removing permission, we don't need to check for assigning permission
            if dag_run.conf.get('should_permission_removed', 'False').lower() == 'true':
                rail.set_result(key="assign_permission", val=False)
            else:
                rail.set_result(key="assign_permission", val=len(not_found_cost_center_details) == len(cost_centers_for_manager))
            return cost_center_details

        get_cost_center_uris = rail.PythonOperator(
            task_id = "get_cost_center_uris",
            python_callable = get_cost_center_uris_callable
        )

        is_cost_centers_not_found = rail.IfOperator(
            task_id='is_cost_centers_not_found',
            test=lambda : rail.result('get_cost_center_uris', 'assign_permission') is True,
            yes_task='log_no_cost_centers_not_found',
            no_task='permission_found_cost_manager_empty'
        )

        log_no_cost_centers_not_found = rail.WriteLogOperator(
            task_id='log_no_cost_centers_not_found',
            log="{{ result('create_process_log') }}",
            items=lambda: rail.result('query_cost_center_data'),
            severity="Exception",
            message="one or more cost centers not found",
            properties=lambda item: {
                'code': item['Code'],
                'name': item['Name'],
                'description': item['Description'],
                'status': "Exception",
                'action': "Update",
                'details': "Cost & Payroll Manager permission and Restrictions skipped as No cost centers not found in Replicon: " + ", ".join(rail.result('get_cost_center_uris', 'not_found_cost_centers')),
                'manager_id': item['Cost_Center_Manager']
            }
        )

        permission_found_cost_manager_empty = rail.EmptyOperator(
            task_id='permission_found_cost_manager_empty'
        )

        # Check if the permission was found
        permission_found_cost_manager = rail.IfOperator(
            task_id='permission_found_cost_manager',
            test=lambda dag_run: bool(dag_run.conf['manager_permission']),
            yes_task='permission_found_payroll_manager',
            no_task='log_missing_permission_cost_manager'
        )

        # Log when permission set is missing
        log_missing_permission_cost_manager = rail.WriteLogOperator(
            task_id='log_missing_permission_cost_manager',
            log="{{ result('create_process_log') }}",
            items=lambda: rail.result('query_cost_center_data'),
            severity="Exception",
            message="Cost Manager permission set not found in Replicon",
            properties=lambda item: {
                'code': item['Code'],
                'name': item['Name'],
                'description': item['Description'],
                'status': "Exception",
                'action': "Update",
                'details': "Cost Manager permission set not found in Replicon",
                'manager_id': item['Cost_Center_Manager'],
            }
        )

        permission_found_payroll_manager = rail.IfOperator(
            task_id='permission_found_payroll_manager',
            test=lambda dag_run: bool(dag_run.conf['payroll_manager_permission']),
            yes_task='find_user',
            no_task='log_missing_permission_payroll_manager'
        )

        # Log when permission set is missing
        log_missing_permission_payroll_manager = rail.WriteLogOperator(
            task_id='log_missing_permission_payroll_manager',
            log="{{ result('create_process_log') }}",
            items=lambda: rail.result('query_cost_center_data'),
            severity="Exception",
            message="Payroll Manager permission set not found in Replicon",
            properties=lambda item: {
                'code': item['Code'],
                'name': item['Name'],
                'description': item['Description'],
                'status': "Exception",
                'action': "Update",
                'details': "Payroll Manager permission set not found in Replicon",
                'manager_id': item['Cost_Center_Manager'],
            }
        )

        # Find the user in Replicon
        find_user = rail.RepliconServiceOperator(
            task_id='find_user',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "employeeId": dag_run.conf['managers']['manager_id'],
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else []
        )

        # Check if the user was found
        user_found = rail.IfOperator(
            task_id='user_found',
            test = lambda: bool(rail.result('find_user')) and (rail.result('find_user')['userDetails']['isEnabled'] is True),
            yes_task='is_permission_removed',
            no_task='log_missing_user'
        )

        is_permission_removed = rail.IfOperator(
            task_id = "is_permission_removed",
            test= lambda dag_run: dag_run.conf.get('should_permission_removed', 'False').lower() == 'true',
            yes_task='remove_permission_cost_manager',
            no_task='process_existing_permissions'
        )

        remove_permission_cost_manager = rail.RepliconServiceOperator(
            task_id='remove_permission_cost_manager',
            endpoint = "/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser",
            data=lambda dag_run:{
                "userUri": rail.result('find_user')['userDetails']['uri'],
                "permissionSetUri": dag_run.conf['manager_permission']['uri']
            }
        )

        remove_permission_payroll_manager = rail.RepliconServiceOperator(
            task_id='remove_permission_payroll_manager',
            endpoint = "/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser",
            data=lambda dag_run:{
                "userUri": rail.result('find_user')['userDetails']['uri'],
                "permissionSetUri": dag_run.conf['payroll_manager_permission']['uri']
            }
        )

        log_permission_removed = rail.WriteLogOperator(
            task_id='log_permission_removed',
            log="{{ result('create_process_log') }}",
            severity="Success",
            message="Cost and Payroll Manager permission removed for Manager",
            properties=lambda dag_run: {
                'code': "NA",
                'name': "NA",
                'description': "NA",
                'status': "Success",
                'action': "Delete",
                'details': "Cost and Payroll Manager permission Removed for manager",
                'manager_id': dag_run.conf['managers']['manager_id'],
            }
        )

        # Process existing permissions
        process_existing_permissions = rail.PythonOperator(
            task_id='process_existing_permissions',
            python_callable=lambda dag_run: {
                'has_cost_manager': any(
                    permission['uri'] == dag_run.conf['manager_permission']['uri']
                    for permission in (rail.result('find_user')['permissionSets'] or [])
                ),
                'has_payroll_manager': any(
                    permission['uri'] == dag_run.conf['payroll_manager_permission']['uri']
                    for permission in (rail.result('find_user')['permissionSets'] or [])
                )
            }
        )
        
        # Check if permission needs to be assigned
        permission_needed = rail.IfOperator(
            task_id='permission_needed',
            test=lambda : (not bool(rail.result('process_existing_permissions')['has_cost_manager'])) or (not bool(rail.result('process_existing_permissions')['has_payroll_manager'])),
            yes_task='assign_permission',
            no_task='set_managed_cost_centers'
        )
        
        # Assign the cost manager permission
        assign_permission = rail.RepliconServiceOperator(
            task_id='assign_permission',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.assign_manager_permission_payload(
                rail.result('find_user')['userDetails']['uri'],
                dag_run.conf['manager_permission']['uri'],
                dag_run.conf['payroll_manager_permission']['uri']
            )
        )

        # Set the updated managed cost centers
        set_managed_cost_centers = rail.RepliconServiceOperator(
            task_id='set_managed_cost_centers',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda: request_payload.set_managed_cost_centers_payload(
                rail.result('find_user')['userDetails']['uri'],
                [cost_center['replicon_details']['URI'] for cost_center in rail.result('get_cost_center_uris') if cost_center['replicon_details']],
                config.instance
            )
        )

        # Log when user is missing
        log_missing_user = rail.WriteLogOperator(
            task_id='log_missing_user',
            log="{{ result('create_process_log') }}",
            items=lambda: rail.result('query_cost_center_data'),
            severity="Error",
            message="Could not find user with ID '{{ dag_run.conf['managers']['manager_id'] }}'",
            properties=lambda item, dag_run: {
                'code': item['Code'],
                'name': item['Name'],
                'description': item['Description'],
                'status': "Exception",
                'action': "Update",
                'details': f"User with ID '{dag_run.conf['managers']['manager_id']}' not available / disabled in Replicon",
                'manager_id': item['Cost_Center_Manager'],
            }
        )

        def get_log_assignment_success_properties(item):
            cost_center_not_available_in_replicon = rail.result("get_cost_center_uris", "not_found_cost_centers")
            if cost_center_not_available_in_replicon is None:
                cost_center_not_available_in_replicon = []
            msg = "Cost and Payroll Manager Permission and Restrictions updated"
            if item['Code'] in cost_center_not_available_in_replicon:
                msg = f"Cost and Payroll Manager Permission updated, however cost center {item['Code']} is not assigned in Restriction as its not available in Replicon"
            return {
                'code': item['Code'],
                'name': item['Name'],
                'description': item['Description'],
                'status': "Success",
                'action': "Update",
                'details': msg,
                'manager_id': item['Cost_Center_Manager'],
            }

        # Log when assignment is successful
        log_assignment_success = rail.WriteLogOperator(
            task_id='log_assignment_success',
            log="{{ result('create_process_log') }}",
            items=lambda: rail.result('query_cost_center_data'),
            severity="Info",
            message="Cost and Payroll Manager Permission and Restrictions updated",
            properties=get_log_assignment_success_properties
        )

        # Error handling task
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_process_log') }}",
            items=lambda: rail.result('query_cost_center_data'),
            severity="Error",
            message="{{ get_error_message() }}",
            properties=lambda item: {
                'code': item['Code'],
                'name': item['Name'],
                'description': item['Description'],
                'status': "Error",
                'action': "Update",
                'details': rail.render_template("{{ get_error_message() }}"),
                'manager_id': item['Cost_Center_Manager'],
            }
        )
        
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_process_log

        # Define task dependencies
        is_permission_removed >> rail.Label("No") >> process_existing_permissions
        is_permission_removed >> rail.Label("Yes") >> remove_permission_cost_manager >> remove_permission_payroll_manager >> log_permission_removed >> rail.Label("On Error") >> catch_and_log_errors
        
        # Permission found branch
        create_process_log >> query_cost_center_data >> get_cost_center_uris >> is_cost_centers_not_found
        is_cost_centers_not_found >> rail.Label("No") >> log_no_cost_centers_not_found >> rail.Label("On Error") >> catch_and_log_errors
        is_cost_centers_not_found >> rail.Label("Yes") >> permission_found_cost_manager_empty >> permission_found_cost_manager >> rail.Label("Yes") >> permission_found_payroll_manager
        permission_found_payroll_manager >> rail.Label("No") >> log_missing_permission_payroll_manager  >> rail.Label("On Error") >> catch_and_log_errors
        permission_found_payroll_manager >> rail.Label("Yes") >> find_user >> user_found
        permission_found_cost_manager >> rail.Label("No") >> log_missing_permission_cost_manager  >> rail.Label("On Error") >> catch_and_log_errors
        
        # User found branch
        user_found >> rail.Label("Yes") >> is_permission_removed
        user_found >> rail.Label("No") >> log_missing_user  >> rail.Label("On Error") >> catch_and_log_errors
        
        # Cost center found branch
        process_existing_permissions >> permission_needed        
        # Permission needed branch
        permission_needed >> rail.Label("Yes") >> assign_permission >> set_managed_cost_centers
        permission_needed >> rail.Label("No") >> set_managed_cost_centers >> log_assignment_success >> rail.Label("On Error") >> catch_and_log_errors
        
        return dag

# Create DAGs for each instance
rail.for_each_instance(create_manager_update_dag)