# update_cost_center.py
import rail
import uuid
from datetime import timedelta

# Import utilities
from tsystems.cost_center_hierarchy_import_v1.utils import request_payload, response_filter, custom_methods
from tsystems.cost_center_hierarchy_import_v1 import config

def create_cost_center_update_dag(config):
    """
    Creates the DAG for updating existing cost centers in T-Systems Cost Center Hierarchy Import.
    This DAG processes a single cost center with changes.
    
    :param config: Configuration module with settings for the instance
    :return: The created DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.update_cost_center_dag_id,
        description=f'T-Systems Cost Center Hierarchy Import - Update DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,  # This DAG is only triggered by the intermediate DAG
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

        # Extract parent path information from cost center name
        get_cost_center_info = rail.PythonOperator(
            task_id='get_cost_center_info',
            python_callable=lambda dag_run: {
                'cost_center': dag_run.conf['cost_center'],
                'hierarchy_level': int(dag_run.conf['hierarchy_level']),
                'name': dag_run.conf['cost_center'].get('Name', ''),
                'cost_center_name': dag_run.conf['cost_center'].get('Name', '').split('|')[-1],
                'parent_path': '|'.join(dag_run.conf['cost_center'].get('Name', '').split('|')[:-1]) 
                    if int(dag_run.conf['hierarchy_level']) > 1 else None,
                'code': dag_run.conf['cost_center'].get('Code', ''),
                'ParentFullPath': dag_run.conf['cost_center']['ParentFullPath'],
                'ParentCode': dag_run.conf['cost_center']['ParentCode']
            },
        )

        is_parent_code_available = rail.IfOperator(
            task_id = "is_parent_code_available",
            test = lambda: bool(rail.result('get_cost_center_info')['ParentCode']),
            yes_task="find_cost_centers_in_replicon_dep",
            no_task="get_replicon_department_details"
        )

        # Refresh departments data if parent code not found
        get_replicon_department_details = rail.RepliconServicePageOperator(
            task_id='get_replicon_department_details',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            page_handler=lambda request, response: {
                'page': request['page'] + 1
            } if response.get('rows', []) and len(response['rows']) >= request['pagesize'] else None,
            data=request_payload.get_departments_payload,
            all_result_data_handler=response_filter.combine_and_map_departments
        )

        # Find the cost center in existing departments data
        def search_parent_in_replicon_data(data, path, value):
            return rail.find_first_by_attr_and_get_attr(data, path, value, default={})

        def find_cost_centers_in_replicon_dep_callable(dag_run):
            replicon_records = rail.load_json_artifact(dag_run.conf['replicon_departments'])
            if rail.result("get_replicon_department_details"):
                replicon_records = rail.result("get_replicon_department_details")
            parent_uri = None
            if rail.result('get_cost_center_info')['ParentCode']:
                parent_uri = search_parent_in_replicon_data(replicon_records, 'Code', rail.result('get_cost_center_info')['ParentCode'])
            if not parent_uri:
                parent_uri= search_parent_in_replicon_data(replicon_records, 'FullPath', rail.result('get_cost_center_info')['parent_path'])
            return{
                "cost_center_uri": search_parent_in_replicon_data(replicon_records, 'Code', rail.result('get_cost_center_info')['code']),
                "parent_uri": parent_uri
            }

        # Find parent in existing departments data
        find_cost_centers_in_replicon_dep = rail.PythonOperator(
            task_id='find_cost_centers_in_replicon_dep',
            python_callable=find_cost_centers_in_replicon_dep_callable,
        )
        
        def parent_cost_center_found_test():
            if not rail.result('find_cost_centers_in_replicon_dep')['cost_center_uri']:
                return False
            if rail.result('get_cost_center_info')['hierarchy_level'] == 1: # Level 1 cant have parent
                return True
            if not rail.result('find_cost_centers_in_replicon_dep')['parent_uri']:
                return False
            if rail.result('find_cost_centers_in_replicon_dep')['parent_uri']:
                return True

        # Check if cost center was found
        parent_cost_center_found = rail.IfOperator(
            task_id='parent_cost_center_found',
            test=parent_cost_center_found_test,
            yes_task='empty_parent_cost_center_found_yes_task',
            no_task='log_missing_cost_center'
        )

        empty_parent_cost_center_found_yes_task = rail.EmptyOperator(
            task_id = "empty_parent_cost_center_found_yes_task"
        )

        is_level_1 = rail.IfOperator(
            task_id = "is_level_1",
            test = lambda: rail.result('get_cost_center_info')['hierarchy_level'] == 1,
            yes_task = "update_cost_center",
            no_task = "move_cost_center"
        )

        # Move cost center to new parent
        move_cost_center = rail.RepliconServiceOperator(
            task_id='move_cost_center',
            endpoint="/services/DepartmentGroupService1.svc/MoveDepartmentGroup",
            data=lambda : request_payload.move_cost_center_payload(
                rail.result('find_cost_centers_in_replicon_dep')['cost_center_uri']['URI'],
                rail.result('find_cost_centers_in_replicon_dep')['parent_uri']['URI']
            ),
        )

        # Update cost center properties
        update_cost_center = rail.RepliconServiceOperator(
            task_id='update_cost_center',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: request_payload.update_cost_center_payload(
                dag_run.conf['cost_center'],
                rail.result('find_cost_centers_in_replicon_dep')['cost_center_uri']['URI']
            ),
        )


        def get_missing_cost_center_message(dag_run):
            if not rail.result('find_cost_centers_in_replicon_dep')['cost_center_uri']:
                return f"Cost center ({dag_run.conf['cost_center'].get('Code', '')}) not found in Replicon"
            if not rail.result('find_cost_centers_in_replicon_dep')['parent_uri']:
                return f"Cost center ({rail.result('get_cost_center_info').get('ParentFullPath', '')}) not found in Replicon"

        # Log when cost center is missing
        log_missing_cost_center = rail.WriteLogOperator(
            task_id='log_missing_cost_center',
            log="{{ result('create_process_log') }}",
            severity="Error",
            message="Could not find cost center '{{ result('get_cost_center_info')['code'] }}' to update",
            properties=lambda dag_run: {
                'code': dag_run.conf['cost_center'].get('Code', ''),
                'name': dag_run.conf['cost_center'].get('Name', ''),
                'description': dag_run.conf['cost_center'].get('Description', ''),
                'status': "Exception",
                'action': "Update",
                'details': get_missing_cost_center_message(dag_run),
                'manager_id': dag_run.conf['cost_center'].get('Cost_Center_Manager', '')
            }
        )

        # Log success for update
        log_update_success = rail.WriteLogOperator(
            task_id='log_update_success',
            log="{{ result('create_process_log') }}",
            severity="Info",
            message="Cost center '{{ dag_run.conf['cost_center'].get('Code') }}' updated successfully",
            properties={
                'code': "{{ dag_run.conf['cost_center'].get('Code', '') }}",
                'name': "{{ dag_run.conf['cost_center'].get('Name', '') }}",
                'description': "{{ dag_run.conf['cost_center'].get('Description', '') }}",
                'status': "Success",
                'action': "Update",
                'details': "Successfully updated cost center",
                'manager_id': "{{ dag_run.conf['cost_center'].get('Cost_Center_Manager', '') }}",
                'uri': "{{ result('update_cost_center').get('uri', '') }}"
            }
        )

        # Error handling task
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_process_log') }}",
            severity="Error",
            message="{{ get_error_message() }}",
            properties={
                'code': "{{ dag_run.conf['cost_center'].get('Code', '') }}",
                'name': "{{ dag_run.conf['cost_center'].get('Name', '') }}",
                'description': "{{ dag_run.conf['cost_center'].get('Description', '') }}",
                'status': "Error",
                'action': "Update",
                'details': "{{ get_error_message() }}",
                'manager_id': "{{ dag_run.conf['cost_center'].get('Cost_Center_Manager', '') }}"
            }
        )
        
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_process_log

        # Define task dependencies
        create_process_log >> get_cost_center_info >> is_parent_code_available >> rail.Label("Yes") >> find_cost_centers_in_replicon_dep >> parent_cost_center_found
        is_parent_code_available >> rail.Label("No") >> get_replicon_department_details >> find_cost_centers_in_replicon_dep
        
        # Cost center found branch
        parent_cost_center_found >> rail.Label("Yes") >> empty_parent_cost_center_found_yes_task >> is_level_1 >> rail.Label("Yes") >> update_cost_center
        is_level_1 >> rail.Label("No") >> move_cost_center
        
        # No cost center found branch
        parent_cost_center_found >> rail.Label("No") >> log_missing_cost_center  >> rail.Label("On Error") >> catch_and_log_errors
        
        # Parent update needed branch
        move_cost_center >> update_cost_center >> log_update_success  >> rail.Label("On Error") >> catch_and_log_errors
        
        # Error handling

        
        return dag

# Create DAGs for each instance
rail.for_each_instance(create_cost_center_update_dag)