# add_cost_center.py
import py
import rail
import uuid
from datetime import timedelta

# Import utilities
from tsystems.cost_center_hierarchy_import_v1.utils import request_payload, response_filter, custom_methods
from tsystems.cost_center_hierarchy_import_v1 import config

def create_cost_center_add_dag(config):
    """
    Creates the DAG for adding new cost centers in T-Systems Cost Center Hierarchy Import.
    This DAG processes a single cost center at a specified hierarchy level.
    
    :param config: Configuration module with settings for the instance
    :return: The created DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.add_cost_center_dag_id,
        description=f'T-Systems Cost Center Hierarchy Import - Add DAG ({config.instance})',
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
        get_parent_info = rail.PythonOperator(
            task_id='get_parent_info',
            python_callable=lambda dag_run: {
                'cost_center':dag_run.conf['cost_center'],
                'hierarchy_level':int(dag_run.conf['hierarchy_level']),
                'name':dag_run.conf['cost_center'].get('Name', ''),
                'cost_center_name':dag_run.conf['cost_center'].get('Name', '').split('|')[-1],
                'parent_path': '|'.join(dag_run.conf['cost_center'].get('Name', '').split('|')[:-1]) 
                    if int(dag_run.conf['hierarchy_level']) > 1 else None,
                'ParentFullPath': dag_run.conf['cost_center']['ParentFullPath'],
                'ParentCode': dag_run.conf['cost_center']['ParentCode']
            },
        )

        # Check if parent is needed (hierarchy level > 1)
        need_parent = rail.IfOperator(
            task_id='need_parent',
            test=lambda: rail.result('get_parent_info')['hierarchy_level'] > 1,
            yes_task='find_parent_in_replicon_dep',
            no_task='add_without_parent'
        )

        def search_parent_in_replicon_data(data, path, value):
            return rail.find_first_by_attr_and_get_attr(data, path, value, default={})

        # Find parent in existing departments data
        find_parent_in_replicon_dep = rail.PythonOperator(
            task_id='find_parent_in_replicon_dep',
            python_callable=lambda dag_run: search_parent_in_replicon_data(rail.load_json_artifact(dag_run.conf['replicon_departments']), 'Code', rail.result('get_parent_info')['ParentCode']),
        )
        
        # Check if parent was found
        parent_found = rail.IfOperator(
            task_id='parent_found',
            test=lambda: bool(rail.result('find_parent_in_replicon_dep')),
            yes_task='use_found_parent',
            no_task='get_replicon_department_details'
        )

        # Use the found parent
        use_found_parent = rail.PythonOperator(
            task_id='use_found_parent',
            python_callable=lambda: {
                'parent_uri': rail.result('find_parent_in_replicon_dep')['URI']
            }
        )

        # Refresh departments data if parent not found
        get_replicon_department_details = rail.RepliconServicePageOperator(
            task_id='get_replicon_department_details',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            page_handler=lambda request, response: {
                'page': request['page'] + 1
            } if response.get('rows', []) and len(response['rows']) >= request['pagesize'] else None,
            data=request_payload.get_departments_payload,
            all_result_data_handler=response_filter.combine_and_map_departments
        )

        find_parent_in_refreshed_data = rail.PythonOperator(
            task_id = "find_parent_in_refreshed_data",
            python_callable=lambda: search_parent_in_replicon_data(rail.result('get_replicon_department_details'), 'FullPath', rail.result('get_parent_info')['parent_path'])
        )

        # Check if parent was found in refreshed data
        parent_found_in_refresh = rail.IfOperator(
            task_id='parent_found_in_refresh',
            test=lambda: bool(rail.result('find_parent_in_refreshed_data')),
            yes_task='use_refreshed_parent',
            no_task='log_missing_parent'
        )
        
        # Use parent from refreshed data
        use_refreshed_parent = rail.PythonOperator(
            task_id='use_refreshed_parent',
            python_callable=lambda: {
                'parent_uri': rail.result('find_parent_in_refreshed_data')['URI']
            }
        )
        
        # Log when parent is missing
        log_missing_parent = rail.WriteLogOperator(
            task_id='log_missing_parent',
            log="{{ result('create_process_log') }}",
            severity="Error",
            message="Could not find parent cost center '{{ result('get_parent_info')['parent_path'] }}' for '{{ dag_run.conf.cost_center.Code }}'",
            properties=lambda dag_run: {
                'code': dag_run.conf['cost_center']['Code'],
                'name': dag_run.conf['cost_center']['Name'],
                'description': dag_run.conf['cost_center']['Description'],
                'status': "Exception",
                'action': "Add",
                'details': f"Could not find parent cost center {rail.result('get_parent_info')['parent_path']}' for '{ dag_run.conf['cost_center']['Code'] }'",
                'manager_id': dag_run.conf['cost_center']['Cost_Center_Manager'],
            }
        )

        def get_parent_uri():
            if rail.result('use_found_parent'):
                return rail.result('use_found_parent')['parent_uri']
            if rail.result('use_refreshed_parent'):
                return rail.result('use_refreshed_parent')['parent_uri']
            raise Exception("Parent URI not found")
        
        # Add cost center with parent
        add_with_parent = rail.RepliconServiceOperator(
            task_id='add_with_parent',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: request_payload.create_cost_center_payload(
                dag_run.conf['cost_center'], 
                get_parent_uri()
            ),
        )
        
        # Add cost center without parent (top level)
        # Ideally the top level should be only 1, still adding this to have support for full level
        # Replicon supports only 1 top level department (Cost Center)
        add_without_parent = rail.RepliconServiceOperator(
            task_id='add_without_parent',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: request_payload.create_cost_center_payload(
                dag_run.conf['cost_center'], 
                None
            ),
        )

        # Log success for add with parent
        log_add_success = rail.WriteLogOperator(
            task_id='log_add_success',
            log="{{ result('create_process_log') }}",
            severity="Info",
            message="Cost center '{{ dag_run.conf.cost_center.Code }}' added successfully",
            properties=lambda dag_run: {
                'code': dag_run.conf['cost_center']['Code'],
                'name': dag_run.conf['cost_center']['Name'],
                'description': dag_run.conf['cost_center']['Description'],
                'status': "Success",
                'action': "Add",
                'details': "Cost center added successfully",
                'manager_id': dag_run.conf['cost_center']['Cost_Center_Manager'],
                'uri': rail.result('add_with_parent')['uri']
            }
        )
        
        # Log success for add without parent
        log_add_without_parent_success = rail.WriteLogOperator(
            task_id='log_add_without_parent_success',
            log="{{ result('create_process_log') }}",
            severity="Info",
            message="Cost center '{{ dag_run.conf.cost_center.Code }}' added successfully at top level",
            properties=lambda dag_run: {
                'code': dag_run.conf['cost_center']['Code'],
                'name': dag_run.conf['cost_center']['Name'],
                'description': dag_run.conf['cost_center']['Description'],
                'status': "Success",
                'action': "Add",
                'details': f"Cost center '{ dag_run.conf['cost_center']['Code']}' added successfully at top level",
                'manager_id': dag_run.conf['cost_center']['Cost_Center_Manager'],
                'uri': rail.result('add_without_parent')['uri']
            }
        )

        # Error handling task
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_process_log') }}",
            severity="Error",
            message="{{ get_error_message() }}",
            properties=lambda dag_run: {
                'code': dag_run.conf['cost_center']['Code'],
                'name': dag_run.conf['cost_center']['Name'],
                'description': dag_run.conf['cost_center']['Description'],
                'status': "Error",
                'action': "Add",
                'details': rail.render_template("{{ get_error_message() }}"),
                'manager_id': dag_run.conf['cost_center']['Cost_Center_Manager'],
            }
        )
        
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_process_log

        # Define task dependencies
        create_process_log >> get_parent_info >> need_parent
        
        # Parent needed branch
        need_parent >> rail.Label("Yes") >> find_parent_in_replicon_dep >> parent_found
        parent_found >> rail.Label("Yes") >> use_found_parent >> add_with_parent >> log_add_success
        parent_found >> rail.Label("No") >> get_replicon_department_details >> find_parent_in_refreshed_data >> parent_found_in_refresh
        
        parent_found_in_refresh >> rail.Label("Yes") >> use_refreshed_parent >> add_with_parent
        parent_found_in_refresh >> rail.Label("No") >> log_missing_parent >> rail.Label("On Error") >> catch_and_log_errors
        
        # No parent needed branch
        need_parent >> rail.Label("No") >> add_without_parent >> log_add_without_parent_success >> rail.Label("On Error") >> catch_and_log_errors
        
        # Success/failure paths
        log_add_success >> rail.Label("On Error") >> catch_and_log_errors

        return dag

# Create DAGs for each instance
rail.for_each_instance(create_cost_center_add_dag)
