# intermediate.py
import rail
import uuid
from datetime import timedelta

# Import utilities
from tsystems.cost_center_hierarchy_import_v1.utils import custom_methods, request_payload, response_filter
from tsystems.cost_center_hierarchy_import_v1 import config

def create_intermediate_dag(config):
    """
    Creates the intermediate DAG for T-Systems Cost Center Hierarchy Import.
    This DAG receives a hierarchy level and operation type, then calls the appropriate
    add or update DAG to process records at that specific hierarchy level.
    
    :param config: Configuration module with settings for the instance
    :return: The created DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.intermediate_dag_id,
        description=f'T-Systems Cost Center Hierarchy Import - Intermediate DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,  # This DAG is only triggered by the master DAG
        max_active_runs=config.intermediate_child_dag_max_active_runs,

    ) as dag:
        
        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: custom_methods.can_run_batch_task_test(config.batch_task_var_name),
            yes_task='batch_task',
            no_task='start_processing'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task= 'start_processing',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
        )


        # Start processing
        start_processing = rail.EmptyOperator(task_id='start_processing')

        # Get all departments from Replicon using paged requests
        get_all_departments = rail.RepliconServicePageOperator(
            task_id='get_all_departments',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_departments_payload(),
            page_handler=lambda request, response: {
                'page': request['page'] + 1
            } if response.get('rows', []) and len(response['rows']) >= request['pagesize'] else None,
            all_result_data_handler=response_filter.combine_and_map_departments
        )
        
        # Filter records by current hierarchy level
        filter_records = rail.QueryCollectionOperator(
            task_id='filter_records',
            query="""SELECT * FROM {{ dag_run.conf.add_update_cost_centers_collection_name }}
                    WHERE hierarchy_level = {{ dag_run.conf.hierarchy_level }}
            """,
            name="level_{{ dag_run.conf['hierarchy_level'] }}_records"
        )
        
        # Check if there are records to process at this level
        has_records = rail.IfOperator(
            task_id='has_records',
            test="{{ result('filter_records', 'length') > 0 }}",
            yes_task='determine_operation',
            no_task='catch_and_log_errors'
        )
        
        # Determine which operation to perform (add or update)
        determine_operation = rail.IfOperator(
            task_id='determine_operation',
            test="{{ dag_run.conf.operation_type == 'add' }}",
            yes_task='trigger_add_dag',
            no_task='trigger_update_dag'
        )
        
        # Trigger add DAG for each cost center at this hierarchy level
        trigger_add_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_add_dag',
            items="{{ result('filter_records') }}",
            trigger_dag_id=config.add_cost_center_dag_id,
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
            conf=lambda dag_run, item: {
                'cost_center': item,
                'hierarchy_level': dag_run.conf['hierarchy_level'],
                'replicon_departments': custom_methods.get_updated_departments(get_all_departments.task_id),
                'file_name': dag_run.conf['file_name']
            }
        )

        wait_for_add_cost_center_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_cost_center_completion',
            dag_runs="{{ result('trigger_add_dag') }}",
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours)
        )
        
        # Trigger update DAG for each cost center at this hierarchy level
        trigger_update_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_update_dag',
            items="{{ result('filter_records') }}",
            trigger_dag_id=config.update_cost_center_dag_id,
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours),
            conf=lambda dag_run, item: {
                'cost_center': item,
                'hierarchy_level': dag_run.conf['hierarchy_level'],
                'replicon_departments': custom_methods.get_updated_departments(get_all_departments.task_id),
                'file_name': dag_run.conf['file_name']
            }
        )

        wait_for_update_cost_center_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_cost_center_completion',
            dag_runs="{{ result('trigger_update_dag') }}",
            execution_timeout=timedelta(hours=config.child_dag_timeout_hours)
        )

        def gather_run_ids_callable(task_ids):
            runs = []
            for task_id in task_ids:
                if rail.result(task_id):
                    runs.extend(rail.result(task_id))
            return runs


        gather_run_ids = rail.PythonOperator(
            task_id = "gather_run_ids",
            python_callable=lambda: gather_run_ids_callable([trigger_add_dag.task_id, trigger_update_dag.task_id])
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.master_log }}",
            items="{{ result('filter_records') }}",
            severity="Error",
            message="{{ get_error_message() }}",
            properties=lambda item: {
                'code': item['Code'],
                'name': item['Name'],
                'description': item['Description'],
                'status': "Error",
                'action': "Add",
                'details': rail.render_template("{{ get_error_message() }}"),
                'manager_id': item['Cost_Center_Manager']
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> start_processing

        # Define task dependencies
        start_processing >> get_all_departments >> filter_records >> has_records
        
        # No records path
        has_records >> rail.Label("No") >> catch_and_log_errors
        
        # Records exist path
        has_records >> rail.Label("Yes") >> determine_operation
        
        # Add branch
        determine_operation >> rail.Label("Yes") >> trigger_add_dag >> wait_for_add_cost_center_completion >> gather_run_ids
        
        # Update branch
        determine_operation >> rail.Label("No") >> trigger_update_dag >> wait_for_update_cost_center_completion >> gather_run_ids

        [gather_run_ids] >> rail.Label("On Error") >> catch_and_log_errors

        return dag

# Create DAGs for each instance
rail.for_each_instance(create_intermediate_dag)