from datetime import timedelta
import rail
from airflow.models import Variable
from dxctechnology.compass_iwo_details_v4.utils import request_payload, custom_methods, python_callable_method

def create_child_dag(config):
    """
    Optimized DAG that processes all tasks for a level without triggering child DAGs.
    All task creation logic is handled within this single DAG.
    """

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_process_tasks_by_level_child_{config.dag_id_postfix}',
        description=f'DXC COMPASS IWO WBS process task by level {config.dag_id_postfix} - Optimized',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Batch task control
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_tasks_for_level'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='get_all_tasks_for_level',
            end_task='finish',
        )

        # Get all tasks for the current level
        get_all_tasks_for_level = rail.QueryCollectionOperator(
            task_id="get_all_tasks_for_level",
            query="SELECT * FROM query_tasks_not_present_in_child_project WHERE levels = :levels",
            query_params={
                "levels": "{{dag_run.conf.level}}"
            }
        )

        # Check if there are tasks to process
        has_tasks_to_process = rail.IfOperator(
            task_id="has_tasks_to_process",
            test="{{ result('get_all_tasks_for_level', 'length') > 0 }}",
            yes_task="prepare_tasks_for_creation",
            no_task="no_tasks_to_process"
        )

        no_tasks_to_process = rail.EmptyOperator(
            task_id="no_tasks_to_process"
        )

        # Prepare task data for batch creation
        prepare_tasks_for_creation = rail.PythonOperator(
            task_id="prepare_tasks_for_creation",
            python_callable=lambda dag_run: [
                {
                    "level": item['levels'],
                    "taskname": item['taskname'],
                    "task_full_path": item['task_fullpath'],
                    "parent": item['parent_task_name'] if item.get('parent_present') else '',
                    "code": item.get('code'),
                    "start_date": item.get('start_date'),
                    "end_date": item.get('end_date'),
                    "parent_wbs_task_uri": item.get('uri'),
                    "parent_wbs": dag_run.conf.get("parent_wbs"),
                    "processing_wbs_uri": dag_run.conf.get('processing_wbs_uri'),
                    "processing_wbs": dag_run.conf.get('processing_wbs'),
                    "task_type": dag_run.conf.get('task_type')
                } for item in rail.load_all_records(rail.result('get_all_tasks_for_level'))
            ]
        )

        # Extract unique parent WBS task URIs
        get_unique_parent_uris = rail.PythonOperator(
            task_id="get_unique_parent_uris",
            python_callable=lambda: list(set([
                task['parent_wbs_task_uri']
                for task in rail.result('prepare_tasks_for_creation')
                if task.get('parent_wbs_task_uri')
            ]))
        )

        # Get parent WBS task details for all tasks in bulk
        get_parent_wbs_task_details_bulk = rail.RepliconServiceOperator(
            task_id="get_parent_wbs_task_details_bulk",
            endpoint="/services/TaskService1.svc/BulkGetTaskDetails",
            data=lambda: {
                "taskUris": rail.result('get_unique_parent_uris')
            }
        )

        # Build mapping of parent WBS URIs to task details
        build_parent_wbs_mapping = rail.PythonOperator(
            task_id="build_parent_wbs_mapping",
            python_callable=lambda: {
                task['uri']: task
                for task in rail.result('get_parent_wbs_task_details_bulk')
            } if rail.result('get_parent_wbs_task_details_bulk') else {}
        )

        # Check if this is level 1 (no parent task resolution needed)
        is_level_one = rail.IfOperator(
            task_id="is_level_one",
            test="{{ dag_run.conf.level == '1' }}",
            yes_task="prepare_final_task_payloads",
            no_task="get_all_tasks_of_child_project"
        )

        # For level 2+ tasks, get all existing tasks in child project to find parent URIs
        get_all_tasks_of_child_project = rail.RepliconServiceOperator(
            task_id='get_all_tasks_of_child_project',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda dag_run: request_payload.get_all_project_tasks_payload(
                dag_run.conf['processing_wbs_uri']
            ),
            response_filter=lambda response: custom_methods.extract_tasks_from_response(response)
        )

        # Build task name to URI mapping for parent resolution
        build_parent_task_mapping = rail.PythonOperator(
            task_id="build_parent_task_mapping",
            python_callable=lambda: {
                task['name']: task['uri']
                for task in rail.result('get_all_tasks_of_child_project')
                if task.get('name') and task.get('uri')
            } if rail.result('get_all_tasks_of_child_project') else {}
        )

        # Prepare final task payloads with all necessary data
        prepare_final_task_payloads = rail.PythonOperator(
            task_id="prepare_final_task_payloads",
            python_callable=lambda dag_run: python_callable_method.prepare_task_payloads_with_parents(dag_run)
        )

        # Create all tasks using ForEachItem pattern
        create_all_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_all_tasks",
            endpoint="/services/ProjectService1.svc/PutTask",
            items=lambda: rail.result('prepare_final_task_payloads'),
            data=lambda item, dag_run: item['payload'],
            retries=1,
            execution_timeout=timedelta(minutes=5)
        )

        # Log successful creations
        finish = rail.EmptyOperator(
            task_id="finish",
        )

        # Define workflow
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> get_all_tasks_for_level

        get_all_tasks_for_level >> has_tasks_to_process
        has_tasks_to_process >> rail.Label("Yes") >> prepare_tasks_for_creation
        has_tasks_to_process >> rail.Label("No") >> no_tasks_to_process >> finish

        prepare_tasks_for_creation >> get_unique_parent_uris >> get_parent_wbs_task_details_bulk
        get_parent_wbs_task_details_bulk >> build_parent_wbs_mapping >> is_level_one

        is_level_one >> rail.Label("Yes") >> prepare_final_task_payloads
        is_level_one >> rail.Label("No") >> get_all_tasks_of_child_project

        get_all_tasks_of_child_project >> build_parent_task_mapping >> prepare_final_task_payloads

        prepare_final_task_payloads >> create_all_tasks >> finish


    return dag

rail.for_each_instance(create_child_dag)