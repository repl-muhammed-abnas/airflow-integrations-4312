"""
Process Service Centers - GuestTek Talent User Import Child DAG

Creates Service Centers in Replicon based on Talent job_type data.
"""
from datetime import timedelta
import rail

null = None


def create_child_dag(config):
    """Create child DAG for processing Service Centers."""
    with rail.create_airflow_dag(
        dag_id=config.process_service_centers_dag_id,
        description='GuestTek Talent User Import - Process Service Centers',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_service_centers,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        create_valid_sc_collection = rail.CreateCollectionOperator(
            task_id="create_valid_sc_collection",
            name="valid_delta_service_centers",
            source="{{ dag_run.conf.delta_service_centers | to_json }}"
        )
        
        create_replicon_sc_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_sc_collection',
            name="replicon_service_centers",
            source="{{ dag_run.conf.replicon_service_centers_details | load_all_records | to_json }}",
        )
        
        query_sc_to_create = rail.QueryCollectionOperator(
            task_id='query_sc_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_service_centers 
                     WHERE LOWER(service_center_name) NOT IN 
                     (SELECT DISTINCT LOWER(displayText) FROM replicon_service_centers)"""
        )
        
        has_new_service_centers = rail.IfOperator(
            task_id='has_new_service_centers',
            test="{{ result('query_sc_to_create','length') > 0 }}",
            yes_task='dummy_process_new_sc',
            no_task='finish'
        )
        
        dummy_process_new_sc = rail.EmptyOperator(task_id='dummy_process_new_sc')
        
        process_new_service_centers = rail.trigger_parallel_dagrun(
            task_id='process_new_service_centers',
            items=lambda: rail.result('query_sc_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_service_centers,
            trigger_dag_id=config.process_each_service_center_dag_id,
            conf={"service_center_name": "{{ item.service_center_name }}"},
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        finish = rail.EmptyOperator(task_id='finish')
        
        create_valid_sc_collection >> create_replicon_sc_collection >> query_sc_to_create
        query_sc_to_create >> has_new_service_centers >> [dummy_process_new_sc, finish]
        dummy_process_new_sc >> process_new_service_centers >> finish
    
    return dag


rail.for_each_instance(create_child_dag)
