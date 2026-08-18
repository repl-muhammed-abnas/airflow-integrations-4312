"""
Process Groups - GuestTek Talent User Import Child DAG

Creates UserType groups (Employee Types) in Replicon based on Talent data.
NOTE: Locations are pre-configured identifiers in the mapper, so no location creation is needed.
"""
from datetime import timedelta
import rail
from guesttekinteractive.talent_user_import.utils import request_payload

null = None


def create_child_dag(config):
    """Create child DAG for processing UserType groups."""
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dag_id,
        description='GuestTek Talent User Import - Process Groups (UserTypes)',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        create_valid_usertypes_collection = rail.CreateCollectionOperator(
            task_id="create_valid_usertypes_collection",
            name="valid_delta_usertypes",
            source="{{ dag_run.conf.delta_usertypes | to_json }}"
        )
        
        create_replicon_usertypes_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_usertypes_collection',
            name="replicon_usertypes",
            source="{{ dag_run.conf.replicon_usertypes_details | load_all_records | to_json }}",
        )
        
        query_usertypes_to_create = rail.QueryCollectionOperator(
            task_id='query_usertypes_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_usertypes 
                     WHERE LOWER(usertype) NOT IN 
                     (SELECT DISTINCT LOWER(fullpath) FROM replicon_usertypes)"""
        )
        
        has_new_usertypes = rail.IfOperator(
            task_id='has_new_usertypes',
            test="{{ result('query_usertypes_to_create','length') > 0 }}",
            yes_task='dummy_process_new_usertypes',
            no_task='finish'
        )
        
        dummy_process_new_usertypes = rail.EmptyOperator(task_id='dummy_process_new_usertypes')
        
        process_new_usertypes = rail.trigger_parallel_dagrun(
            task_id='process_new_usertypes',
            items=lambda: rail.result('query_usertypes_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_usertypes,
            trigger_dag_id=config.process_new_usertypes,
            conf={"full_path": "{{ item.usertype }}"},
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        finish = rail.EmptyOperator(task_id='finish')
        
        create_valid_usertypes_collection >> create_replicon_usertypes_collection >> query_usertypes_to_create
        query_usertypes_to_create >> has_new_usertypes >> [dummy_process_new_usertypes, finish]
        dummy_process_new_usertypes >> process_new_usertypes >> finish
    
    return dag


rail.for_each_instance(create_child_dag)
