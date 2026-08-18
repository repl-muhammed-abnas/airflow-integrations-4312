"""
Process Roles - GuestTek Talent User Import Child DAG

Creates Project Roles in Replicon based on Talent job_title data.
"""
from datetime import timedelta
from uuid import uuid4
import rail

null = None


def create_child_dag(config):
    """Create child DAG for processing Project Roles."""
    with rail.create_airflow_dag(
        dag_id=config.process_roles_dag_id,
        description='GuestTek Talent User Import - Process Roles',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_roles,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        create_valid_roles_collection = rail.CreateCollectionOperator(
            task_id="create_valid_roles_collection",
            name="valid_delta_roles",
            source="{{ dag_run.conf.delta_roles | to_json }}"
        )
        
        create_replicon_roles_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_roles_collection',
            name="replicon_roles",
            source="{{ dag_run.conf.replicon_roles_details | load_all_records | to_json }}",
        )
        
        query_roles_to_create = rail.QueryCollectionOperator(
            task_id='query_roles_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_roles 
                     WHERE LOWER(role_name) NOT IN 
                     (SELECT DISTINCT LOWER(displayText) FROM replicon_roles)"""
        )
        
        has_new_roles = rail.IfOperator(
            task_id='has_new_roles',
            test="{{ result('query_roles_to_create','length') > 0 }}",
            yes_task='dummy_process_new_roles',
            no_task='finish'
        )
        
        dummy_process_new_roles = rail.EmptyOperator(task_id='dummy_process_new_roles')
        
        process_new_roles = rail.trigger_parallel_dagrun(
            task_id='process_new_roles',
            items=lambda: rail.result('query_roles_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_roles,
            trigger_dag_id=config.process_each_role_dag_id,
            conf={"role_name": "{{ item.role_name }}"},
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        finish = rail.EmptyOperator(task_id='finish')
        
        create_valid_roles_collection >> create_replicon_roles_collection >> query_roles_to_create
        query_roles_to_create >> has_new_roles >> [dummy_process_new_roles, finish]
        dummy_process_new_roles >> process_new_roles >> finish
    
    return dag


rail.for_each_instance(create_child_dag)
