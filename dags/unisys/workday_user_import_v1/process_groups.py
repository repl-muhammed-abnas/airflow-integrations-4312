"""
Workday-Replicon User Import - Group Processing Child DAG

This module creates a child DAG that processes organizational groups including:
- Locations (office locations and hierarchies)
- Employee Types (user type classifications)
- Divisions (Company Code and Cost Center combinations)

The DAG compares Workday data against existing Replicon data and triggers
parallel child DAGs to create or update missing entities.

Workflow:
    1. Query distinct locations, user types, and divisions from valid input data
    2. Compare against existing Replicon entities
    3. Identify entities that need to be created or updated
    4. Trigger parallel child DAGs for batch processing

Functions:
    create_child_dag(config): Creates the group processing child DAG
"""
from datetime import timedelta
from uuid import uuid4
import rail
from unisys.workday_user_import_v1.utils import request_payload, response_filters, custom_method

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    """
    Create child DAG for processing groups (locations, user types, divisions).

    This DAG orchestrates the creation of organizational entities in Replicon
    by comparing input data with existing entities and triggering appropriate
    child DAGs for parallel processing.

    Args:
        config: Configuration object containing:
            - process_groups_dag_id (str): DAG identifier
            - company_key (str): Replicon company identifier
            - replicon_conn_id (str): Airflow connection ID
            - max_active_runs_process_groups (int): Concurrent execution limit
            - trigger_parallel_dagrun_count_* (int): Parallel processing counts
            - process_new_locations (str): Location creation child DAG ID
            - process_new_usertypes (str): User type creation child DAG ID
            - process_update_divisions (str): Division update child DAG ID
            - execution_timeout_days (int): Maximum execution timeout

    Expected dag_run.conf:
        replicon_location_details: Existing Replicon location data
        replicon_usertypes_details: Existing Replicon user type data
        replicon_division_details: Existing Replicon division data

    Returns:
        airflow.DAG: Configured child DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.process_groups_dag_id,
        description='Unisys Workday User Import - Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_locations = rail.QueryCollectionOperator(
            task_id='query_valid_delta_records_locations',
            query="""SELECT DISTINCT location, location_description FROM valid_data""",
            name='valid_delta_locations',
        )

        get_payload_locations = rail.PythonOperator(
            task_id='get_payload_locations',
            python_callable=custom_method.get_payload_locations
        )

        create_payload_location_collection = rail.CreateCollectionOperator(
            task_id="create_payload_location_collection",
            name="payload_locations",
            columns=['full_path', 'locationcode'],
            source="{{ result('get_payload_locations') | to_json }}"
        )

        create_replicon_location_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_location_collection",
            name="replicon_locations",
            source="{{ dag_run.conf.replicon_location_details | load_all_records | to_json }}"
        )

        query_locations_to_create = rail.QueryCollectionOperator(
            task_id='query_locations_to_create',
            query="""SELECT DISTINCT * FROM payload_locations where LOWER(full_path) NOT IN
                    (SELECT DISTINCT LOWER(fullpath) FROM replicon_locations)"""
        )

        has_new_locations = rail.IfOperator(
            task_id='has_new_locations',
            test="{{ result('query_locations_to_create','length') > 0 }}",
            yes_task='dummy_process_new_locations',
            no_task='finish'
        )

        dummy_process_new_locations = rail.EmptyOperator(
            task_id='dummy_process_new_locations'
        )

        process_new_locations = rail.trigger_parallel_dagrun(
            task_id='process_new_locations',
            items=lambda: rail.result('query_locations_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_locations,
            trigger_dag_id=config.process_new_locations,
            conf={
                "full_path": "{{ item.full_path }}",
                "locationcode": "{{ item.locationcode }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_valid_delta_records_usertypes = rail.QueryCollectionOperator(
            task_id='query_valid_delta_records_usertypes',
            name='valid_delta_usertypes',
            query="""SELECT DISTINCT user_type FROM valid_data"""
        )

        create_replicon_usertypes_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_usertypes_collection',
            name="replicon_usertypes",
            source="{{ dag_run.conf.replicon_usertypes_details | load_all_records | to_json }}",
        )

        query_usertypes_to_create = rail.QueryCollectionOperator(
            task_id='query_usertypes_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_usertypes where LOWER(user_type) NOT IN
                    (SELECT DISTINCT LOWER(fullpath) FROM replicon_usertypes)"""
        )

        has_new_usertypes = rail.IfOperator(
            task_id='has_new_usertypes',
            test="{{ result('query_usertypes_to_create','length') > 0 }}",
            yes_task='dummy_process_new_usertypes',
            no_task='finish'
        )

        dummy_process_new_usertypes = rail.EmptyOperator(
            task_id='dummy_process_new_usertypes'
        )

        process_new_usertypes = rail.trigger_parallel_dagrun(
            task_id='process_new_usertypes',
            items=lambda: rail.result('query_usertypes_to_create'),
            parallel_count=config.trigger_parallel_dagrun_count_process_usertypes,
            trigger_dag_id=config.process_new_usertypes,
            conf={
                "full_path": "{{ item.user_type }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        query_valid_delta_records_locations >> get_payload_locations >> create_payload_location_collection >> create_replicon_location_collection
        create_replicon_location_collection >> query_locations_to_create >> has_new_locations >> rail.Label('No') >> finish
        has_new_locations >> rail.Label('Yes') >> dummy_process_new_locations >> process_new_locations >> finish

        query_valid_delta_records_usertypes >> create_replicon_usertypes_collection >> query_usertypes_to_create >> has_new_usertypes
        has_new_usertypes >> rail.Label('No') >> finish
        has_new_usertypes >> rail.Label('Yes') >> dummy_process_new_usertypes >> process_new_usertypes >> finish

    return dag

rail.for_each_instance(create_child_dag)
