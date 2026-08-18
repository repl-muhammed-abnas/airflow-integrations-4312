from datetime import timedelta
import rail

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_groups,
        description='Deltek Costpoint User Import - Process Groups',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_valid_delta_records_departments = rail.QueryCollectionOperator(
            task_id='query_valid_delta_records_departments',
            query="""SELECT DISTINCT org, org_id, org_name FROM valid_records""",
            name='valid_delta_departments',
        )

        get_all_department_grps = rail.RepliconServiceOperator(
            task_id="get_all_department_grps",
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )

        create_replicon_departments_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_departments_collection',
            columns=['displayText', 'parameterCorrelationId', 'slug', 'uri'],
            name="replicon_departments",
            source="{{ result('get_all_department_grps') | to_json }}",
        )

        query_departments_to_create = rail.QueryCollectionOperator(
            task_id='query_departments_to_create',
            query="""SELECT DISTINCT * FROM valid_delta_departments where LOWER(org) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicon_departments)"""
        )

        has_new_departments = rail.IfOperator(
            task_id='has_new_departments',
            test="{{ result('query_departments_to_create','length') > 0 }}",
            yes_task='dummy_process_new_departments',
            no_task='finish'
        )

        dummy_process_new_departments = rail.EmptyOperator(
            task_id='dummy_process_new_departments'
        )

        process_new_departments = rail.trigger_parallel_dagrun(
            task_id='process_new_departments',
            items=lambda: rail.result('query_departments_to_create'),
            parallel_count=config.trigger_parallel_process_departments,
            trigger_dag_id=config.process_new_departments,
            conf={
                "description": "{{ item.org_name }}",
                "departmentname": "{{ item.org }}",
                "departmentcode": "{{ item.org_id }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_valid_delta_records_departments >> get_all_department_grps >> create_replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_departments >> rail.Label('No') >> finish
        has_new_departments >> rail.Label('Yes') >> dummy_process_new_departments >> process_new_departments >> finish

    return dag

rail.for_each_instance(create_child_dag)
