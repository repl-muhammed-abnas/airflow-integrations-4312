import rail

from incyte_biosciences_international_sarl.user_import_v1.utils.response_filter import filter_departments_data
from incyte_biosciences_international_sarl.user_import_v1.utils.request_payload import get_add_department_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_departments_dagid,
        description='Lanter Delivery Systems User Import - Process Department Group',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_departments,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_parent_department_details = rail.RepliconServiceOperator(
            task_id="get_parent_department_details",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:name",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_department_full_path'],
                filter_departments_data(response)))
        )

        create_new_departments = rail.RepliconServiceOperator(
            task_id="create_new_departments",
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=get_add_department_payload
        )

        get_parent_department_details >> create_new_departments

    return dag

rail.for_each_instance(create_child_dag)
