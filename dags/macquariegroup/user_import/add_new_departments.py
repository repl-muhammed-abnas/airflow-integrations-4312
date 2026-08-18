import rail
from macquariegroup.user_import.utils.data_handlers import get_all_department_filter
from macquariegroup.user_import.utils.request_payload import get_add_department_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_user_import_add_new_departments_{config.instance}',
        description=f'Macquarie User Import process_groups and location(UDF) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.add_departments_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        get_parent_department_details = rail.RepliconServiceOperator(
            task_id="get_parent_department_details",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:name",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_department_full_path'],
                                                               get_all_department_filter(response)))
        )

        create_new_departments = rail.RepliconServiceOperator(
            task_id="create_new_departments",
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=get_add_department_payload
        )

        get_parent_department_details >> create_new_departments

    return dag


rail.for_each_instance(create_child_dag)
