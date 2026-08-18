import rail

from tpg.user_import.utils.response_filter import groups_filter
from tpg.user_import.utils.request_payload import get_add_employeetype_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_employee_types,
        description='TPG User Import - Process Department',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_employee_types,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_parent_employee_type_details = rail.RepliconServiceOperator(
            task_id="get_parent_employee_type_details",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:name",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_employeetype_full_path'],
                                                               groups_filter(response)))
        )

        create_new_employee_types = rail.RepliconServiceOperator(
            task_id="create_new_employee_types",
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data=get_add_employeetype_payload
        )

        get_parent_employee_type_details >> create_new_employee_types

    return dag

rail.for_each_instance(create_child_dag)
