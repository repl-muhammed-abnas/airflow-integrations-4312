from uuid import uuid4
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils.response_filter import get_all_employee_type_from_replicon_filter


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.employee_type_dag_id,
        description=f'vialto_partners_user_import_add_employee_type_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_groups_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        get_parent_employee_type_details = rail.RepliconServiceOperator(
            task_id="get_parent_employee_type_details",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:name",
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{ dag_run.conf.parent_name }}"
                        }
                    }
                }
            },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_employee_full_path'],
                                                               get_all_employee_type_from_replicon_filter(response)))
        )

        create_new_employee_type = rail.RepliconServiceOperator(
            task_id="create_new_employee_type",
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data=lambda dag_run: {
                "employeeTypeGroup": {
                    "parent": {
                        "uri": rail.result("get_parent_employee_type_details")[0]['uri']
                    }
                } if dag_run.conf['length'] != '1' else None,
                "modifications": {
                    "name": dag_run.conf['name'],
                    "isEnabled": 1
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        get_parent_employee_type_details >> create_new_employee_type

    return dag


rail.for_each_instance(create_child_dag)
