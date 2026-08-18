import rail

from tpg.user_import.utils.response_filter import groups_filter
from tpg.user_import.utils.request_payload import get_add_division_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_divisions,
        description='TPG User Import - Process Divisions',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_divisions,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_parent_division_details = rail.RepliconServiceOperator(
            task_id="get_parent_division_details",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:division-list-column:name",
                    "urn:replicon:division-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_division_full_path'],
                groups_filter(response)))
        )

        create_new_location = rail.RepliconServiceOperator(
            task_id="create_new_division",
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data=get_add_division_payload
        )

        get_parent_division_details >> create_new_location

    return dag

rail.for_each_instance(create_child_dag)
