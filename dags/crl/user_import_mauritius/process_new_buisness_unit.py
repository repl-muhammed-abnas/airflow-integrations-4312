import rail

from crl.user_import_mauritius.utils.response_filter import filter_full_path_data
from crl.user_import_mauritius.utils.request_payload import get_add_buisness_unit_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_buisness_unit_dagid,
        description='CRL User Import Mauritius- Process Buisness Unit',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_buisness_unit,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_parent_buisness_unit_details = rail.RepliconServiceOperator(
            task_id="get_parent_buisness_unit_details",
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
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_buisness_unit_full_path'],
                filter_full_path_data(response)))
        )

        create_new_buisness_unit = rail.RepliconServiceOperator(
            task_id="create_new_buisness_unit",
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data=get_add_buisness_unit_payload
        )

        get_parent_buisness_unit_details >> create_new_buisness_unit

    return dag

rail.for_each_instance(create_child_dag)
