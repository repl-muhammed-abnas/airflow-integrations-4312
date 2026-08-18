import rail

from crl.user_import_usa_v1.utils.response_filter import filter_full_path_data
from crl.user_import_usa_v1.utils.request_payload import get_add_cost_center_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_cost_center_dagid,
        description='CRL User Import USA- Process Cost Center',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_cost_center,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_parent_cost_center_details = rail.RepliconServiceOperator(
            task_id="get_parent_cost_center_details",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:name",
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
                },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_cost_center_full_path'],
                filter_full_path_data(response)))
        )

        create_new_cost_center = rail.RepliconServiceOperator(
            task_id="create_new_cost_center",
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data=get_add_cost_center_payload
        )

        get_parent_cost_center_details >> create_new_cost_center

    return dag

rail.for_each_instance(create_child_dag)
