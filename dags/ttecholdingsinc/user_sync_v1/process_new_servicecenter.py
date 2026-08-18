import rail

from ttecholdingsinc.user_sync_v1.utils.response_filter import groups_filter
from ttecholdingsinc.user_sync_v1.utils.request_payload import get_add_servicecenter_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_servicecenter_dagid,
        description='TTEC HOLDINGS INC - User Sync Process Service Center',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_servicecenter,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_parent_servicecenter_details = rail.RepliconServiceOperator(
            task_id="get_parent_servicecenter_details",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:name",
                    "urn:replicon:service-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
                },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_servicecenter_full_path'],
                groups_filter(response)))
        )

        create_new_servicecenter = rail.RepliconServiceOperator(
            task_id="create_new_servicecenter",
            endpoint="/services//ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data=get_add_servicecenter_payload
        )

        get_parent_servicecenter_details >> create_new_servicecenter

    return dag

rail.for_each_instance(create_child_dag)
