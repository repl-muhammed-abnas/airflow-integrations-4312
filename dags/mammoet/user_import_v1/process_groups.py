from datetime import timedelta
from uuid import uuid4
import rail
from mammoet.user_import_v1.utils.response_filter import get_groups_data_handler
from mammoet.user_import_v1.utils.custom_methods import PARENT_LEGAL_ENTITY, LOCATION_DELIMITER, get_location_details_from_code

null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_process_groups_child_dag_id,
        description="Mammoet User Import Process groups",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_groups_max_active_runs

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        query_location = rail.QueryCollectionOperator(
            task_id="query_location",
            query="""SELECT DISTINCT pd.location, pd.location_code, pd.legal_entity_code from payload_data pd""",
            name="payload_location_details_raw"
        )

        def get_converted_locations_data(item):
            if not item:
                return []
            parent_location = get_location_details_from_code(
                config.LOCATION_CODE_MAPPER_TO_USE, item['legal_entity_code'][:2])
            return [
                {
                    'location': parent_location['Country'],
                    'location_fullpath':parent_location['Country'],
                    'location_code': parent_location['ISO_Code'],
                    'parent_location_code': parent_location['ISO_Code'],
                    'length': 1
                },
                {
                    'location': item['location'],
                    'location_fullpath':f"""{parent_location['Country']}{LOCATION_DELIMITER}{item['location']}""",
                    'location_code': item['location_code'],
                    'parent_location_code': parent_location['ISO_Code'],
                    'length': 2
                }
            ]

        convert_location_data = rail.DataAdaptorOperator(
            task_id="convert_location_data",
            source="{{result('query_location')}}",
            columns=['location', 'location_fullpath',
                     'location_code', 'parent_location_code', 'length'],
            data=get_converted_locations_data
        )

        create_final_location_data = rail.CreateCollectionOperator(
            task_id="create_final_location_data",
            source="{{result('convert_location_data')}}",
            name="payload_location_details"
        )

        get_replicon_location_details = rail.RepliconServiceOperator(
            task_id="get_replicon_location_details",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:effectively-enabled",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        create_location_collection = rail.CreateCollectionOperator(
            task_id="create_location_collection",
            source=lambda: rail.result("get_replicon_location_details"),
            name="replicon_location_details",
            columns={'code': 'code', 'enabled': 'enabled',
                     'full_path': 'full_path', 'name': 'name', 'uri': 'uri'}
        )

        query_location_to_add = rail.QueryCollectionOperator(
            task_id="query_location_to_add",
            query="""SELECT * FROM payload_location_details pld
                     WHERE pld.location_fullpath NOT IN (SELECT DISTINCT rld.full_path FROM replicon_location_details rld) ORDER BY pld."length"
                    """
        )

        has_any_location_to_add = rail.IfOperator(
            task_id="has_any_location_to_add",
            test="{{result('query_location_to_add','length') > 0 }}",
            yes_task="process_add_location_to_replicon"
        )

        process_add_location_to_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id="process_add_location_to_replicon",
            trigger_dag_id=config.user_import_add_location_child_dag_id,
            items="{{result('query_location_to_add')}}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                **{
                    "parent_location_full_path": LOCATION_DELIMITER.join(
                        item['location_fullpath'].split(LOCATION_DELIMITER)[:-1]) if item['length'] != '1' else ""
                }
            }
        )

        wait_for_process_add_location_to_replicon = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_add_location_to_replicon",
            dag_runs="{{result('process_add_location_to_replicon')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        query_cost_center = rail.QueryCollectionOperator(
            task_id="query_cost_center",
            query="""SELECT DISTINCT pd.cost_center, pd.cost_center_code from payload_data pd""",
            name="payload_cost_center_details"
        )

        get_replicon_cost_center_details = rail.RepliconServiceOperator(
            task_id="get_replicon_cost_center_details",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:effectively-enabled",
                    "urn:replicon:cost-center-list-column:full-path",
                    "urn:replicon:cost-center-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        create_cost_center_collection = rail.CreateCollectionOperator(
            task_id="create_cost_center_collection",
            source=lambda: rail.result("get_replicon_cost_center_details"),
            name="replicon_cost_center_details",
            columns={'code': 'code', 'enabled': 'enabled',
                     'full_path': 'full_path', 'name': 'name', 'uri': 'uri'}
        )

        query_cost_center_to_add = rail.QueryCollectionOperator(
            task_id="query_cost_center_to_add",
            query="""SELECT * FROM payload_cost_center_details pccd \
                WHERE pccd.cost_center NOT IN (SELECT DISTINCT rccd.name FROM replicon_cost_center_details rccd)
                """
        )

        has_any_cost_center_to_add = rail.IfOperator(
            task_id="has_any_cost_center_to_add",
            test="{{result('query_cost_center_to_add', 'length') > 0}}",
            yes_task="add_cost_center_to_replicon"
        )

        add_cost_center_to_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_cost_center_to_replicon",
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            items="{{result('query_cost_center_to_add')}}",
            data=lambda item: {
                "costcenter": null,
                "modifications": {
                    "name": item['cost_center'],
                    "codeToApply": {
                        "value": item['cost_center_code']
                    },
                    "descriptionToApply": null,
                    "isEnabled": "1"
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        query_legal_entities = rail.QueryCollectionOperator(
            task_id="query_legal_entities",
            query="""SELECT DISTINCT pd.legal_entity, pd.legal_entity_code, pd.legal_entity_full_path from valid_payload_data pd""",
            name="payload_legal_entities"
        )

        get_replicon_legal_entities_details = rail.RepliconServiceOperator(
            task_id="get_replicon_legal_entities_details",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:effectively-enabled",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        create_legal_entities_collection = rail.CreateCollectionOperator(
            task_id="create_legal_entities_collection",
            source=lambda: rail.result("get_replicon_legal_entities_details"),
            name="replicon_legal_entities_details",
            columns={'code': 'code', 'enabled': 'enabled',
                     'full_path': 'full_path', 'name': 'name', 'uri': 'uri'}
        )

        query_legal_entities_to_add = rail.QueryCollectionOperator(
            task_id="query_legal_entities_to_add",
            query="""SELECT * FROM payload_legal_entities ple
                WHERE ple.legal_entity_full_path NOT IN (SELECT DISTINCT rled.full_path FROM replicon_legal_entities_details rled)
                """
        )

        has_any_legal_entities_to_add = rail.IfOperator(
            task_id="has_any_legal_entities_to_add",
            test="{{result('query_legal_entities_to_add','length') > 0 }}",
            yes_task="add_legal_entities_to_replicon",
        )

        add_legal_entities_to_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="add_legal_entities_to_replicon",
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            items="{{result('query_legal_entities_to_add')}}",
            data=lambda item: {
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": null,
                        "parent": null,
                        "name": PARENT_LEGAL_ENTITY,
                        "parameterCorrelationId": null
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": item['legal_entity'],
                    "codeToApply": {
                        "value": item['legal_entity_code']
                    },
                    "descriptionToApply": null,
                    "isEnabled": "1"
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        query_location >> convert_location_data >> create_final_location_data\
            >> get_replicon_location_details >> create_location_collection >> query_location_to_add\
            >> has_any_location_to_add >> process_add_location_to_replicon >> wait_for_process_add_location_to_replicon
        query_cost_center >> get_replicon_cost_center_details >> create_cost_center_collection >> query_cost_center_to_add\
            >> has_any_cost_center_to_add >> add_cost_center_to_replicon
        query_legal_entities >> get_replicon_legal_entities_details >> create_legal_entities_collection >> query_legal_entities_to_add\
            >> has_any_legal_entities_to_add >> add_legal_entities_to_replicon

    return dag


rail.for_each_instance(create_main_dag)
