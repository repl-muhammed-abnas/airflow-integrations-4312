from datetime import timedelta
import rail
from dxctechnology.psa_user_profile_gsap.user_profile_sync.utils.request_payload import create_costcenter_in_replicon, update_costcenter_in_replicon, \
    get_child_cost_centers_data


def process_costcenters_task_group(execution_timeout_days):
    with rail.TaskGroup(group_id='process_cost_centers_task', prefix_group_id=False) as process_costcenters_task:

        load_valid_costcenters_data = rail.QueryCollectionOperator(
            task_id='load_valid_costcenters_data',
            name='feedfilecostcenter',
            query="""SELECT DISTINCT costcenter FROM inputdatacollection WHERE NULLIF(costcenter,'') IS NOT NULL"""
        )

        get_all_replicon_costcenters = rail.RepliconServiceOperator(
            task_id='get_all_replicon_costcenters',
            endpoint='/services/CostCenterService1.svc/GetAllCostCenters'
        )

        replicon_costcenters_collection = rail.CreateCollectionOperator(
            task_id='replicon_costcenters_collection',
            source=lambda: rail.result('get_all_replicon_costcenters'),
            name='repliconcostcenters'
        )

        query_costcenters_to_create = rail.QueryCollectionOperator(
            task_id='query_costcenters_to_create',
            query="""SELECT DISTINCT costcenter FROM feedfilecostcenter WHERE LOWER(costcenter) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM repliconcostcenters)"""
        )

        create_costcenters_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_costcenters_in_replicon',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterOrApplyModification',
            items="{{ result('query_costcenters_to_create') }}",
            execution_timeout=timedelta(days=execution_timeout_days),
            data=create_costcenter_in_replicon
        )

        query_costcenters_to_update = rail.QueryCollectionOperator(
            task_id='query_costcenters_to_update',
            query="""SELECT DISTINCT costcenter FROM feedfilecostcenter WHERE LOWER(costcenter) IN
                    (SELECT DISTINCT LOWER(displayText) FROM repliconcostcenters) and costcenter NOT IN ("PSA Cost Center") """,
            name='repliconexistingcostcenters'
        )

        get_all_child_cost_centers = rail.RepliconServiceOperator(
            task_id='get_all_child_cost_centers',
            endpoint='/services/CostCenterListService1.svc/GetChildHierarchyData',
            data=get_child_cost_centers_data,
            response_filter=lambda response: list(map(lambda item: {
                'name': item['cells'][0]['textValue']
            }, response.json()['d']['rows']))
        )

        replicon_child_costcenters_collection = rail.CreateCollectionOperator(
            task_id='replicon_child_costcenters_collection',
            source=lambda: rail.result('get_all_child_cost_centers'),
            name='repliconchildcostcenters'
        )

        query_child_costcenters_to_update = rail.QueryCollectionOperator(
            task_id='query_child_costcenters_to_update',
            query="""SELECT DISTINCT costcenter FROM repliconexistingcostcenters WHERE LOWER(costcenter) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM repliconchildcostcenters)"""
        )

        update_costcenters_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_costcenters_in_replicon',
            endpoint='/services/CostCenterService1.svc/MoveCostCenter',
            items="{{ result('query_child_costcenters_to_update') }}",
            execution_timeout=timedelta(days=execution_timeout_days),
            data=update_costcenter_in_replicon
        )

        [load_valid_costcenters_data, get_all_replicon_costcenters >> replicon_costcenters_collection] >> \
            query_costcenters_to_create >> create_costcenters_in_replicon >> query_costcenters_to_update >> \
            get_all_child_cost_centers >> replicon_child_costcenters_collection >> query_child_costcenters_to_update >> \
            update_costcenters_in_replicon

    return process_costcenters_task
