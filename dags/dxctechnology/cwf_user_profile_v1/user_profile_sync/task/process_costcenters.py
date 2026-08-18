from datetime import timedelta
import rail
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils.request_payload import create_costcenter_in_replicon


def process_costcenters_task_group(execution_timeout_days):
    with rail.TaskGroup(group_id='process_cost_centers_task', prefix_group_id=False) as process_costcenters_task:

        load_valid_costcenters_data = rail.QueryCollectionOperator(
            task_id='load_valid_costcenters_data',
            name='feedfilecostcenter',
            query="""SELECT DISTINCT afmcostcenter FROM inputdatacollection WHERE NULLIF(afmcostcenter,'') IS NOT NULL"""
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
            query="""SELECT DISTINCT afmcostcenter FROM feedfilecostcenter WHERE LOWER(afmcostcenter) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM repliconcostcenters)"""
        )

        create_costcenters_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_costcenters_in_replicon',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterOrApplyModification',
            items="{{ result('query_costcenters_to_create') }}",
            execution_timeout=timedelta(days=execution_timeout_days),
            data=create_costcenter_in_replicon
        )

        [load_valid_costcenters_data, get_all_replicon_costcenters >> replicon_costcenters_collection] >> \
            query_costcenters_to_create >> create_costcenters_in_replicon

    return process_costcenters_task
