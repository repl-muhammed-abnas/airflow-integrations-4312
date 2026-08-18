from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload


def process_cost_center_task_group(execution_timeout_days):
    with rail.TaskGroup(group_id='process_cost_center_task', prefix_group_id=False):

        load_valid_cost_center_data = rail.QueryCollectionOperator(
            task_id='load_valid_cost_center_data',
            name='feedfilecostcenter',
            query="""SELECT DISTINCT homecostnumbercode, homecostnumberdescription FROM validrecords WHERE NULLIF(homecostnumbercode, '') IS NOT NULL
                AND NULLIF(homecostnumberdescription, '') IS NOT NULL"""
        )

        load_valid_cost_center_data_2 = rail.QueryCollectionOperator(
            task_id='load_valid_cost_center_data_2',
            name='feedfilecostcenter2',
            query="""SELECT DISTINCT unionlocalcode, unionlocaldescription FROM validrecords WHERE NULLIF(unionlocalcode, '') IS NOT NULL AND
                NULLIF(unionlocaldescription, '') IS NOT NULL"""
        )

        get_all_cost_center_grps = rail.RepliconServiceOperator(
            task_id="get_all_cost_center_grps",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        replicon_cost_center_collection = rail.CreateCollectionOperator(
            task_id='replicon_cost_center_collection',
            source=lambda: rail.result('get_all_cost_center_grps'),
            name='repliconcostcenter'
        )

        query_cost_center_to_create = rail.QueryCollectionOperator(
            task_id='query_cost_center_to_create',
            query="""SELECT DISTINCT homecostnumbercode AS CostCode, homecostnumberdescription AS CostName FROM feedfilecostcenter
                    where LOWER(homecostnumberdescription)
                    NOT IN (SELECT DISTINCT LOWER(displayText) FROM repliconcostcenter) UNION
                    SELECT DISTINCT unionlocalcode AS CostCode, unionlocaldescription AS CostName FROM feedfilecostcenter2
                    where LOWER(unionlocaldescription) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM repliconcostcenter)"""
        )

        has_new_cost_center = rail.IfOperator(
            task_id='has_new_cost_center',
            test="{{ result('query_cost_center_to_create','length') > 0 }}",
            yes_task='create_cost_center_in_replicon',
            no_task='finish_process_cost_center'
        )

        create_cost_center_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_cost_center_in_replicon',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterHierarchyOrApplyModifications',
            items="{{ result('query_cost_center_to_create') }}",
            execution_timeout=timedelta(days=execution_timeout_days),
            data=request_payload.create_cost_center_in_replicon
        )

        finish_process_cost_center = rail.EmptyOperator(
            task_id='finish_process_cost_center'
        )

        load_valid_cost_center_data >> load_valid_cost_center_data_2 >> get_all_cost_center_grps
        get_all_cost_center_grps >> replicon_cost_center_collection >> query_cost_center_to_create
        query_cost_center_to_create >> has_new_cost_center >> rail.Label(
            'Yes') >> create_cost_center_in_replicon >> finish_process_cost_center
        has_new_cost_center >> rail.Label('No') >> finish_process_cost_center

    return load_valid_cost_center_data, finish_process_cost_center
