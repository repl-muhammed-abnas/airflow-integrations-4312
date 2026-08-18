from datetime import timedelta
import rail
from matlensilver.user_sync_integration.user_sync.utils import request_payload


def process_locations_task_group(execution_timeout_days):
    with rail.TaskGroup(group_id='process_locations_task', prefix_group_id=False):

        load_valid_locations_data = rail.QueryCollectionOperator(
            task_id='load_valid_locations_data',
            name='feedfilelocations',
            query="""SELECT DISTINCT homezip as zipcode FROM validrecords WHERE NULLIF(homezip, '') IS NOT NULL
                    UNION
                    SELECT DISTINCT workzip as zipcode FROM validrecords WHERE NULLIF(workzip, '') IS NOT NULL"""
        )

        get_all_locations_grps = rail.RepliconServiceOperator(
            task_id="get_all_locations_grps",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        replicon_locations_collection = rail.CreateCollectionOperator(
            task_id='replicon_locations_collection',
            source=lambda: rail.result('get_all_locations_grps'),
            name='repliconlocations'
        )

        query_locations_to_create = rail.QueryCollectionOperator(
            task_id='query_locations_to_create',
            query="""SELECT DISTINCT zipcode FROM feedfilelocations where LOWER(zipcode) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM repliconlocations)"""
        )

        has_new_locations = rail.IfOperator(
            task_id='has_new_locations',
            test="{{ result('query_locations_to_create','length') > 0 }}",
            yes_task='create_locations_in_replicon',
            no_task='finish_process_locations'
        )

        create_locations_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_locations_in_replicon',
            endpoint='/services/LocationService1.svc/CreateLocationHierarchyOrApplyModifications',
            items="{{ result('query_locations_to_create') }}",
            execution_timeout=timedelta(days=execution_timeout_days),
            data=request_payload.create_locations_in_replicon
        )

        finish_process_locations = rail.EmptyOperator(
            task_id='finish_process_locations'
        )

        load_valid_locations_data >> get_all_locations_grps >> replicon_locations_collection >> query_locations_to_create
        query_locations_to_create >> has_new_locations >> rail.Label(
            'Yes') >> create_locations_in_replicon >> finish_process_locations
        has_new_locations >> rail.Label('No') >> finish_process_locations

    return load_valid_locations_data, finish_process_locations
