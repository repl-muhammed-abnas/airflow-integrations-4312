from datetime import timedelta
import rail
from matlensilver.user_sync_integration.user_sync.utils import request_payload


def process_employeetypes_task_group(execution_timeout_days):
    with rail.TaskGroup(group_id='process_employeetypes_task', prefix_group_id=False):

        load_valid_employeetype_data = rail.QueryCollectionOperator(
            task_id='load_valid_employeetype_data',
            name='feedfileemployeetypes',
            query="""SELECT DISTINCT employeetype, employeetypecode FROM validrecords
                    WHERE NULLIF(employeetype, '') IS NOT NULL and  NULLIF(employeetypecode, '') IS NOT NULL"""
        )

        get_all_employeetype_grps = rail.RepliconServiceOperator(
            task_id="get_all_employeetype_grps",
            endpoint="services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
        )

        replicon_employeetypes_collection = rail.CreateCollectionOperator(
            task_id='replicon_employeetypes_collection',
            source=lambda: rail.result('get_all_employeetype_grps'),
            name='repliconemployeetypes'
        )

        query_employeetypes_to_create = rail.QueryCollectionOperator(
            task_id='query_employeetypes_to_create',
            query="""SELECT DISTINCT employeetype, employeetypecode FROM feedfileemployeetypes where LOWER(employeetype) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM repliconemployeetypes)"""
        )

        has_new_employeetypes = rail.IfOperator(
            task_id='has_new_employeetypes',
            test="{{ result('query_employeetypes_to_create','length') > 0 }}",
            yes_task='create_employeetypes_in_replicon',
            no_task='finish_process_employeetypes'
        )

        create_employeetypes_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_employeetypes_in_replicon',
            endpoint='/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupHierarchyOrApplyModifications',
            items="{{ result('query_employeetypes_to_create') }}",
            execution_timeout=timedelta(days=execution_timeout_days),
            data=request_payload.create_employeetypes_in_replicon
        )

        finish_process_employeetypes = rail.EmptyOperator(
            task_id='finish_process_employeetypes'
        )

        load_valid_employeetype_data >> get_all_employeetype_grps >> replicon_employeetypes_collection >> query_employeetypes_to_create
        query_employeetypes_to_create >> has_new_employeetypes >> rail.Label(
            'Yes') >> create_employeetypes_in_replicon >> finish_process_employeetypes
        has_new_employeetypes >> rail.Label(
            'No') >> finish_process_employeetypes

    return load_valid_employeetype_data, finish_process_employeetypes
