from datetime import timedelta
import rail
from matlensilver.user_sync_integration.user_sync.utils import request_payload


def process_departments_task_group(execution_timeout_days):
    with rail.TaskGroup(group_id='process_deparments_task', prefix_group_id=False):

        load_valid_departments_data = rail.QueryCollectionOperator(
            task_id='load_valid_departments_data',
            name='feedfiledepartments',
            query="""SELECT DISTINCT departmentname, departmentcode FROM validrecords WHERE NULLIF(departmentname, '') IS NOT NULL"""
        )

        get_all_department_grps = rail.RepliconServiceOperator(
            task_id="get_all_department_grps",
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )

        replicon_departments_collection = rail.CreateCollectionOperator(
            task_id='replicon_departments_collection',
            source=lambda: rail.result('get_all_department_grps'),
            name='replicondepartments'
        )

        query_departments_to_create = rail.QueryCollectionOperator(
            task_id='query_departments_to_create',
            query="""SELECT DISTINCT departmentname, departmentcode FROM feedfiledepartments where LOWER(departmentname) NOT IN
                    (SELECT DISTINCT LOWER(displayText) FROM replicondepartments)"""
        )

        has_new_department = rail.IfOperator(
            task_id='has_new_department',
            test="{{ result('query_departments_to_create','length') > 0 }}",
            yes_task='create_departments_in_replicon',
            no_task='finish_process_department'
        )

        create_departments_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_departments_in_replicon',
            endpoint='/services/DepartmentGroupService1.svc/CreateDepartmentGroupHierarchyOrApplyModifications',
            items="{{ result('query_departments_to_create') }}",
            execution_timeout=timedelta(days=execution_timeout_days),
            data=request_payload.create_departments_in_replicon
        )

        finish_process_department = rail.EmptyOperator(
            task_id='finish_process_department'
        )

        load_valid_departments_data >> get_all_department_grps >> replicon_departments_collection >> query_departments_to_create
        query_departments_to_create >> has_new_department >> rail.Label(
            'Yes') >> create_departments_in_replicon >> finish_process_department
        has_new_department >> rail.Label('No') >> finish_process_department

    return load_valid_departments_data, finish_process_department
