"""
Get Updated User Prerequisites Task Group - Unisys Workday User Import

Refreshes specific prerequisite data from Replicon during user processing.
This lightweight task group retrieves only the frequently-changing reference data
(locations, user types, schedules) needed for processing user batches.

Key features:
    - Fetches updated location hierarchy data
    - Retrieves current employee type groups
    - Gets latest office schedules
    - Uses pagination for large datasets
    - Returns filtered and formatted data

Functions:
    get_updated_user_prereqs_task_group(config): Creates the updated prerequisites task group
"""
import rail
from unisys.workday_user_import_v1.utils import request_payload, response_filters
from unisys.workday_user_import_v1.utils.custom_method import get_unique_project_codes

null = None


def get_updated_user_prereqs_task_group(config):
    """
    Create task group for refreshing key user processing prerequisites.

    This task group retrieves updated reference data during user processing batches.
    It focuses on data that may change frequently (locations, user types, schedules)
    while skipping static data already loaded.

    Args:
        config: Configuration object (used for consistency, may not have specific attributes)

    Returns:
        tuple: (entry_task, task_group)
            - entry_task: EmptyOperator to use as task group entry point
            - task_group: TaskGroup object containing prerequisite refresh tasks

    Replicon API Endpoints Used:
        - /services/LocationListService1.svc/GetData: Updated location hierarchy
        - /services/EmployeeTypeGroupListService1.svc/GetData: Updated user type groups
        - /services/OfficeScheduleService1.svc/GetAllOfficeSchedules: Current schedules

    Task Group Results:
        - get_updated_location_details: Current location hierarchy data
        - get_updated_employeetype_groups_data: Current user type group data
        - get_updated_all_office_schedule: Current office schedule list

    Note:
        This is used within processing loops to ensure the latest data is available
        for user assignments without re-fetching all prerequisite data.
    """

    with rail.TaskGroup(group_id='get_updated_user_prereqs', prefix_group_id=False) as get_updated_user_prereqs:

        dummy_get_updated_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_updated_user_prereqs"
        )

        def page_handler(request, response):
            if len(response['rows']) > 0:
                request['page'] += 1
                return request
            return None

        get_updated_location_details = rail.RepliconServicePageOperator(
            task_id='get_updated_location_details',
            endpoint="/services/LocationListService1.svc/GetData",
            data=request_payload.get_location_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_location_data
        )

        get_updated_employeetype_groups_data = rail.RepliconServicePageOperator(
            task_id='get_updated_employeetype_groups_data',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_employeetype_group_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_employeetype_groups_data
        )

        get_updated_all_office_schedule = rail.RepliconServiceOperator(
            task_id = 'get_updated_all_office_schedule',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjects2",
            data=lambda: {
                "projects": get_unique_project_codes()
            },
            data_handler=response_filters.filter_project_uris
        )
        
        dummy_process_projects = rail.EmptyOperator(
            task_id="dummy_process_projects"
        )
        
        get_all_project_tasks = rail.RepliconServiceOperator(
            task_id="get_all_project_tasks",
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data=request_payload.get_project_uris,
            # page_handler=project_page_handler,
            data_handler=response_filters.filter_project_tasks_hash
        )
        
        finish = rail.EmptyOperator(
            task_id="finish"
        )

        dummy_get_updated_user_prereqs >> [get_updated_location_details, get_updated_employeetype_groups_data,
        get_updated_all_office_schedule, get_project_details] >> dummy_process_projects >> get_all_project_tasks >> finish

    return dummy_get_updated_user_prereqs, get_updated_user_prereqs
