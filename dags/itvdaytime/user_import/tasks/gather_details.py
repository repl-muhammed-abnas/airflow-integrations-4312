import rail
from itvdaytime.user_import.utils import data_handler


def get_gather_details():
    with rail.TaskGroup(group_id="gather_details", prefix_group_id=False):

        gather_details_start = rail.EmptyOperator(
            task_id="gather_details_start"
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id="get_all_service_centers",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_required_permission_sets = rail.RepliconServiceOperator(
            task_id="get_required_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=data_handler.get_required_permission_sets
        )

        get_user_custom_field_group = rail.RepliconServiceOperator(
            task_id="get_user_custom_field_group",
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'User', 'uri')
        )

        get_user_custom_fields = rail.RepliconServiceOperator(
            task_id="get_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_user_custom_field_group')}}"
            },
            data_handler=data_handler.get_required_custom_fields
        )

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id="get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=data_handler.get_timeoff_types
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id="get_timeoff_details",
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails",
            data=lambda: {
                "timeOffTypeUris": [item['uri'] for item in rail.result('get_all_timeoffs')]
            },
            data_handler=data_handler.get_timeoff_types_details
        )

        gather_details_start >> [get_required_permission_sets, get_user_custom_field_group,
                                 get_all_service_centers, get_all_timeoffs] >> get_timeoff_details >> get_user_custom_fields

    return gather_details_start, get_user_custom_fields
