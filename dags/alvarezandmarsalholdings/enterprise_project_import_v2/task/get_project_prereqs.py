import rail
from alvarezandmarsalholdings.enterprise_project_import_v2.utils import request_payload, response_filter

null = None

def get_project_prereqs_task_group(config):

    with rail.TaskGroup(group_id='get_project_prereqs', prefix_group_id=False) as get_project_prereqs:

        dummy_get_project_prereqs = rail.EmptyOperator(
            task_id="dummy_get_project_prereqs"
        )

        def page_handler(request, response):
            if len(response['rows']) > 0:
                request['page'] += 1
                return request
            return None

        get_all_costcenters = rail.RepliconServicePageOperator(
            task_id='get_all_costcenters',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_cost_center_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filter.filter_all_costcenters_data
        )

        get_all_users = rail.RepliconServicePageOperator(
            task_id='get_all_users',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_users_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filter.filter_all_users_data
        )

        get_permission_sets = rail.RepliconServiceOperator(
            task_id='get_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler= lambda response: response_filter.get_required_permission(response, config)
            
        )

        get_all_task_oef_details = rail.RepliconServiceOperator(
            task_id='get_all_task_oef_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:task"}
        )

        get_all_project_oef_details = rail.RepliconServiceOperator(
            task_id='get_all_project_oef_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:project"}
        )

        dummy_process_tags = rail.EmptyOperator(
            task_id="dummy_process_tags"
        )

        get_project_profile_oef_details = rail.RepliconServiceOperator(
            task_id='get_project_profile_oef_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_project_oef_details'),
                    'name', config.PROJECT_PROFILE, 'uri'
                )
            },
            data_handler=response_filter.filter_all_tags_details
        )

        dummy_custom_fields = rail.EmptyOperator(
            task_id="dummy_custom_fields"
        )

        dummy_get_project_prereqs >> [get_all_costcenters, get_all_users, get_permission_sets, 
            get_all_task_oef_details, get_all_project_oef_details] >> dummy_process_tags >> [
            get_project_profile_oef_details] >> dummy_custom_fields

    return dummy_get_project_prereqs, get_project_prereqs
