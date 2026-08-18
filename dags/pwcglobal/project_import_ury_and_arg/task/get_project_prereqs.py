import rail

null = None

def get_project_prereqs_task_group():

    with rail.TaskGroup(group_id='get_project_prereqs', prefix_group_id=False) as get_project_prereqs:

        dummy_get_project_prereqs = rail.EmptyOperator(
            task_id="dummy_get_project_prereqs"
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_division_uri_code = rail.RepliconServiceOperator(
            task_id='get_all_division_uri_code',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000000",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationService1.svc/GetAllLocations'
        )

        get_all_client_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_client_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:client"}
        )

        get_all_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:project"}
        )

        get_all_project_object_extension_field_details = rail.RepliconServiceOperator(
            task_id='get_all_project_object_extension_field_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:project"}
        )

        get_object_extension_tag_definition_details = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data={"objectExtensionTagDefinitionUri": "{{ result('get_all_project_object_extension_field_details') | \
                find_first_by_attr_and_get_attr('name', 'Type', 'uri') }}"}
        )

        get_client_udfs = rail.RepliconServiceOperator(
            task_id="get_client_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:client"
            },
            data_handler=lambda udfs: {
                'clientpriduri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Client PRID', 'uri')
            },
        )

        dummy_custom_fields = rail.EmptyOperator(
            task_id="dummy_custom_fields"
        )

        get_lan_ac_los_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_lan_ac_los_custom_field_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={"customFieldUri": "{{ result('get_all_project_custom_fields') | \
                find_first_by_attr_and_get_attr('displayText', 'LAN AC LOS', 'uri') }}"}
        )

        get_lan_ac_project_type_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_lan_ac_project_type_custom_field_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={"customFieldUri": "{{ result('get_all_project_custom_fields') | \
                find_first_by_attr_and_get_attr('displayText', 'LAN AC Project Type', 'uri') }}"}
        )

        dummy_get_project_prereqs >> [get_all_permission_set, get_all_division_uri_code, get_all_locations, get_all_client_custom_fields,
            get_all_project_custom_fields,get_all_project_object_extension_field_details,get_object_extension_tag_definition_details,
            get_client_udfs] >> dummy_custom_fields >> \
            get_lan_ac_los_custom_field_dropdown_options >> get_lan_ac_project_type_custom_field_dropdown_options


    return dummy_get_project_prereqs, get_project_prereqs
