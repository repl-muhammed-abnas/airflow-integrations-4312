import rail


def create_management_level_task(user_uri=None):
    with rail.TaskGroup(group_id="management_level_task", prefix_group_id=False):
        get_managementlevel_enabled_dropdown_option = rail.RepliconServiceOperator(
            task_id="get_managementlevel_enabled_dropdown_option",
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{dag_run.conf.management_level_customfield_uri}}"
            },
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", dag_run.conf['management_level'], 'uri')
        )
        is_managementlevel_dropdown_option_available = rail.IfOperator(
            task_id="is_managementlevel_dropdown_option_available",
            test="{{result('get_managementlevel_enabled_dropdown_option') | is_truthy}}",
            yes_task="update_managementlevel_dropdown",
            no_task="managementlevel_complete"
        )
        update_managementlevel_dropdown = rail.RepliconServiceOperator(
            task_id="update_managementlevel_dropdown",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": user_uri if user_uri else "{{result('add_new_user').uri}}",
                "customFieldUri": "{{dag_run.conf.management_level_customfield_uri}}",
                "customFieldDropDownOptionUri": "{{result('get_managementlevel_enabled_dropdown_option')}}"
            }
        )
        managementlevel_complete = rail.EmptyOperator(
            task_id="managementlevel_complete"
        )

        get_managementlevel_enabled_dropdown_option >> is_managementlevel_dropdown_option_available >> rail.Label(
            "Yes") >> update_managementlevel_dropdown >> managementlevel_complete
        is_managementlevel_dropdown_option_available >> rail.Label(
            "No") >> managementlevel_complete

        return get_managementlevel_enabled_dropdown_option, managementlevel_complete
