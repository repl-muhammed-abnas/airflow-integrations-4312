import rail


def update_oef_status_task(state, oef_definition_uri):
    with rail.TaskGroup(group_id=f'update_{state}_oef_status', prefix_group_id=False):

        is_input_status_enable = rail.IfOperator(
            task_id=f'is_input_status_enable_{state}',
            test='{{ dag_run.conf.object_data.status == "Enabled" }}',
            yes_task=f'enable_{state}_oef',
            no_task=f'disable_{state}_oef'
        )

        enable_oef = rail.RepliconServiceOperator(
            task_id=f'enable_{state}_oef',
            endpoint='/services/ObjectExtensionTagService1.svc/Enable',
            data={
                "objectExtensionTagUri": oef_definition_uri
            }
        )

        disable_oef = rail.RepliconServiceOperator(
            task_id=f'disable_{state}_oef',
            endpoint='/services/ObjectExtensionTagService1.svc/Disable',
            data={
                "objectExtensionTagUri": oef_definition_uri
            }
        )

        is_input_status_enable >> rail.Label("Yes") >> enable_oef
        is_input_status_enable >> rail.Label("No") >> disable_oef

        return is_input_status_enable, [enable_oef, disable_oef]
