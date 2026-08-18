from tsystems.user_import_v1.utils import response_filters
import rail
null = None


def get_all_prerequisites_data(config):
    with rail.TaskGroup(
        group_id="get_all_prerequisites_data",
        prefix_group_id=False
    ) as groups_data:

        start_prerequisites = rail.EmptyOperator(task_id="start_prerequisites")

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        get_required_time_off_types = rail.RepliconServiceOperator(
            task_id="get_required_time_off_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: response_filters.get_required_timeoffs_data(response, config.time_off_type_mapper_data)
        )

        get_required_permission_sets = rail.RepliconServiceOperator(
            task_id="get_required_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: response_filters.get_required_permissions_data(response, config.permissions_mapper_data)
        )

        get_required_timesheet_templates = rail.RepliconServiceOperator(
            task_id="get_required_timesheet_templates",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: response_filters.get_required_timesheet_templates_data(response, config.timesheet_template_mapper_data)
        )

        get_required_activities = rail.RepliconServiceOperator(
            task_id="get_required_activities",
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=lambda response: response_filters.get_required_activities(response, config.activities_mapper_data)
        )

        get_all_user_oefs = rail.RepliconServiceOperator(
            task_id="get_all_user_oefs",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=lambda: {
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                f'{oef["field_name"]}_oef_uri': rail.find_first_by_attr_and_get_attr(response, 'name', oef['oef_name'], 'uri')
                    for oef in config.oef_field_mapper_data
            }
        )

        # Create OEF value tasks only for dropdown fields
        oef_tasks = []

        for oef in config.oef_field_mapper_data:
            if oef['type'] == 'dropdown':
                task = rail.RepliconServiceOperator(
                    task_id=f'get_{oef["field_name"]}_oef_values',
                    endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
                    data=lambda current_oef=oef: {  # Bind current value
                        "objectExtensionTagDefinitionUri": rail.result("get_all_user_oefs")[f"{current_oef['field_name']}_oef_uri"],
                    },
                    data_handler=lambda response: list(
                        map(lambda item: {
                            "uri": item["uri"],
                            "name": item["name"]
                        }, response.get("tags", []))
                    )
                )
                oef_tasks.append(task)

        end_prerequisites = rail.EmptyOperator(task_id="end_prerequisites")

        # Chain the tasks properly
        prerequisite_tasks = [
            get_all_holiday_calendars, 
            get_all_timezones,
            get_required_time_off_types, 
            get_required_permission_sets, 
            get_required_timesheet_templates,
            get_required_activities
        ]
        
        # Set up the dependencies
        start_prerequisites >> prerequisite_tasks >> get_all_user_oefs >> oef_tasks >> end_prerequisites

        return groups_data
