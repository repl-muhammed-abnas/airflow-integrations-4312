import rail
from dxctechnology.gsap_task_import_project_fields_v1.utils import response_filter


def get_system_level_attribute():
    with rail.TaskGroup(group_id='get_system_level_attributes', prefix_group_id=False) as get_system_level_attributes:

        get_gsap_task_uri = rail.RepliconServiceOperator(
            task_id="get_gsap_task_uri",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings",
            data=lambda: {
                    "bindingContextUri": "urn:replicon:object-type:time-entry"
            },
            response_filter=response_filter.map_attribute_2_uri
        )

        get_unique_task_name = rail.QueryCollectionOperator(
            task_id="get_unique_task_name",
            name="uniqueattribute2names",
            query="""Select DISTINCT task_name, task_code from valid_input_records"""
        )

        get_gsap_task_uri >> get_unique_task_name

        return get_system_level_attributes
