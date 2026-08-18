import rail
from dxctechnology.compass_attribute_1_2_new import response_filter


def get_system_level_attribute():
    with rail.TaskGroup(group_id='get_system_level_attributes', prefix_group_id=False) as get_system_level_attributes:

        is_check_attribute_1_file = rail.IfOperator(
            task_id='is_check_attribute_1_file',
            test="{{ 'Attributes_1' in result('new_file_sensor') }}",
            yes_task='query_eligible_wbs_attribute_1_record_collection',
            no_task='query_eligible_wbs_attribute_2_record_collection',
        )

        query_eligible_wbs_attribute_1_record_collection = rail.QueryCollectionOperator(
            task_id="query_eligible_wbs_attribute_1_record_collection",
            name="eligibleattributewbsrecords",
            query="""SELECT * FROM xmlwbsrecords WHERE (AttributeNumber ='1' AND Attribute!='' AND AttributeNumber !='' AND EndDate !='')"""
        )

        query_eligible_wbs_attribute_2_record_collection = rail.QueryCollectionOperator(
            task_id="query_eligible_wbs_attribute_2_record_collection",
            name="eligibleattributewbsrecords",
            query="""SELECT * FROM xmlwbsrecords WHERE (AttributeNumber ='2' AND Attribute!='' AND AttributeNumber !='' AND EndDate !='')"""
        )

        get_attribute_1_uri = rail.RepliconServiceOperator(
            task_id="get_attribute_1_uri",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings",
            data=lambda: {
                    "bindingContextUri": "urn:replicon:object-type:time-entry"
            },
            response_filter=response_filter.map_attribute_1_uri
        )

        get_attribute_2_uri = rail.RepliconServiceOperator(
            task_id="get_attribute_2_uri",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings",
            data=lambda: {
                    "bindingContextUri": "urn:replicon:object-type:time-entry"
            },
            response_filter=response_filter.map_attribute_2_uri
        )

        get_unique_name_attribute_1 = rail.QueryCollectionOperator(
            task_id="get_unique_name_attribute_1",
            name="uniqueattribute1names",
            query="""Select DISTINCT NAME,Attribute from (SELECT (NULLIF(e.Attribute,'') || " - " || e.Description) AS NAME,e.Attribute AS Attribute FROM \
            eligibleattributewbsrecords e WHERE e.Description IS NOT NULL UNION SELECT ew.Attribute || " - ",ew.Attribute FROM eligibleattributewbsrecords \
            ew WHERE ew.Description IS NULL)"""
        )

        get_unique_name_attribute_2 = rail.QueryCollectionOperator(
            task_id="get_unique_name_attribute_2",
            name="uniqueattribute2names",
            query="""Select DISTINCT NAME,Attribute from (SELECT (NULLIF(e.Attribute,'') || " - " || e.Description) AS NAME,e.Attribute AS Attribute FROM \
            eligibleattributewbsrecords e WHERE e.Description IS NOT NULL UNION SELECT ew.Attribute || " - ",ew.Attribute FROM eligibleattributewbsrecords ew \
            WHERE ew.Description IS NULL)"""
        )

        is_check_attribute_1_file >> rail.Label(
            "Yes") >> query_eligible_wbs_attribute_1_record_collection
        is_check_attribute_1_file >> rail.Label(
            "No") >> query_eligible_wbs_attribute_2_record_collection
        query_eligible_wbs_attribute_1_record_collection >> get_attribute_1_uri >> get_unique_name_attribute_1
        query_eligible_wbs_attribute_2_record_collection >> get_attribute_2_uri >> get_unique_name_attribute_2

        return get_system_level_attributes
