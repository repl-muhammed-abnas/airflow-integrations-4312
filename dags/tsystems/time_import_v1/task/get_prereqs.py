import rail
from tsystems.time_import_v1.utils import response_filters

null = None

def get_prereqs_task_group(config):
    """
    Create task group for fetching prerequisite data needed for time entry processing.
    
    This task group retrieves essential configuration data including billing rates,
    Object Extension Field definitions, and worktype tag configurations that are
    required for proper time entry creation and validation.
    
    Args:
        config: Configuration module containing worktype definitions and settings
        
    Returns:
        Tuple[rail.EmptyOperator, rail.TaskGroup]: Entry and exit points for the task group
    """

    with rail.TaskGroup(group_id='get_prereqs', prefix_group_id=False) as get_prereqs:

        # Task: Entry point for prerequisite data gathering
        # Synchronization point for starting prerequisite data collection
        dummy_get_prereqs = rail.EmptyOperator(
            task_id="dummy_get_prereqs"
        )

        # Task: Retrieve all enabled billing rates for the company
        # Fetches billing rate definitions used for time entry metadata
        get_enabled_company_billing_rates = rail.RepliconServiceOperator(
            task_id='get_enabled_company_billing_rates',
            endpoint="/services/BillingRateService1.svc/GetEnabledCompanyBillingRates",
        )

        # Task: Retrieve all Object Extension Field definitions for time entries
        # Gets OEF schemas and configurations available for time entry customization
        get_all_timeentry_oef_details = rail.RepliconServiceOperator(
            task_id='get_all_timeentry_oef_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:time-entry"}
        )

        # Task: Get standard worktype OEF tag definitions
        # Retrieves enabled tags for the standard worktype OEF configuration
        get_worktype_oef_details = rail.RepliconServiceOperator(
            task_id='get_worktype_oef_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timeentry_oef_details'),
                    'name', config.worktype, 'uri'
                )
            },
            data_handler=response_filters.filter_all_tags_details
        )

        # Task: Get tarif worktype OEF tag definitions
        # Retrieves enabled tags for the tarif-specific worktype OEF configuration
        get_worktype_tarif_oef_details = rail.RepliconServiceOperator(
            task_id='get_worktype_tarif_oef_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timeentry_oef_details'),
                    'name', config.worktype_tarif, 'uri'
                )
            },
            data_handler=response_filters.filter_all_tags_details
        )

        # Task: Get tariffrei worktype OEF tag definitions
        # Retrieves enabled tags for the tariff-free worktype OEF configuration
        get_worktype_tariffrei_oef_details = rail.RepliconServiceOperator(
            task_id='get_worktype_tariffrei_oef_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timeentry_oef_details'),
                    'name', config.worktype_tariffrei, 'uri'
                )
            },
            data_handler=response_filters.filter_all_tags_details
        )

        # Task: Exit point for prerequisite data gathering
        # Synchronization point indicating all prerequisite data has been collected
        dummy_custom_fields = rail.EmptyOperator(
            task_id="dummy_custom_fields"
        )

        dummy_get_prereqs >> get_enabled_company_billing_rates >> get_all_timeentry_oef_details >> [get_worktype_oef_details,
            get_worktype_tarif_oef_details, get_worktype_tariffrei_oef_details] >> dummy_custom_fields

    return dummy_get_prereqs, get_prereqs
