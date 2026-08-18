# # Uncomment this and run it to generate the mapper

# """Utility functions for generating and processing mapper data."""

# import os
# import json
# import logging
# from typing import Dict, List, Tuple, Any

# # Setup logging
# logger = logging.getLogger(__name__)

# # Constants
# PROJECT_FOLDER = "C:/workspace/airflow-integrations/dags/galaxyusopcoinc/workday_user_sync/user_import/mapper/"

# # Field mapping dictionary
# FIELD_MAPPINGS = {
#     'Country': 'country',
#     'Location': 'location',
#     'WorkerType': 'worker_type',
#     'CompensationGrade': 'compensation_grade',
#     'ContractType': 'contract_type',
#     'EmployeeType': 'employee_type',
#     'JobCategory': 'job_category',
#     'ManagementLevel': 'management_level',
#     'AdditionalJobClassification': 'additional_job_classification',
#     'CompanyCode': 'company_code',
#     'Schedule Type': 'schedule_type',
#     'Work Week': 'work_week',
#     'Timesheet Template': 'timesheet_template',
#     'Time Entry Approval Path': 'time_entry_approval_path',
#     'Timesheet Approval': 'timesheet_approval',
#     'Timesheet Period': 'timesheet_period',
#     'Time Off Template': 'time_off_template',
#     'Time Off Approval': 'time_off_approval',
#     'Time Off Types': 'time_off_types',
#     'Payrule': 'payrule',
#     'Time Zone': 'time_zone',
#     'Holiday Calendar': 'holiday_calendar',
#     'Authentication Type': 'authentication_type'
# }


# def normalize_value(
#     data: Any,
#     key: str,
#     blank_key_names: List[str],
#     blank_feed_file: List[str]
# ) -> Any:
#     """Normalize and clean data values based on key and content.

#     Args:
#         data: The value to normalize
#         key: The field key name
#         blank_key_names: List to track blank keys
#         blank_feed_file: List to track blank feed file entries

#     Returns:
#         Normalized value
#     """
#     # Special handling for location field
#     if key == "location" and data in ["N/A", "N\\/A"]:
#         blank_key_names.append(key)
#         return "ALL"

#     # Handle N/A values
#     if data in ["N/A", "N\\/A"]:
#         blank_key_names.append(key)
#         return "NA"

#     # Handle blank feed file notation
#     if data == "N/A (Blank in feed file)":
#         blank_key_names.append(key)
#         blank_feed_file.append(key)
#         return "NA (Blank in feed file)"

#     # Fix timezone character encoding
#     if key == "time_zone" and isinstance(data, str):
#         return data.replace("−", "-")

#     # Filter empty values from lists
#     if isinstance(data, list):
#         return [item for item in data if item]

#     # Ensure proper string escaping
#     if isinstance(data, str):
#         try:
#             return json.loads(f'"{data}"')
#         except json.JSONDecodeError:
#             return data

#     return data


# def process_mapper_data(
#     mapper_data: List[Dict],
#     blank_key_names: List[str],
#     blank_feed_file: List[str]
# ) -> List[Dict]:
#     """Process and transform mapper data from Excel format.

#     Args:
#         mapper_data: Raw mapper data from Excel
#         blank_key_names: List to track blank keys
#         blank_feed_file: List to track blank feed file entries

#     Returns:
#         Processed mapper data
#     """
#     processed_data = []

#     for item in mapper_data:
#         processed_item = {}

#         for original_key, value in item.items():
#             # Skip empty country entries
#             if not value and original_key.lower() == "country":
#                 break

#             # Get mapped field name
#             if original_key not in FIELD_MAPPINGS:
#                 logger.warning("Unknown field: %s", original_key)
#                 continue

#             mapped_key = FIELD_MAPPINGS[original_key]

#             # Handle multi-line values
#             if isinstance(value, str) and "\n" in value:
#                 value = [
#                     normalize_value(v.strip(), mapped_key,
#                                     blank_key_names, blank_feed_file)
#                     for v in value.split("\n") if v.strip()
#                 ]
#             else:
#                 value = normalize_value(
#                     value, mapped_key, blank_key_names, blank_feed_file)

#             processed_item[mapped_key] = value

#         if processed_item:  # Only add non-empty items
#             processed_data.append(processed_item)

#     return processed_data


# def generate_proper_mapper_and_upload_to_correct_path(
#     mapper_data: List[Dict],
#     version: str,
#     integration_version: int
# ) -> Tuple[List[str], List[str]]:
#     """Generate properly formatted mapper files and save to project directory.

#     Args:
#         mapper_data: Raw mapper data from Excel
#         version: Version string (e.g., "v7")
#         integration_version: Integration version number

#     Returns:
#         Tuple of (blank_key_names, blank_feed_file) lists
#     """
#     # Build output path
#     base_path = f"C:/workspace/airflow-integrations/dags/galaxyusopcoinc/workday_user_sync/"
#     mapper_path = f"{base_path}user_import_v{integration_version}/mapper/"
#     output_dir = f"{mapper_path}user_import_mapper_per_country_{version}"

#     # Create output directory if needed
#     if not os.path.isdir(output_dir):
#         os.makedirs(output_dir, mode=0o755)
#         logger.info("Created directory: %s", output_dir)

#     # Track blank values
#     blank_key_names = []
#     blank_feed_file = []

#     # Process mapper data
#     processed_data = process_mapper_data(
#         mapper_data, blank_key_names, blank_feed_file)

#     # Validate data
#     if not processed_data or 'country' not in processed_data[0]:
#         raise ValueError("Invalid mapper data: missing country information")

#     # Get country name and format for filename
#     country = processed_data[0]['country']
#     country_formatted = country.lower().replace(" ", "_")

#     # Check for empty rows
#     for idx, row in enumerate(processed_data):
#         if not row:
#             logger.warning("%s has blank row at index %d", country, idx)

#     # Write mapper file
#     output_file = f"{output_dir}/{country_formatted}.py"
#     variable_name = f"{country_formatted.upper()}_USER_MAPPER"

#     with open(output_file, mode="w+", encoding="utf-8") as f:
#         f.write(f"{variable_name} = ")
#         json.dump(processed_data, f, indent=4)
#         f.write("\n")

#     logger.info("Generated mapper for %s at %s", country, output_file)

#     # Log blank fields if any
#     unique_blank_keys = list(set(blank_key_names))
#     unique_blank_feed = list(set(blank_feed_file))

#     if unique_blank_keys:
#         logger.info("Blank key names for %s: %s", country, unique_blank_keys)
#     if unique_blank_feed:
#         logger.info("Blank feed file keys for %s: %s", country, unique_blank_feed)

#     return unique_blank_keys, unique_blank_feed
