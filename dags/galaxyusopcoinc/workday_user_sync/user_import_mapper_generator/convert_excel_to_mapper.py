# # Uncomment this and run it to generate the mapper
# """Convert Excel sheets to JSON formatted mapper files for Workday user sync."""

# import os
# import logging
# from json import loads
# from typing import List

# from openpyxl import load_workbook
# from pandas import read_excel

# from mapper_generator_utils import generate_proper_mapper_and_upload_to_correct_path

# # Configuration
# # Integration mapper version number
# VERSION_NUMBER = 7

# # mapper file version
# MAPPER_NUMBER = 16

# VERSION = f"v{VERSION_NUMBER}"
# SELECTED_SHEETS = ['kenya', 'kazakhstan']
# CONVERT_SELECTED_ONLY = True

# # File and folder paths
# # Mapper file name should be like mapper1.xlsx, mapper2.xlsx, mapper3.xlsx, etc.
# EXCEL_FILE = f"mapper{MAPPER_NUMBER}.xlsx"

# #Integration version
# INTEGRATION_VERSION = 2

# OUTPUT_FOLDER = f"mappers{MAPPER_NUMBER}"
# LOGS_FOLDER = f"{OUTPUT_FOLDER}/logs{MAPPER_NUMBER}"

# # Setup logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)


# def create_directories() -> None:
#     """Create necessary directories if they don't exist."""
#     for folder in [OUTPUT_FOLDER, LOGS_FOLDER]:
#         if not os.path.isdir(folder):
#             os.makedirs(folder, mode=0o755)
#             logger.info("Created directory: %s", folder)


# def load_excel_sheets(excel_file: str) -> List[str]:
#     """Load Excel file and return list of sheet names.

#     Args:
#         excel_file: Path to the Excel file

#     Returns:
#         List of sheet names (excluding the first sheet)
#     """
#     try:
#         workbook = load_workbook(excel_file, read_only=True)
#         sheets = workbook.sheetnames[1:]
#         logger.info("Loaded %d sheets from %s", len(sheets), excel_file)
#         return sheets
#     except Exception as e:
#         logger.error("Failed to load Excel file: %s", e)
#         raise


# def process_sheets(sheets: List[str]) -> tuple:
#     """Process Excel sheets and convert them to mapper files.

#     Args:
#         sheets: List of sheet names to process

#     Returns:
#         Tuple of (blank_key_names, blank_feed_file)
#     """
#     log_file = f'{LOGS_FOLDER}/logs{MAPPER_NUMBER}.txt'
#     error_file = f"{LOGS_FOLDER}/error{MAPPER_NUMBER}.txt"

#     blank_key_names = []
#     blank_feed_file = []

#     with open(log_file, mode="w+", encoding="utf-8") as logs:
#         logs.write("Mapper Generation Started\n")
#         logs.write(f"Total Sheets to convert: {len(sheets)}\n")
#         logs.write(f"Sheets: {', '.join(sheets)}\n")
#         logs.write("-" * 70 + "\n")

#         for index, sheet in enumerate(sheets, 1):
#             sheet_lower = sheet.lower()

#             if CONVERT_SELECTED_ONLY and sheet_lower not in SELECTED_SHEETS:
#                 logger.info("Skipping sheet %d: %s", index, sheet)
#                 continue

#             logger.info("Processing sheet %d: %s", index, sheet)
#             logs.write(
#                 f"\nSheet index: {index}\nConverting sheet '{sheet}' to Mapper\n")

#             try:
#                 # Read Excel sheet data
#                 excel_data_df = read_excel(
#                     EXCEL_FILE,
#                     sheet_name=sheet,
#                     keep_default_na=False
#                 )

#                 # Convert to JSON format
#                 json_str = excel_data_df.to_json(orient='records', indent=4)

#                 # Save initial mapper file
#                 mapper_file = f"{OUTPUT_FOLDER}/{sheet}.py"
#                 with open(mapper_file, mode="w+", encoding="utf-8") as mapper:
#                     mapper_var_name = f"{sheet.upper().replace(' ', '_')}_MAPPER"
#                     mapper.write(f"{mapper_var_name} = ")
#                     mapper.write(json_str)

#                 logs.write(f"Generated mapper for {sheet} at {mapper_file}\n")
#                 logs.write(
#                     f"Processing JSON mapper corrections for {sheet}...\n")

#                 # Apply custom mapper transformations
#                 sheet_blank_keys, sheet_blank_feed = generate_proper_mapper_and_upload_to_correct_path(
#                     loads(json_str),
#                     VERSION,
#                     INTEGRATION_VERSION
#                 )

#                 blank_key_names.extend(sheet_blank_keys)
#                 blank_feed_file.extend(sheet_blank_feed)

#                 logs.write(f"Completed JSON mapper corrections for {sheet}\n")
#                 logger.info("Successfully converted sheet: %s", sheet)

#             except Exception as e:
#                 error_msg = f"Error processing sheet {sheet}: {str(e)}"
#                 logger.error(error_msg)
#                 logs.write("Error occurred - see error log\n")

#                 with open(error_file, mode="a", encoding="utf-8") as error:
#                     error.write(f"{sheet} -> {str(e)}\n")
#                     error.write("-" * 40 + "\n")

#             logs.write("-" * 70 + "\n")

#     # Return unique values
#     return list(set(blank_key_names)), list(set(blank_feed_file))


# def generate_mapper_reference_file(sheets: List[str]) -> None:
#     """Generate a reference file that imports all mapper modules.

#     Args:
#         sheets: List of sheet names to include in reference file
#     """
#     import_path = "galaxyusopcoinc.workday_user_sync.user_import.mapper.user_import_mapper_per_country_6"
#     reference_file = f"general_mapper_reference{MAPPER_NUMBER}.py"

#     logger.info("Creating mapper reference file: %s", reference_file)

#     def format_name(name: str) -> str:
#         """Convert sheet name to module name format."""
#         return name.lower().replace(' ', '_')

#     with open(reference_file, mode="w+", encoding="utf-8") as ref_file:
#         # Write import statements
#         for sheet in sheets:
#             module_name = format_name(sheet)
#             ref_file.write(f"from {import_path} import {module_name}\n")

#         ref_file.write('\n\n')

#         # Write mapper dictionary
#         ref_file.write('MAPPER_TO_USE_FOR_COUNTRY_TRIAL = {\n')
#         for sheet in sheets:
#             module_name = format_name(sheet)
#             mapper_var = f"{module_name.upper()}_USER_MAPPER"
#             ref_file.write(
#                 f"    '{module_name}': {module_name}.{mapper_var},\n")
#         ref_file.write('}\n')

#     logger.info("Reference file created successfully")


# def main():
#     """Main execution function."""
#     try:
#         # Setup
#         create_directories()

#         # Load sheets
#         sheets = load_excel_sheets(EXCEL_FILE)

#         # Process sheets
#         blank_keys, blank_feed = process_sheets(sheets)

#         # Log results
#         logger.info("Blank key names: %s", blank_keys)
#         logger.info("Blank feed file keys: %s", blank_feed)
#         logger.info("Logs saved in: %s", os.path.abspath(LOGS_FOLDER))

#         # Generate reference file
#         generate_mapper_reference_file(sheets)

#         logger.info("Mapper generation completed successfully")

#     except Exception as e:
#         logger.error("Fatal error: %s", e)
#         raise


# if __name__ == "__main__":
#     main()
