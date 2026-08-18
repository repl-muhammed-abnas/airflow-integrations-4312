# T-Systems Time Export to SAP - Complete Project Implementation Guide

This document contains all the prompts and requirements needed to recreate the T-Systems Time Export to SAP integration project from scratch.

## Original Requirements (Base Prompt)

Create a Time Extract project in the below path 
airflow-integrations\dags\tsystems\time_export_to_sap

### Constraints
•	Only approved time entries will be considered for the extract (During the time of execution)
•	The time extract will be in .CSV format
•	Any modification done on the already extracted data by reopening the approved timesheet will not be part of the extract if it is not submitted post-modification.
•	Replicon will only send delta values upon modification of already extracted timesheets.
•	The input files to be provided are flat files (sequential; un-indexed).
•	The data is coded in ASCII.
•	The data format is CSV and the fields are separated by semicolon ";".
•	Empty fields must also be reflected with separator.
•	The last field must be followed by a semi-column.
•	The numbers are in extended form (i.e. not in compacted form).

### Assumptions 
●	All the data on the timesheet will be against Project/task.
●	Only approved timesheets will be extracted via automated integration from Time workbench.
●	Each extract file when sent to T-Systems will be marked as complete in Replicon and will have the same name as the sent file name.
●	Time workbench will have predefined filters which will be set with required values for the extract. 
●	At a scheduled interval (Twice a month as per the time export calendar) the integration will extract the data from Time workbench and send it to SFTP as a .csv file. Each record sent will have a unique Time entry ID to identify any modification on already extracted and sent data.
●	Filters set on the time work bench for T-Systems are given below
●	Each record sent in the extracted file will have Time entry id which is a unique identifier for each time entry, for all the modification action and deletion performed on already extracted timesheet
●	Replicon will not extract the data against a project / Task if there are no hours entered.
●	Replicon will create a transaction log for each run which is processed, and the log will be made available. 
●	The CSV file name has YYMMDDHHMMSS which is the timestamp
●	The file name of the extract will be REPLICON_ICM_TSI_PROJTIME_YYYYMMDD_HHMMSS.csv
●	A log file will be generated and sent to T-Systems team, the file name format will be (sample: Time extract log_YYMMDDHHMMSS_Territory code.txt)

### Field Mapping -
Column	Value/Replicon Fields	Sample Value
time_entry_id	Time Entry ID	1235 (Unique ID for identifying deltas)
employee_ID	Employee_ID	200255075
project_ID	Project Code	F.37074702.04
entry_date	Entry Date (formatted as YYYY.MM period)	2025.05
billing_entry	Billing Entry (Billable/Non-Billable)	Billable
billing_rate_name	Billing Rate Name	A37074702|Consulting services
hours	Hours	60.25
task_name 	Task Name	Task Name associated to the Project in Replicon
task_code	Task Code 	T1234 (Code associated to the task in Replicon)
task_activity_name 	Activity Name 	Working Time 
task_description 	Task Description	Description of the task
sap_activity_type 	Activity Type ( Group)	CE2  (From user profile)
transaction_id	Transaction ID	Unique identifier per transaction

### Extract file example - 
time_entry_id;employee_ID;project_ID;Entry Date;billing_entry;billing_rate_name;hours;task_name;task_code;task_activity_name;task_description;sap_activity_type;transcation_ID
74b0f635-aebd-4166-a8a1-7ce6c294845344;200255075;F.37074702.04;2025.05;Billable;A;60.50;Task Architecture Solution;T1234;Working Time;all Tasks of Architecture;CE2;uuid-1234

### Rules to follow :- 

●	Instead of airflow library use rail library from the below path
replicon-airflow-library
●	Process items in parallel with TriggerDagRunForEachItemOperator
●	Service calls using RepliconServiceOperator
●	Conditional paths using IfOperator
●	Error handling with WriteLogOperator and FailOperator

●	Refer to 
    ●	airflow-integrations\dags\pwcglobal\time_export_v3 for integration logics
    ●	airflow-integrations\dags\crl\time_export for folder structure 

●	Also look out for other time export projects in the code base and work accordingly.

●	Use the service calls from time export projects wherever necessary 

●	try to avoid using try-except logics in function. 

---

## User Corrections and Additional Requirements

### Correction 1: T-Systems Schedule Clarification
**User Input**: "Tsystem runs twice"

**Corrected Business Rules**:
- Current month export: Last day - 7 working days  
- Previous month corrections: First day + 6 working days
- Working days: Monday to Friday only
- Timezone: Europe/Berlin

### Correction 2: PWC Pattern Implementation
**User Input**: "implement the project something similar to the one in airflow-integrations\dags\pwcglobal\time_export_v3\task\time_data_export.py"

**Requirements**:
- Follow PWC pattern structure exactly
- Use Replicon Time Data Export Service
- Implement batch execution and download workflow
- Include proper task group organization

### Correction 3: File Organization  
**User Input**: "put the python callable, request payload and response filter files under the utils folder"

**Action Required**:
- Move python callable, request payload, and response filter files to utils/ folder
- Update all import statements accordingly

### Correction 4: Data Filtering Enhancement
**User Input**: "validate_and_filter_data function uses translate_rows, even though i pass billing_entry to it but still am not able to get it"

**Solution Applied**:
- Fixed translate_rows function to preserve original billing_entry values
- Added hours > 0 filter in valid_extracted_data query
- Corrected field mappings from period to entry_date

### Correction 5: Code Cleanup
**User Input**: "clean up project , remove unwanted functions, tasks, files, variables, imports"

**Actions Completed**:
- Removed unused functions (9 functions from response_filter.py, 1 from request_payload.py)
- Removed unused configuration variables (export_types, validation_rules, email templates, etc.)
- Removed unused email notification file (send_email_notification.py)
- Cleaned up unused imports and variables (null = None declarations)
- Removed commented-out code and extra whitespace

---

## Final Implementation Specifications

### Current Project Structure (After Cleanup)
```
airflow-integrations/dags/tsystems/time_export_to_sap/
├── config.py                          # Streamlined T-Systems configuration
├── main_dag.py                        # Main DAG with export scheduling
├── timeexport_child_dag.py            # Child DAG implementation
├── instances/
│   └── trial.py                       # Instance-specific configuration
├── mappers/
│   └── export_schedule_mapper.py      # Export schedule mapping
├── task/
│   ├── __init__.py
│   └── time_data_export.py           # Main export task group (PWC pattern)
├── utils/
│   ├── python_callable.py           # Date calculation utilities  
│   ├── request_payload.py            # API request payload generators
│   └── response_filter.py            # Data filtering and transformation
└── templates/
    ├── email_export_success.html     # Success email template
    ├── email_export_failure.html     # Failure email template
    ├── email_invalid_records_in_export.html # Invalid records template
    ├── email_no_data.html            # No data email template
    └── email_valid_import_complete.html # Import complete template
```

### Core Configuration (config.py)
```python
# Essential configurations only
region = 'eu-central-1'
environment = "pre-production"
pacific_timezone = 'Europe/Berlin'
dag_max_active_tasks = 128
master_dag_max_active_runs = 1
child_max_active_runs = 5
execution_timeout_days = 14

# T-Systems specific configurations
current_month_working_days_offset = 7
corrections_working_days_offset = 6
working_days = [0, 1, 2, 3, 4]        # Monday=0 to Friday=4

# File format configuration
file_name_prefix = "REPLICON_ICM_TSI_PROJTIME"
csv_delimiter = ";"
csv_encoding = "ascii"
territory_code = "TSI"
default_file_format = "TimeExport_SAP"

# Service endpoints
replicon_time_export_service = '/services/TimeDataExportService1.svc'
default_file_format_uri = 'urn:replicon:file-format-script:tsystems-csv-semicolon'

# SFTP settings
sftp_upload_filepath = "/TSystems/TimeExport/Inbound"
sftp_log_filepath = "/TSystems/TimeExport/Logs"

# Field mappings for T-Systems SAP integration
field_mappings = {
    'time_entry_id': 'TimeEntryId',
    'employee_id': 'EmployeeId', 
    'project_name': 'ProjectCode',
    'booking_date': 'EntryDate',
    'period': 'TimesheetPeriod',
    'billing_rate_name': 'BillingRateName',
    'hours': 'Hours',
    'time_entry_comment': 'Comments',
    'task_name': 'TaskName',
    'task_code': 'TaskCode',
    'task_activity_name': 'ActivityName',
    'task_description': 'TaskDescription',
    'sap_activity_type': 'ActivityType'
}

# Email settings
email_conn_id = 'smtp_default'
email_from = 'replicon-integrations@company.com'
```

### Key Functions Implemented

#### utils/response_filter.py (Cleaned)
```python
def retrieve_export_uri(response)  # Extract export URI from API response
def extract_download_url(response)  # Extract download URL from batch response  
def translate_rows(row)            # Transform row data, preserve billing_entry
```

#### utils/request_payload.py (Cleaned)
```python
def get_uuid()                     # Generate unique UUID
def get_berlin_timenow_in_fmt()    # Get Berlin timezone timestamp
def parse_date(date_str)           # Parse date string to date object
def get_export_data_from_mapper()  # Extract configuration from mapper
def get_export_request(dag_run)    # Generate API request payload
def get_final_extract_data_row()   # Format final CSV row data
```

#### utils/python_callable.py
```python
def check_export_date_matches()    # Check if current date matches export schedule
def get_company_key()              # Get company identifier
def current_time_in_specified_tz() # Get current time in specified timezone
```

### Core Data Processing Logic

#### Data Filtering (timeexport_child_dag.py)
```python
# Only extract records with valid employee_ID and hours > 0
valid_extracted_data = rail.QueryCollectionOperator(
    task_id='valid_extracted_data',
    query="""SELECT * FROM validateddata WHERE NULLIF(employee_ID, '') IS NOT NULL AND CAST(hours AS FLOAT) > 0"""
)
```

#### CSV Generation with Proper Headers
```python
# Generate semicolon-delimited CSV with ASCII encoding
header=[
    'time_entry_id', 'employee_ID', 'project_ID', 'Entry Date',
    'billing_entry', 'billing_rate_name', 'hours', 'task_name',
    'task_code', 'task_activity_name', 'task_description',
    'sap_activity_type', 'transcation_ID'
],
delimiter=";",
encoding="ascii"
```

#### Field Mapping (task/time_data_export.py)
```python
# Data transformation with correct field names
columns=['time_entry_id', 'employee_ID', 'project_ID', 'entry_date', 
         'billing_entry', 'billing_rate_name', 'hours', 'task_name', 
         'task_code', 'task_activity_name', 'task_description', 
         'sap_activity_type','transaction_id']
```

### Business Logic Implementation
- **Schedule**: Driven by export_schedule_mapper.py with specific dates and legal units
- **Data Filter**: Only approved time entries with hours > 0
- **File Format**: Semicolon-delimited CSV with ASCII encoding  
- **Field Mapping**: Complete T-Systems SAP field mapping with billing_entry preservation
- **Timezone**: All operations in Europe/Berlin timezone
- **Error Handling**: Comprehensive logging and error tracking with SumoLogic integration

---

## Key Improvements Made

### Data Accuracy Fixes
1. **Fixed billing_entry handling**: Now preserves original values instead of always deriving from billing_rate_name
2. **Added hours > 0 filter**: Ensures only records with actual time logged are exported
3. **Corrected field mappings**: Updated from 'period' to 'entry_date' throughout the codebase
4. **Fixed translate_rows function**: Properly handles missing fields with safe access patterns

### Code Cleanup Results
1. **Removed 9 unused functions** from response_filter.py (formatting, validation, mapping functions)
2. **Removed 1 unused function** from request_payload.py (get_file_format_script_uri)
3. **Deleted unused file**: send_email_notification.py (entire file was unused)
4. **Removed unused config variables**: export_types, validation_rules, email templates, etc.
5. **Cleaned up imports**: Removed unused imports and null variable declarations
6. **Fixed indentation**: Corrected get_export_request function indentation issues

### Performance Optimizations
1. **Streamlined configuration**: Kept only actively used config variables
2. **Optimized data processing**: Removed redundant validation and formatting layers
3. **Improved error handling**: Uses rail functions consistently throughout
4. **Better memory usage**: Eliminated unused code and variables

---

## Execution Instructions

To recreate this project from scratch:

1. **Execute Initial Prompt**: Create the base project structure and implement initial requirements

2. **Apply User Corrections in Order**:
   - Implement PWC pattern structure with proper task groups
   - Add export schedule mapper with specific dates and legal units
   - Fix data filtering logic to preserve billing_entry and filter hours > 0
   - Reorganize files to utils folder structure
   - Clean up unused code, functions, variables, and imports
   - Ensure proper field mappings (entry_date instead of period)

3. **Validate Final Implementation**:
   - Ensure billing_entry values are preserved from source data
   - Verify hours > 0 filtering in valid_extracted_data query
   - Confirm semicolon CSV format with ASCII encoding and proper headers
   - Test error handling and logging functionality
   - Validate SFTP file upload with correct naming conventions
   - Ensure Berlin timezone usage throughout

This prompt.md file serves as the complete blueprint for recreating the T-Systems Time Export to SAP integration project with all user corrections, optimizations, and cleanup implemented.