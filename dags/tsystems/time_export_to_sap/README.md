# T-Systems Time Export to SAP

This project implements an automated time export integration from Replicon to T-Systems SAP system using a scheduler-driven approach with export schedule mapping.

## Overview

The integration extracts approved time entries from Replicon and exports them in CSV format to T-Systems SFTP server for SAP processing. The system uses a unified DAG architecture that runs daily at 6:00 PM CET, checking the export schedule mapper for matching dates and processing exports inline when matches are found.

## Key Features

- **Schedule-Driven Export**: Uses export_schedule_mapper.py for precise date and legal unit control
- **Approved Entries Only**: Only exports approved timesheets with hours > 0
- **CSV Format**: Semicolon-separated values in ASCII encoding
- **Data Filtering**: Preserves billing_entry values and filters out zero-hour records
- **Transaction Logging**: Comprehensive logging with unique transaction IDs
- **Email Notifications**: Automated status emails to T-Systems administrators
- **Error Handling**: Robust error handling with SumoLogic integration
- **PWC Pattern**: Follows PWC time export pattern for consistency

## Project Structure (Current)

```
tsystems/time_export_to_sap/
├── config.py                      # Core system configuration
├── main_dag.py                     # Unified DAG definition (schedule check + export)
├── process_each_export.py          # Individual export processing logic
├── timeexport_child_dag.py         # Child DAG implementation for processing
├── prompt.md                       # Development documentation
├── instances/                      # Instance-specific configurations
│   └── trial.py                   # Development/trial configuration
├── mappers/                        # Export scheduling
│   └── export_schedule_mapper.py  # Date and legal unit mapping with file formats
├── task/                          # Core task implementations
│   └── time_data_export.py        # PWC pattern time export task group
├── utils/                         # Utility functions
│   ├── python_callable.py        # Date calculation and timezone utilities
│   ├── request_payload.py         # API request payload generation
│   └── response_filter.py         # Data filtering and transformation
└── templates/                     # Email templates
    ├── email_invalid_records_in_export.html
    └── email_valid_import_complete.html
```

## Configuration

### Export Schedule Mapping
The system uses `mappers/export_schedule_mapper.py` to define specific export dates, legal units, and file naming:

```python
export_schedule_mapper = [
  {
    "legal_unit": ["All"],
    "company_code": ["0370", "0377", "1046"],
    "time_entry_start_date": "01.08.2025",
    "time_entry_end_date": "05.08.2025",
    "export_date": "12.08.2025",
    "file_name_format": "REPLICON_ICM_TSI_PROJTIME_2380_YYYYMMDD_HHMSS.csv"
  },
  {
    "legal_unit": ["2380"],
    "company_code": ["All"],
    "time_entry_start_date": "28.07.2025",
    "time_entry_end_date": "31.07.2025",
    "export_date": "12.08.2025",
    "file_name_format": "REPLICON_ICM_TSI_PROJTIME_2380_YYYYMMDD_HHMSS.csv"
  },
  # Additional mappings...
]
```

### File Naming Convention
- **CSV File**: `REPLICON_ICM_TSI_PROJTIME_{identifier}_YYYYMMDD_HHMMSS.csv`
- **TWB File**: `REPLICON_ICM_Export_YYYYMMDD_HHMMSS`
- **Log File**: `Time_extract_log_YYYYMMDD_HHMMSS.txt`

### CSV Format Specifications
- **Delimiter**: Semicolon (`;`)
- **Encoding**: ASCII
- **Empty Fields**: Reflected with separator
- **Last Field**: Followed by semicolon
- **Numbers**: Extended form (not compacted)

## Data Processing

### Field Mapping (Updated)

| CSV Header | Internal Field | Description | Sample Value |
|------------|---------------|-------------|--------------|
| time_entry_id | time_entry_id | Unique time entry identifier | 1235 |
| employee_ID | employee_ID | Employee identifier | 200255075 |
| project_ID | project_ID | Project code | F.37074702.04 |
| Entry Date | entry_date | Formatted as YYYY.MM period | 2025.05 |
| billing_entry | billing_entry | Billing status (preserved from source) | Billable |
| billing_rate_name | billing_rate_name | Billing rate name | A37074702 |
| hours | hours | Hours worked | 60.25 |
| task_name | task_name | Task name | Task Architecture Solution |
| task_code | task_code | Task code | T1234 |
| task_activity_name | task_activity_name | Activity name | Working Time |
| task_description | task_description | Task description | Description of the task |
| sap_activity_type | sap_activity_type | SAP activity type | CE2 |
| transcation_ID | transaction_id | Unique transaction identifier | uuid-1234 |

### Data Filtering Logic

```python
# Only extract records with valid employee_ID and hours > 0
valid_extracted_data = rail.QueryCollectionOperator(
    task_id='valid_extracted_data',
    query="""SELECT * FROM validateddata WHERE NULLIF(employee_ID, '') IS NOT NULL AND CAST(hours AS FLOAT) > 0"""
)
```

### Data Transformation

The `translate_rows` function in `utils/response_filter.py` handles:
- **Field Mapping**: Maps Replicon fields to T-Systems format
- **Date Formatting**: Converts entry_date to YYYY.MM format
- **Billing Entry Preservation**: Maintains original billing_entry values
- **Safe Field Access**: Handles missing fields gracefully

## DAG Architecture & Export Process Flow

### Unified DAG Structure
The system now uses a **unified DAG approach** where the main DAG handles both schedule checking and export processing:

```
Unified DAG (Daily at 6:00 PM CET)
        │
        ▼
┌─────────────────────────────────┐
│ Check Export Schedule Mapper    │
│ for Today's Date               │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│ If Match Found:                │
│ Process Export Inline          │
│ Using Legal Unit & Company     │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│     TIME EXPORT PROCESS         │
│                                 │
│ 1. Get File Format Script       │
│ 2. Get Export Data from Mapper  │
│ 3. Create Export Batch          │
│ 4. Execute & Wait for Batch     │
│ 5. Get Export URI & Update Name │
│ 6. Create Download Batch        │
│ 7. Execute & Wait for Download  │
│ 8. Download & Load CSV Data     │
│ 9. Filter: hours > 0, valid ID │
│ 10. Transform & Preserve Fields │
│ 11. Generate CSV with Headers   │
│ 12. Upload to SFTP              │
│ 13. Send Notifications          │
└─────────────────────────────────┘
```

### Child DAG Processing
The system also maintains `process_each_export.py` for individual export processing and `timeexport_child_dag.py` for advanced child DAG scenarios.

## Key Optimizations & Fixes

### Data Accuracy Improvements
1. **Billing Entry Preservation**: Fixed translate_rows to preserve original billing_entry values
2. **Hours Filtering**: Added `CAST(hours AS FLOAT) > 0` filter to exclude zero-hour records
3. **Field Mapping Corrections**: Updated from 'period' to 'entry_date' throughout codebase
4. **Safe Field Access**: Improved error handling with .get() methods

### Code Cleanup Results
1. **Removed 9 unused functions** from response_filter.py (formatting, validation, mapping)
2. **Removed 1 unused function** from request_payload.py
3. **Deleted unused file**: send_email_notification.py (entire file was unused)
4. **Streamlined config**: Removed unused variables (export_types, validation_rules, etc.)
5. **Cleaned imports**: Removed unused imports and null declarations
6. **Fixed indentation**: Corrected get_export_request function structure

### Performance Enhancements
- **Streamlined Configuration**: Kept only actively used config variables
- **Optimized Data Processing**: Removed redundant validation layers
- **Better Memory Usage**: Eliminated unused code and variables
- **Consistent Rail Usage**: Leveraged rail functions throughout

## Current Core Functions

### utils/response_filter.py (Cleaned)
```python
def retrieve_export_uri(response)     # Extract export URI from API response
def extract_download_url(response)    # Extract download URL from batch response  
def translate_rows(row)              # Transform row data, preserve billing_entry
```

### utils/request_payload.py (Cleaned)
```python
def get_uuid()                       # Generate unique UUID
def get_berlin_timenow_in_fmt()      # Get Berlin timezone timestamp
def parse_date(date_str)             # Parse date string to date object
def get_export_data_from_mapper()    # Extract configuration from mapper
def get_export_request(dag_run)      # Generate API request payload with filters
def get_final_extract_data_row()     # Format final CSV row data
```

### utils/python_callable.py
```python
def check_export_date_matches()      # Check if current date matches export schedule
def get_company_key()                # Get company identifier
def current_time_in_specified_tz()   # Get current time in specified timezone
```

## Email Notifications

The system sends automated notifications using templates in the `templates/` directory:
- **Invalid Records in Export** (`email_invalid_records_in_export.html`): Details about filtered/invalid entries
- **Valid Import Complete** (`email_valid_import_complete.html`): Confirmation of successful processing

Note: Additional email templates for export success, failure, and no data scenarios may be handled through external systems or rail operators.

## Error Handling

- **Comprehensive Logging**: SumoLogic integration for centralized logs
- **Graceful Failure**: Proper error handling without try-catch blocks
- **Transaction Tracking**: Unique transaction IDs for audit trails
- **Status Updates**: Export name updates reflect processing status

## Constraints and Assumptions

### Constraints
- Only approved time entries with hours > 0 are exported
- CSV format with semicolon delimiter and ASCII encoding
- Modifications require resubmission for delta extraction
- Sequential, un-indexed flat file processing

### Assumptions
- All timesheet data is project/task-based
- Export schedule mapper drives all export timing
- Time workbench has predefined filters configured
- Each record has unique Time Entry ID for delta identification
- Billing entry values are preserved from source data

## Usage

### Monitoring
- Airflow UI for DAG run status
- SumoLogic for centralized logging
- SFTP server for uploaded files
- Email notifications for export status

### Manual Execution
1. Navigate to Airflow UI
2. Find main DAG: `tsystems_time_export_to_sap_{instance}` (unified DAG)
3. Alternatively, find child DAG: `timeexport_to_sap_process_export_child_dag_{instance}`
4. Trigger DAG run with appropriate configuration

## Development

### Configuration Updates
- Modify `mappers/export_schedule_mapper.py` for schedule changes and file naming formats
- Update `instances/{environment}.py` for environment-specific settings (currently: `trial.py`)
- Adjust `config.py` for system-wide configuration changes
- Review `prompt.md` for development notes and documentation

### Testing
- Use trial instance for development testing
- Verify CSV format meets T-Systems specifications
- Test SFTP upload functionality and file naming
- Validate email notifications and error handling

## Dependencies

- **Rail Library**: Custom Airflow operators and utilities
- **Replicon Time Data Export Service**: Source system integration
- **SFTP Server**: File upload destination
- **SumoLogic**: Centralized logging and monitoring

## Support

For issues or questions:
- Check Airflow DAG logs in UI
- Review SumoLogic for detailed error tracking  
- Verify export schedule mapper for date conflicts
- Contact integration support team