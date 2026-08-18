# Guidehouse Time Export DAG - Design Document

## Overview

The Guidehouse Time Export system is a multi-stage data pipeline that exports time and timeoff data from Replicon's Time Data system and transforms it for integration with PeopleSoft. The system uses a master-child DAG architecture to orchestrate complex batch processing and validation workflows.

**Purpose**: Extract daily/hourly time and timeoff entries from Replicon, validate data quality, transform timeoff types to PeopleSoft project/task codes, and upload formatted CSV files to SFTP for PeopleSoft consumption.

**Instances**: 
- `trial_daily` - Daily export scheduled at 7:00 PM ET (0 19 * * *)
- `trial_hourly` - Hourly export (configuration available)

---

## Architecture

### Master-Child DAG Pattern

The implementation follows the RAIL multi-DAG architecture pattern:

```
┌─────────────────────────────────────────────────┐
│  Master DAG (guidehouse_time_export_daily_...)  │
│  - Validates prerequisites                       │
│  - Queries row counts                           │
│  - Initiates time data export in Replicon       │
│  - Orchestrates child DAG execution             │
│  - Validates & processes final results          │
└─────────────────────────────────────────────────┘
                         │
                         ↓ TriggerDagRunOperator
┌─────────────────────────────────────────────────┐
│  Child DAG (time_export_peoplesoft_export...)   │
│  - Downloads exported data from Replicon        │
│  - Parses and validates CSV records             │
│  - Transforms timeoff types to PS codes         │
│  - Writes formatted CSV with pipe delimiters    │
│  - Uploads to SFTP for PeopleSoft ingestion     │
│  - Sends completion email                       │
└─────────────────────────────────────────────────┘
```

### Information Flow

1. **Prerequisite Validation**: Check that previous time export child DAG runs completed successfully
2. **Date Window Calculation**: Compute start/end dates based on run type (hourly/daily)
3. **Service Center Discovery**: Retrieve all enabled service centers from Replicon
4. **Data Availability Check**: Query row counts for time data to determine if export should proceed
5. **Batch Export Creation**: Create time data export batch in Replicon
6. **Child DAG Invocation**: Trigger child DAG with export URI and date parameters
7. **Result Aggregation**: Collect results from child DAG execution
8. **Status Management**: Update export status based on child DAG outcomes

---

## Data Flow Details

### Master DAG Flow

```
check_can_trigger_next_run
    ↓
if_previous_time_export_child_unsuccessful
    ├─ [Yes] → fail_current_run (exit)
    └─ [No] ↓
    get_date_window
    ↓
    get_all_service_centers
    ↓
    create_timedata_row_counts_batch
    ↓
    execute_row_counts_batch → wait_for_row_counts_batch
    ↓
    get_timeoffdata_row_counts_results
    ↓
    export_has_data?
    ├─ [No] → (skip to end)
    └─ [Yes] ↓
    start_export
    ↓
    time_export (task group)
        ├─ create_export
        ├─ execute_export
        ├─ wait_for_export
        ├─ get_export_uri
        ├─ update_export_name
        └─ mark_as_completed
    ↓
    trigger_ps_export (Child DAG)
    ↓
    wait_for_exports
    ↓
    gather_results_from_dag_runs
    ↓
    check_response_from_all_dags
    ↓
    mark_timedata_export_error?
    ├─ [Yes] → cancel_export → ... → fail_time_export
    └─ [No] ↓
    if_no_data_in_all_child_dags?
        ├─ [Yes] → rename_export_name_to_no_data
        └─ [No] → (success)
```

### Child DAG Flow

```
view_dagrun_conf
↓
response_from_dag_var (initialize to "Success")
↓
time_export_download_script_uri
↓
create_download_batch
↓
execute_download_batch → wait_for_download_batch
↓
get_download_url
↓
download_export
↓
load_export (parse CSV)
↓
create_raw_timeexport_data_collection
↓
query_timeexport_records (filter by conditions)
↓
has_any_timeexport_data?
├─ [No] → set_response_from_dag_no_data
└─ [Yes] ↓
update_timeexport_records (transform timeoff types)
↓
create_timeexport_records
↓
write_export_csv (pipe-delimited format)
↓
upload_time_export_to_sftp
↓
send_valid_export_complete_email
↓
catch_error (trigger_rule: one_failed)
↓
final_response_from_dag
```

---

## Components

### Master DAG Configuration

**File**: `time_export_master/config.py`

| Setting | Value | Purpose |
|---------|-------|---------|
| `region` | us-east-1 | AWS region |
| `environment` | pre-production | Deployment environment |
| `execution_timeout_days` | 14 | Max execution time for child DAGs |
| `timezone` | America/New_York | Eastern Time for scheduling |
| `max_active_runs_master` | 1 | Prevent concurrent master runs |

### Child DAG Configuration

**File**: `time_export_peoplesoft/config.py`

| Setting | Value | Purpose |
|---------|-------|---------|
| `export_file_format_name` | PeopleSoftTimeExport | Download script identifier in Replicon |
| `ps_file_prefix` | PPSTime | Filename prefix for PeopleSoft exports |
| `file_extension` | .csv.pgp | File extension (PGP encryption ready) |
| `master_max_active_run` | 1 | Prevent concurrent child runs |
| `paycodes_to_exclude` | (tuple of 18 codes) | Pay types to filter out from export |

### Instance Definitions

**File**: `time_export_master/instances/trial_daily.py`

```python
instance = "trial"
company_key = "GuideHouseIncSB2"
replicon_conn_id = "replicon_guidehouse_repliconint"
master_dag_id = "guidehouse_time_export_daily_master_trial"
ps_export_dag_id = "guidehouse_time_export_peoplesoft_export_child_trial"
sftp_conn_id = "guidehouse_sftp"
run_type = "daily"
schedule_interval = "0 19 * * *"  # 7 PM ET daily
```

---

## Key Components & RAIL Operators Used

### Replicon Service Operators

1. **GetEnabledServiceCenters** - Retrieve available service centers
2. **CreateTimeDataItemRowCountsBatch** - Initiate row count query batch
3. **GetTimeDataItemRowCountsBatchResults** - Fetch row count results
4. **CreateTimeDataExportBatch** - Initiate data export batch
5. **GetCreateTimeDataExportBatchResults** - Retrieve export URI and status
6. **UpdateTimeDataExportName** - Rename export with timestamp
7. **MarkTimeDataExportAsComplete** - Mark export as complete status
8. **GetTimeDataExportDetails** - Query export status
9. **CreateTimeDataExportStatusBatch** - Create status change batch
10. **CreateTimeDataDownloadBatch** - Prepare download batch
11. **GetTimeDataDownloadBatchResults** - Get download URL

### Data Processing Operators

1. **HTTPDownloadFileOperator** - Download CSV from Replicon
2. **LoadCSVFileOperator** - Parse CSV into records
3. **CreateCollectionOperator** - Create in-memory collections
4. **QueryCollectionOperator** - SQL-like filtering on collections
5. **DataAdaptorOperator** - Transform records using custom logic
6. **WriteCSVFileOperator2** - Write pipe-delimited CSV output

### File & Network Operators

1. **SFTPUploadFileOperator** - Upload to SFTP server
2. **EmailOperator** - Send completion email

### Control Flow Operators

1. **IfOperator** - Conditional branching
2. **TriggerDagRunOperator** - Trigger child DAG with config
3. **WaitForDagRunsSensor** - Wait for child DAG completion
4. **GatherResultsFromDagRunsOperator** - Collect child DAG results
5. **FailOperator** - Explicit failure with message
6. **PythonOperator** - Custom Python logic

### Batch Execution

The system uses RAIL's `batch_execution()` pattern for long-running Replicon operations:
- Creates batch job in Replicon
- Executes batch asynchronously
- Waits for batch completion with timeout

---

## Timeoff Type Mapping

**File**: `time_export_master/mapper/timeoff_project_task_mapper.py`

Maps Replicon timeoff types to PeopleSoft project and task codes:

| Replicon Type | PS Project | PS Task | CP Project | CP Task | Pay Code |
|---------------|-----------|--------|-----------|---------|----------|
| Bereavement | 911405 | 001 | LEAVE1.00.0000 | BERV | BER |
| Caregiver Leave | 913110 | 006 | LEAVE1.00.0000 | CARE | CAR |
| Holiday | 911310 | 001 | LEAVE1.00.0000 | HOLI | HOL |
| Jury Duty | 911400 | 002 | LEAVE1.00.0000 | JURY | JUR |
| Lost Time (Weather/NAT) | 913100 | 001 | LEAVE1.00.0000 | LOST | LOS |
| Lost Time (Internet/Power) | 913100 | 002 | LEAVE1.00.0000 | LOST | LOS |
| Parental Leave | 911500 | 002 | LEAVE1.00.0000 | MATN | MAT |
| Voting Leave | 911400 | 005 | LEAVE1.00.0000 | VOTE | VOT |
| Military Leave | 911400 | 003 | LEAVE1.00.0000 | MILI | MIL |

*Additional mappings exist in the full mapper configuration.*

---

## Data Filtering & Transformation

### Master DAG

**Row Count Query**: Checks if time data exists for the export window by querying row counts for all time data items.

### Child DAG

**Query Filters**: `query_timeexport_records`
```sql
SELECT * FROM raw_timeexport_data 
WHERE nullif(employee_id,'') IS NOT NULL 
  AND (
    (NULLIF(pay_type, '') IS NULL AND NULLIF(timeoff_type,'') IS NOT NULL)
    OR (NULLIF(pay_type, '') IS NOT NULL AND pay_type NOT IN (paycodes_to_exclude))
  )
```

**Transformation**: `update_timeexport_records`
- Uses `DataAdaptorOperator` with custom method `get_peoplesoft_export_rows`
- Maps timeoff types via `TIMEOFF_PROJECT_TASK_MAPPER`
- Filters excluded paycodes
- Extracts: Employee ID, Entry Date, Project Code, Task Name, Hours, Pay Type, Comments

**Output CSV Format** (pipe-delimited):
```
Employee ID|Short Entry ID|Transaction Date|PeopleSoft Project ID|PeopleSoft Activity ID|Number of Hours|Pay Types|Comments
100524|TE-012345|2026-05-10|911405|001|8.0|BER|Family emergency leave
```

---

## Error Handling & Recovery

### Prerequisite Validation
- Checks previous master DAG run success
- Fails current run if predecessor unsuccessful
- Prevents cascading failures

### Data Availability
- Queries row counts before initiating export
- Skips processing if no data exists
- Renames export to "NoData" suffix for audit trail

### Child DAG Results
- Master DAG validates all child DAG responses received
- Checks if all responses contain errors → cancels export
- Checks if all responses indicate no data → renames export
- Mixes of success/no-data → proceeds normally

### Export Cancellation Path
When errors occur in master DAG processing:
1. Retrieve export URI
2. Query export status
3. If status is "draft" → immediately cancel
4. If status is "completed" → change to "draft" then "cancelled"
5. Rename export with "Cancelled_" prefix
6. Fail DAG with error message

### Child DAG Error Handling
- All processing tasks feed into `catch_error` with `trigger_rule="one_failed"`
- Sets response variable to "Error in child dag - Time export to PeopleSoft"
- Final task runs with `trigger_rule="all_done"` regardless of success/failure

### Response Tracking
Child DAG returns status via variable:
- "Success" - Data exported successfully
- "No Data in export" - No matching records found
- "Error in child dag - Time export to PeopleSoft" - Processing failed

---

## Key Features

### Idempotency & Uniqueness
- Export names include timestamp: `PPSTime_YYYYMMDD_HHMMSS`
- No-data exports tagged: `PPSTime__NoDataYYYYMMDD_HHMMSS`
- Supports hourly and daily scheduling

### Batch Processing
- All Replicon operations use batch execution pattern
- Row count batch: 5-hour timeout
- Download/export batches: Default timeouts
- Asynchronous execution with polling

### Notifications
- **HTML Email Template**: `/templates/email_valid_export_complete.html`
- **Recipient**: `config.tenant_email` (Guidehouse contact)
- **BCC**: `config.internal_logs_email` (Internal logging)
- **Subject**: Format includes company key and timestamp

### Future Capabilities
- **PGP Encryption**: Commented code available for `encrypt_time_export_data_csv`
- **DataLake Export**: Commented `trigger_dl_export` for future data warehouse integration
- **Multiple Export Destinations**: Architecture supports extending to additional export targets

---

## Scheduling & Timing

### Time Zones
- All scheduling uses America/New_York (Eastern Time)
- Handles DST transitions via Pendulum library

### Date Window Calculation

**Hourly Mode**:
- Extracts data for the immediately preceding hour
- Format: `YYYY-MM-DD HH:00:00` to `YYYY-MM-DD HH:59:59`

**Daily Mode**:
- Extracts data from start of current business day to end of previous hour
- Aligns with daily payroll processing cycles
- Start: 00:00:00 of run date
- End: 23:59:59 of previous day

### Deployment Info
- **Start Date**: May 1, 2026
- **Environment**: Pre-production
- **Max Active Runs**: 1 (prevents queue buildup)
- **Execution Timeout**: 14 days (default for child DAGs)

---

## Replicon Connections & Credentials

**Primary Connection** (`replicon_guidehouse_repliconint`):
- Base URL: Replicon API endpoint
- Authentication: API token/credentials configured in Airflow

**SFTP Connection** (`guidehouse_sftp`):
- Target path: Configured per instance
- Credentials: SFTP user/password
- Output location: PeopleSoft inbound directory

---

## Dependencies & File References

### External Templates
- `/templates/email_valid_export_complete.html` - Email template for completion notification

### Configuration Files
- `config.py` - Global settings
- Instance files - Instance-specific overrides:
  - `trial_daily.py` - Daily export instance
  - `trial_hourly.py` - Hourly export instance

### Utilities
- `utils/date_range.py` - Date window calculations
- `utils/request_payload.py` - Replicon API payload construction
- `utils/custom_methods.py` - Business logic and data transformation
- `mapper/timeoff_project_task_mapper.py` - Timeoff type mappings

---

## Testing & Validation

### Data Quality Checks
1. Row count validation - confirm data exists before processing
2. Record count matching - verify all records processed
3. Null value handling - filters empty employee IDs
4. Pay code filtering - excludes configured non-workable pay types

### Process Validation
1. Previous run completion check - prevents dependent failures
2. Response count validation - confirms all child DAGs returned results
3. Error response detection - validates success vs. error states
4. Export status transitions - tracks draft/complete/cancelled states

---

## Migration & Future Enhancements

### Potential Enhancements
1. **Multi-destination Export**: Add DataLake/Hadoop targets
2. **Encryption**: Enable PGP encryption for SFTP transmission
3. **Archive Management**: Implement retention policies for exported files
4. **Enhanced Notifications**: Add Slack/Teams alerts for failures
5. **Dynamic Pay Code Exclusion**: Load exclusion list from configuration database

### Known Limitations
1. Single concurrent master DAG run (prevents queue issues)
2. 14-day execution timeout (may need increase for very large exports)
3. Manual timeoff mapping maintenance (consider database-driven approach)

---

## Troubleshooting Guide

### Common Issues

**Issue**: Master DAG fails with "Previous time export run not Successful"
- **Cause**: Prior child DAG execution failed
- **Resolution**: Investigate and fix previous child DAG, then retry master

**Issue**: Master DAG completes but shows "No Data in export"
- **Cause**: No time entries matching filter criteria in date window
- **Resolution**: Verify date calculation; check if any time data exists in Replicon for period

**Issue**: Child DAG fails with download error
- **Cause**: Export URI invalid or batch execution incomplete
- **Resolution**: Check row counts; verify batch creation succeeded

**Issue**: SFTP upload fails
- **Cause**: Connection error, path invalid, or disk full
- **Resolution**: Verify SFTP credentials and target directory permissions

**Issue**: Email not sent
- **Cause**: Email template missing or tenant email invalid
- **Resolution**: Verify template path and email configuration in instance config

---

## Appendix: File Structure

```
dags/guidehouse/time_export/
├── DESIGN.md                                          # This file
├── time_export_master/
│   ├── config.py                                      # Master config
│   ├── main.py                                        # Master DAG factory
│   ├── instances/
│   │   ├── trial_daily.py                            # Trial daily instance
│   │   └── trial_hourly.py                           # Trial hourly instance
│   ├── tasks/
│   │   ├── time_export_task.py                       # Time data export task group
│   │   └── update_time_export_status.py              # Export status updates
│   ├── mapper/
│   │   └── timeoff_project_task_mapper.py            # Timeoff → PS mapping
│   └── utils/
│       ├── date_range.py                             # Date window logic
│       ├── request_payload.py                        # API request builders
│       └── custom_methods.py                         # Custom transforms
└── time_export_peoplesoft/
    ├── config.py                                      # Child config
    ├── ps_time_export_child.py                       # Child DAG factory
    ├── instances/
    │   └── trial.py                                  # Trial instance
    └── utils/
        ├── custom_methods.py                         # Child-specific transforms
        └── request_payload.py                        # Child-specific payloads
```

---

## Document Metadata

| Property | Value |
|----------|-------|
| **Version** | 1.0 |
| **Last Updated** | 2026-05-11 |
| **Maintainer** | Guidehouse Integration Team |
| **Status** | Active (Trial Instances) |
| **Related Docs** | @file/06_code_review_standards.md, @file/patterns/error_handling.md |
