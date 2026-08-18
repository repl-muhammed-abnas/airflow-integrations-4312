# Design Document

**Timeoff Booking Export from Polaris to Resource Planner**

| Property | Value |
|----------|-------|
| Version | 1.0 |
| Date | March 13, 2026 |
| Author | Data Engineering Team |
| Pipeline Name | `resource_planner_timeoff_export_{instance}` |

## Summary

This document outlines the design for an automated Apache Airflow pipeline that exports **timeoff (absence/holiday) booking data** from Polaris to the Resource Planner database. The pipeline uses Polaris's TimeDataExportService batch API to extract approved timeoff entries, processes them into the `rp_source` table, and handles insert/delete operations based on hours values.

This is a **single-DAG pipeline** (no master-child architecture) since the data volume is manageable within a single execution. The pipeline also includes skipped-records tracking with email notifications for entries missing employee IDs.

---

## Table of Contents

1. [Overview and Goals](#1-overview-and-goals)
2. [Integration Workflow](#2-integration-workflow)
3. [Non-Functional Requirements](#3-non-functional-requirements)
4. [High-Level Architecture](#4-high-level-architecture)
5. [Data Sources](#5-data-sources)
6. [Data Transformation and Mapping](#6-data-transformation-and-mapping)
7. [Airflow-Specific Design](#7-airflow-specific-design)
8. [Error Handling, Observability, and Alerting](#8-error-handling-observability-and-alerting)
9. [Conclusion](#9-conclusion)

---

## 1. Overview and Goals

### Purpose

This pipeline synchronizes **timeoff booking data** (vacation, sick leave, holidays) from Polaris to Resource Planner. Each approved timeoff entry becomes a row in the `rp_source` table, classified as either "Absence" or "Holiday" based on the timeoff type name.

### Business Goals

- **Automate Timeoff Sync:** Replace manual extraction of timeoff booking data
- **Track Absences:** Ensure Resource Planner has up-to-date absence/holiday data for capacity planning
- **Handle Deletions:** Remove timeoff entries that have been zeroed out in Polaris
- **Audit Trail:** Track and notify about skipped records (missing employee ID)

### Success Criteria

- All approved, unexported timeoff entries are synced to Resource Planner
- Entries with 0 hours that exist in DB are deleted (cancelled timeoff)
- Skipped records are logged and emailed to stakeholders
- Pipeline completes without data loss

---

## 2. Integration Workflow

### Core Functionality

The pipeline operates in three phases:

**Phase 1 — Data Availability Check:**
1. Get download script URI from TimeDataDownloadScriptAdministrationService
2. Create row counts batch to check if exportable data exists
3. Execute and wait for row counts batch
4. If no data → skip to end

**Phase 2 — Export and Download:**
5. Create timeoff export batch
6. Execute and wait for export batch
7. Check for batch errors → fail if error
8. Update export name with timestamp
9. Mark export as complete
10. Create download batch
11. Execute and wait for download batch
12. Get download URL and download CSV file

**Phase 3 — Process and Write:**
13. Load CSV into export collection
14. Fetch timeoff type master data (name → URI map) from Polaris API
15. Query existing booking IDs from database
16. Fetch user ID map from `dbo.users` table
17. Classify records:
    - **INSERT:** hours > 0 (new timeoff booking)
    - **DELETE:** hours = 0 AND booking ID exists in DB (cancelled timeoff)
    - **SKIP:** missing employee_id (log and notify)
18. Execute insert and delete operations
19. Send email notification with skipped records CSV (if any)

### Data Flow

```
Polaris Export Service             Polaris API                   Database
(TimeDataExportService)            (TimeOffTypeList)             (SQL Server)
     │                                │                           │
     ▼                                │                           │
┌──────────────────┐                  │                           │
│ Phase 1: Check   │                  │                           │
│ Row Counts       │                  │                           │
│ (has data?)      │                  │                           │
└────────┬─────────┘                  │                           │
         │ Yes                        │                           │
         ▼                            │                           │
┌──────────────────┐                  │                           │
│ Phase 2: Export  │                  │                           │
│                  │                  │                           │
│ Create batch     │                  │                           │
│ Execute batch    │                  │                           │
│ Mark complete    │                  │                           │
│ Create download  │                  │                           │
│ Download CSV     │                  │                           │
└────────┬─────────┘                  │                           │
         │                            │                           │
         ▼                            │                           │
┌──────────────────┐                  │                           │
│ Phase 3: Process │                  │                           │
│                  │                  │                           │
│ Load CSV         │                  │                           │
│ Fetch timeoff    │◄── type map ─────┤                           │
│   types          │                  │                           │
│ Query existing   │◄─────────────────┼───────────────────────────┤
│   booking IDs    │                  │                           │
│ Fetch user ID    │◄─────────────────┼───────────────────────────┤
│   map            │                  │                           │
│                  │                  │                           │
│ Classify:        │                  │                           │
│   INSERT (>0h)   │── INSERT ───────►┼──────────────────────────►│
│   DELETE (=0h)   │── DELETE ───────►┼──────────────────────────►│
│   SKIP (no emp)  │── EMAIL ────────►│                    rp_source
└──────────────────┘                  │                           │
```

---

## 3. Non-Functional Requirements

### 3.1 Scalability

- Single DAG handles typical timeoff volume
- Multi-row INSERT (230 rows/batch) for efficient bulk loading
- Paginated API calls for timeoff type master data

### 3.2 Reliability

- Feature flag (Airflow Variable) to enable/disable
- Export cancellation on failure (creates cancel status batch)
- Skipped records tracking with email notification
- Transactional database operations

### 3.3 Performance

- Batch execution with configurable timeout (5 hours)
- Parallel data gathering (timeoff types, existing records, user IDs fetched concurrently)
- Multi-row INSERT for speed

### 3.4 Maintainability

- Utility modules for request payloads and response filters
- Centralized configuration

---

## 4. High-Level Architecture

### 4.1 System Components

| Component | Type | Description |
|-----------|------|-------------|
| TimeDataExportService | REST API | Polaris batch export service for timeoff data |
| TimeDataDownloadScriptAdministrationService | REST API | Provides download script URIs |
| TimeOffTypeListService | REST API | Provides timeoff type master data |
| Apache Airflow 2.x+ | Platform | Workflow orchestration |
| Resource Planner DB | SQL Server | Target database |
| `rp_source` | SQL Table | Destination table (dev: `dbo.rp_source`, default: `dbo.dummy_rp_source`) |
| `dbo.users` | SQL Table | User ID → employee ID mapping reference |

### 4.2 Integration Points

| Integration | Protocol | Direction | Description |
|-------------|----------|-----------|-------------|
| Polaris → Airflow | HTTPS (REST) | Inbound | Batch export API (create, execute, download) |
| Polaris → Airflow | HTTPS (REST) | Inbound | TimeOff type list API (paginated) |
| Polaris → Airflow | HTTPS | Inbound | CSV file download via presigned URL |
| Airflow → SQL Server | ODBC | Outbound | Database reads and writes |
| Airflow → SMTP | SMTP | Outbound | Skipped records email notification |

---

## 5. Data Sources

### 5.1 TimeDataExportService (Batch Export)

The exported CSV contains the following columns:

| Column | Description | Used For |
|--------|-------------|----------|
| `Employee ID` | Employee identifier | `employee_id`, user ID resolution |
| `Time Off Type Name` | Name of the timeoff type (e.g., "Vacation", "Holiday") | `time_code` resolution, `hours_type` classification |
| `Time Off Booking ID` | Unique booking identifier | Reference |
| `Short Time Entry ID` | Short entry identifier | `source_booking_id` |
| `Entry Date` | Date of the timeoff entry | `work_date` |
| `Hours (Current)` | Hours for this entry (0 = cancelled) | `hours`, insert/delete routing |
| `User` | User display name | Skipped record reporting |
| `LastModifiedDate_UTC` | Last modification timestamp | `last_updated_date` |

### 5.2 TimeOffTypeListService (Master Data)

**Endpoint:** `/services/TimeOffTypeListService1.svc/GetData`

Provides paginated list of all timeoff types with their names and URIs. Combined into a `{name → URI_last_segment}` map for time_code resolution.

### 5.3 Export Filter Criteria

The export uses a 4-nested AND filter expression:

| Filter | Value | Purpose |
|--------|-------|---------|
| `entry-date-range` | Current month ± 2 months | Date window for export |
| `time-data-export-status` | `none` (unexported only) | Only process new entries |
| `time-entry-type` | `time-off` | Only timeoff entries |
| `time-entry-approval-status` | `approved` | Only approved entries |

---

## 6. Data Transformation and Mapping

### 6.1 Destination Table: `rp_source`

| Column | Type | Source | Mapping Logic |
|--------|------|--------|---------------|
| `source_booking_id` | nvarchar | Export → `Short Time Entry ID` | Direct mapping |
| `source_system` | nvarchar | Hardcoded | Always `"Polaris"` |
| `time_code` | nvarchar | Export → TimeOff Type Name → TypeList API | Last URI segment of the matching timeoff type |
| `users_user_id` | nvarchar | Export → Employee ID → `dbo.users` | Lookup employee_id in users table to get user_id |
| `hours` | decimal | Export → `Hours (Current)` | Parsed as float |
| `work_date` | datetime | Export → `Entry Date` | Direct mapping |
| `hours_type` | nvarchar | Export → `Time Off Type Name` | `"Holiday"` if name contains "holiday" (case-insensitive), else `"Absence"` |
| `last_updated_date` | datetime | Export → `LastModifiedDate_UTC` | Direct mapping |
| `employee_id` | nvarchar | Export → `Employee ID` | Direct mapping |

### 6.2 Record Classification Logic

```
For each exported record:
    IF employee_id is empty:
        → SKIP (add to skipped records log)
    ELIF hours > 0:
        → INSERT into rp_source
    ELIF hours == 0 AND source_booking_id exists in DB:
        → DELETE from rp_source (cancelled timeoff)
    ELSE:
        → Ignore (hours == 0, not in DB)
```

### 6.3 Hours Type Classification

```
IF "holiday" in timeoff_type_name.lower():
    hours_type = "Holiday"
ELSE:
    hours_type = "Absence"
```

---

## 7. Airflow-Specific Design

### 7.1 DAG Structure

```
┌───────────────────────────────┐
│      can_run_batch_task       │
└───────────────┬───────────────┘
          ┌─────┼──────┐
          │Yes  │      │No
          ▼     │      ▼
   ┌────────┐   │   ┌──────────────────────────────────────┐
   │batch   │   │   │ PHASE 1: Data Availability Check     │
   │_task   │   │   │                                      │
   └───┬────┘   │   │ get_export_download_script            │
       │        │   │ create_row_counts_batch               │
       │        │   │ execute_row_counts_batch              │
       │        │   │ get_row_counts_results                │
       │        │   │ export_has_data?  ──No──► end_task    │
       │        │   └────────────┬─────────────────────────┘
       │        │                │ Yes
       │        │                ▼
       │        │   ┌──────────────────────────────────────┐
       │        │   │ PHASE 2: Export & Download            │
       │        │   │                                      │
       │        │   │ create_export_batch                   │
       │        │   │ execute_export_batch                  │
       │        │   │ get_export_batch_results              │
       │        │   │ has_batch_error? ──Yes──► fail_export │
       │        │   │ update_export_name                    │
       │        │   │ mark_as_completed                     │
       │        │   │ create_download_batch                 │
       │        │   │ execute_download_batch                │
       │        │   │ get_download_url                      │
       │        │   │ download_export                       │
       │        │   │ load_export                           │
       │        │   └────────────┬─────────────────────────┘
       │        │                │
       │        │      ┌────────┴────────┐
       │        │      │On Error         │On Success
       │        │      ▼                 ▼
       │        │   ┌──────────┐  ┌──────────────────────────────┐
       │        │   │Cancel    │  │ PHASE 3: Process & Write      │
       │        │   │export    │  │                                │
       │        │   │batch     │  │ create_export_collection       │
       │        │   │→ fail    │  │         │                      │
       │        │   └──────────┘  │   ┌─────┼─────┐                │
       │        │                 │   ▼     ▼     ▼                │
       │        │                 │ get_   get_  fetch_            │
       │        │                 │ timeoff exist  user_            │
       │        │                 │ _types  _db   id_map           │
       │        │                 │   │     │     │                │
       │        │                 │   └─────┼─────┘                │
       │        │                 │         ▼                      │
       │        │                 │ identify_records_to_process    │
       │        │                 │         │                      │
       │        │                 │   ┌─────┼─────┐                │
       │        │                 │   ▼     ▼     ▼                │
       │        │                 │ INSERT DELETE SKIPPED           │
       │        │                 │ path   path   path             │
       │        │                 │   │     │   email notification  │
       │        │                 └───┼─────┼──────────────────────┘
       │        │                     │     │
       ▼        ▼                     ▼     ▼
     ┌───────────────────────────────────────────┐
     │                 end_task                   │
     └───────────────────────────────────────────┘
```

### 7.1.1 Task Descriptions

#### Phase 1: Data Availability Check

| Task ID | Operator | Description |
|---------|----------|-------------|
| `get_export_download_script` | RepliconServiceOperator | Gets download script URI for "Time Off Export" |
| `create_row_counts_batch` | RepliconServiceOperator | Creates batch to count exportable rows |
| `execute_row_counts_batch` | batch_execution | Executes and waits for row count batch (5h timeout) |
| `get_row_counts_results` | RepliconServiceOperator | Gets the row count result |
| `export_has_data` | IfOperator | Checks if rowCounts[0] > 0 |

#### Phase 2: Export & Download

| Task ID | Operator | Description |
|---------|----------|-------------|
| `create_export_batch` | RepliconServiceOperator | Creates timeoff data export batch |
| `execute_export_batch` | batch_execution | Executes and waits for export (5h timeout) |
| `get_export_batch_results` | RepliconServiceOperator | Gets export results (URI + errors) |
| `has_batch_error` | IfOperator | Checks for batch errors |
| `fail_export` | FailOperator | Fails DAG on batch error |
| `update_export_name` | RepliconServiceOperator | Names the export with timestamp |
| `create_export_status_complete_batch` | RepliconServiceOperator | Marks export as complete |
| `create_download_batch` | RepliconServiceOperator | Creates download batch for the export |
| `execute_download_batch` | batch_execution | Executes and waits for download (5h timeout) |
| `get_download_url` | RepliconServiceOperator | Extracts download URL |
| `download_export` | HTTPDownloadFileOperator | Downloads the CSV file |
| `load_export` | LoadCSVFileOperator | Parses CSV into records |

#### Error Handling

| Task ID | Operator | Description |
|---------|----------|-------------|
| `catch_export_error` | EmptyOperator | Triggered on failure (trigger_rule: one_failed) |
| `create_export_status_cancel_batch` | RepliconServiceOperator | Cancels the export batch |
| `execute_cancel_batch` | batch_execution | Executes cancel batch |
| `fail_timeoff_export` | FailOperator | Fails DAG after cancellation |

#### Phase 3: Process & Write

| Task ID | Operator | Description |
|---------|----------|-------------|
| `create_export_collection` | CreateCollectionOperator | Maps CSV columns to structured collection |
| `get_timeoff_types` | RepliconServicePageOperator | Fetches all timeoff types (paginated) |
| `get_existing_db_records` | SQLExecuteQueryOperator | Gets existing booking IDs from DB |
| `fetch_user_id_map` | SQLExecuteQueryOperator | Gets user_id → employee_id map from dbo.users |
| `identify_records_to_process` | PythonOperator | Classifies records as INSERT, DELETE, or SKIP |
| `has_records_to_insert` | IfOperator | Branch for INSERT path |
| `insert_records` | PythonOperator | Multi-row INSERT (230 rows/batch) |
| `has_records_to_delete` | IfOperator | Branch for DELETE path |
| `delete_records` | PythonOperator | Row-by-row DELETE by booking ID |
| `has_skipped_records` | IfOperator | Branch for skipped records notification |
| `prepare_skipped_log` | PythonOperator | Formats skipped records for CSV |
| `render_skipped_csv` | WriteCSVFileOperator | Renders CSV file from skipped records |
| `generate_skipped_download_link` | GeneratePresignedDownloadUrlOperator | Generates 7-day presigned URL |
| `send_skipped_records_email` | EmailOperator | Sends notification with download link |

### 7.2 DAG Configuration

```
DAG_ID:   resource_planner_timeoff_export_{instance}
SCHEDULE: None (manual trigger)
CATCHUP:  False
MAX_ACTIVE_RUNS: 1
```

### 7.2.1 Instance Configuration

| Parameter | Type | Description |
|-----------|------|-------------|
| `instance` | String | Instance identifier |
| `company_key` | String | Replicon company key |
| `replicon_conn_id` | String | Airflow connection for Replicon API |
| `resource_planner_timeoff_export_enable_batch_task` | String | Airflow Variable for feature flag |
| `mssql_conn_id` | String | Airflow connection for SQL Server |
| `mssql_database` | String | Target database |
| `target_table` | String | Target table (e.g., `dbo.rp_source`) |
| `export_script_display_text` | String | Download script filter text (default: "Time Off Export") |

### 7.2.2 Report Configuration

| Report Name | Used In | Purpose |
|-------------|---------|---------|
| `Resource Planner TimeOff Booking Export` | config.py | Primary report name (reference) |
| `Resource Planner Deleted TimeOff Booking Export` | config.py | Deleted bookings report (reserved for future use) |

---

## 8. Error Handling, Observability, and Alerting

### 8.1 Error Handling Strategy

| Error Category | Examples | Handling |
|----------------|----------|----------|
| No data to export | Row count = 0 | Skip to end_task (not an error) |
| Batch execution error | Timeout, API error | Check for error in batch results, fail DAG |
| Download failure | URL expired, network error | Trigger error path → cancel export → fail DAG |
| Missing employee_id | Entry has no employee | Skip record, log, send email notification |
| Database connection | SQL Server unreachable | Retry with backoff, then fail |
| Invalid hours value | Non-numeric hours string | Default to 0.0 (treated as potential delete) |

### 8.2 Export Lifecycle Management

The pipeline carefully manages the Polaris export lifecycle:

```
Create Export → Execute → Mark Complete → Create Download → Download
                                                              │
                                        On Failure:           │
                                        Cancel Export ←───────┘
```

This ensures exports are properly closed in Polaris, preventing orphaned exports.

### 8.3 Skipped Records Notification

When records are skipped due to missing employee_id:
1. Skipped records are collected during classification
2. A CSV file is generated with columns: Short Time Entry ID, User, Timeoff Type Name, Entry Date, Hours, Reason
3. A presigned download URL is generated (valid for 7 days)
4. An email is sent with the download link

### 8.4 Transaction Management

- INSERT operations: committed atomically (multi-row batches within single connection)
- DELETE operations: committed atomically (all deletes within single connection)
- INSERT and DELETE paths run in parallel (independent operations)

---

## 9. Conclusion

This pipeline provides a reliable mechanism for synchronizing timeoff booking data from Polaris to Resource Planner. The three-phase design (check → export → process) ensures efficient use of the Polaris batch export API, while the parallel processing paths (insert, delete, skipped notification) maximize throughput.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Single DAG (no master-child) | Timeoff volume is manageable in a single execution |
| Batch export API (not report API) | TimeDataExportService handles the export lifecycle and status tracking |
| Hours-based classification | hours > 0 = active booking, hours = 0 = cancelled booking |
| Holiday vs Absence based on name | Simple keyword matching ("holiday" in type name) |
| Export cancellation on failure | Prevents orphaned exports in Polaris |
| Skipped records email | Critical for audit trail when employee IDs are missing |
| Parallel fetch (types, existing, user map) | Three independent queries run concurrently |

### Open Items

| Item | Status | Notes |
|------|--------|-------|
| Deleted bookings report | Reserved | `deleted_report_name` in config, not yet used in pipeline |
| Email recipients | Hardcoded | Currently sends to single address — should be configurable |

---

*Data Engineering Team - Resource Planner Integration*
