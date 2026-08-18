# Design Document

**Timeoff Type Master Data Export from Polaris to Resource Planner**

| Property | Value |
|----------|-------|
| Version | 1.0 |
| Date | March 13, 2026 |
| Author | Data Engineering Team |
| Pipeline Name | `resource_planner_timeoff_type_export_{instance}` |

## Summary

This document outlines the design for an automated Apache Airflow pipeline that exports **timeoff type master data** (e.g., Vacation, Sick Leave, Holiday) from Polaris to the Resource Planner database. This is a simple, single-DAG pipeline that fetches all timeoff types from the Polaris API and inserts any types not already present in the `rp_source_time_codes` table.

This pipeline maintains the **reference data** that the [timeoff_export](../timeoff_export/airflow-pipeline-design.md) pipeline depends on for time_code resolution.

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

This pipeline synchronizes the **timeoff type master data catalog** from Polaris to Resource Planner. Timeoff types define the categories of absence (Vacation, Sick Leave, Personal Day, Holiday, etc.) and are stored as time codes in the `rp_source_time_codes` table with `type = 'timeoff-type'`.

### Business Goals

- **Maintain Reference Data:** Ensure Resource Planner has the complete set of timeoff types from Polaris
- **Automate Catalog Sync:** Detect and insert new timeoff types automatically
- **Support Timeoff Export:** Provide the time_code reference data used by the timeoff booking export pipeline

### Success Criteria

- All Polaris timeoff types are represented in the database
- New timeoff types are detected and inserted automatically
- Existing types are not duplicated
- Pipeline is idempotent — safe to re-run

---

## 2. Integration Workflow

### Core Functionality

1. **Fetch all timeoff types** from Polaris via TimeOffTypeListService API (paginated)
2. **Query existing timeoff types** from database where `type = 'timeoff-type'`
3. **Identify new types** — timeoff types in Polaris but not in DB
4. **Batch insert** new types into `rp_source_time_codes`

### Data Flow

```
Polaris API                                    Database
(TimeOffTypeListService)                       (SQL Server)
     │                                           │
     ▼                                           │
┌────────────────────────┐                       │
│ Fetch all timeoff types│                       │
│ (paginated, 100/page)  │                       │
└──────────┬─────────────┘                       │
           │                                     │
           ▼                                     │
┌────────────────────────┐                       │
│ Query existing types   │◄──────────────────────┤
│ from database          │  (time_code WHERE     │
│                        │   type='timeoff-type') │
└──────────┬─────────────┘                       │
           │                                     │
           ▼                                     │
┌────────────────────────┐                       │
│ Identify new types     │                       │
│ (Polaris - DB)         │                       │
└──────────┬─────────────┘                       │
           │                                     │
           ▼                                     │
┌────────────────────────┐                       │
│ Batch insert new types │──── INSERT ──────────►│
│                        │              rp_source_time_codes
└────────────────────────┘                       │
```

---

## 3. Non-Functional Requirements

### 3.1 Scalability

- Handles any number of timeoff types via paginated API (100 per page)
- Single batch insert with transaction

### 3.2 Reliability

- Feature flag (Airflow Variable) to enable/disable
- Idempotent design — existing types are skipped
- Transactional insert (all-or-nothing)

### 3.3 Performance

- Single API call sequence (paginated) + single DB query + single batch insert
- Minimal processing — expected to complete in seconds

### 3.4 Maintainability

- Simple, linear DAG with clear task progression
- Centralized configuration

---

## 4. High-Level Architecture

### 4.1 System Components

| Component | Type | Description |
|-----------|------|-------------|
| TimeOffTypeListService | REST API | Polaris API providing timeoff type master data |
| Apache Airflow 2.x+ | Platform | Workflow orchestration |
| Resource Planner DB | SQL Server | Target database |
| `rp_source_time_codes` | SQL Table | Destination table (with `type = 'timeoff-type'`) |

### 4.2 Integration Points

| Integration | Protocol | Direction | Description |
|-------------|----------|-----------|-------------|
| Polaris → Airflow | HTTPS (REST) | Inbound | TimeOff type list API (paginated) |
| Airflow → SQL Server | ODBC | Outbound | Database reads and writes |

---

## 5. Data Sources

### 5.1 TimeOffTypeListService API

**Endpoint:** `/services/TimeOffTypeListService1.svc/GetData`

**Request:**
```json
{
  "page": "1",
  "pagesize": "100",
  "columnUris": [
    "urn:replicon:time-off-type-list-column:name",
    "urn:replicon:time-off-type-list-column:enabled"
  ],
  "sort": [
    {
      "columnUri": "urn:replicon:time-off-type-list-column:name",
      "isAscending": "true"
    }
  ],
  "filterExpression": null
}
```

**Response Structure (per row):**

| Field | Path | Description | Used For |
|-------|------|-------------|----------|
| URI | `row.cells[0].uri` | Unique timeoff type URI | `timeoff_type_id` (last segment) |
| Name | `row.cells[0].textValue` | Display name | `timeoff_type_name` |
| Enabled | `row.cells[1].textValue` | Whether type is active | `time_entry_enabled` |

**Pagination:** Continues requesting pages until a page returns fewer rows than `pagesize`.

### 5.2 Data Extraction

The `all_result_data_handler` transforms paginated API responses into structured records:

```python
{
    "timeoff_type_id": "123",           # URI last segment
    "timeoff_type_name": "Vacation",    # Display name
    "enabled": "true",                  # Active flag
    "uri": "urn:replicon:...:123"       # Full URI
}
```

---

## 6. Data Transformation and Mapping

### 6.1 Destination Table: `rp_source_time_codes`

| Column | Type | Source | Mapping Logic |
|--------|------|--------|---------------|
| `source_system` | nvarchar | Hardcoded | Always `"Polaris"` |
| `parent_time_code` | nvarchar | API → URI | `timeoff_type_id` (URI last segment) |
| `time_code` | nvarchar | API → URI | `timeoff_type_id` (same as parent_time_code) |
| `time_code_name` | nvarchar | API → Name | `timeoff_type_name` |
| `parent_time_code_name` | nvarchar | API → Name | `timeoff_type_name` (same as time_code_name) |
| `project_manager_id` | nvarchar | N/A | `NULL` (not applicable for timeoff types) |
| `type` | nvarchar | Hardcoded | Always `"timeoff-type"` |
| `task_level` | int | Hardcoded | Always `0` |
| `time_entry_enabled` | bit | API → Enabled | `1` if `enabled = 'true'`, else `0` |

### 6.2 Key Behavior

- `parent_time_code` = `time_code` = `timeoff_type_id` (flat hierarchy, no parent-child)
- `time_code_name` = `parent_time_code_name` = `timeoff_type_name` (same name at both levels)
- `project_manager_id` is always NULL (no manager for timeoff types)
- `task_level` is always 0 (no hierarchy depth)

---

## 7. Airflow-Specific Design

### 7.1 DAG Structure

```
┌───────────────────────────────┐
│      can_run_batch_task       │
│   (Check Airflow Variable)   │
└───────────────┬───────────────┘
          ┌─────┼──────┐
          │Yes  │      │No
          ▼     │      ▼
   ┌────────┐   │   ┌──────────────────────────┐
   │batch   │   │   │ get_all_timeoff_types     │
   │_task   │   │   │ (Paginated API call)      │
   └───┬────┘   │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ get_existing_timeoff_types│
       │        │   │ (SQL: WHERE type =        │
       │        │   │  'timeoff-type')          │
       │        │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ identify_timeoff_types    │
       │        │   │ _to_add                   │
       │        │   │ (Filter: not in DB)       │
       │        │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ has_any_timeoff_types     │
       │        │   │ _to_add?                  │
       │        │   └─────┬──────────┬─────────┘
       │        │         │Yes       │No
       │        │         ▼          │
       │        │   ┌──────────────┐ │
       │        │   │ insert       │ │
       │        │   │ _timeoff     │ │
       │        │   │ _types       │ │
       │        │   │ (executemany)│ │
       │        │   └──────┬───────┘ │
       │        │          │         │
       ▼        ▼          ▼         ▼
     ┌───────────────────────────────────┐
     │            end_task               │
     └───────────────────────────────────┘
```

### 7.1.1 Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `can_run_batch_task` | IfOperator | Feature flag check via Airflow Variable |
| `batch_task` | BatchTaskRunOperator | Error boundary wrapper |
| `get_all_timeoff_types` | RepliconServicePageOperator | Fetches all timeoff types from Polaris (paginated, 100/page, sorted by name) |
| `get_existing_timeoff_types` | SQLExecuteQueryOperator | Queries existing timeoff type IDs from DB (`WHERE type = 'timeoff-type'`) |
| `identify_timeoff_types_to_add` | PythonOperator | Filters Polaris types to only those not in DB |
| `has_any_timeoff_types_to_add` | IfOperator | Conditional branch based on new types count |
| `insert_timeoff_types` | PythonOperator | Batch insert using `cursor.executemany()` |
| `end_task` | EmptyOperator | DAG completion marker |

### 7.2 DAG Configuration

```
DAG_ID:   resource_planner_timeoff_type_export_{instance}
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
| `resource_planner_timeoff_type_export_enable_batch_task` | String | Airflow Variable for feature flag |
| `mssql_conn_id` | String | Airflow connection for SQL Server |
| `mssql_database` | String | Target database |
| `target_table` | String | Target table (e.g., `dbo.rp_source_time_codes`) |

---

## 8. Error Handling, Observability, and Alerting

### 8.1 Error Handling Strategy

| Error Category | Examples | Handling |
|----------------|----------|----------|
| API failure | Polaris API down, timeout | Retry with backoff, then fail DAG |
| No new types | All types already in DB | Skip to end_task (not an error) |
| Database connection | SQL Server unreachable | Retry with backoff, then fail |
| Insert failure | Constraint violation | Transaction rollback, fail DAG |

### 8.2 Transaction Management

- Uses `cursor.executemany()` for batch insert
- Single `conn.commit()` at the end — all-or-nothing
- If any insert fails, the entire batch is rolled back

### 8.3 Idempotent Design

- Safe to re-run at any time
- Existing types are identified by `time_code` and excluded from insertion
- No update or delete operations — insert-only

---

## 9. Conclusion

This pipeline provides a straightforward mechanism for keeping the timeoff type catalog in sync between Polaris and Resource Planner. Its simplicity reflects the nature of the data — timeoff types are reference data that changes infrequently.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Single DAG (no master-child) | Timeoff types are few in number (typically < 50) |
| Insert-only (no update/delete) | Types rarely change once created; manual intervention for renames/deletions |
| `executemany()` for insert | Simpler than multi-row INSERT for small volumes |
| Sorted by name | Deterministic ordering for consistent processing |
| Flat hierarchy (parent = child) | Timeoff types have no parent-child relationship |

### Relationship to Other Pipelines

- **timeoff_export** depends on this pipeline's data for `time_code` resolution
- Shares the `rp_source_time_codes` table with **project_task_export_bulk** and **project_task_export_delta** (but uses `type = 'timeoff-type'` vs `type = 'project'/'task'`)

---

*Data Engineering Team - Resource Planner Integration*
