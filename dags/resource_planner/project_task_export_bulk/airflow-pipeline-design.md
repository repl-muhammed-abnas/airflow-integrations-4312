# Design Document

**Project Task Export (Bulk) from Polaris to Resource Planner**

| Property | Value |
|----------|-------|
| Version | 1.0 |
| Date | March 13, 2026 |
| Author | Data Engineering Team |
| Pipeline Name | `resource_planner_project_task_export_bulk_{instance}` |

## Summary

This document outlines the design for an automated Apache Airflow pipeline that performs a **full bulk export** of all projects and tasks from Polaris to the Resource Planner database. The pipeline uses a **master-child DAG architecture** to process projects in parallel batches, inserting new project and task records into the `rp_source_time_codes` table.

This is a full-refresh pipeline — it pulls all projects and tasks from Polaris on every run and inserts any records not already present in the database. It does **not** handle updates or deletes (see the [delta pipeline](../project_task_export_delta/airflow-pipeline-design.md) for incremental sync).

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

This pipeline synchronizes **project and task master data** from Polaris to the Resource Planner database. Projects and tasks form the time code hierarchy used for resource allocation tracking. Each project becomes a parent time code, and each task becomes a child time code under its project.

### Business Goals

- **Automate Time Code Sync:** Replace manual data extraction with an automated pipeline
- **Maintain Time Code Hierarchy:** Ensure Resource Planner always has the complete set of projects and tasks from Polaris
- **Parallel Processing:** Process projects in batches to minimize total execution time
- **Track Time Entry Eligibility:** Flag tasks that have time entry enabled in Polaris

### Success Criteria

- All projects and tasks from Polaris are represented in the Resource Planner database
- New projects and tasks are detected and inserted automatically
- Zero data loss or corruption during transfer
- Execution completes within acceptable time window

---

## 2. Integration Workflow

### Core Functionality

1. **Fetch Project URI Report** from Polaris — provides list of all projects with their URIs and project managers
2. **Fetch Users Report** from Polaris — provides user names and employee IDs for project manager resolution
3. **Create project batches** — index projects and divide into batches of 20 (configurable)
4. **Map project managers** to employee IDs using the users report
5. **Trigger one child DAG per batch** — each child handles a subset of projects
6. **Child: Fetch Project Task Report** for the batch's projects (filtered by project URI)
7. **Child: Fetch time-entry-enabled tasks** from Polaris TaskListService API
8. **Child: Query existing records** from the database to identify what's already synced
9. **Child: Insert new records** — only projects/tasks not already in the database
10. **Wait for all child DAGs** to complete

### Data Flow

```
Polaris Reports                  Polaris API                   Database
(Project URI + Users)            (TaskListService)             (SQL Server)
     │                                │                           │
     ▼                                │                           │
┌──────────────┐                      │                           │
│ Master DAG   │                      │                           │
│              │                      │                           │
│ 1. Download  │                      │                           │
│    Project   │                      │                           │
│    URI &     │                      │                           │
│    Users     │                      │                           │
│    Reports   │                      │                           │
│              │                      │                           │
│ 2. Build     │                      │                           │
│    manager   │                      │                           │
│    → empID   │                      │                           │
│    map       │                      │                           │
│              │                      │                           │
│ 3. Batch     │                      │                           │
│    projects  │                      │                           │
│    (size=20) │                      │                           │
│              │                      │                           │
│ 4. Trigger   │                      │                           │
│    children  │                      │                           │
└──────┬───────┘                      │                           │
       │                              │                           │
       ▼ (per batch)                  │                           │
┌──────────────┐                      │                           │
│ Child DAG    │                      │                           │
│              │                      │                           │
│ 5. Run task  │                      │                           │
│    report    │                      │                           │
│    (filtered)│                      │                           │
│              │                      │                           │
│ 6. Fetch     │── TaskListService ──►│                           │
│    time      │◄── enabled tasks ────┤                           │
│    entry     │                      │                           │
│    tasks     │                      │                           │
│              │                      │                           │
│ 7. Query     │──────────────────────┼──────────────────────────►│
│    existing  │◄─────────────────────┼───────────────────────────┤
│    records   │  (time_code, type)   │                           │
│              │                      │                           │
│ 8. Insert    │──────────────────────┼──────────────────────────►│
│    new       │  INSERT new records  │                  rp_source_time_codes
│    records   │                      │                           │
└──────────────┘                      │                           │
```

---

## 3. Non-Functional Requirements

### 3.1 Scalability

- Master-child architecture scales horizontally — more projects = more child DAG runs
- Configurable batch size (default: 20 projects per child)
- Multi-row INSERT with MSSQL 2100 parameter limit awareness (230 rows per batch)

### 3.2 Reliability

- Feature flag (Airflow Variable) to enable/disable the pipeline
- Fault isolation per batch — if one child fails, others still complete
- Transactional database writes (all-or-nothing per child)

### 3.3 Performance

- Parallel batch processing via concurrent child DAGs (max 20 active)
- Only inserts new records — skips existing ones
- Multi-row INSERT for speed

### 3.4 Maintainability

- Centralized configuration via `config.py` and instance files
- Modular design with separate master and child DAGs

---

## 4. High-Level Architecture

### 4.1 System Components

| Component | Type | Description |
|-----------|------|-------------|
| Polaris Report API | REST API | Provides project URI, project task, and users data via CSV reports |
| Polaris TaskListService API | REST API | Provides time-entry-enabled task list |
| Apache Airflow 2.x+ | Platform | Workflow orchestration |
| Resource Planner DB | SQL Server | Target database for time code data |
| `rp_source_time_codes` | SQL Table | Destination table (dev: `dbo.rp_source_time_codes`, default: `dbo.dummy_rp_source_time_codes`) |

### 4.2 Integration Points

| Integration | Protocol | Direction | Description |
|-------------|----------|-----------|-------------|
| Polaris → Airflow | HTTPS (REST) | Inbound | Report API calls returning CSV data |
| Polaris → Airflow | HTTPS (REST) | Inbound | TaskListService API for time entry flags |
| Airflow → SQL Server | ODBC | Outbound | Database reads and writes |

---

## 5. Data Sources

### 5.1 Resource Planner Project URI Report

| Column | Description | Used For |
|--------|-------------|----------|
| `project_uri` | Unique project identifier (URN format) | Project ID extraction, child DAG filtering |
| `Project Manager` | Project manager's user name | Manager → employee ID resolution |

### 5.2 Resource Planner Project Task Report

| Column | Description | Used For |
|--------|-------------|----------|
| `Project Name` | Human-readable project name | `time_code_name`, `parent_time_code_name` |
| `Project Code` | Project code identifier | Reference |
| `project_uri` | Unique project identifier | `parent_time_code` extraction |
| `Task Name (Full Path)` | Full task hierarchy path (separated by ` / `) | `time_code_name`, `task_level` calculation |
| `Task Code` | Task code identifier | Reference |
| `task_uri` | Unique task identifier | `time_code` generation, time entry lookup |
| `Project Manager` | Project manager name | `project_manager_id` resolution |

### 5.3 Resource Planner Users Report

| Column | Description | Used For |
|--------|-------------|----------|
| `User Name` | User's display name | Project manager matching |
| `Employee ID` | Employee identifier | `project_manager_id` value |
| `User Status` | User status | Reference |

### 5.4 TaskListService API

**Endpoint:** `/services/TaskListService1.svc/GetData`

Provides a paginated list of tasks filtered by project URIs and time-entry-allowed flag. Returns task URIs that have time entry enabled.

---

## 6. Data Transformation and Mapping

### 6.1 Destination Table: `rp_source_time_codes`

| Column | Type | Source | Mapping Logic |
|--------|------|--------|---------------|
| `source_system` | nvarchar | Hardcoded | Always `"Polaris"` |
| `parent_time_code` | nvarchar | Report → `project_uri` | Last segment of project URI (e.g., `...project:2974` → `2974`) |
| `time_code` | nvarchar | Report | **Project:** same as `parent_time_code`; **Task:** `{project_id}~{task_id}` |
| `time_code_name` | nvarchar(255) | Report | **Project:** project name (truncated to 255); **Task:** `{project_name}~{task_path}` where `/` → `~` |
| `parent_time_code_name` | nvarchar | Report → `Project Name` | Project name as-is |
| `project_manager_id` | nvarchar | Report → Users lookup | Employee ID of the project manager (resolved via users report) |
| `type` | nvarchar | Derived | `"project"` or `"task"` |
| `task_level` | int | Derived | **Project:** `0`; **Task:** count of segments in `Task Name (Full Path)` split by ` / ` |
| `time_entry_enabled` | bit | TaskListService API | `True` if task URI appears in time-entry-enabled task list |

### 6.2 Record Type Detection

- **Project record:** Row from the task report where `task_uri` is empty — represents the project itself
- **Task record:** Row from the task report where `task_uri` is present — represents a task under the project

### 6.3 Time Code Generation

```
Project: time_code = "2974"                     (project_uri last segment)
Task:    time_code = "2974~161481"              (project_id ~ task_id)
```

### 6.4 Task Name Transformation

```
Raw:     "Phase 1 / Design / UI Mockups"
Display: "My Project~Phase 1~Design~UI Mockups"   (project_name ~ path with / → ~)
Level:   3                                          (count of segments)
```

---

## 7. Airflow-Specific Design

### 7.1 DAG Overview

| DAG | ID Pattern | Purpose |
|-----|------------|---------|
| **Master** | `resource_planner_project_task_export_bulk_{instance}` | Downloads reports, builds manager map, triggers child DAGs per batch |
| **Child** | `resource_planner_project_task_export_bulk_child_{instance}` | Processes a batch of projects — fetches tasks, checks existing, inserts new |

### 7.2 Master DAG Structure

```
┌───────────────────────────────┐
│      can_run_batch_task       │
│   (Check Airflow Variable)   │
└───────────────┬───────────────┘
          ┌─────┼──────┐
          │ Yes │      │ No
          ▼     │      ▼
   ┌────────┐   │   ┌──────────────────────────┐
   │batch   │   │   │ get_project_uri_report    │
   │_task   │   │   └────────────┬─────────────┘
   └───┬────┘   │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ run_project_uri_report    │
       │        │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ report_has_data?          │──No──►end_task
       │        │   └────────────┬─────────────┘
       │        │                │ Yes
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ load_report_data          │
       │        │   │ get_user_report           │
       │        │   │ run_user_report           │
       │        │   │ load_user_report_data     │
       │        │   │ create_user_collection    │
       │        │   │ create_project_uri_coll   │
       │        │   │ create_indexed_table      │
       │        │   │ get_project_manager_id    │
       │        │   │ create_artifact_for_batch │
       │        │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ trigger_batch_processing  │
       │        │   │ (1 child per batch of 20)│
       │        │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ wait_for_completion       │
       │        │   └────────────┬─────────────┘
       │        │                │
       ▼        ▼                ▼
     ┌───────────────────────────────┐
     │           end_task            │
     └───────────────────────────────┘
```

### 7.2.1 Master DAG — Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `can_run_batch_task` | IfOperator | Checks Airflow Variable to enable/disable the pipeline |
| `batch_task` | BatchTaskRunOperator | Error boundary wrapper |
| `get_project_uri_report` | RepliconReportDetailsOperator | Gets URI for "Resource Planner Project URI Report" |
| `run_project_uri_report` | run_report2 | Executes the Project URI report and retrieves CSV |
| `report_has_data` | IfOperator | Checks if report returned data |
| `load_report_data` | LoadCSVFileOperator | Parses Project URI report CSV |
| `get_user_report` | RepliconReportDetailsOperator | Gets URI for "Resource Planner Users Report" |
| `run_user_report` | run_report2 | Executes the Users report |
| `load_user_report_data` | LoadCSVFileOperator | Parses Users report CSV |
| `create_user_collection` | CreateCollectionOperator | In-memory collection of user names and employee IDs |
| `create_project_uri_collection` | CreateCollectionOperator | In-memory collection of project URIs and managers |
| `create_indexed_table` | QueryCollectionOperator | Assigns row numbers to projects for batch slicing |
| `get_project_manager_employee_id` | QueryCollectionOperator | Joins user collection to resolve manager employee IDs |
| `create_artifact_for_batch` | PythonOperator | Writes manager → employee ID map as JSON artifact |
| `trigger_batch_processing_for_project_tasks` | TriggerDagRunForEachItemOperator | Triggers one child DAG per batch |
| `wait_for_completion_of_batch_tasks` | WaitForDagRunsSensor | Waits for all child DAGs to complete |
| `end_task` | EmptyOperator | DAG completion marker |

### 7.2.2 Data Passed to Each Child DAG

```json
{
  "instance": "dev",
  "batch_index": 0,
  "start_index": 1,
  "end_index": 20,
  "batch_size": 20,
  "user_name_id_map": "<json_artifact_name>"
}
```

### 7.3 Child DAG Structure

```
┌──────────────────────────────────┐
│     get_batch_project_details    │
│  (Query projects by row_index    │
│   range from parent's indexed    │
│   table)                         │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     get_project_task_report      │
│     run_project_task_report      │
│  (Filtered by batch project URIs)│
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     report_has_data?             │──No──►end_task
└──────────────┬───────────────────┘
               │ Yes
               ▼
┌──────────────────────────────────┐
│     load & create collection     │
└──────────────┬───────────────────┘
               │
         ┌─────┴─────┐
         ▼           ▼
┌────────────┐ ┌──────────────────┐
│ fetch_time │ │ get_existing     │
│ _entry_    │ │ _time_codes      │
│ enabled    │ │ (SQL query)      │
│ _tasks     │ └────────┬─────────┘
│ (API)      │          │
└──────┬─────┘          │
       └────────┬───────┘
                ▼
┌──────────────────────────────────┐
│     identify_records_to_add      │
│  (Compare report vs DB, build    │
│   insert list)                   │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     has_records_to_add?          │──No──►end_task
└──────────────┬───────────────────┘
               │ Yes
               ▼
┌──────────────────────────────────┐
│     insert_records               │
│  (Multi-row INSERT, 230 rows/    │
│   batch, all-or-nothing commit)  │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│           end_task               │
└──────────────────────────────────┘
```

### 7.3.1 Child DAG — Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `view_dag_run_conf` | ViewDagRunConfOperator | Logs the DAG run configuration for debugging |
| `can_run_batch_task` | IfOperator | Feature flag check |
| `batch_task` | BatchTaskRunOperator | Error boundary wrapper |
| `get_batch_project_details` | QueryCollectionOperator | Queries parent's indexed table for this batch's project URIs |
| `get_project_task_report` | RepliconReportDetailsOperator | Gets URI for "Resource Planner Project Task Report" |
| `run_project_task_report` | run_report2 | Runs task report filtered to batch project URIs |
| `report_has_data` | IfOperator | Checks if filtered report returned data |
| `load_report_data` | LoadCSVFileOperator | Parses task report CSV |
| `create_project_task_collection` | CreateCollectionOperator | In-memory collection of project/task data |
| `fetch_time_entry_enabled_tasks` | RepliconServicePageOperator | Fetches task URIs with time entry enabled via TaskListService API |
| `get_existing_time_codes` | SQLExecuteQueryOperator | Queries existing (time_code, type) pairs from DB |
| `identify_records_to_add` | PythonOperator | Compares report data vs DB, builds list of new records |
| `has_records_to_add` | IfOperator | Conditional branch based on new records count |
| `insert_records` | PythonOperator | Multi-row INSERT into target table (230 rows/batch) |
| `end_task` | EmptyOperator | DAG completion marker |

### 7.4 DAG Configuration

```
MASTER DAG_ID:  resource_planner_project_task_export_bulk_{instance}
CHILD DAG_ID:   resource_planner_project_task_export_bulk_child_{instance}
SCHEDULE:       None (manual trigger)
CATCHUP:        False
MAX_ACTIVE_RUNS: 1 (master), 10-20 (child)
```

### 7.4.1 Instance Configuration

| Parameter | Type | Description |
|-----------|------|-------------|
| `instance` | String | Instance identifier (e.g., `dev`, `dev2`) |
| `company_key` | String | Replicon company key |
| `replicon_conn_id` | String | Airflow connection ID for Replicon API |
| `resource_planner_project_task_export_enable_batch_task` | String | Airflow Variable name for feature flag |
| `mssql_conn_id` | String | Airflow connection ID for SQL Server |
| `mssql_database` | String | Target database name |
| `target_table` | String | Target table (e.g., `dbo.rp_source_time_codes`) |
| `BATCH_SIZE` | Integer | Projects per child DAG batch (default: 20) |
| `INSERT_BATCH_SIZE` | Integer | Rows per INSERT statement (default: 500, actual: 230 due to MSSQL limit) |

### 7.4.2 Report Configuration

| Report Name | Used In | Purpose |
|-------------|---------|---------|
| `Resource Planner Project URI Report` | Master DAG | List of all projects with URIs and managers |
| `Resource Planner Project Task Report` | Child DAG | Project and task details (filtered by project) |
| `Resource Planner Users Report` | Master DAG | User names to employee ID mapping |

---

## 8. Error Handling, Observability, and Alerting

### 8.1 Error Handling Strategy

| Error Category | Examples | Handling |
|----------------|----------|----------|
| Report API failure | Empty response, malformed CSV | Check `report_has_data`, skip to end_task if no data |
| TaskListService failure | API timeout | Retry with backoff, then fail child DAG |
| Database connection | SQL Server unreachable | Retry with backoff, then fail |
| Insert failure | Constraint violation | Transaction rollback, fail child DAG |
| No tasks for batch | Report returns empty for project filter | Skip to end_task |

### 8.2 Transaction Management

- All INSERTs within a child DAG are committed atomically
- Multi-row INSERT batches are executed within a single connection; final `conn.commit()` ensures all-or-nothing
- MSSQL parameter limit enforced: 9 columns x 230 rows = 2,070 params (limit: 2,100)

### 8.3 Idempotent Design

- Safe to re-run — existing records are skipped based on `(time_code, type)` key
- No update or delete operations — only inserts

---

## 9. Conclusion

This pipeline provides a reliable full-refresh mechanism for synchronizing project and task time codes from Polaris to Resource Planner. The master-child architecture enables parallel batch processing, while the insert-only design ensures idempotent, safe re-execution.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Master-child architecture (batch of 20 projects) | Parallelism and fault isolation |
| Insert-only (no update/delete) | Bulk load for initial population; delta pipeline handles changes |
| Multi-row INSERT (230 rows/batch) | Speed optimization within MSSQL 2100 parameter limit |
| TaskListService API for time entry flags | Report API does not expose this field |
| Manager → employee ID via JSON artifact | Avoids re-querying for every child DAG |

### Relationship to Delta Pipeline

This bulk pipeline is designed for **initial population** of the time codes table. For ongoing incremental synchronization (inserts, updates, and deletes), use the [project_task_export_delta](../project_task_export_delta/airflow-pipeline-design.md) pipeline, which uses the audit report to detect changes.

---

*Data Engineering Team - Resource Planner Integration*
