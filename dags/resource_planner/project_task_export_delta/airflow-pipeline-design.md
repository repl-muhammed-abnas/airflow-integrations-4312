# Design Document

**Project Task Export (Delta) from Polaris to Resource Planner**

| Property | Value |
|----------|-------|
| Version | 1.0 |
| Date | March 13, 2026 |
| Author | Data Engineering Team |
| Pipeline Name | `resource_planner_project_task_export_delta_{instance}` |

## Summary

This document outlines the design for an automated Apache Airflow pipeline that performs **incremental (delta) synchronization** of project and task data from Polaris to the Resource Planner database. Unlike the [bulk pipeline](../project_task_export_bulk/airflow-pipeline-design.md) which only inserts new records, this pipeline detects **inserts, updates, and deletes** by leveraging the Polaris Audit Report.

The pipeline uses a **master-child DAG architecture** with three child DAG types:
- **Upsert child** — inserts new and updates changed project/task records
- **Delete child** — removes projects/tasks that were deleted in Polaris

The audit report provides an `Action` column that identifies which projects have been created, modified, or deleted since the last export.

---

## Table of Contents

1. [Overview and Goals](#1-overview-and-goals)
2. [Integration Workflow](#2-integration-workflow)
3. [Non-Functional Requirements](#3-non-functional-requirements)
4. [High-Level Architecture](#4-high-level-architecture)
5. [Data Sources](#5-data-sources)
6. [Data Transformation and Mapping](#6-data-transformation-and-mapping)
7. [Delta Detection Strategy](#7-delta-detection-strategy)
8. [Airflow-Specific Design](#8-airflow-specific-design)
9. [Error Handling, Observability, and Alerting](#9-error-handling-observability-and-alerting)
10. [Conclusion](#10-conclusion)

---

## 1. Overview and Goals

### Purpose

This pipeline provides **incremental synchronization** of project and task time code data from Polaris to the Resource Planner database. It complements the bulk pipeline by handling ongoing changes — new projects, renamed tasks, updated project managers, and deleted projects/tasks.

### Business Goals

- **Real-Time Accuracy:** Keep Resource Planner time codes in sync with Polaris changes
- **Efficient Processing:** Only process records that have actually changed (inserts, updates, deletes)
- **Data Consistency:** Handle all change types including project/task deletion
- **Parallel Execution:** Process multiple project batches concurrently

### Success Criteria

- All changes from the audit report are reflected in the database
- Deleted projects/tasks are removed from the database
- Updated fields (name, manager, time entry flag) are correctly updated
- Zero data loss or corruption during transfer

---

## 2. Integration Workflow

### Core Functionality

1. **Fetch Project URI Audit Report** — contains `Action` and `Field` columns identifying changes
2. **Fetch Users Report** — for project manager → employee ID resolution
3. **Identify delete operations** — separate projects with `Action = delete`
4. **Process deletes first** (if any):
   a. Trigger delete child DAGs in batches
   b. Wait for delete completion before proceeding to upserts
5. **Process upserts** — for remaining non-delete projects:
   a. Map project managers to employee IDs
   b. Trigger upsert child DAGs in batches
6. **Upsert child:** Fetch task report, compare with existing DB records, INSERT new and UPDATE changed
7. **Delete child:** Handle two deletion paths:
   - **URI-based delete:** Project still partially exists — compare report vs DB, delete missing tasks
   - **Name-based delete:** Project fully deleted — remove all records by project name prefix

### Data Flow

```
Polaris Audit Report              Polaris Task Report           Database
(Action: create/update/delete)    (Current task state)          (SQL Server)
     │                                │                           │
     ▼                                │                           │
┌──────────────┐                      │                           │
│ Master DAG   │                      │                           │
│              │                      │                           │
│ 1. Download  │                      │                           │
│    Audit +   │                      │                           │
│    Users     │                      │                           │
│    Reports   │                      │                           │
│              │                      │                           │
│ 2. Separate  │                      │                           │
│    deletes   │                      │                           │
│    from      │                      │                           │
│    upserts   │                      │                           │
│              │                      │                           │
│ 3. Process   │                      │                           │
│    deletes   │─── delete children ──┼──── DELETE ──────────────►│
│    first     │                      │                           │
│              │                      │                           │
│ 4. Process   │─── upsert children ──┼──── INSERT / UPDATE ────►│
│    upserts   │                      │                           │
└──────────────┘                      │                           │
```

---

## 3. Non-Functional Requirements

### 3.1 Scalability

- Master-child architecture scales horizontally
- Configurable batch size (default: 20 projects per child)
- Separate child DAGs for deletes vs upserts

### 3.2 Reliability

- Delete operations processed before upserts to prevent conflicts
- Feature flag (Airflow Variable) to enable/disable
- Fault isolation per batch

### 3.3 Performance

- Parallel batch processing via concurrent child DAGs (max 10 active)
- Only processes changed records (delta approach)
- Targeted SQL queries filtered to batch projects only

### 3.4 Maintainability

- Centralized configuration via `config.py` and instance files
- Modular design with three separate DAGs (master, upsert child, delete child)

---

## 4. High-Level Architecture

### 4.1 System Components

| Component | Type | Description |
|-----------|------|-------------|
| Polaris Report API | REST API | Provides audit report, task report, and users data via CSV |
| Polaris TaskListService API | REST API | Provides time-entry-enabled task list |
| Apache Airflow 2.x+ | Platform | Workflow orchestration |
| Resource Planner DB | SQL Server | Target database for time code data |
| `rp_source_time_codes` | SQL Table | Destination table |

### 4.2 DAG Architecture

| DAG | ID Pattern | Purpose |
|-----|------------|---------|
| **Master** | `resource_planner_project_task_export_delta_{instance}` | Orchestration — fetches audit report, routes to delete/upsert children |
| **Upsert Child** | `resource_planner_project_task_export_delta_child_{instance}` | Inserts new and updates changed project/task records |
| **Delete Child** | `resource_planner_project_task_export_delta_delete_child_{instance}` | Handles project/task deletion |

---

## 5. Data Sources

### 5.1 Resource Planner Project URI Audit Report

This is the **key differentiator** from the bulk pipeline. The audit report includes `Action` and `Field` columns.

| Column | Description | Used For |
|--------|-------------|----------|
| `ProjectUri` | Unique project identifier | Project ID extraction |
| `Project Name` | Human-readable project name | Name-based deletion, display |
| `Project Manager` | Project manager's user name | Manager resolution |
| `Action` | Change type: `create`, `update`, `delete` | Routing to upsert vs delete paths |
| `Field` | Which field was modified | Reference / debugging |

### 5.2 Resource Planner Project Task Report

Same report as the bulk pipeline — provides current state of project tasks.

### 5.3 Resource Planner Users Report

Same report as the bulk pipeline — provides user name → employee ID mapping.

---

## 6. Data Transformation and Mapping

### 6.1 Destination Table: `rp_source_time_codes`

Same schema as the bulk pipeline:

| Column | Type | Mapping |
|--------|------|---------|
| `source_system` | nvarchar | Always `"Polaris"` |
| `parent_time_code` | nvarchar | Project URI last segment |
| `time_code` | nvarchar | **Project:** project ID; **Task:** `{project_id}~{task_id}` |
| `time_code_name` | nvarchar(255) | **Project:** project name; **Task:** `{project_name}~{task_path}` |
| `parent_time_code_name` | nvarchar | Project name |
| `project_manager_id` | nvarchar | Employee ID of project manager |
| `type` | nvarchar | `"project"` or `"task"` |
| `task_level` | int | **Project:** 0; **Task:** path segment count |
| `time_entry_enabled` | bit | From TaskListService API |

### 6.2 Operations

| Operation | Condition | Action |
|-----------|-----------|--------|
| **INSERT** | `(time_code, type)` not in DB | Insert full record |
| **UPDATE** | `(time_code, type)` exists but fields differ | Update mutable fields |
| **DELETE** | Record in DB but not in current report (for audited projects) | Delete by `(time_code, type)` |

### 6.3 Mutable Fields (Compared for Updates)

- `time_code_name` — project/task name may change
- `parent_time_code_name` — project name may change
- `project_manager_id` — manager may be reassigned
- `task_level` — task hierarchy may change
- `time_entry_enabled` — flag may be toggled

---

## 7. Delta Detection Strategy

### 7.1 Audit Report-Based Detection

The Polaris Audit Report provides a change log with `Action` values:

| Action | Meaning | Pipeline Handling |
|--------|---------|-------------------|
| `create` | New project created | Route to upsert child → INSERT |
| `update` | Project/task modified | Route to upsert child → compare & UPDATE/INSERT |
| `delete` | Project/task deleted | Route to delete child → DELETE |

### 7.2 Delete Processing (Priority)

Deletes are processed **before** upserts to prevent conflicts. The delete child handles two scenarios:

**Scenario 1: URI-Based Delete** (project still partially exists in Polaris)
- The project URI is still valid in Polaris
- Run the task report filtered to this project
- Compare report results against DB records
- Delete DB records not present in the report (removed tasks)
- If the report returns no data for the URI, delete ALL records for that project

**Scenario 2: Name-Based Delete** (project fully deleted from Polaris)
- The project no longer exists in Polaris — no URI available
- Delete all records where `parent_time_code_name LIKE '{project_name}%'`

### 7.3 Upsert Processing

For non-delete projects from the audit report:
1. Fetch current task report (filtered to batch projects)
2. Query existing DB records for these projects
3. Compare each report record against DB:
   - Not in DB → INSERT
   - In DB but fields differ → UPDATE
   - In DB and unchanged → SKIP

---

## 8. Airflow-Specific Design

### 8.1 Master DAG Structure

```
┌───────────────────────────────┐
│      can_run_batch_task       │
└───────────────┬───────────────┘
          ┌─────┼──────┐
          │Yes  │      │No
          ▼     │      ▼
   ┌────────┐   │   ┌──────────────────────────┐
   │batch   │   │   │ get_project_uri_report    │  (Audit Report)
   │_task   │   │   │ run_project_uri_report    │
   └───┬────┘   │   │ report_has_data?          │──No──►end_task
       │        │   └────────────┬─────────────┘
       │        │                │ Yes
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ load_report_data          │
       │        │   │ Fetch Users Report        │
       │        │   │ create_project_uri_coll   │
       │        │   │ identify_deletes          │
       │        │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ has_deletes_to_process?   │
       │        │   └─────┬──────────┬─────────┘
       │        │         │Yes       │No
       │        │         ▼          │
       │        │   ┌──────────────┐ │
       │        │   │ Trigger      │ │
       │        │   │ delete child │ │
       │        │   │ DAGs         │ │
       │        │   │              │ │
       │        │   │ Wait for     │ │
       │        │   │ completion   │ │
       │        │   └──────┬───────┘ │
       │        │          │         │
       │        │          ▼         ▼
       │        │   ┌──────────────────────────┐
       │        │   │ create_indexed_table      │  (Non-delete projects only)
       │        │   └────────────┬─────────────┘
       │        │                │
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ has_upserts_to_process?   │──No──►end_task
       │        │   └────────────┬─────────────┘
       │        │                │ Yes
       │        │                ▼
       │        │   ┌──────────────────────────┐
       │        │   │ Resolve manager IDs       │
       │        │   │ Trigger upsert child DAGs │
       │        │   │ Wait for completion        │
       │        │   └────────────┬─────────────┘
       │        │                │
       ▼        ▼                ▼
     ┌───────────────────────────────┐
     │           end_task            │
     └───────────────────────────────┘
```

### 8.1.1 Master DAG — Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `can_run_batch_task` | IfOperator | Feature flag check |
| `batch_task` | BatchTaskRunOperator | Error boundary wrapper |
| `get_project_uri_report` | RepliconReportDetailsOperator | Gets URI for "Resource Planner Project URI Audit Report" |
| `run_project_uri_report` | run_report2 | Executes the Audit Report |
| `report_has_data` | IfOperator | Checks if report returned data |
| `load_report_data` | LoadCSVFileOperator | Parses audit report CSV |
| `get_user_report` | RepliconReportDetailsOperator | Gets URI for "Resource Planner Users Report" |
| `run_user_report` | run_report2 | Executes users report |
| `create_project_uri_collection` | CreateCollectionOperator | Collection with Action/Field columns |
| `identify_deletes` | PythonOperator | Counts distinct delete operations from audit |
| `has_deletes_to_process` | IfOperator | Routes to delete path if deletes exist |
| `create_delete_indexed_table` | QueryCollectionOperator | Indexes delete projects for batching |
| `trigger_delete_processing` | TriggerDagRunForEachItemOperator | Triggers delete child per batch |
| `wait_for_delete_completion` | WaitForDagRunsSensor | Waits for all delete children |
| `create_indexed_table` | QueryCollectionOperator | Indexes non-delete projects (excluding delete URIs) |
| `has_upserts_to_process` | IfOperator | Checks if upsert projects exist |
| `get_project_manager_employee_id` | QueryCollectionOperator | Resolves manager employee IDs |
| `create_artifact_for_batch` | PythonOperator | Writes manager map as JSON artifact |
| `trigger_batch_processing_for_project_tasks` | TriggerDagRunForEachItemOperator | Triggers upsert child per batch |
| `wait_for_completion_of_batch_tasks` | WaitForDagRunsSensor | Waits for all upsert children |
| `end_task` | EmptyOperator | DAG completion marker |

### 8.2 Upsert Child DAG Structure

```
┌──────────────────────────────────┐
│     get_batch_project_details    │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     Fetch task report + time     │
│     entry tasks + existing DB    │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     identify_records_to_upsert   │
│  (Compare report vs DB:          │
│   new → INSERT, changed → UPDATE)│
└──────────────┬───────────────────┘
               │
         ┌─────┴─────┐
         ▼           ▼
┌─────────────┐ ┌─────────────┐
│has_records  │ │has_records  │
│_to_insert?  │ │_to_update?  │
│  ↓Yes       │ │  ↓Yes       │
│insert_records│ │update_records│
└──────┬──────┘ └──────┬──────┘
       └────────┬──────┘
                ▼
         ┌──────────┐
         │ end_task  │
         └──────────┘
```

### 8.2.1 Upsert Child — Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `get_batch_project_details` | QueryCollectionOperator | Queries batch project URIs |
| `get_project_task_report` | RepliconReportDetailsOperator | Gets task report URI |
| `run_project_task_report` | run_report2 | Runs task report filtered to batch projects |
| `fetch_time_entry_enabled_tasks` | RepliconServicePageOperator | Fetches time entry flags via TaskListService |
| `get_existing_time_codes` | PythonOperator | Targeted query for existing records (batch projects only) |
| `identify_records_to_upsert` | PythonOperator | Compares report vs DB, classifies as INSERT or UPDATE |
| `has_records_to_insert` | IfOperator | Branch for INSERT path |
| `insert_records` | PythonOperator | Multi-row INSERT (230 rows/batch) |
| `has_records_to_update` | IfOperator | Branch for UPDATE path |
| `update_records` | PythonOperator | Row-by-row UPDATE by (time_code, type) key |

### 8.3 Delete Child DAG Structure

```
┌──────────────────────────────────┐
│     get_delete_batch_details     │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     separate_delete_types        │
│  (URI-present vs name-only)      │
└──────────────┬───────────────────┘
               │
         ┌─────┴─────┐
         ▼           ▼
┌─────────────┐ ┌─────────────┐
│URI PATH     │ │NAME PATH    │
│             │ │             │
│Run report   │ │Delete by    │
│for URI      │ │project name │
│projects     │ │prefix       │
│             │ │(LIKE name%) │
│Compare vs   │ └──────┬──────┘
│DB records   │        │
│             │        │
│Delete       │        │
│missing      │        │
│records      │        │
│             │        │
│OR if no     │        │
│report data: │        │
│Delete ALL   │        │
│for project  │        │
└──────┬──────┘        │
       └────────┬──────┘
                ▼
         ┌──────────┐
         │ end_task  │
         └──────────┘
```

### 8.3.1 Delete Child — Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `get_delete_batch_details` | QueryCollectionOperator | Queries batch of delete projects from parent |
| `separate_delete_types` | PythonOperator | Separates URI-present from name-only delete records |
| `has_uri_deletes` | IfOperator | Branch for URI-based deletes |
| `get_project_task_report` | RepliconReportDetailsOperator | Gets task report URI |
| `run_project_task_report` | run_report2 | Runs report filtered to URI projects |
| `report_has_data` | IfOperator | Routes to comparison or full delete |
| `get_existing_time_codes` | PythonOperator | Queries existing DB records for batch projects |
| `identify_records_to_delete` | PythonOperator | Finds DB records not in current report |
| `has_records_to_delete` | IfOperator | Branch for comparison-based delete |
| `delete_records_from_db` | PythonOperator | Deletes identified records by (time_code, type) |
| `delete_all_for_uri_projects` | PythonOperator | Deletes ALL records when report returns no data |
| `has_name_deletes` | IfOperator | Branch for name-based deletes |
| `delete_by_name` | PythonOperator | Deletes by `parent_time_code_name LIKE '{name}%'` |

### 8.4 DAG Configuration

```
MASTER DAG_ID:          resource_planner_project_task_export_delta_{instance}
UPSERT CHILD DAG_ID:    resource_planner_project_task_export_delta_child_{instance}
DELETE CHILD DAG_ID:     resource_planner_project_task_export_delta_delete_child_{instance}
SCHEDULE:               None (manual trigger)
CATCHUP:                False
MAX_ACTIVE_RUNS:        1 (master), 10 (children)
```

### 8.4.1 Report Configuration

| Report Name | Used In | Purpose |
|-------------|---------|---------|
| `Resource Planner Project URI Audit Report` | Master DAG | Change detection (Action/Field columns) |
| `Resource Planner Project Task Report` | Upsert + Delete Children | Current state of tasks per project |
| `Resource Planner Users Report` | Master DAG | Manager → employee ID resolution |

---

## 9. Error Handling, Observability, and Alerting

### 9.1 Error Handling Strategy

| Error Category | Examples | Handling |
|----------------|----------|----------|
| Audit report empty | No changes since last run | Skip to end_task |
| Report API failure | Timeout, malformed response | Retry, then fail DAG |
| Database connection | SQL Server unreachable | Retry with backoff |
| Delete failure | Constraint violation on DELETE | Transaction rollback, fail child |
| Insert/Update failure | Type mismatch, constraint error | Transaction rollback, fail child |

### 9.2 Transaction Management

- **Upsert child:** INSERTs committed atomically (multi-row batch); UPDATEs committed atomically (row-by-row within transaction)
- **Delete child:** DELETEs committed atomically per child DAG
- All operations are transactional — failure rolls back the entire child

### 9.3 Ordering Guarantee

Deletes are processed **before** upserts in the master DAG. This prevents scenarios where:
- A project is deleted and re-created with the same name
- An upsert inserts a record that a subsequent delete would remove

---

## 10. Conclusion

This pipeline provides comprehensive incremental synchronization of project and task time codes from Polaris to Resource Planner. The audit report-based delta detection ensures only changed records are processed, while the three-DAG architecture (master, upsert child, delete child) provides clear separation of concerns and parallel execution.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Audit report for delta detection | Polaris provides Action/Field columns specifically for change tracking |
| Deletes before upserts | Prevents ordering conflicts when projects are deleted and re-created |
| Two delete paths (URI vs name) | Fully deleted projects have no URI — fallback to name-based matching |
| Targeted SQL queries (batch only) | Reduces DB load by only querying records for the current batch |
| Separate INSERT and UPDATE paths | UPDATEs use row-by-row execution for targeted field changes |

### Relationship to Bulk Pipeline

The bulk pipeline handles **initial population** (insert-only). This delta pipeline handles **ongoing synchronization** (insert, update, delete). They share the same target table and data model, but use different source reports.

---

*Data Engineering Team - Resource Planner Integration*
