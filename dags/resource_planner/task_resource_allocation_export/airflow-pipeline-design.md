# Design Document

**Task Resource Allocation Export from Polaris to Resource Planner**

## Summary

This document outlines the design for an automated Apache Airflow pipeline that exports task resource allocation data from Polaris (Replicon's PSA module) into the Resource Planner database.

This is a **bulk load pipeline** — on every run, it pulls **all** allocation data from Polaris because Polaris currently has no change notification or delta API. A SHA256-based comparison layer is applied after extraction to minimize unnecessary database operations (only inserting, updating, or deleting records that actually changed). This is an **interim solution** until Polaris delivers webhook-based change notifications (see [Section 12](#12-future-state--webhook-based-approach)).

The pipeline uses a **master-child DAG architecture** with **batched child DAGs** to achieve parallelism — projects are distributed across N child DAGs (default N=5) using modulo assignment (`project_index % N`), where each child processes multiple projects. Each child fetches allocation data via a GraphQL API, expands schedule rules into daily rows, and writes the results to a SQL Server database. With `max_active_runs=4`, up to 4 child DAGs run concurrently.

---

## Table of Contents

1. [Overview and Goals](#1-overview-and-goals)
2. [Integration Workflow](#2-integration-workflow)
3. [Non-Functional Requirements](#3-non-functional-requirements)
4. [High-Level Architecture](#4-high-level-architecture)
5. [Data Sources](#5-data-sources)
6. [GraphQL API Specification](#6-graphql-api-specification)
7. [Data Transformation and Mapping](#7-data-transformation-and-mapping)
8. [Delta Detection Strategy](#8-delta-detection-strategy)
9. [Airflow-Specific Design](#9-airflow-specific-design)
10. [Error Handling, Observability, and Alerting](#10-error-handling-observability-and-alerting)
11. [Conclusion](#11-conclusion)
12. [Future State — Webhook-Based Approach](#12-future-state--webhook-based-approach)

---

## 1. Overview and Goals

### Purpose

This document describes the design of a production-grade Apache Airflow pipeline to synchronize **task resource allocation data** (who is assigned to which project task, for how many hours, on which days) from **Polaris** to **Resource Planner Database**.

Task resource allocations represent the planned work schedule — for each project, each assigned user has allocations across tasks with specific hours per day. This data is critical for resource planning, capacity forecasting, and project tracking.

### Why Bulk Load?

Polaris currently does not provide any mechanism to identify what has changed since the last sync — there is no change log, no last-modified timestamp on allocations, and no webhook or event notification. This means the only way to get accurate data is to **pull everything on every run**. An enhancement request has been submitted to the Polaris product team to add webhook-based change notifications (see [Section 12](#12-future-state--webhook-based-approach)). Until that is delivered, this bulk load pipeline is the interim solution.

### Business Goals

- **Automate Allocation Sync:** Replace manual data extraction with an automated daily pipeline
- **Maintain Data Consistency:** Ensure Resource Planner always reflects the current state of allocations in Polaris
- **Delta Processing:** Only process records that have changed, been added, or been removed — minimizing database load and processing time
- **Parallel Execution:** Process multiple projects concurrently to minimize total execution time
- **Full Audit Trail:** Log all operations, including skipped records, for compliance and troubleshooting

### Success Criteria

- Daily synchronization completes within an acceptable time window (TBD based on data volume)
- Zero data loss or corruption during transfer
- 100% audit trail for all insert, update, and delete operations
- Accurate delta detection — no missed changes, no unnecessary re-processing

---

## 2. Integration Workflow

### Core Functionality

1. **Fetch project-user assignments** from Polaris via the "Resource Planner Project - User Template" report — this provides which users are assigned to which projects, along with their employee ID, role, and project type
2. **Fetch project-task associations** from Polaris via the "Resource Planner Project - Task Template" report — this provides which tasks belong to which projects
3. **Resolve labor codes** by looking up each user's Primary Role against the `rp_labor_code` reference table in the database
4. **Distribute projects across N child DAGs** using modulo assignment — each child handles multiple projects
5. **Call the GraphQL API** for each user within each project in the batch, batching tasks in groups of 50 (configurable), to retrieve allocation schedule rules
6. **Expand schedule rules into daily rows** — each date range is broken into individual day records, skipping excluded weekdays
7. **Generate unique booking IDs** per daily row using the allocation UUID and a sequential index
8. **Detect deltas** using a SHA256 hash-based reference snapshot — insert new records, delete removed records, and replace changed records
9. **Write results to the database** (`dbo.rp_source` / `dbo.dummy_rp_source`)
10. **Log skipped records** (e.g., unmatched labor codes) and send a failure summary via email

### Data Flow

```
Polaris Reports                  GraphQL API                 Database
(User + Task)                    (Allocations)               (SQL Server)
     │                                │                           │
     ▼                                │                           │
┌──────────────┐                      │                           │
│ Master DAG   │                      │                           │
│              │                      │                           │
│ 1. Download  │                      │                           │
│    User &    │                      │                           │
│    Task      │                      │                           │
│    Reports   │                      │                           │
│              │                      │                           │
│ 2. Fetch     │◄─────────────────────┼───────────────────────────┤
│    labor     │  rp_labor_code       │                           │
│    code map  │  lookup              │                           │
│              │                      │                           │
│ 3. Load prev │                      │                           │
│    reference │                      │                           │
│              │                      │                           │
│ 4. Trigger   │                      │                           │
│    1 child   │                      │                           │
│    per       │                      │                           │
│    project   │                      │                           │
└──────┬───────┘                      │                           │
       │                              │                           │
       ▼ (per project)                │                           │
┌──────────────┐                      │                           │
│ Child DAG    │                      │                           │
│              │                      │                           │
│ 5. For each  │─── GraphQL call ────►│                           │
│    user:     │◄── allocations ──────┤                           │
│    batch     │                      │                           │
│    tasks     │                      │                           │
│    (max 50)  │                      │                           │
│              │                      │                           │
│ 6. Expand    │                      │                           │
│    schedule  │                      │                           │
│    rules     │                      │                           │
│    into days │                      │                           │
│              │                      │                           │
│ 7. Delta     │                      │                           │
│    detection │                      │                           │
│    (SHA256)  │                      │                           │
│              │                      │                           │
│ 8. Write to  │──────────────────────┼──────────────────────────►│
│    database  │  INSERT / DELETE     │                    dbo.rp_source
│              │                      │                           │
│ 9. Update    │                      │                           │
│    reference │                      │                           │
│    snapshot  │                      │                           │
└──────────────┘                      │                           │
```

---

## 3. Non-Functional Requirements

### 3.1 Scalability

- Handle hundreds of projects with thousands of users and tasks
- Task batching (max 50 per GraphQL call) prevents API overload
- Master-child architecture scales horizontally — more projects just means more child DAG runs

### 3.2 Reliability


- Automatic retry with exponential backoff on transient API/DB failures
- Fault isolation per project — if one project fails, all others still complete
- Idempotent design — safe to re-execute any child DAG

### 3.3 Performance

- Parallel project processing via concurrent child DAGs
- Delta processing minimizes database operations — only changed data is written
- Task batching reduces total GraphQL API calls

### 3.4 Maintainability

- Centralized configuration via `config.py` and instance files
- Modular design — report fetching, GraphQL calls, date expansion, delta detection, and DB writes are separate, testable components
- Comprehensive logging for troubleshooting

---

## 4. High-Level Architecture

### 4.1 System Components

| Component | Type | Description |
|-----------|------|-------------|
| Polaris Report API | REST API | Provides project-user and project-task data via CSV reports |
| Polaris GraphQL API | GraphQL | Provides task resource allocation schedule rules per project-user-task combination |
| Apache Airflow 2.x+ | Platform | Workflow orchestration — runs the master and child DAGs |
| Resource Planner DB | SQL Server | Target database for allocation data |
| `dbo.rp_source` | SQL Table | Destination table for allocation rows (prod: `dbo.rp_source`, dev: `dbo.dummy_rp_source`) |
| `rp_labor_code` | SQL Table | Reference table for mapping user roles to labor codes |

### 4.2 Integration Points

| Integration | Protocol | Direction | Description |
|-------------|----------|-----------|-------------|
| Polaris → Airflow | HTTPS (REST) | Inbound | Report API calls returning CSV data |
| Polaris → Airflow | HTTPS (GraphQL) | Inbound | Allocation queries returning JSON data |
| Airflow → SQL Server | ODBC | Outbound | Database reads (labor codes, reference) and writes (allocations) |
| Airflow → SMTP | SMTP | Outbound | Email notifications for failures and skipped records |

---

## 5. Data Sources

### 5.1 Resource Planner Project - User Template (Report)

This Replicon report provides the mapping of users to projects, along with employee metadata.

| Column | Description | Used For |
|--------|-------------|----------|
| Project Name | Human-readable project name | Logging / debugging |
| ProjectUri | Unique project identifier (e.g., `urn:replicon-tenant:...:project:2974`) | GraphQL input, time_code generation |
| Employee ID | Employee identifier (e.g., `EN11165`, `TS10710`) | `employee_id` column in destination |
| UserUri | Unique user identifier (e.g., `urn:replicon-tenant:...:user:325`) | GraphQL input, `users_user_id` column |
| Primary Role (Current) | User's current role (e.g., `Customization`, `Senior Delivery Manager - Development`) | Labor code lookup |
| *[TBD Field]* | Project type classification | `hours_type` (internal / customer) — **field to be confirmed once actual data is available** |

### 5.2 Resource Planner Project - Task Template (Report)

This Replicon report provides the mapping of tasks to projects.

| Column | Description | Used For |
|--------|-------------|----------|
| ProjectUri | Unique project identifier | Grouping tasks by project |
| TaskUri | Unique task identifier (e.g., `urn:replicon-tenant:...:task:146283`) | GraphQL input, time_code generation |

### 5.3 rp_labor_code Table (Database Lookup)

This reference table in the Resource Planner database maps human-readable role names to standardized labor codes.

| Column | Description | Filter |
|--------|-------------|--------|
| `labor_code` | Standardized code (e.g., `ASSOC`, `PRNCON`, `SRSYSC`) | — |
| `labor_code_name` | Descriptive name (e.g., `Associate Consultant`) | — |
| `mapping_to_source` | Source system role name to match against (e.g., `Associate Developer`) | Matched against `Primary Role (Current)` from User Report |
| `source_system` | Source system filter | `= 'Polaris'` |
| `is_active` | Active flag | `= 1` |

**Lookup Logic:** For each user, take their `Primary Role (Current)` from the User Report, find the row in `rp_labor_code` where `mapping_to_source` matches and `source_system = 'Polaris'` and `is_active = 1`, then use the corresponding `labor_code` value.

**If no match is found:** The record is **skipped** (not inserted) and logged as a failure. A summary of all skipped records is included in the failure email notification.

---

## 6. GraphQL API Specification

### 6.1 Endpoint

`/graphql`

### 6.2 Query Structure

```graphql
query {
  taskResourceUserAllocationsForUser(
    filter: {
      projectUri: "<projectUri>",     # String — one project per call
      userUri: "<userUri>",           # String — one user per call
      taskUris: [                     # List — max 50 tasks per call
        "<taskUri_1>",
        "<taskUri_2>",
        ...
      ]
    }
  ) {
    taskUri                           # Which task this allocation is for
    totalHours                        # Total hours across all schedule rules
    id                                # Unique allocation ID (per project+user+task)
    scheduleRules {                   # One or more date ranges
      dateRange {
        startDate                     # ISO 8601 datetime
        endDate                       # ISO 8601 datetime
      }
      do {
        load                          # Load percentage (e.g., 100)
        setHours                      # Hours per day within this date range
        excludeWeekdays               # Days to skip (e.g., ["sa", "su"])
      }
    }
  }
}
```

### 6.3 API Behavior

**Batching:** The `taskUris` list is capped at **50 task URIs per call** as a design decision — not an API limit. This prevents the GraphQL service from timing out when a project has a large number of tasks with many allocation records. If a project has more than 50 tasks, multiple calls are made:
- Call 1: tasks 1–50
- Call 2: tasks 51–100
- Call N: tasks (N-1)*50+1 to min(N*50, total)

No task is duplicated across batches. The batch size of 50 is configurable via `task_batch_size` in the instance configuration, allowing it to be tuned based on observed performance.

**Clubbing behavior:** The API groups consecutive weekdays with the **same hours** into a single schedule rule. For example, if a user is allocated 8 hours/day from Monday to Friday, this returns as one rule with `startDate: Monday`, `endDate: Friday`, `setHours: 8`. Weekends are excluded — the API splits ranges at weekend boundaries.

**Empty responses:** If a project+user combination has no allocations for the given tasks, the API returns an empty array. This is a valid response (not an error) and means the user has no planned work on those tasks.

### 6.4 Sample Response

```json
{
  "data": {
    "taskResourceUserAllocationsForUser": [
      {
        "taskUri": "urn:replicon-tenant:...:task:161480",
        "totalHours": 60,
        "id": "urn:replicon-tenant:...:psa-task-allocation:b72caee4-...",
        "scheduleRules": [
          {
            "dateRange": {
              "startDate": "2026-02-25T00:00:00.000Z",
              "endDate": "2026-02-25T00:00:00.000Z"
            },
            "do": { "load": 100, "setHours": 4, "excludeWeekdays": [] }
          },
          {
            "dateRange": {
              "startDate": "2026-02-26T00:00:00.000Z",
              "endDate": "2026-02-27T00:00:00.000Z"
            },
            "do": { "load": 100, "setHours": 8, "excludeWeekdays": [] }
          },
          {
            "dateRange": {
              "startDate": "2026-03-02T00:00:00.000Z",
              "endDate": "2026-03-06T00:00:00.000Z"
            },
            "do": { "load": 100, "setHours": 8, "excludeWeekdays": [] }
          }
        ]
      }
    ]
  }
}
```

The above allocation for task `161480` expands to **8 daily rows**:

| Day | Date | Hours | Explanation |
|-----|------|-------|-------------|
| Tue | Feb 25 | 4h | Single-day rule |
| Wed | Feb 26 | 8h | Range rule (Feb 26–27), day 1 |
| Thu | Feb 27 | 8h | Range rule (Feb 26–27), day 2 |
| — | Feb 28 (Sat) | — | Weekend — not in any rule |
| — | Mar 1 (Sun) | — | Weekend — not in any rule |
| Mon | Mar 2 | 8h | Range rule (Mar 2–6), day 1 |
| Tue | Mar 3 | 8h | Range rule (Mar 2–6), day 2 |
| Wed | Mar 4 | 8h | Range rule (Mar 2–6), day 3 |
| Thu | Mar 5 | 8h | Range rule (Mar 2–6), day 4 |
| Fri | Mar 6 | 8h | Range rule (Mar 2–6), day 5 |

**Total: 4 + 8 + 8 + (5 × 8) = 60 hours** ✓ (matches `totalHours`)

---

## 7. Data Transformation and Mapping

### 7.1 Destination Table: `dbo.rp_source`

Development environment uses `dbo.dummy_rp_source`.

| Column | Type | Source | Mapping Logic |
|--------|------|--------|---------------|
| `source_id` | int IDENTITY(1,1) | Auto | Auto-generated primary key — no input required |
| `source_booking_id` | nvarchar(50) | GraphQL → `id` | Extract the UUID from the allocation ID (`id.split(":")[-1]`), then append a sequential index per expanded day: `<uuid>__001`, `<uuid>__002`, etc. See [Section 7.2](#72-booking-id-generation) |
| `source_system` | nvarchar(50) | Hardcoded | Always `"Polaris"` |
| `time_code` | nvarchar(MAX) | Reports + GraphQL | `<project_last_segment>.<task_last_segment>` — e.g., for ProjectUri `...project:2974` and TaskUri `...task:161481`, the time_code is `2974.161481` |
| `labor_code` | nvarchar(3) | User Report → `rp_labor_code` | Take the user's `Primary Role (Current)`, look up in `rp_labor_code` where `mapping_to_source` matches and `source_system = 'Polaris'`. Use the `labor_code` value (e.g., `ASSOC`, `PRNCON`). If no match → skip record, log failure |
| `users_user_id` | nvarchar(50) | User Report → UserUri | Last segment of the URI — e.g., `urn:replicon-tenant:...:user:325` → `325` |
| `hours` | decimal(18,4) | GraphQL → `scheduleRules[n].do.setHours` | Hours allocated for that specific day |
| `work_date` | datetime | GraphQL → `scheduleRules[n].dateRange` | Each individual day within the date range (expanded from startDate to endDate, skipping days listed in `excludeWeekdays`) |
| `hours_type` | nvarchar(50) | User Report → *[TBD field]* | Project type classification: `"internal"` or `"customer"`. **Exact source field to be confirmed once actual data is available** |
| `last_updated_date` | datetime | DAG execution | UTC timestamp of when the DAG processed this record — since Polaris does not provide a last-modified date for allocations, this represents the last sync time |
| `employee_id` | nvarchar(50) | User Report → Employee ID | Employee identifier as-is from the report (e.g., `EN11165`, `TS10710`, `1500129`) |

### 7.2 Booking ID Generation

Each allocation from the GraphQL API has a unique `id` per project + user + task combination. However, one allocation can produce **multiple daily rows** after expanding schedule rules. Each row needs a unique `source_booking_id`.

**Format:** `<allocation_uuid>__<sequential_index>`

**Example:** Allocation ID `urn:replicon-tenant:...:psa-task-allocation:b72caee4-894d-440e-8e7a-0edaa7175e59` with 8 expanded days produces:

| source_booking_id | work_date | hours |
|---|---|---|
| `b72caee4-894d-440e-8e7a-0edaa7175e59__001` | 2026-02-25 | 4 |
| `b72caee4-894d-440e-8e7a-0edaa7175e59__002` | 2026-02-26 | 8 |
| `b72caee4-894d-440e-8e7a-0edaa7175e59__003` | 2026-02-27 | 8 |
| `b72caee4-894d-440e-8e7a-0edaa7175e59__004` | 2026-03-02 | 8 |
| ... | ... | ... |
| `b72caee4-894d-440e-8e7a-0edaa7175e59__008` | 2026-03-06 | 8 |

**Why sequential indexing is safe:** Delta detection operates at the allocation level (see [Section 8](#8-delta-detection-strategy)). When any change is detected for an allocation, **all** its rows are deleted and re-inserted with fresh booking IDs. Individual rows are never updated in-place, so index shifting is not a concern.

### 7.3 Date Range Expansion Logic

Each schedule rule contains a date range (`startDate` to `endDate`) and optionally an `excludeWeekdays` list. The expansion works as follows:

1. Parse `startDate` and `endDate` from the schedule rule
2. Iterate through each calendar day from start to end (inclusive)
3. For each day, check if its weekday abbreviation is in `excludeWeekdays`
   - Weekday abbreviations: `mo`, `tu`, `we`, `th`, `fr`, `sa`, `su`
   - If the day is excluded → skip it
   - If not excluded → create a row with `setHours` as the hours value
4. Assign sequential booking ID indices across all expanded days for the allocation

### 7.4 Labor Code Lookup

```
User Report: Primary Role (Current) = "Associate Developer"
                     │
                     ▼ (lookup)
rp_labor_code table: mapping_to_source = "Associate Developer"
                     source_system = "Polaris"
                     is_active = 1
                     │
                     ▼ (result)
labor_code = "ASSOC"
```

**Failure handling:** If a user's Primary Role does not match any `mapping_to_source` entry in `rp_labor_code`, all allocation records for that user are **skipped** and logged. The failure log is emailed at the end of the DAG run.

---

## 8. Delta Detection Strategy (Interim)

> **Context:** This section describes an optimization layer within the bulk load pipeline. Because Polaris has no change notification mechanism, we pull all data on every run. The delta detection described here is applied **after extraction** to avoid blindly deleting and re-inserting all records in the database on every cycle. Once Polaris delivers webhook-based change notifications (see [Section 12](#12-future-state--webhook-based-approach)), this mechanism will no longer be needed.

### 8.1 Why Delta Detection?

Running a full database refresh (delete all + re-insert all) on every execution would be inefficient and could cause issues with downstream systems that react to inserts and deletes. Instead, after pulling all data from Polaris, we compare the current run's data against a **reference snapshot** from the previous run to identify only what has actually changed — and limit database operations to just those records.

### 8.2 How It Works

**Scope:** Delta detection operates at the **allocation level** (one allocation = one project + user + task combination), not at the individual day-row level.

**Hash computation:** For each allocation, compute a SHA256 hash of all its expanded daily rows (sorted deterministically). This hash captures the complete state of the allocation — any change in dates, hours, or schedule rules will produce a different hash.

```
Hash Input = sorted JSON of:
  [
    {"date": "2026-02-25", "hours": 4},
    {"date": "2026-02-26", "hours": 8},
    {"date": "2026-02-27", "hours": 8},
    ...
  ]

allocation_hash = SHA256(hash_input)
```

**Reference snapshot:** After each successful run, a reference file (CSV) is stored containing:

| allocation_id | allocation_hash |
|---|---|
| `b72caee4-894d-440e-8e7a-0edaa7175e59` | `a3f2b8c1...` |
| `fb0a4b32-da18-4241-8c30-c96e4dcc6deb` | `7e91d4f0...` |

### 8.3 Delta Actions

On each run, the current allocations are compared against the previous reference:

| Scenario | Condition | Action |
|----------|-----------|--------|
| **New allocation** | Allocation ID exists in current run but NOT in reference | INSERT all daily rows for this allocation |
| **Changed allocation** | Allocation ID exists in both, but hash differs | DELETE all existing rows for this allocation ID, then INSERT all current daily rows |
| **Deleted allocation** | Allocation ID exists in reference but NOT in current run | DELETE all rows for this allocation ID from the database |
| **Unchanged allocation** | Allocation ID exists in both, hash matches | No action — skip |

**Important:** The "deleted allocation" scenario is why the reference file is essential. Without it, we would not know that an allocation that existed in the previous run has been removed in Polaris. The GraphQL API simply returns nothing for removed allocations — there is no "deleted" flag.

---

## 9. Airflow-Specific Design

### 9.1 DAG Overview

The pipeline consists of two DAGs:

| DAG | ID Pattern | Purpose |
|-----|------------|---------|
| **Master** | `resource_planner_task_resource_allocation_export_{instance}` | Downloads reports, builds project-level payloads, distributes across N batched child DAGs |
| **Child** | `resource_planner_task_resource_allocation_export_child_{instance}` | Processes a batch of projects — calls GraphQL, expands dates, detects deltas, writes to DB |

### 9.2 Master DAG Structure

```
┌──────────────────────────────────┐
│       can_run_batch_task         │
│    (Check Airflow Variable)      │
└──────────────┬───────────────────┘
               │
     ┌─────────┼──────────┐
     │ Yes     │          │ No
     ▼         │          ▼
┌──────────┐   │   ┌───────────────────────────┐
│batch_task│   │   │  get_user_report_details   │
└────┬─────┘   │   │  get_task_report_details   │
     │         │   └─────────────┬─────────────┘
     │         │                 │
     │         │                 ▼
     │         │   ┌───────────────────────────┐
     │         │   │  run_user_report           │
     │         │   │  run_task_report           │
     │         │   └─────────────┬─────────────┘
     │         │                 │
     │         │                 ▼
     │         │   ┌───────────────────────────┐
     │         │   │  load_user_report_data     │
     │         │   │  load_task_report_data     │
     │         │   └─────────────┬─────────────┘
     │         │                 │
     │         │                 ▼
     │         │   ┌───────────────────────────┐
     │         │   │  create_user_collection    │
     │         │   │  create_task_collection    │
     │         │   └─────────────┬─────────────┘
     │         │                 │
     │         │                 ▼
     │         │   ┌───────────────────────────┐
     │         │   │  fetch_labor_code_map      │
     │         │   │  (Query rp_labor_code)     │
     │         │   └─────────────┬─────────────┘
     │         │                 │
     │         │                 ▼
     │         │   ┌───────────────────────────┐
     │         │   │  build_project_payloads    │
     │         │   │  (Group users + tasks by   │
     │         │   │   project, attach metadata)│
     │         │   └─────────────┬─────────────┘
     │         │                 │
     │         │                 ▼
     │         │   ┌───────────────────────────┐
     │         │   │  trigger_child_per_project │
     │         │   │  (TriggerDagRunForEach)    │
     │         │   └─────────────┬─────────────┘
     │         │                 │
     │         │                 ▼
     │         │   ┌───────────────────────────┐
     │         │   │  wait_for_children         │
     │         │   │  (WaitForDagRunsSensor)    │
     │         │   └─────────────┬─────────────┘
     │         │                 │
     ▼         ▼                 ▼
   ┌───────────────────────────────┐
   │           end_task            │
   └───────────────────────────────┘
```

### 9.2.1 Master DAG — Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `can_run_batch_task` | IfOperator | Checks Airflow Variable to enable/disable the pipeline |
| `batch_task` | BatchTaskRunOperator | Error boundary — wraps the main flow |
| `get_user_report_details` | RepliconReportDetailsOperator | Gets the URI for the "Resource Planner Project - User Template" report |
| `get_task_report_details` | RepliconReportDetailsOperator | Gets the URI for the "Resource Planner Project - Task Template" report |
| `run_user_report` | run_report2 | Executes the User report and retrieves CSV |
| `run_task_report` | run_report2 | Executes the Task report and retrieves CSV |
| `load_user_report_data` | LoadCSVFileOperator | Parses User report CSV into records |
| `load_task_report_data` | LoadCSVFileOperator | Parses Task report CSV into records |
| `create_user_collection` | CreateCollectionOperator | Creates in-memory collection of user-project mappings |
| `create_task_collection` | CreateCollectionOperator | Creates in-memory collection of project-task mappings |
| `fetch_labor_code_map` | PythonOperator | Queries `rp_labor_code` table and builds a `{mapping_to_source → labor_code}` lookup dictionary |
| `build_project_payloads` | PythonOperator | Groups users and tasks by project, attaches labor codes and metadata, writes as JSON artifact |
| `trigger_child_per_project` | TriggerDagRunForEachItemOperator | Distributes projects across N child DAGs using modulo (default N=5) |
| `wait_for_children` | WaitForDagRunsSensor | Waits for all child DAG runs to complete |
| `end_task` | EmptyOperator | DAG completion marker |

### 9.2.2 Data Passed to Each Child DAG

Projects are distributed across N child DAGs using modulo assignment: project at index `i` goes to child `i % N`. Each child DAG receives a batch of projects via `dag_run.conf`:

```json
{
  "instance": "prod",
  "batch_index": 0,
  "task_batch_size": 50,
  "projects": [
    {
      "project_uri": "urn:replicon-tenant:...:project:2974",
      "project_name": "Project Alpha",
      "users": [
        {
          "user_uri": "urn:replicon-tenant:...:user:325",
          "employee_id": "EN11165",
          "labor_code_id": "ASSOC",
          "hours_type": "customer"
        }
      ],
      "task_uris": [
        "urn:replicon-tenant:...:task:161480",
        "urn:replicon-tenant:...:task:161481"
      ],
      "previous_reference": {
        "b72caee4-...": "a3f2b8c1..."
      }
    },
    {
      "project_uri": "urn:replicon-tenant:...:project:3001",
      "project_name": "Project Beta",
      "users": [ ... ],
      "task_uris": [ ... ],
      "previous_reference": { ... }
    }
  ]
}
```

**Example distribution:** 40 projects with `child_batch_count=5`:
- Child 0: projects at indices 0, 5, 10, 15, 20, 25, 30, 35 (8 projects)
- Child 1: projects at indices 1, 6, 11, 16, 21, 26, 31, 36 (8 projects)
- Child 2: projects at indices 2, 7, 12, 17, 22, 27, 32, 37 (8 projects)
- Child 3: projects at indices 3, 8, 13, 18, 23, 28, 33, 38 (8 projects)
- Child 4: projects at indices 4, 9, 14, 19, 24, 29, 34, 39 (8 projects)

### 9.3 Child DAG Structure

```
┌──────────────────────────────────┐
│     fetch_allocations            │
│  (For each user:                 │
│    batch tasks into groups ≤50,  │
│    call GraphQL per batch,       │
│    collect all allocation data)  │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     expand_schedule_rules        │
│  (Expand date ranges → daily     │
│   rows, skip excludeWeekdays,   │
│   assign booking IDs)            │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     detect_deltas                │
│  (Compare current allocations    │
│   against previous reference     │
│   using SHA256 hashes)           │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     write_to_database            │
│  (Execute INSERT / DELETE based  │
│   on delta results)              │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     update_reference_snapshot    │
│  (Save current allocation hashes │
│   for next run's delta check)    │
└──────────────────────────────────┘
```

### 9.3.1 Child DAG — Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `fetch_allocations` | RepliconServiceCallForEachItemOperator | Iterates through all projects in the batch, for each user batches tasks (max 50), calls GraphQL API, collects all allocation responses with project metadata |
| `expand_schedule_rules` | PythonOperator | Expands each allocation's schedule rules into individual daily rows (using per-allocation project_uri), handles `excludeWeekdays`, assigns sequential booking IDs |
| `detect_deltas` | PythonOperator | Computes SHA256 hash per allocation across all projects in the batch, compares against combined previous references, classifies each allocation as new/changed/deleted/unchanged |
| `write_to_database` | PythonOperator | Executes INSERT for new/changed allocations, DELETE for removed allocations, using MsSqlHook |
| `publish_hashes` | PythonOperator | Publishes current allocation hashes grouped by project_uri (list of per-project hash dicts) for master consolidation |

### 9.4 DAG Configuration

```
MASTER DAG_ID:        resource_planner_task_resource_allocation_export_{instance}
CHILD DAG_ID:         resource_planner_task_resource_allocation_export_child_{instance}
SCHEDULE:             TBD (based on processing time benchmarks)
CATCHUP:              False
MAX_ACTIVE_RUNS:      1 (master), 4 (child)
CHILD_BATCH_COUNT:    5 (number of child DAGs, projects distributed via modulo)
```

### 9.4.1 Instance Configuration

```python
# instances/prod.py
instance = "prod"
company_key = 'Repliconpincstream6dev'
replicon_conn_id = 'replicon_Repliconpincstream6dev_replicon.integration'

# Feature flag
resource_planner_task_resource_allocation_export_enable_batch_task = (
    f"resource_planner_task_resource_allocation_export_enable_batch_task_{instance}"
)

# Database
mssql_conn_id = 'resource_planning_database_connection'
mssql_database = 'ResourcePlanning_development'
target_table = 'dbo.dummy_rp_source'

# Reports
user_report_name = "Resource Planner Project - User Template"
task_report_name = "Resource Planner Project - Task Template"

# GraphQL
graphql_endpoint = '/graphql'

# Batching
task_batch_size = 50           # GraphQL task URIs per API call
child_batch_count = 5          # Number of child DAGs (projects distributed via modulo)
max_active_runs_child = 4      # Max concurrent child DAG runs
```

---

## 10. Error Handling, Observability, and Alerting

### 10.1 Error Handling Strategy

| Error Category | Examples | Handling |
|----------------|----------|----------|
| Report API failure | API down, empty response, malformed CSV | Retry with backoff, then fail master DAG |
| GraphQL API failure | Timeout, 5xx error, malformed response | Retry the specific call, then fail the child DAG (other projects unaffected) |
| Labor code not found | User role not in `rp_labor_code` | Skip all records for that user, log as failure |
| Database connection | SQL Server unreachable, timeout | Retry with backoff, then fail |
| Transaction failure | Insert/delete fails mid-operation | Rollback transaction, fail child DAG |
| No tasks for project | Project exists in User Report but not in Task Report | Skip project — do not trigger child DAG |
| No users for project | Project exists in Task Report but not in User Report | Skip project — do not trigger child DAG |
| Empty GraphQL response | No allocations for a project+user combo | Skip — no rows to insert. If allocations existed in previous reference, they are treated as deletions |

### 10.2 Failure Logging and Email

Records that are skipped due to unmatched labor codes are collected into a failure log. At the end of the DAG run, this log is sent via email to the configured recipients.

**Failure log format:**

| Project | User | Employee ID | Primary Role | Reason |
|---------|------|-------------|--------------|--------|
| project:2974 | user:325 | EN11165 | Custom Role XYZ | No matching labor_code in rp_labor_code |

### 10.3 Transaction Management

- All database writes within a child DAG are wrapped in a transaction
- DELETEs and INSERTs for changed allocations happen atomically
- If any operation fails, the entire transaction is rolled back
- The child DAG can be safely re-executed (idempotent with delta detection)

---

## 11. Conclusion

This design provides a scalable, fault-tolerant **bulk load pipeline** for synchronizing task resource allocation data from Polaris to Resource Planner. The master-child architecture enables parallel processing across projects, while the SHA256-based delta detection minimizes unnecessary database operations despite pulling all data on every run.

This is an **interim solution** — it exists because Polaris currently has no change notification mechanism. Once Polaris delivers webhook support (Section 12), this pipeline will be retired or simplified to event-driven processing.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Master-child architecture (N batched children, modulo distribution) | Parallelism with reduced DAG overhead — fewer child DAGs (default 5) each processing multiple projects |
| Task batching (max 50 per GraphQL call) | Prevents GraphQL service timeouts on large projects (design choice, not API limit) |
| Date range expansion to daily rows | Destination table requires one row per day per allocation |
| Bulk load with delta optimization | Polaris has no change notification — pull everything, then compare locally to minimize DB writes |
| Allocation-level delta detection | Simpler than row-level; allocation is the natural unit of change |
| SHA256 hash comparison | Efficient change detection without storing full previous data |
| Sequential booking ID indexing | Safe because delta operates at allocation level (delete all + re-insert) |
| DAG execution timestamp for `last_updated_date` | Polaris does not provide a last-modified date for allocations |

### Open Items

| Item | Status | Notes |
|------|--------|-------|
| `hours_type` source field | Pending | Exact field in User Report to be confirmed once actual data is available |
| Schedule interval | Pending | To be determined based on processing time benchmarks |
| `rp_source.labor_code` column width | To verify | Column defined as `nvarchar(3)` but actual labor codes are up to 6 characters |
| Email recipients for failure log | Pending | To be configured per instance |

### Next Steps

1. Review and approve this design document with stakeholders
2. Confirm the `hours_type` source field once actual report data is available
3. Verify `labor_code` column width in the destination table
4. Implement and test the master DAG
5. Implement and test the child DAG
6. Run end-to-end integration test with development environment
7. Benchmark execution time to determine scheduling interval

---

## 12. Future State — Webhook-Based Approach

### 12.1 Context

An enhancement request has been submitted to the Polaris product team (document: *"Polaris — Task-Resource Allocation Change Notification"*) requesting webhook-based change notifications for allocations. This would fundamentally change how this pipeline operates.

### 12.2 What Changes With Webhooks

| Aspect | Current (Bulk Load) | Future (Webhook-Based) |
|--------|---------------------|------------------------|
| **Trigger** | Scheduled (cron) | Event-driven (on allocation change) |
| **Data volume per run** | All allocations across all projects | Only the single allocation that changed |
| **Delta detection** | SHA256 hash comparison after full extraction | Not needed — Polaris tells us exactly what changed |
| **Reference snapshot** | Required (to detect deletions) | Not needed — Polaris sends `ALLOCATION.DELETED` events |
| **Latency** | Hours (depends on schedule interval) | Near real-time |
| **API load** | High — pulls everything every cycle | Minimal — one event per change |

### 12.3 Expected Webhook Events

The enhancement request asks for three event types:

| Event | When It Fires | What It Contains |
|-------|---------------|------------------|
| `ALLOCATION.CREATED` | A new task-resource allocation is saved | Complete allocation record |
| `ALLOCATION.UPDATED` | Any field of an allocation is modified | Complete allocation record with updated values |
| `ALLOCATION.DELETED` | An allocation is removed | Last known state before deletion |

Each event would also include an `acting_user` field to identify whether the change originated from a Polaris user or from the RP Tool (via API). This is critical to prevent sync loops when data flows in both directions.

### 12.4 Impact on This Pipeline

Once Polaris delivers this enhancement:
- This bulk load pipeline will be **retired**
- A new, simpler event-driven pipeline will replace it
- The SHA256 delta detection, reference snapshots, and full data extraction will no longer be needed
- Processing time will drop from minutes/hours to seconds per event

Until then, this bulk load pipeline is the interim solution that keeps the two systems in sync.

---