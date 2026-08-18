# Apache Airflow Data Pipeline Design Document

**User Export from Polaris to Resource Planner**

| Property | Value |
|----------|-------|
| Version | 1.2 |
| Date | January 27, 2026 |
| Author | Data Engineering Team |
| Pipeline Name | resource_planner_user_export_{instance} |
| Audience | Data Engineers, DevOps, Solution Architects, Stakeholders |

## Executive Summary

This document outlines the design for an automated Apache Airflow pipeline that synchronizes user data from Polaris SaaS to Resource Planner via daily batch processing. The solution uses SHA256 hashing for efficient delta detection, ensuring only changed records are processed while maintaining full audit trails and transactional integrity.

---

## Table of Contents

1. [Overview and Goals](#1-overview-and-goals)
2. [Functional Requirements](#2-functional-requirements)
3. [Non-Functional Requirements](#3-non-functional-requirements)
   - [3.1 Scalability](#31-scalability)
   - [3.2 Reliability](#32-reliability)
   - [3.3 Performance](#33-performance)
   - [3.4 Latency](#34-latency)
   - [3.5 Cost Efficiency](#35-cost-efficiency)
   - [3.6 Maintainability](#36-maintainability)
4. [High-Level Architecture](#4-high-level-architecture)
   - [4.1 System Components](#41-system-components)
   - [4.2 Component Description](#42-component-description)
   - [4.3 Integration Points](#43-integration-points)
5. [Airflow-Specific Design](#5-airflow-specific-design)
   - [5.1 DAG Structure](#51-dag-structure)
   - [5.2 DAG Configuration](#52-dag-configuration)
   - [5.3 Scheduling Strategy](#53-scheduling-strategy)
6. [Data Handling](#6-data-handling)
   - [6.1 Data Ingestion](#61-data-ingestion)
   - [6.2 Data Transformation](#62-data-transformation)
   - [6.3 Data Validation](#63-data-validation)
7. [Error Handling, Observability, and Alerting](#7-error-handling-observability-and-alerting)
   - [7.1 Error Handling Strategy](#71-error-handling-strategy)
   - [7.2 Transaction Management](#72-transaction-management)
8. [Conclusion](#conclusion)

---

## 1. Overview and Goals

### Purpose

This document describes the design of a production-grade Apache Airflow pipeline to synchronize user data from **Polaris** (a SaaS application) to **Resource Planner** (an on-premise SQL Server database). User data is retrieved using the Polaris Report API, which returns data in CSV format.

### Business Goals

- **Maintain Data Consistency:** Ensure Resource Planner always reflects the current state of users in Polaris
- **Automate Daily Sync:** Eliminate manual data transfer processes and reduce human error
- **Provide Visibility:** Enable comprehensive logging and audit trails for compliance and troubleshooting
- **Minimize Sync Duration:** Optimize pipeline execution to complete within acceptable time windows

### Success Criteria

The pipeline will be considered successful when:

- Daily synchronization completes in under **10 minutes**
- **Zero data loss** or corruption during transfer
- **100% audit trail** for all operations performed
- **Transactional integrity** maintained (all-or-nothing operations)

---

## 2. Functional Requirements

### Core Functionality

1. Call Polaris Report API to retrieve user data in CSV format (EmployeeID, Status)
2. Check feature flag (Airflow Variable) to determine execution mode
3. Identify users to add (present in Polaris AND in `active_users_view` but NOT in `target_table`)
4. Identify users to delete (Status="Disabled" in Polaris) *[Phase 2]*
5. Batch insert new users to configurable target table
6. Delete disabled users from Resource Planner *[Phase 2]*
7. Log disabled users in Polaris that are still present in Resource Planner

### Data Flow

```
Polaris SaaS  --(REST)-->  CSV Response  -->  Airflow Pipeline  --(ODBC)-->  Resource Planner
   (Report API)              (In-Memory)       (SHA256 Hash          (SQL Server)
                                                Generation &
                                                Delta Calculation)
                                                     |
                                                     v
                                            Reference Snapshot File
                                              (SHA256 Hashes)
```

### Data Elements

#### Polaris Report Fields (Source)

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `Employee_ID` | String | NOT NULL | Unique identifier for the employee |
| `Status` | String | NOT NULL | Employee status (e.g., Active, Disabled) |

#### Target Table: `dbo.rp_source_resources`

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `id` | Integer | PK, AUTO | Auto-generated primary key |
| `source_system` | String | NOT NULL | Source system identifier ("Polaris") |
| `resource_id` | String | NOT NULL | Employee ID from Polaris |

---

## 3. Non-Functional Requirements

### 3.1 Scalability

- Handle 15,000+ user records daily
- Support growth to 50,000+ records without architectural changes
- SHA256 delta processing to minimize data transfer and processing overhead

### 3.2 Reliability

- 99.5% pipeline success rate target
- Automatic retry with exponential backoff on transient failures
- Transactional database operations with rollback capability
- Idempotent design allowing safe re-execution

### 3.3 Performance

- Target total execution time: <10 minutes
- API call and CSV retrieval: <2 minutes
- Delta calculation: <3 minutes
- Database operations: <5 minutes

### 3.4 Latency

- 1 business day latency is acceptable for data synchronization
- Daily scheduled execution meets business requirements
- No real-time synchronization needs identified

### 3.5 Cost Efficiency

- Minimize compute resources via delta-based processing
- Reuse existing Airflow infrastructure
- Optimized SQL queries to reduce database load

### 3.6 Maintainability

- Clear, well-documented code structure
- Centralized configuration management
- Modular task design for easy updates
- Comprehensive logging for troubleshooting

---

## 4. High-Level Architecture

### 4.1 System Components

```
┌─────────────────────────────┐     ┌──────────────────────────────────────────────────────┐
│         Cloud               │     │                    On-Premise                         │
│  ┌───────────────────────┐  │     │  ┌────────────────────────────────────────────────┐  │
│  │       Polaris         │  │     │  │           Apache Airflow 2.x+                  │  │
│  │    Report API (REST)  │──┼─CSV─┼─>│  ┌──────────────┐  ┌──────────────┐           │  │
│  └───────────────────────┘  │     │  │  │Call Polaris  │  │  Parse CSV   │           │  │
│                             │     │  │  │    API       │  │              │           │  │
│                             │     │  │  └──────────────┘  └──────────────┘           │  │
│                             │     │  │  ┌──────────────┐  ┌──────────────┐           │  │
│                             │     │  │  │Generate      │  │Compare &     │           │  │
│                             │     │  │  │  SHA256      │  │   Sync       │──────┐    │  │
│                             │     │  │  └──────────────┘  └──────────────┘      │    │  │
│                             │     │  │       ┌──────────────┐                   │    │  │
│                             │     │  │       │Update        │                   │    │  │
│                             │     │  │       │  Snapshot    │                   │    │  │
│                             │     │  │       └──────────────┘                   │    │  │
│                             │     │  └────────────────────────────────────────────────┘  │
│                             │     │                                             │ODBC    │
│                             │     │  ┌────────────────────────────────────┐     v        │
│                             │     │  │       SQL Server 2016+             │              │
│                             │     │  │  ┌──────────────────────────────┐  │              │
│                             │     │  │  │ dbo.rp_source_resources      │  │              │
│                             │     │  │  │      (Target Table)          │  │              │
│                             │     │  │  │ dbo.vw_d_staff_replica       │  │              │
│                             │     │  │  │      (Active Users View)     │  │              │
│                             │     │  │  └──────────────────────────────┘  │              │
│                             │     │  └────────────────────────────────────┘              │
│                             │     │                                                      │
│                             │     │  ┌────────────────────┐                              │
│                             │     │  │    SMTP Server     │                              │
│                             │     │  │   (Notifications)  │                              │
│                             │     │  └────────────────────┘                              │
└─────────────────────────────┘     └──────────────────────────────────────────────────────┘
```

### 4.2 Component Description

| Component | Type | Location | Description |
|-----------|------|----------|-------------|
| Polaris Report API | REST API | Cloud | Source system providing user data via HTTP endpoint |
| CSV Response | CSV Format | In-Memory | Temporary data structure holding API response |
| Apache Airflow 2.x+ | Orchestration | On-Premise | Workflow orchestration and task scheduling platform |
| Resource Planner DB | SQL Server 2016+ | On-Premise | Target database (configurable via `mssql_database`) |
| `dbo.rp_source_resources` | SQL Table | On-Premise | Target table for synced users (id, source_system, resource_id) |
| `dbo.vw_d_staff_replica` | SQL View | On-Premise | Active users view for eligibility filtering |

### 4.3 Integration Points

| Integration | Protocol | Description |
|-------------|----------|-------------|
| Polaris → Airflow | REST API (HTTPS) | API call returning CSV-formatted user data |
| Airflow → SQL Server | ODBC Driver | Database connectivity for read/write operations |
| Airflow Metadata | Internal | DAG execution history, task logs, run statistics |
| Email Notifications | SMTP | Alert delivery for failures and completion status |

---

## 5. Airflow-Specific Design

### 5.1 DAG Structure

```
                        ┌───────────────────────────────┐
                        │      can_run_batch_task       │
                        │  (Check Airflow Variable)     │
                        └───────────────┬───────────────┘
                                        │
                  ┌─────────────────────┼─────────────────────┐
                  │ Yes                 │                     │ No
                  v                     │                     v
    ┌─────────────────────────┐         │     ┌───────────────────────────────┐
    │       batch_task        │         │     │   get_eligible_employee_ids   │
    │  (BatchTaskRunOperator) │         │     │   (SQL: active_users_view     │
    └───────────┬─────────────┘         │     │    NOT IN target_table)       │
                │                       │     └───────────────┬───────────────┘
                │                       │                     │
                │                       │                     v
                │                       │     ┌───────────────────────────────┐
                │                       │     │      get_report_details       │
                │                       │     │   (Get Polaris Report URI)    │
                │                       │     └───────────────┬───────────────┘
                │                       │                     │
                │                       │                     v
                │                       │     ┌───────────────────────────────┐
                │                       │     │      export_users_report      │
                │                       │     │   (Run Polaris Report API)    │
                │                       │     └───────────────┬───────────────┘
                │                       │                     │
                │                       │                     v
                │                       │     ┌───────────────────────────────┐
                │                       │     │       load_user_report        │
                │                       │     │      (Parse CSV Response)     │
                │                       │     └───────────────┬───────────────┘
                │                       │                     │
                │                       │                     v
                │                       │     ┌───────────────────────────────┐
                │                       │     │     create_user_collection    │
                │                       │     │   (Create Polaris User Set)   │
                │                       │     └───────────────┬───────────────┘
                │                       │                     │
                │                       │                     v
                │                       │     ┌───────────────────────────────┐
                │                       │     │     identify_users_to_add     │
                │                       │     │  (Filter: Polaris users IN    │
                │                       │     │   eligible_ids from SQL)      │
                │                       │     └───────────────┬───────────────┘
                │                       │                     │
                │                       │                     v
                │                       │     ┌───────────────────────────────┐
                │                       │     │     has_any_users_to_add      │
                │                       │     │        (IfOperator)           │
                │                       │     └───────────────┬───────────────┘
                │                       │                     │
                │                       │           ┌────────┴────────┐
                │                       │           │ Yes             │ No
                │                       │           v                 │
                │                       │     ┌─────────────────────────────────┐
                │                       │     │ insert_users_to_rp_source_      │
                │                       │     │ resources (Batch Insert)        │
                │                       │     └───────────────┬─────────────────┘
                │                       │                     │
                v                       v                     v
              ┌─────────────────────────────────────────────────┐
              │                    end_task                     │
              └─────────────────────────────────────────────────┘
```

### 5.1.1 Task Descriptions

| Task ID | Operator | Description |
|---------|----------|-------------|
| `can_run_batch_task` | IfOperator | Checks Airflow Variable to determine if batch mode is enabled |
| `batch_task` | BatchTaskRunOperator | Orchestrates the batch execution flow (when enabled) |
| `get_eligible_employee_ids` | SQLExecuteQueryOperator | Single optimized SQL query to get users in `active_users_view` but NOT in `target_table` |
| `get_report_details` | RepliconReportDetailsOperator | Retrieves Polaris report URI |
| `export_users_report` | run_report2 | Executes Polaris Report API and retrieves CSV |
| `load_user_report` | LoadCSVFileOperator | Parses CSV response into records |
| `create_user_collection` | CreateCollectionOperator | Creates in-memory collection of Polaris users |
| `identify_users_to_add` | PythonOperator | Filters Polaris users against eligible IDs from SQL |
| `has_any_users_to_add` | IfOperator | Conditional branch based on users to process |
| `insert_users_to_rp_source_resources` | PythonOperator | Batch inserts users using MsSqlHook.insert_rows() |
| `end_task` | EmptyOperator | DAG completion marker |

### 5.2 DAG Configuration

```
DAG_ID: resource_planner_user_export_{instance}
SCHEDULE: Configurable per instance (default: None)
CATCHUP: False
MAX_ACTIVE_RUNS: 1 (configurable)
```

### 5.2.1 Instance Configuration

Configuration is managed via instance files located at `instances/{env}.py`:

```python
# instances/dev.py example
instance = "dev"
environment = 'pre-production'

# Replicon configuration
company_key = 'Repliconpincstream6dev'
replicon_conn_id = 'replicon_Repliconpincstream6dev_replicon'

# Feature flag for batch task (Airflow Variable name)
resource_planner_user_export_enable_batch_task = f"resource_planner_user_export_enable_batch_task_{instance}"

# MS SQL configuration
mssql_conn_id = 'resource_planning_database_connection'
mssql_database = 'ResourcePlanning_development'
target_table = 'dbo.rp_source_resources'
active_users_view = 'dbo.vw_d_staff_replica'

# DAG configuration
start_date = datetime(2025, 1, 1)
max_active_runs = 1
schedule_interval = None
```

### 5.2.2 Configuration Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `instance` | String | Instance identifier used in DAG ID |
| `environment` | String | Environment name (e.g., pre-production, production) |
| `company_key` | String | Replicon company key |
| `replicon_conn_id` | String | Airflow connection ID for Replicon API |
| `resource_planner_user_export_enable_batch_task` | String | Airflow Variable name to enable/disable batch mode |
| `mssql_conn_id` | String | Airflow connection ID for SQL Server |
| `mssql_database` | String | Target database name |
| `target_table` | String | Target table for user inserts (e.g., `dbo.rp_source_resources`) |
| `active_users_view` | String | View containing active users (e.g., `dbo.vw_d_staff_replica`) |
| `start_date` | datetime | DAG start date |
| `max_active_runs` | Integer | Maximum concurrent DAG runs |
| `schedule_interval` | String/None | Cron expression or None for manual |

### 5.3 Scheduling Strategy

- **Schedule:** Daily at 2:00 AM (off-peak hours to minimize impact)
- **API Availability:** Polaris Report API available 24/7
- **Catchup:** Disabled to prevent backlog processing
- **Concurrency:** Single active run to prevent race conditions

---

## 6. Data Handling

### 6.1 Data Ingestion

#### CSV Response Specifications

| Property | Value |
|----------|-------|
| Encoding | UTF-8 with header row |
| Delimiter | Comma (,) |
| Columns | EmployeeID, Status |
| Source | Polaris Report API |

#### Ingestion Process

1. Call Polaris Report API endpoint with authentication
2. Validate HTTP response code (expect 200 OK)
3. Parse CSV response body (check for empty response)
4. Validate CSV structure matches expected schema
5. Load data to DataFrame or staging structure
6. Validate data types for each column

### 6.2 Data Transformation

#### SHA256 Hash Generation

```
FOR each record in CSV:
    concat_value = EmployeeID + "|" + Status
    record_hash = SHA256(concat_value)
    STORE record_hash with EmployeeID
```

#### Delta Calculation (Optimized SQL Approach)

The delta calculation is optimized by pushing the filtering logic to the database level:

**Step 1: Get Eligible Employee IDs (SQL Query)**

Table names are configurable via instance config (`config.active_users_view`, `config.target_table`):

```sql
SELECT vdsr.employee_id as EmployeeID
FROM {config.active_users_view} vdsr
WHERE NOT EXISTS (
    SELECT 1 FROM {config.target_table} rsr
    WHERE rsr.resource_id = vdsr.employee_id
    AND rsr.source_system = 'Polaris'
)
```

This query returns only employee IDs that:
- Exist in `active_users_view` (active users)
- Do NOT exist in `target_table` (not already synced)

**Step 2: Filter Polaris Users (Python)**
```python
def identify_users_to_add_function():
    # Master data from Replicon report
    polaris_users = load_all_records(result(create_user_collection.task_id))

    # Eligible employee IDs from optimized SQL query
    eligible_ids = {row[0] for row in result(get_eligible_employee_ids.task_id)}

    # Filter: Polaris users that are eligible for insertion
    return [user for user in polaris_users if user['Employee_ID'] in eligible_ids]
```

**Benefits of this approach:**
- Single database round-trip instead of multiple queries
- Database handles the set difference operation (more efficient)
- Reduced data transfer over the network
- Simpler Python code with O(1) set lookup
- Configurable table names per environment

### 6.3 Data Validation

| Validation Type | Check | Action on Failure |
|-----------------|-------|-------------------|
| File Existence | API returns valid response | Retry, then fail pipeline |
| File Format | Valid CSV structure | Fail pipeline with error |
| Empty File | CSV contains data rows | Log warning, skip processing |
| EmployeeID | Non-null, unique values | Log invalid records, fail pipeline |
| Status | Non-null values | Log invalid records, fail pipeline |
| Duplicates | No duplicate EmployeeIDs | Log duplicates, use first occurrence |

---

## 7. Error Handling, Observability, and Alerting

### 7.1 Error Handling Strategy

| Error Category | Examples | Handling Strategy |
|----------------|----------|-------------------|
| API Not Available | Polaris API down, 5xx errors | Retry 3 times with exponential backoff, then fail pipeline |
| Data Quality Issues | Null EmployeeID, invalid format | Log invalid records with details, fail pipeline |
| Database Connection | SQL Server unreachable, timeout | Retry with exponential backoff up to 30 minutes |
| Transaction Failure | Insert fails mid-operation | Rollback entire transaction, fail task |
| Network Issues | Connection timeout, DNS failure | Automatic retry with backoff |

### 7.2 Transaction Management

- **Add Operations:** Executed as batch insert using `MsSqlHook.insert_rows()`
- **Idempotent Design:** Safe to re-execute; SQL query excludes already-synced users
- **Configurable Target:** Table name is configurable via `config.target_table`

### 7.3 Insert Operation

The `insert_users_to_rp_source_resources` task performs batch insert using Airflow's MsSqlHook:

```python
def insert_users_to_rp_source_resources_function():
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

    users_to_add = result(identify_users_to_add.task_id)
    if not users_to_add:
        return 0

    hook = MsSqlHook(mssql_conn_id=config.mssql_conn_id, schema=config.mssql_database)

    # Build batch insert values
    values = [(user['Employee_ID'], 'Polaris') for user in users_to_add]

    # Execute batch insert
    hook.insert_rows(
        table=config.target_table,
        rows=values,
        target_fields=['resource_id', 'source_system']
    )

    return len(values)
```

**Return Value:**
- Returns the count of inserted users (integer)

---

## Conclusion

This design document outlines a production-grade Apache Airflow pipeline for synchronizing user data from Polaris SaaS to Resource Planner. The solution leverages SHA256 hashing for efficient delta detection, implements robust error handling with transactional integrity, and provides comprehensive logging for operational visibility.

The phased approach allows for immediate value delivery with user additions and disabled user logging, while deferring more complex deletion operations to Phase 2 with enhanced safety controls. The design prioritizes data consistency and reliability while maintaining operational simplicity.

### Next Steps

1. Review and approve design document with stakeholders
2. Obtain API credentials and test connectivity to Polaris Report API
3. Create Reference Snapshot File schema in SQL Server
4. Develop and unit test individual Airflow tasks
5. Configure SMTP settings for email notifications
6. Deploy to development environment and perform integration testing
7. Execute user acceptance testing with sample data

---

| Property | Value |
|----------|-------|
| **Version** | 1.2 |
| **Date** | January 27, 2026 |
| **Status** | Implementation Complete |

### Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | January 22, 2026 | Initial design document |
| 1.1 | January 27, 2026 | Updated with implementation details: optimized SQL query, instance configuration |
| 1.2 | January 27, 2026 | Added `can_run_batch_task` feature flag, configurable table names (`target_table`, `active_users_view`), simplified batch insert |

*© 2026 Data Engineering Team. All rights reserved.*
