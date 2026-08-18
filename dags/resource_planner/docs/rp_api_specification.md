# Resource Planner API Specification

**Version:** 1.9  
**Date:** April 23, 2026  
**Author:** Data Engineering Team  
**Audience:** Resource Planner Backend Team  

---

## 1. Overview

### Purpose

This document defines the REST API services required by the Polaris-to-Resource Planner integration pipelines. These APIs replace direct MSSQL database connectivity (which is no longer possible due to network restrictions) and serve as the data interface between the Airflow integration layer and the Resource Planner database.

### Context

The Polaris integration pipelines extract data from Polaris SaaS (via Replicon APIs) and synchronize it to the Resource Planner database. The pipelines currently perform direct SQL operations (SELECT, INSERT, UPDATE, DELETE) against multiple RP tables. Each SQL operation maps to one API endpoint defined in this document.

### Consumers

| Pipeline | Description |
|----------|-------------|
| User Export | Syncs Polaris users to RP |
| Task Resource Allocation Export (Batch) | Bulk sync of task allocations (one-time initial load) |
| Task Resource Allocation Export (Webhooks) | Real-time allocation sync (created/modified/deleted) |
| Project Task Export (Bulk) | Bulk sync of projects and tasks (one-time initial load) |
| Project Task Export (Delta) | Incremental sync of projects and tasks |
| Time Off Export | Syncs time-off bookings |
| Time Off Type Export | Syncs time-off type definitions |
| Confirmed Bookings Export | Reverse sync: RP confirmed bookings → Polaris |

> **Note:** The bulk/batch pipelines (Task Resource Allocation Export Batch, Project Task Export Bulk) are designed for a one-time initial data load. After the initial load, ongoing synchronization is handled by the delta/webhook pipelines. The APIs must support both bulk and incremental payloads, but bulk-scale traffic will only occur during the initial migration.

### Naming Convention

All API endpoint paths use **camelCase**. JSON request/response field names use **camelCase**.

### URL Prefix

All endpoints in this document are mounted under **`/api/v1/rp/`**. The `rp` segment namespaces Resource Planner endpoints so the same gateway can host other target systems (e.g. `/api/v1/vision/...`) without collision.

### General Requirements

- **Protocol:** HTTPS (REST)
- **Content-Type:** `application/json`
- **Authentication:** Required. Preferred method: **Bearer token** in `Authorization` header. Final implementation to be determined by the RP team.
- **Error Responses:** Standard HTTP status codes with JSON error body
- **Idempotency:** Write operations should be safe to retry
- **Concurrency:** Webhook-driven pipelines can generate burst traffic (e.g., multiple allocation changes in quick succession). The API must handle concurrent requests gracefully without hard rate limits.

### Target Table Override (All Endpoints)

All endpoints accept an optional `targetTable` parameter that overrides the default production table. This enables testing pipelines against isolated tables without affecting production data.

**Parameter:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | Production table for the endpoint | Override table name. **Must start with `dummy_`** — the API prepends `dbo.` internally. API must reject any value not starting with `dummy_` with `400 Bad Request`. |

**Validation rules:**
- Must start with `dummy_` (case-insensitive)
- Must contain only alphanumeric characters and underscores after the prefix
- Must not contain SQL keywords, semicolons, dots, or special characters
- The API internally prepends `dbo.` to form the full table name (e.g., caller sends `dummy_rp_source` → API uses `dbo.dummy_rp_source`)
- If validation fails, return `400 Bad Request` with error message

**Default tables per endpoint:**

| Endpoint | Default Table |
|----------|--------------|
| `/api/v1/rp/resources` | `rp_resources` |
| `/api/v1/rp/users` | `users` |
| `/api/v1/rp/laborCodes` | `rp_labor_code` |
| `/api/v1/rp/eligiblePolarisEmployees` | `vw_d_staff_replica` + `rp_source_resources` |
| `/api/v1/rp/sourceResources` | `rp_source_resources` |
| `/api/v1/rp/sourceAllocations` | `rp_source` |
| `/api/v1/rp/sourceTimeCodesProjectTasks` | `rp_source_time_codes` |
| `/api/v1/rp/sourceTimeCodesTimeOffTypes` | `rp_source_time_codes` |
| `/api/v1/rp/confirmedBookings/batches` | `vw_rp_integration_confirmed_source` |
| `/api/v1/rp/confirmedBookings` | `vw_rp_integration_confirmed_source` |
| `/api/v1/rp/confirmedBookings/failures` | `rp_export_sync_failures` (GET/POST/PATCH all share this table) |

**Example — write to test table:**

```json
{
  "targetTable": "dummy_rp_source",
  "records": [ ... ]
}
```

**Example — lookup from test table:**

```json
{
  "targetTable": "dummy_rp_resources",
  "employeeIds": ["EMP001"]
}
```

**Example — rejected (invalid prefix):**

```json
{
  "targetTable": "rp_source"
}
```
Response: `400 Bad Request` — `"targetTable must start with 'dummy_'"`

---

### Standard Error Response

```json
{
  "error": {
    "code": "<HTTP_STATUS_CODE>",
    "message": "<Human-readable error description>",
    "details": "<Optional additional context>"
  }
}
```

---

## 2. Database Tables Overview

The APIs expose operations on the following RP database tables:

| Table/View | Type | Purpose |
|------------|------|---------|
| `rp_resources` | Table | Resource registry (resource_id, users_user_id, employee_id) |
| `users` | Table | User ID to employee ID mapping |
| `vw_d_staff_replica` | View | Active users (eligibility check for user export) |
| `rp_labor_code` | Table | Labor code definitions (maps Polaris roles to RP labor codes) |
| `rp_source` | Table | Allocation and time-off daily rows (the main data table) |
| `rp_source_time_codes` | Table | Project, task, and time-off type definitions |
| `rp_source_resources` | Table | Synced user records from Polaris |
| `vw_rp_integration_confirmed_source` | View | Joined view of RP confirmed bookings + `rp_source` — used by the reverse-sync export to detect changes and build Polaris mutations |
| `rp_export_sync_failures` | Table | Failure log for the confirmed-bookings export retry pipeline (see migration `001_rp_export_sync_failures.sql`) |

> **Note:** All table names above omit the `dbo.` schema prefix. The API prepends `dbo.` internally.

---

## 3. API Endpoints

### 3.1 Resources

---

#### `POST /api/v1/rp/resources`

**Purpose:** Retrieve resource registry entries matching a list of employee IDs. Supports both single lookups (webhook events) and bulk lookups (batch DAGs).

**Used by:** User Export, Task Resource Allocation Webhooks (new, modified)

**Current SQL:**
```sql
SELECT resource_id, users_user_id, employee_id FROM dbo.rp_resources
```

**Request:**

```json
{
  "employeeIds": ["EMP001", "EMP002"],
  "targetTable": "dummy_rp_resources"
}
```

Pass an empty array `[]` for `employeeIds` to retrieve all resources (for batch DAGs).

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `employeeIds` | array of strings | Yes | — | List of employee IDs to look up. Empty array `[]` returns all resources. |
| `targetTable` | string | No | `rp_resources` | Override table. Must start with `dummy_`. |

**Response:**

```json
{
  "data": [
    {
      "resourceId": "12345",
      "usersUserId": "678",
      "employeeId": "EMP001"
    }
  ],
  "count": 1
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `resourceId` | string | Unique resource identifier in RP |
| `usersUserId` | string | User ID in RP (maps to users.user_id) |
| `employeeId` | string | Employee ID from Polaris |

---

### 3.2 Users

---

#### `POST /api/v1/rp/users`

**Purpose:** Retrieve user_id to employee_id mappings for a list of employee IDs. Supports both single lookups and bulk lookups.

**Used by:** Task Resource Allocation Export (batch), Time Off Export

**Current SQL:**
```sql
SELECT user_id, employee_id FROM dbo.users
```

**Request:**

```json
{
  "employeeIds": ["EMP001", "EMP002"],
  "targetTable": "dummy_users"
}
```

Pass an empty array `[]` for `employeeIds` to retrieve all users (for batch DAGs).

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `employeeIds` | array of strings | Yes | — | List of employee IDs to look up. Empty array `[]` returns all users. |
| `targetTable` | string | No | `users` | Override table. Must start with `dummy_`. |

**Response:**

```json
{
  "data": [
    {
      "userId": "678",
      "employeeId": "EMP001"
    }
  ],
  "count": 1
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `userId` | string | Internal RP user identifier |
| `employeeId` | string | Employee ID from Polaris |

---

### 3.3 Labor Codes

---

#### `POST /api/v1/rp/laborCodes`

**Purpose:** Retrieve active Polaris labor code mappings for a list of Polaris role names. Supports both single lookups (webhook events) and bulk lookups (batch DAGs).

**Used by:** Task Resource Allocation Export (batch + webhooks)

**Current SQL:**
```sql
SELECT labor_code_id, mapping_to_source
FROM rp_labor_code
WHERE source_system = 'Polaris' AND is_active = 1
```

**Request:**

```json
{
  "mappingToSourceValues": ["Senior Consultant", "Project Manager"],
  "sourceSystem": "Polaris",
  "isActive": 1,
  "targetTable": "dummy_rp_labor_code"
}
```

Pass an empty array `[]` for `mappingToSourceValues` to retrieve all matching labor codes (for batch DAGs).

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mappingToSourceValues` | array of strings | Yes | — | List of Polaris "Primary Role (Current)" values to look up. Empty array `[]` returns all matching codes. |
| `sourceSystem` | string | No | `"Polaris"` | Filter by source system. |
| `isActive` | integer (0 or 1) | No | `1` | Filter by active status. `1` = active only, `0` = inactive only. |
| `targetTable` | string | No | `rp_labor_code` | Override table. Must start with `dummy_`. |

**Response:**

```json
{
  "data": [
    {
      "laborCodeId": "LC001",
      "mappingToSource": "Senior Consultant"
    }
  ],
  "count": 1
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `laborCodeId` | string | RP labor code identifier |
| `mappingToSource` | string | Polaris "Primary Role (Current)" value that maps to this labor code |

---

### 3.4 Eligible Employees

---

#### `GET /api/v1/rp/eligiblePolarisEmployees`

**Purpose:** Retrieve active employees that are NOT already synced to the source resources table. Used to identify new users to add.

**Used by:** User Export

**Current SQL:**
```sql
SELECT vdsr.employee_id as EmployeeID
FROM dbo.vw_d_staff_replica vdsr
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.rp_source_resources rsr
    WHERE rsr.employee_id = vdsr.employee_id AND rsr.source_system = 'Polaris'
)
```

**Request Parameters (query string):**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `targetTable` | string | No | `vw_d_staff_replica` | Override the active users view. Must start with `dummy_`. |
| `sourceResourcesTable` | string | No | `rp_source_resources` | Override the source resources table for the NOT EXISTS check. Must start with `dummy_`. |

**Example Request:**
```
GET /api/v1/rp/eligiblePolarisEmployees?targetTable=dummy_vw_d_staff_replica&sourceResourcesTable=dummy_rp_source_resources
```

**Response:**

```json
{
  "data": [
    {
      "employeeId": "EMP001"
    }
  ],
  "count": 1
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `employeeId` | string | Employee ID present in active users view but not yet in source resources |

**Notes:**
- The API must internally join the active users view with the source resources table to exclude already-synced users where `source_system = 'Polaris'`.
- Returns only the delta (new users not yet synced).

---

### 3.5 Source Resources (User Sync)

---

#### `POST /api/v1/rp/sourceResources`

**Purpose:** Insert new user records into the source resources table.

**Used by:** User Export

**Request:**

```json
{
  "targetTable": "dummy_rp_source_resources",
  "records": [
    {
      "employeeId": "EMP001",
      "sourceSystem": "Polaris",
      "usersUserId": "678",
      "resourceId": "12345"
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source_resources` | Override table. Must start with `dummy_`. |
| `records[].employeeId` | string | Yes | — | Employee ID from Polaris |
| `records[].sourceSystem` | string | No | `"Polaris"` | Source system identifier |
| `records[].usersUserId` | string | No | — | RP user ID (looked up from `POST /api/v1/rp/resources` by caller) |
| `records[].resourceId` | string | No | — | RP resource ID (looked up from `POST /api/v1/rp/resources` by caller) |

**Response:**

```json
{
  "insertedCount": 1,
  "status": "success"
}
```

**Notes:**
- Batch payload — array of 1 to N records.
- Operation should be transactional (all-or-nothing).
- The caller must resolve `usersUserId` and `resourceId` by calling `POST /api/v1/rp/resources` first, then pass the values here.
- If no match found in resources lookup, pass empty string for `usersUserId` and `resourceId`.

---

### 3.6 Source Allocations (rp_source)

This table stores daily allocation rows, time-off booking rows, and related data. Multiple operations are needed.

---

#### `POST /api/v1/rp/sourceAllocations`

**Purpose:** Insert allocation daily rows (task resource allocations or time-off bookings).

**Used by:** Task Resource Allocation Export (batch + webhooks new/modified), Time Off Export

**Current SQL:**
```sql
INSERT INTO dbo.rp_source
(source_booking_id, source_system, time_code, labor_code,
 users_user_id, hours, work_date, hours_type,
 last_updated_date, employee_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
```

**Request:**

```json
{
  "targetTable": "dummy_rp_source",
  "records": [
    {
      "sourceBookingId": "470324c1-000b-410e-b699-4b47ea8b0538_00001",
      "sourceSystem": "Polaris",
      "timeCode": "57060~200245",
      "laborCode": "LC001",
      "usersUserId": "678",
      "hours": 8.0,
      "workDate": "2026-04-14",
      "hoursType": "Client Project",
      "lastUpdatedDate": "2026-04-13T15:30:00Z",
      "employeeId": "EMP001"
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source` | Override table. Must start with `dummy_`. |
| `records[].sourceBookingId` | string | Yes | — | Unique row ID: `{allocation_uuid}_{day_index:05d}` for allocations, `{booking_id}_{counter:03d}` for time-off |
| `records[].sourceSystem` | string | Yes | — | Always `"Polaris"` |
| `records[].timeCode` | string | Yes | — | `{project_id}~{task_id}` for allocations, timeoff_type_uri segment for time-off |
| `records[].laborCode` | string | No | — | Labor code ID from rp_labor_code (empty for time-off records) |
| `records[].usersUserId` | string | No | — | RP user ID |
| `records[].hours` | decimal | Yes | — | Hours for this day |
| `records[].workDate` | string (date) | Yes | — | Calendar date in `YYYY-MM-DD` format |
| `records[].hoursType` | string | Yes | — | `"Client Project"`, `"Internal Non-Billable"`, `"Holiday"`, `"Absence"`, or raw type value |
| `records[].lastUpdatedDate` | string (datetime) | No | — | Last modification timestamp from Polaris |
| `records[].employeeId` | string | Yes | — | Employee ID from Polaris |

**Response:**

```json
{
  "insertedCount": 150,
  "status": "success"
}
```

**Notes:**
- Batch payload — can contain 1 to thousands of records.
- Operation should be transactional (all-or-nothing).
- For batch DAG runs, payloads can be up to ~5,000 rows. API should handle this efficiently.

---

#### `PUT /api/v1/rp/sourceAllocations`

**Purpose:** Atomically replace all daily rows for a given allocation. Internally performs DELETE + INSERT in a single transaction. Eliminates the data loss risk of separate DELETE and POST calls.

**Used by:** Task Resource Allocation Webhooks (created, modified, deleted)

**Request:**

```json
{
  "targetTable": "dummy_rp_source",
  "replacements": [
    {
      "sourceBookingIdPrefix": "470324c1-000b-410e-b699-4b47ea8b0538",
      "sourceSystem": "Polaris",
      "records": [
        {
          "sourceBookingId": "470324c1-000b-410e-b699-4b47ea8b0538_00001",
          "sourceSystem": "Polaris",
          "timeCode": "57060~200245",
          "laborCode": "LC001",
          "usersUserId": "678",
          "hours": 8.0,
          "workDate": "2026-04-14",
          "hoursType": "Client Project",
          "lastUpdatedDate": "2026-04-13T15:30:00Z",
          "employeeId": "EMP001"
        }
      ]
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source` | Override table. Must start with `dummy_`. |
| `replacements[].sourceBookingIdPrefix` | string | Yes | — | The allocation UUID. All existing rows with this prefix are deleted first. |
| `replacements[].sourceSystem` | string | Yes | — | Always `"Polaris"` |
| `replacements[].records` | array of objects | Yes | — | New rows to insert after deletion. **Empty array `[]` = delete only** (used for deleted allocations). Same fields as POST sourceAllocations records. |

**Behavior by event type:**

| Event | records | Effect |
|-------|---------|--------|
| Created | populated | No existing rows to delete, inserts new rows |
| Modified | populated | Deletes old rows, inserts updated rows |
| Deleted | `[]` (empty) | Deletes all rows, inserts nothing |

**Response:**

```json
{
  "deletedCount": 30,
  "insertedCount": 35,
  "status": "success"
}
```

**Notes:**
- Batch payload — array of replacements (one per allocation).
- Each replacement is atomic: DELETE + INSERT in a single transaction.
- This is the **preferred endpoint for webhook-driven pipelines** for CREATED/MODIFIED events.
- **For DELETED events, use `PATCH` (soft-delete) instead.** PUT is for replace-with-new-data only.

---

#### `PATCH /api/v1/rp/sourceAllocations`

**Purpose:** Soft-delete allocations by setting `hours = 0` for all existing rows matching each prefix. Preserves rows for audit/reporting instead of removing them.

**Used by:** Task Resource Allocation Webhooks (DELETED event), Task Resource Allocation Export batch (allocations removed from Polaris)

**Current SQL:**
```sql
UPDATE dbo.rp_source
SET hours = 0
WHERE source_booking_id LIKE '{allocation_uuid}_%' AND source_system = 'Polaris'
```

**Request:**

```json
{
  "targetTable": "dummy_rp_source",
  "markDeleted": [
    {
      "sourceBookingIdPrefix": "470324c1-000b-410e-b699-4b47ea8b0538",
      "sourceSystem": "Polaris"
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source` | Override table. Must start with `dummy_`. |
| `markDeleted[].sourceBookingIdPrefix` | string | Yes | — | The allocation UUID. API sets hours=0 for all rows where `source_booking_id` starts with this value followed by `_`. |
| `markDeleted[].sourceSystem` | string | Yes | — | Always `"Polaris"` |

**Response:**

```json
{
  "updatedCount": 30,
  "status": "success"
}
```

**Notes:**
- Batch payload — array of soft-delete requests (one per allocation).
- Rows are updated in place with `hours = 0`; nothing is physically deleted.
- This is used in place of DELETE for allocations to preserve audit history.

---

#### `DELETE /api/v1/rp/sourceAllocations`

**Purpose:** Hard-delete rows (physical removal). Used primarily for time-off bookings which don't require soft-delete semantics.

**Used by:** Time Off Export

> **Note:** For task resource allocations, use `PATCH` (soft-delete) instead. DELETE should not be called for allocations from Polaris — they should be marked with `hours = 0` via PATCH to preserve audit trail.

**Current SQL (allocation):**
```sql
DELETE FROM dbo.rp_source
WHERE source_booking_id LIKE '{allocation_uuid}_%' AND source_system = 'Polaris'
```

**Current SQL (time-off):**
```sql
DELETE FROM dbo.rp_source
WHERE source_system = 'Polaris'
  AND source_booking_id LIKE '{time_off_id}_%'
  AND hours_type IN ('Absence', 'Holiday')
```

**Request:**

```json
{
  "targetTable": "dummy_rp_source",
  "deletions": [
    {
      "sourceBookingIdPrefix": "470324c1-000b-410e-b699-4b47ea8b0538",
      "sourceSystem": "Polaris",
      "hoursTypeFilter": null
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source` | Override table. Must start with `dummy_`. |
| `deletions[].sourceBookingIdPrefix` | string | Yes | — | The allocation UUID or booking ID prefix. API should delete all rows where `source_booking_id` starts with this value followed by `_`. |
| `deletions[].sourceSystem` | string | Yes | — | Always `"Polaris"` |
| `deletions[].hoursTypeFilter` | array of strings or null | No | — | If provided, only delete rows matching these hours_type values (e.g., `["Absence", "Holiday"]`). If null, delete all matching rows regardless of hours_type. |

**Response:**

```json
{
  "deletedCount": 45,
  "status": "success"
}
```

**Notes:**
- Batch payload — array of deletion requests.
- Each deletion removes all daily rows for one allocation/booking.
- The `hoursTypeFilter` is only used by the Time Off Export to avoid accidentally deleting allocation rows that share the same table.
- Operation should be transactional.

---

### 3.7 Source Time Codes — Project Tasks (rp_source_time_codes)

This endpoint handles project and task definitions stored in `rp_source_time_codes` where `type IN ('project', 'task')`.

> **Note:** No GET endpoint is needed. The Polaris audit report provides the action (`Added`, `Modified`, `Delete`) per record. The upsert endpoint handles both insert and update based on whether the `timeCode` exists. Deletes are handled by the separate DELETE endpoint.

---

#### `PUT /api/v1/rp/sourceTimeCodesProjectTasks`

**Purpose:** Upsert (insert or update) project and task records. If a record with the same `sourceSystem` + `timeCode` + `type` exists, it is updated. Otherwise, it is inserted.

**Used by:** Project Task Export (bulk + delta)

**Current SQL (insert):**
```sql
INSERT INTO dbo.rp_source_time_codes
(source_system, parent_time_code, time_code, time_code_name,
 parent_time_code_name, project_manager_id, type, task_level, time_entry_enabled)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
```

**Current SQL (update):**
```sql
UPDATE dbo.rp_source_time_codes
SET parent_time_code = %s, time_code_name = %s, parent_time_code_name = %s,
    project_manager_id = %s, task_level = %s, time_entry_enabled = %s
WHERE source_system = 'Polaris' AND time_code = %s AND type = %s
```

**Request:**

```json
{
  "targetTable": "dummy_rp_source_time_codes",
  "records": [
    {
      "sourceSystem": "Polaris",
      "parentTimeCode": "57060",
      "timeCode": "57060~200245",
      "timeCodeName": "RPT-1~Implementation~Phase 1",
      "parentTimeCodeName": "RPT-1",
      "projectManagerId": "EMP001",
      "type": "task",
      "taskLevel": 2,
      "timeEntryEnabled": true
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source_time_codes` | Override table. Must start with `dummy_`. |
| `records[].sourceSystem` | string | Yes | — | Always `"Polaris"` |
| `records[].parentTimeCode` | string | Yes | — | Parent project ID |
| `records[].timeCode` | string | Yes | — | `{project_id}` for projects, `{project_id}~{task_id}` for tasks |
| `records[].timeCodeName` | string | Yes | — | Display name (max 255 characters) |
| `records[].parentTimeCodeName` | string | Yes | — | Parent project name |
| `records[].projectManagerId` | string | No | — | Employee ID of project manager |
| `records[].type` | string | Yes | — | `"project"` or `"task"` |
| `records[].taskLevel` | integer | Yes | — | Hierarchy depth |
| `records[].timeEntryEnabled` | boolean | Yes | — | Whether time entry is allowed |

**Match key:** `sourceSystem` + `timeCode` + `type`
- If a record with this key exists → **update** all other fields
- If no record with this key exists → **insert** as new record

**Response:**

```json
{
  "insertedCount": 20,
  "updatedCount": 5,
  "status": "success"
}
```

**Notes:**
- Batch payload — can contain hundreds of records (mix of new and existing).
- Operation should be transactional (all-or-nothing).
- This single endpoint replaces the need for separate insert and update calls.
- **Implementation:** the reference backend executes one `MERGE … WITH (HOLDLOCK) … OUTPUT $action` statement per batch of 230 records rather than a SELECT-then-INSERT/UPDATE pattern. See §8.2 for the SQL shape and rationale. Any implementation is acceptable as long as the match-key contract and transactional guarantee above are met.

---

#### `DELETE /api/v1/rp/sourceTimeCodesProjectTasks`

**Purpose:** Delete project and/or task records that no longer exist in Polaris.

**Used by:** Project Task Export (delta delete processor)

**Current SQL patterns:**

Pattern 1 — Delete specific records by timeCode:
```sql
DELETE FROM dbo.rp_source_time_codes
WHERE source_system = 'Polaris' AND time_code = %s AND type = %s
```

Pattern 2 — Delete entire project and all its tasks:
```sql
DELETE FROM dbo.rp_source_time_codes
WHERE source_system = 'Polaris' AND time_code = %s AND type = 'project'

DELETE FROM dbo.rp_source_time_codes
WHERE source_system = 'Polaris' AND time_code LIKE '{project_id}~%' AND type = 'task'
```

Pattern 3 — Delete by project name (fallback when URI unavailable):
```sql
DELETE FROM dbo.rp_source_time_codes
WHERE source_system = 'Polaris' AND parent_time_code_name LIKE '{project_name}%'
```

**Request:**

```json
{
  "targetTable": "dummy_rp_source_time_codes",
  "deletions": [
    {
      "mode": "specific",
      "sourceSystem": "Polaris",
      "timeCode": "57060~200245",
      "type": "task"
    },
    {
      "mode": "projectCascade",
      "sourceSystem": "Polaris",
      "projectTimeCode": "57060"
    },
    {
      "mode": "byProjectName",
      "sourceSystem": "Polaris",
      "projectName": "RPT-1"
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source_time_codes` | Override table. Must start with `dummy_`. |
| `deletions[].mode` | string | Yes | — | Deletion mode: `"specific"`, `"projectCascade"`, or `"byProjectName"` |
| `deletions[].sourceSystem` | string | Yes | — | Always `"Polaris"` |
| `deletions[].timeCode` | string | For `specific` | — | The exact timeCode to delete |
| `deletions[].type` | string | For `specific` | — | The type of the record (`"project"` or `"task"`) |
| `deletions[].projectTimeCode` | string | For `projectCascade` | — | The project ID. Deletes the project record AND all tasks where timeCode starts with `{project_id}~` |
| `deletions[].projectName` | string | For `byProjectName` | — | The project name. Deletes all records where `parentTimeCodeName` starts with this value. |

**Response:**

```json
{
  "deletedCount": 12,
  "status": "success"
}
```

**Notes:**
- Batch payload — array of deletion requests with mixed modes.
- `projectCascade` is the most common mode — removes a project and all its tasks atomically.
- `byProjectName` is a fallback used when the project URI is no longer available in Polaris (project fully deleted). This performs a prefix match on `parentTimeCodeName`.
- Operation should be transactional.

---

### 3.8 Source Time Codes — Time Off Types (rp_source_time_codes)

This endpoint handles time-off type definitions stored in `rp_source_time_codes` where `type = 'timeoff-type'`.

---

#### `GET /api/v1/rp/sourceTimeCodesTimeOffTypes`

**Purpose:** Retrieve existing time-off type records for deduplication.

**Used by:** Time Off Type Export

**Current SQL:**
```sql
SELECT time_code
FROM dbo.rp_source_time_codes
WHERE source_system = 'Polaris' AND type = 'timeoff-type'
```

**Request Parameters (query string):**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `sourceSystem` | string | Yes | — | Filter by source system (always `"Polaris"`) |
| `targetTable` | string | No | `rp_source_time_codes` | Override table. Must start with `dummy_`. |

**Example Request:**
```
GET /api/v1/rp/sourceTimeCodesTimeOffTypes?sourceSystem=Polaris&targetTable=dummy_rp_source_time_codes
```

**Response:**

```json
{
  "data": [
    {
      "sourceSystem": "Polaris",
      "parentTimeCode": "142",
      "timeCode": "142",
      "timeCodeName": "Annual Leave",
      "parentTimeCodeName": "Annual Leave",
      "type": "timeoff-type",
      "taskLevel": 0,
      "timeEntryEnabled": true
    }
  ],
  "count": 1
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `sourceSystem` | string | Source system identifier |
| `parentTimeCode` | string | Time-off type ID (same as timeCode) |
| `timeCode` | string | Time-off type ID from Polaris |
| `timeCodeName` | string | Time-off type name |
| `parentTimeCodeName` | string | Time-off type name (same as timeCodeName) |
| `type` | string | Always `"timeoff-type"` |
| `taskLevel` | integer | Always `0` |
| `timeEntryEnabled` | boolean | Whether this time-off type is enabled |

---

#### `POST /api/v1/rp/sourceTimeCodesTimeOffTypes`

**Purpose:** Insert new time-off type records.

**Used by:** Time Off Type Export

**Current SQL:**
```sql
INSERT INTO dbo.rp_source_time_codes
(source_system, parent_time_code, time_code, time_code_name,
 parent_time_code_name, project_manager_id, type, task_level, time_entry_enabled)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
```

**Request:**

```json
{
  "targetTable": "dummy_rp_source_time_codes",
  "records": [
    {
      "sourceSystem": "Polaris",
      "parentTimeCode": "142",
      "timeCode": "142",
      "timeCodeName": "Annual Leave",
      "parentTimeCodeName": "Annual Leave",
      "taskLevel": 0,
      "timeEntryEnabled": true
    }
  ]
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `targetTable` | string | No | `rp_source_time_codes` | Override table. Must start with `dummy_`. |
| `records[].sourceSystem` | string | Yes | — | Always `"Polaris"` |
| `records[].parentTimeCode` | string | Yes | — | Time-off type ID |
| `records[].timeCode` | string | Yes | — | Time-off type ID (same as parentTimeCode) |
| `records[].timeCodeName` | string | Yes | — | Time-off type name |
| `records[].parentTimeCodeName` | string | Yes | — | Time-off type name (same as timeCodeName) |
| `records[].taskLevel` | integer | Yes | — | Always `0` |
| `records[].timeEntryEnabled` | boolean | Yes | — | Whether this time-off type is enabled |

**Response:**

```json
{
  "insertedCount": 3,
  "status": "success"
}
```

**Notes:**
- Batch payload — typically small (10-50 records).
- `type` is always `"timeoff-type"` and `projectManagerId` is always null — the API can set these internally.
- Operation should be transactional.

---

### 3.9 Confirmed Bookings (Reverse Sync: RP → Polaris)

Five endpoints support the reverse sync pipeline — a master DAG paginates through bookings changed since its cursor, three child DAGs process pages in parallel (modulo-routed), and a retry DAG drains transient failures. See §8.7 Implementation Notes below for the rationale.

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/rp/confirmedBookings/batches` | Metadata: total bookings, page count, `upperBound` snapshot, `nextCursor` |
| POST | `/api/v1/rp/confirmedBookings` | One page of bookings, grouped by `bookingGuid` with `days[]` nested |
| POST | `/api/v1/rp/confirmedBookings/failures` | Log a failed page (replay recipe + error) |
| GET  | `/api/v1/rp/confirmedBookings/failures` | List pending retries |
| PATCH | `/api/v1/rp/confirmedBookings/failures/{id}` | Mark a failure record RESOLVED or MANUAL_REVIEW |

All timestamps are ISO-8601 with explicit `+05:30` offset. All amounts and identifiers follow camelCase.

---

#### `POST /api/v1/rp/confirmedBookings/batches`

**Purpose:** Discover the batch metadata (`pageCount`, `upperBound` snapshot, `nextCursor`) so the master DAG can fan out the correct number of page fetches and know what value to write to its cursor on success.

**Used by:** Confirmed Bookings Export Master DAG

**Request:**

```json
{
  "sourceSystem": "Polaris",
  "lastModifiedAfter": "2026-04-22T10:00:00.000+05:30",
  "pageSize": 100,
  "targetTable": "dummy_vw_rp_integration_confirmed_source"
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `sourceSystem` | string | Yes | — | Filter by `demand_source_system` (e.g. `"Polaris"`) |
| `lastModifiedAfter` | string | Yes | — | Cursor. ISO-8601 with `+05:30` offset. Server filters `last_modified_at > this value`. |
| `pageSize` | integer | No | `100` | Bookings per page (not rows). Range 1-5000. |
| `targetTable` | string | No | `vw_rp_integration_confirmed_source` | Override view; must start with `dummy_`. |

**Response:**

```json
{
  "totalBookings": 523,
  "totalRows": 1832,
  "pageCount": 6,
  "pageSize": 100,
  "upperBound": "2026-04-22T14:30:15.123+05:30",
  "nextCursor": "2026-04-22T14:30:15.123+05:30"
}
```

**Response Fields:**

| Field | Type | Description |
|---|---|---|
| `totalBookings` | integer | Distinct `bookingGuid`s whose ANY day row has `last_modified_at > lastModifiedAfter AND <= upperBound` |
| `totalRows` | integer | Sum of day rows across those bookings (all days, not just changed ones) |
| `pageCount` | integer | `ceil(totalBookings / pageSize)` |
| `pageSize` | integer | Echoes the requested pageSize |
| `upperBound` | string | Server's `NOW()` at call time — freezes the snapshot window for the page fetches that follow |
| `nextCursor` | string | Equal to `upperBound`. Master DAG writes this to its Airflow Variable on successful completion. |

**Notes:**
- A booking is "in the batch" if ANY of its day rows falls in `(lastModifiedAfter, upperBound]`. All days of matching bookings are returned by the page endpoint (not just the changed ones) — giving the DAG full context when deciding what to push.
- `upperBound` must be passed back to the page endpoint on every subsequent call this run, so page contents stay stable even while new data arrives in the underlying view.

---

#### `POST /api/v1/rp/confirmedBookings`

**Purpose:** Fetch one page worth of bookings, grouped by `bookingGuid` with the day rows nested under `days[]`. Pagination is **booking-cohesive** — a given `bookingGuid` always lands on exactly one page.

**Used by:** Confirmed Bookings Export Child DAGs, Sync-Failure Retry DAG

**Request:**

```json
{
  "sourceSystem": "Polaris",
  "lastModifiedAfter": "2026-04-22T10:00:00.000+05:30",
  "upperBound":        "2026-04-22T14:30:15.123+05:30",
  "pageSize": 100,
  "pageNumber": 2,
  "targetTable": "dummy_vw_rp_integration_confirmed_source"
}
```

**Request Fields:**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `sourceSystem` | string | Yes | — | `demand_source_system` filter |
| `lastModifiedAfter` | string | Yes | — | Cursor from the batches call (ISO-8601 +offset) |
| `upperBound` | string | Yes | — | Snapshot ceiling from the batches call (same run) |
| `pageSize` | integer | No | `100` | Bookings per page (1-5000) |
| `pageNumber` | integer | Yes | — | 1-indexed |
| `targetTable` | string | No | `vw_rp_integration_confirmed_source` | Override; must start with `dummy_` |

**Response (grouped; booking-level fields appear once per booking, day rows nested):**

```json
{
  "pageNumber": 2,
  "bookingCount": 2,
  "rowCount": 5,
  "data": [
    {
      "bookingGuid":     "BD543FB5-28B3-4C67-ABC1-9B3D95C871B6",
      "polarisRef":      "BD543FB5-28B3-4C67-ABC1-9B3D95C871B6",
      "matchStatus":     "UNMATCHED",
      "sourceBookingId": null,
      "bookingType": "confirmed",
      "status": "active",
      "resourceId": "R-447",
      "projectId": "PRJ-2025-118",
      "projectName": "RPT-1 Implementation",
      "taskName": "Phase 1 / Backend Build",
      "sourceTimeCode": "57060~200245",
      "sourceTimeCodeName": "RPT-1~Implementation~Phase 1~Backend Build",
      "externalIdPolaris": "BD543FB5-28B3-4C67-ABC1-9B3D95C871B6_00001",
      "days": [
        { "workDate": "2026-04-27", "hoursPerDay": 8.0, "lastModifiedAt": "2026-04-22T13:12:04.221+05:30", "hoursVariance": 8.0, "bookingSeq": 1 },
        { "workDate": "2026-04-28", "hoursPerDay": 8.0, "lastModifiedAt": "2026-04-22T13:12:04.221+05:30", "hoursVariance": 8.0, "bookingSeq": 2 },
        { "workDate": "2026-04-29", "hoursPerDay": 6.0, "lastModifiedAt": "2026-04-22T13:12:04.221+05:30", "hoursVariance": 6.0, "bookingSeq": 3 }
      ]
    },
    {
      "bookingGuid":     "AE223FB5-28B3-4C67-ABC1-9B3D95C87100",
      "polarisRef":      "urn:polaris:alloc:req:AE223FB5-28B3-4C67-ABC1-9B3D95C87100",
      "matchStatus":     "MATCHED",
      "sourceBookingId": "urn:polaris:alloc:req:AE223FB5-28B3-4C67-ABC1-9B3D95C87100_00007",
      "sourceSystem": "Polaris",
      "projectId": "PRJ-2025-118",
      "taskName": "Phase 1 / Requirements",
      "sourceTimeCode": "57060~200240",
      "days": [
        { "workDate": "2026-04-23", "hoursPerDay": 4.0, "sourceHours": 8.0, "lastModifiedAt": "2026-04-22T12:48:55.001+05:30", "hoursVariance": -4.0, "bookingSeq": 1 },
        { "workDate": "2026-04-24", "hoursPerDay": 8.0, "sourceHours": 8.0, "lastModifiedAt": "2026-04-15T09:03:12.000+05:30", "hoursVariance":  0.0, "bookingSeq": 2 }
      ]
    }
  ]
}
```

**Booking-level response fields:**

| Field | Type | Description |
|---|---|---|
| `bookingGuid` | string | Stable per (project, user, task). No `_NNNNN` suffix. |
| `polarisRef` | string | **Server-computed identifier to use in the Polaris mutation.** For `MATCHED`: `sourceBookingId` with trailing `_NNNNN` stripped. For `UNMATCHED`: equals `bookingGuid`. |
| `matchStatus` | string | `"MATCHED"` — booking exists in `rp_source` (has a Polaris counterpart). `"UNMATCHED"` — booking originated in RP, never pushed to Polaris. |
| `sourceBookingId` | string \| null | Raw Polaris URN from `rp_source` (MATCHED) or `null` (UNMATCHED). Prefer `polarisRef`. |
| `bookingType`, `status` | string | Booking type and status |
| `resourceId`, `personaId` | string | RP resource/persona |
| `projectId`, `projectName`, `projectNameDemand`, `customerName` | string | Project metadata |
| `demandHdrId`, `demandLnId`, `demandSourceSystem` | string | Demand linkage |
| `wbs1ProjectId`, `wbs2PhaseId`, `wbs3TaskId`, `chargeCodeLevelId`, `taskChargeCode`, `taskName` | string | Task/WBS structure |
| `sourceSystem`, `sourceTimeCode`, `sourceTimeCodeName`, `sourceTimeCodeId` | string | Source time-code data |
| `bookingGuidVision`, `externalIdPolaris`, `externalIdVision`, `sourceId` | string/int | Auxiliary cross-system identifiers |
| `days` | array | Day rows for this booking (see below) |

**Day-level fields (inside `days[]`):**

| Field | Type | Description |
|---|---|---|
| `workDate` | string (date) | `YYYY-MM-DD` |
| `hoursPerDay` | decimal | RP-side hours for this day. 0 means "no work scheduled". |
| `sourceHours` | decimal \| null | Hours currently in Polaris (via rp_source) for this day. Null for UNMATCHED. |
| `sourceWorkDate`, `sourceHoursType`, `sourceLastUpdated` | string | Corresponding rp_source fields |
| `hoursVariance` | decimal | `hoursPerDay - sourceHours` |
| `lastModifiedAt` | string | **Driver for change detection** — `last_modified_at` on the confirmed booking row. Used by DAGs to decide which days to push via PARTIAL mode. |
| `createdAt`, `updatedAt`, `bookingSeq`, `runId` | misc | Audit metadata |
| `bookingId` | int/string | Per-day PK of the underlying row |

**Contract guarantees:**
1. **Deterministic ordering** — `ORDER BY booking_guid, work_date`. Required so sync-failure replay returns identical data.
2. **Booking-cohesion** — a given `bookingGuid` is never split across pages.
3. **Stable pagination** — same `(lastModifiedAfter, upperBound, pageSize, pageNumber)` always returns the same rows. The `upperBound` snapshot is what makes this possible.
4. **`polarisRef` is always populated** — server strips `_\d{5}$` from `sourceBookingId` for MATCHED, uses `bookingGuid` for UNMATCHED. DAG never handles the suffix directly.

**Classification (DAG-side, not API-side):**

The API no longer computes an `action` field. DAG rules, applied per grouped booking:

| Group state | Action | Polaris mutation |
|---|---|---|
| All rows `matchStatus=UNMATCHED`, ≥1 day `hoursPerDay > 0` | **ADD** | CREATE with non-zero days as schedule rules, `taskAllocationId = polarisRef` |
| Any row `matchStatus=MATCHED` | **UPDATE** | PARTIAL mutation per day where `lastModifiedAt > lastModifiedAfter` (hours=0 and hours>0 both valid; 0 zeroes the day without a full-allocation DELETE) |
| All `UNMATCHED`, all days = 0 | **SKIP** | none |

No full-allocation DELETE mutation is ever sent — day removal is expressed as PARTIAL with `hours=0`.

---

#### `POST /api/v1/rp/confirmedBookings/failures`

**Purpose:** Log a page that failed after Airflow's automatic retries exhausted. Stored in `rp_export_sync_failures` for the sync-failure retry DAG to drain.

**Used by:** Child DAG `on_failure_callback`

**Request:**

```json
{
  "lastModifiedAfter": "2026-04-22T10:00:00.000+05:30",
  "upperBound":        "2026-04-22T14:30:15.123+05:30",
  "pageSize": 100,
  "pageNumber": 3,
  "masterRunId": "manual__2026-04-23T09:00:00+05:30",
  "childDagId":  "resource_planner_confirmed_bookings_export_child_dev_3",
  "childRunId":  "manual__2026-04-23T09:00:05+05:30",
  "errorMessage": "Polaris 500: Internal Server Error"
}
```

**Request Fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `lastModifiedAfter`, `upperBound`, `pageSize`, `pageNumber` | mixed | Yes | Replay recipe — identical values fed back to the page endpoint deterministically retrieve the same rows |
| `masterRunId` | string | Yes | Airflow run_id of the master DAG that triggered this child — for correlation |
| `childDagId` | string | Yes | The child DAG id |
| `childRunId` | string | No | The child DAG's run_id |
| `errorMessage` | string | Yes | Error summary (truncated to 2000 chars) |

**Response:**

```json
{
  "id": 142,
  "status": "PENDING_RETRY",
  "attemptCount": 1
}
```

**Behavior:**
- If a `PENDING_RETRY` record already exists with the same `(lastModifiedAfter, upperBound, pageNumber)`, the API updates it (bumps `attemptCount`, refreshes `lastAttemptedAt` + `errorMessage`) rather than inserting a duplicate.
- When `attemptCount` reaches 5, the record is auto-escalated to `MANUAL_REVIEW` — the retry DAG will no longer pick it up; operator intervention is required.

---

#### `GET /api/v1/rp/confirmedBookings/failures`

**Purpose:** List pending sync-failure records for the retry DAG to process.

**Query parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `status` | string | `PENDING_RETRY` | Filter by status (`PENDING_RETRY`, `MANUAL_REVIEW`, `RESOLVED`) |
| `limit` | integer | `100` | Max records (1-1000) |

**Response:** `{ "data": [ { failure record fields... } ], "count": N }` — ordered by `firstFailedAt` ascending.

---

#### `PATCH /api/v1/rp/confirmedBookings/failures/{id}`

**Purpose:** Transition a failure record out of `PENDING_RETRY` — called by the child DAG on successful retry (to mark `RESOLVED`) or by ops to escalate to `MANUAL_REVIEW`.

**Request:**

```json
{ "status": "RESOLVED" }
```

`status` must be one of `RESOLVED` or `MANUAL_REVIEW`. Returns `404` if the id doesn't exist.

---

## 4. API Summary Matrix

| # | Method | Endpoint | Table | Operations | Used By |
|---|--------|----------|-------|------------|---------|
| 1 | POST | `/api/v1/rp/resources` | rp_resources | Lookup by employeeIds | User Export, Webhooks |
| 2 | POST | `/api/v1/rp/users` | users | Lookup by employeeIds | Allocation Export (batch), Time Off Export |
| 3 | POST | `/api/v1/rp/laborCodes` | rp_labor_code | Lookup by mappingToSourceValues | Allocation Export (batch + webhooks) |
| 4 | GET | `/api/v1/rp/eligiblePolarisEmployees` | vw_d_staff_replica + rp_source_resources | Read (join + NOT EXISTS) | User Export |
| 5 | POST | `/api/v1/rp/sourceResources` | rp_source_resources | Batch insert | User Export |
| 6 | POST | `/api/v1/rp/sourceAllocations` | rp_source | Batch insert | Allocation Export (batch), Time Off Export |
| 7 | PUT | `/api/v1/rp/sourceAllocations` | rp_source | Atomic replace (delete + insert) | Allocation Webhooks (created, modified), Batch (changed allocations) |
| 8 | PATCH | `/api/v1/rp/sourceAllocations` | rp_source | Soft-delete (set hours=0) | Allocation Webhooks (deleted), Batch (removed allocations) |
| 9 | DELETE | `/api/v1/rp/sourceAllocations` | rp_source | Hard delete by prefix | Time Off Export |
| 10 | PUT | `/api/v1/rp/sourceTimeCodesProjectTasks` | rp_source_time_codes | Batch upsert (insert or update by timeCode) | Project Task Export (bulk + delta) |
| 11 | DELETE | `/api/v1/rp/sourceTimeCodesProjectTasks` | rp_source_time_codes | Multi-mode delete (project + task) | Project Task Export (delta) |
| 12 | GET | `/api/v1/rp/sourceTimeCodesTimeOffTypes` | rp_source_time_codes | Read filtered (timeoff-type) | Time Off Type Export |
| 13 | POST | `/api/v1/rp/sourceTimeCodesTimeOffTypes` | rp_source_time_codes | Batch insert (timeoff-type) | Time Off Type Export |
| 14 | POST | `/api/v1/rp/confirmedBookings/batches` | vw_rp_integration_confirmed_source | Metadata: pageCount, upperBound snapshot, nextCursor | Confirmed Bookings Export master |
| 15 | POST | `/api/v1/rp/confirmedBookings` | vw_rp_integration_confirmed_source | One page of bookings, grouped by bookingGuid with days[] nested | Confirmed Bookings Export children, Sync-Failure Retry |
| 16 | POST | `/api/v1/rp/confirmedBookings/failures` | rp_export_sync_failures | Log a failed page (replay recipe) | Child DAG on_failure_callback |
| 17 | GET  | `/api/v1/rp/confirmedBookings/failures` | rp_export_sync_failures | List pending retries | Sync-Failure Retry DAG |
| 18 | PATCH | `/api/v1/rp/confirmedBookings/failures/{id}` | rp_export_sync_failures | Mark failure RESOLVED / MANUAL_REVIEW | Child DAG on success (when failureId is in conf) |

---

## 5. Data Flow by Pipeline

### 5.1 User Export

```
1. GET  /api/v1/rp/eligiblePolarisEmployees  -> Get new employees to sync
2. POST /api/v1/rp/resources                 -> Lookup resourceId + usersUserId (pass employeeIds)
3. POST /api/v1/rp/sourceResources           -> Insert new user records
```

### 5.2 Task Resource Allocation Export (Batch) — One-time initial load

```
1. POST  /api/v1/rp/laborCodes             -> Lookup labor codes (pass [] for all)
2. POST  /api/v1/rp/users                  -> Lookup users (pass [] for all)
3. PUT   /api/v1/rp/sourceAllocations      -> Replace changed allocation rows (delete + insert)
4. PATCH /api/v1/rp/sourceAllocations      -> Soft-delete removed allocations (set hours=0)
```

### 5.3 Task Resource Allocation Webhooks (Created)

```
1. POST /api/v1/rp/laborCodes             -> Lookup labor code for this user's role
2. POST /api/v1/rp/resources              -> Lookup resource for this employee
3. PUT  /api/v1/rp/sourceAllocations      -> Atomic replace (no existing rows, inserts new)
```

### 5.4 Task Resource Allocation Webhooks (Modified)

```
1. POST /api/v1/rp/laborCodes             -> Lookup labor code for this user's role
2. POST /api/v1/rp/resources              -> Lookup resource for this employee
3. PUT  /api/v1/rp/sourceAllocations      -> Atomic replace (deletes old, inserts new)
```

### 5.5 Task Resource Allocation Webhooks (Deleted)

```
1. PATCH /api/v1/rp/sourceAllocations     -> Soft-delete: set hours=0 for all rows of this allocation
```

### 5.6 Project Task Export (Bulk) — One-time initial load

```
1. PUT /api/v1/rp/sourceTimeCodesProjectTasks  -> Upsert all projects and tasks
```

### 5.7 Project Task Export (Delta)

```
1. PUT    /api/v1/rp/sourceTimeCodesProjectTasks  -> Upsert added/modified projects/tasks
2. DELETE /api/v1/rp/sourceTimeCodesProjectTasks  -> Delete removed projects/tasks
```

> The Polaris audit report provides the action per record: `Added` and `Modified` records go to PUT (upsert), `Delete` records go to DELETE.

### 5.8 Time Off Export

```
1. POST   /api/v1/rp/users                        -> Lookup users (pass [] for all)
2. DELETE /api/v1/rp/sourceAllocations            -> Delete removed time-off bookings
3. POST   /api/v1/rp/sourceAllocations            -> Insert new time-off booking rows
```

### 5.9 Time Off Type Export

```
1. GET  /api/v1/rp/sourceTimeCodesTimeOffTypes    -> Get existing time-off types
2. POST /api/v1/rp/sourceTimeCodesTimeOffTypes    -> Insert new time-off types
```

### 5.10 Confirmed Bookings Export (RP → Polaris) — master + 3 child DAGs

```
MASTER DAG (max_active_runs=1; cursor in Airflow Variable)
├─ 1. POST /api/v1/rp/confirmedBookings/batches
│        { sourceSystem, lastModifiedAfter (cursor), pageSize }
│     -> { pageCount, upperBound, nextCursor }
├─ 2. Compute page groups: ((pageNumber - 1) % 3) + 1
├─ 3. TriggerDagRunForEachItem → child_dev_{1,2,3} with conf
│        { pageNumber, lastModifiedAfter, upperBound, pageSize, masterRunId }
├─ 4. WaitForDagRunsSensor for all triggered child runs
└─ 5. Advance cursor: Airflow Variable := nextCursor   (trigger_rule=ALL_DONE)

CHILD DAGs (3 files, looped at parse; max_active_runs=3 each → 9 peak)
├─ 1. POST /api/v1/rp/confirmedBookings
│        { sourceSystem, lastModifiedAfter, upperBound, pageSize, pageNumber }
│     -> { data: [ { bookingGuid, polarisRef, matchStatus, days:[...] } ] }
├─ 2. Classify per booking:
│        UNMATCHED + non-zero days → ADD (CREATE, schedule rules)
│        MATCHED                   → UPDATE (PARTIAL per day where lastModifiedAt > cursor)
│        UNMATCHED + all zeros     → SKIP
├─ 3. GraphQL CREATE to Polaris for ADD bookings
├─ 4. GraphQL UPDATE (PARTIAL) per day for MATCHED bookings (hours=0 zeroes a day)
├─ 5. If dag_run.conf carries failureId (retry-driven): PATCH /failures/{id} → RESOLVED
└─ on_failure_callback → POST /api/v1/rp/confirmedBookings/failures (replay recipe)

SYNC-FAILURE RETRY DAG (separate DAG, scheduled hourly or on-demand)
├─ 1. GET /api/v1/rp/confirmedBookings/failures?status=PENDING_RETRY
├─ 2. Modulo-route failures to the three child DAGs with failureId in conf
└─ 3. Wait — children that succeed auto-PATCH their failure record to RESOLVED.
        Repeated failures bump attemptCount via the POST /failures endpoint,
        auto-escalating to MANUAL_REVIEW at attempt 5.
```

---

## 6. Payload Size Expectations

| Endpoint | Typical Payload | Max Payload | Frequency |
|----------|----------------|-------------|-----------|
| POST /resources (lookup) | 1 employeeId (webhook), all (batch) | 15,000 | Event-driven / Daily |
| POST /users (lookup) | 1 employeeId (webhook), all (batch) | 15,000 | Event-driven / Daily |
| POST /laborCodes (lookup) | 1 role (webhook), all (batch) | 500 | Event-driven / Daily |
| POST /sourceResources | 10-50 records | 500 records | Daily |
| POST /sourceAllocations | 1,000-5,000 records (batch) | 10,000 records | One-time |
| PUT /sourceAllocations | 1 replacement, 1-150 rows (webhook) | 500 rows | Event-driven |
| PATCH /sourceAllocations | 1 markDeleted (webhook), 10-50 (batch) | 500 items | Event-driven / Daily |
| DELETE /sourceAllocations | 10-50 deletions (timeoff) | 500 deletions | Daily |
| PUT /sourceTimeCodesProjectTasks (upsert) | 10-100 records (delta), up to 5,000 (bulk) | 5,000 records | Daily / One-time |
| DELETE /sourceTimeCodesProjectTasks | 1-20 deletions | 200 deletions | Daily |
| POST /sourceTimeCodesTimeOffTypes | 1-10 records | 50 records | Daily |
| POST /confirmedBookings/batches | 1 call per master run | — | Every 15-30 min |
| POST /confirmedBookings | 1 page (100 bookings, ~500-1000 day rows) | 5,000 bookings/page | 1 per page per master run (3 in parallel across children) |
| POST /confirmedBookings/failures | 1 failure record per failed page | unbounded | Only on task failure |
| GET /confirmedBookings/failures | 100 pending records | 1,000 | Every retry-DAG tick |
| PATCH /confirmedBookings/failures/{id} | single record | — | On retry success |

---

## 7. Non-Functional Requirements

### 7.1 Performance

All API calls are **synchronous** — the caller sends a request and waits for the response. There is no async/polling pattern. The RP team must ensure that insert, upsert, and delete operations are fast enough to return within the targets below.

| Requirement | Target |
|-------------|--------|
| POST lookup endpoints (resources, users, laborCodes) | < 2 seconds |
| GET endpoints | < 5 seconds |
| POST (batch insert) response time | < 30 seconds for 5,000 records |
| PUT sourceAllocations (atomic replace, webhook) | < 5 seconds for single allocation |
| DELETE response time | < 10 seconds |
| PUT sourceTimeCodesProjectTasks (batch upsert) | < 30 seconds for 5,000 records |
| Concurrent requests supported | At least 10 simultaneous |

### 7.2 Reliability

- All write operations (POST, PUT, DELETE, PATCH) must be **transactional** — either all records succeed or none are committed.
- APIs should return appropriate HTTP status codes (200, 201, 400, 401, 404, 500).
- Retry-safe: repeated POST/PUT with the same data should not create duplicates (where applicable).
- Deadlock-safe: transient MSSQL deadlocks (error 1205 / SQLSTATE 40001) must be retried internally with exponential backoff before surfacing a 500. Clients should not need to retry on deadlock.

### 7.3 Availability

- APIs must be available during Airflow pipeline execution windows (typically 24/7 for webhooks, off-peak hours for batch).
- Planned maintenance windows should be communicated to the integration team.

---

## 8. Implementation Notes (Reference Backend)

This section documents the patterns used in the reference FastAPI backend. RP team implementations are free to deviate, but must meet the reliability and performance targets in §7.

### 8.1 Batching and the MSSQL 2100-parameter limit

SQL Server caps parameterized statements at **2100 parameters**. Multi-row INSERT/MERGE statements must stay under this limit by choosing a batch size that accounts for the column count:

| Endpoint | Columns per row | Batch size | Params per batch |
|----------|-----------------|-----------|------------------|
| `POST /sourceResources` | 4 | 500 | 2,000 |
| `POST /sourceAllocations`, `PUT /sourceAllocations` | 10 | 200 | 2,000 |
| `PUT /sourceTimeCodesProjectTasks` (MERGE) | 9 | 230 | 2,070 |
| `POST /sourceTimeCodesTimeOffTypes` | 9 | 230 | 2,070 |

Each batch is a single round-trip. A 5,000-record payload dispatches as 10–25 batches depending on endpoint.

### 8.2 Upsert via `MERGE … OUTPUT $action`

`PUT /sourceTimeCodesProjectTasks` uses a single MERGE statement per batch instead of a SELECT-then-INSERT/UPDATE pattern:

```sql
MERGE dbo.rp_source_time_codes WITH (HOLDLOCK) AS t
USING (VALUES (...), (...)) AS s (source_system, time_code, type, ...)
  ON t.source_system = s.source_system
 AND t.time_code = s.time_code
 AND t.type = s.type
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED BY TARGET THEN INSERT (...) VALUES (...)
OUTPUT $action;
```

**Why:**
- 1 round-trip per batch instead of 2–3 (SELECT + INSERT + UPDATE)
- Single atomic statement — SQL Server picks one optimal plan
- `OUTPUT $action` reports `INSERT` vs `UPDATE` per row for accurate response counts
- `WITH (HOLDLOCK)` takes key-range locks during the MERGE, preventing a known race where concurrent MERGEs can both miss the same row and both insert (causing PK violation or duplicates)

**Observed improvement:** 264-record batch went from ~3000ms (SELECT 1235ms + INSERT/UPDATE 1700ms) to ~500–800ms.

### 8.3 Collapsed statements for bulk operations on source_allocations

`PUT`, `PATCH`, and `DELETE` on `/sourceAllocations` accept a list of prefix-scoped operations. Rather than executing one statement per item, the backend collapses them into a single statement with OR-joined conditions:

```sql
-- PATCH (mark deleted)
UPDATE dbo.rp_source SET hours = 0
 WHERE (source_booking_id LIKE :prefix_0 AND source_system = :ss_0)
    OR (source_booking_id LIKE :prefix_1 AND source_system = :ss_1)
    OR ...

-- DELETE (bulk, no filter)
DELETE FROM dbo.rp_source
 WHERE (source_booking_id LIKE :prefix_0 AND source_system = :ss_0)
    OR ...
```

One round-trip covers the full batch, regardless of how many prefixes are supplied. Items with `hoursTypeFilter` fall back to per-item statements since each filter value set differs.

### 8.4 Deadlock retry wrapper

All write endpoints wrap their transaction in `run_with_deadlock_retry`, which:
- Catches exceptions where SQLSTATE is `40001` or native error is `1205`, or the message contains "deadlock"
- Rolls back the session
- Retries with exponential backoff + jitter: 100ms, 200ms, 400ms, 800ms, capped at 3s, up to 5 attempts
- Re-raises any non-deadlock exception immediately

This is important because `MERGE` with `HOLDLOCK`, and concurrent webhook-triggered `PUT`/`PATCH` against the same prefix range, can produce legitimate deadlock-victim rollbacks under burst traffic. Clients see a successful response after transparent retry rather than a 500.

### 8.5 Isolation level

The backend assumes the RP database has **READ_COMMITTED_SNAPSHOT (RCSI)** enabled at the database level. RCSI makes readers use row-versioning rather than shared locks, which dramatically reduces writer-vs-reader deadlocks and contention during concurrent webhook processing.

Recommended:
```sql
ALTER DATABASE [ResourcePlanner] SET READ_COMMITTED_SNAPSHOT ON;
```

### 8.6 Structured logging

Each request logs:
- A correlation ID (generated in middleware) attached to all log lines in that request
- Request method, path, and duration
- Per-batch timing for multi-batch operations (`batch N/M (rows) in Xms — inserted=…, updated=…`)
- Deadlock retry attempts with backoff duration

Target log format: `%(asctime)s | %(levelname)s | %(name)s | %(message)s`.

---

## 9. Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | April 13, 2026 | Initial specification |
| 1.1 | April 13, 2026 | camelCase naming convention for paths and JSON fields; marked batch pipelines as one-time initial load |
| 1.2 | April 14, 2026 | Lookup endpoints changed to POST with input lists; added PUT /sourceAllocations for atomic replace; split sourceTimeCodes into projectTasks and timeOffTypes; all calls synchronous; eligibleEmployees renamed to eligiblePolarisEmployees |
| 1.3 | April 14, 2026 | Merged POST + PUT sourceTimeCodesProjectTasks into single PUT (upsert); removed GET sourceTimeCodesProjectTasks (report provides action); added concurrency/burst note for webhooks |
| 1.4 | April 14, 2026 | Added `targetTable` override (must start with `dummy_`, API prepends `dbo.` internally); removed `dbo.` prefix from all payloads and table references; added `sourceSystem` and `isActive` params to laborCodes |
| 1.5 | April 14, 2026 | Added POST /confirmedBookings endpoint for reverse sync (RP → Polaris); 13 endpoints total |
| 1.6 | April 14, 2026 | Added PATCH /sourceAllocations for soft-delete (hours=0); PUT reverted to always delete+insert; DELETE scoped to timeoff only; 14 endpoints total |
| 1.7 | April 22, 2026 | Added §8 Implementation Notes documenting MERGE+HOLDLOCK upsert for sourceTimeCodesProjectTasks, collapsed OR-clause statements for sourceAllocations, deadlock retry wrapper (error 1205 / SQLSTATE 40001), batch-size table for the 2100-param MSSQL limit, RCSI isolation recommendation, and structured logging format. Strengthened §7.2 reliability to require internal deadlock retry. No contract changes. |
| 1.8 | April 22, 2026 | **URL prefix change**: all endpoints now mount under `/api/v1/rp/` (was `/api/v1/`) to namespace RP endpoints and reserve `/api/v1/vision/` for a future Deltek Vision integration on the same gateway. Request/response shapes and semantics unchanged. All calling Airflow DAGs updated in lockstep. |
| 1.9 | April 23, 2026 | **Reverse-sync redesign**: `POST /confirmedBookings` replaced (breaking — old flat-row shape is gone). New endpoints: `POST /confirmedBookings/batches` (metadata), `POST /confirmedBookings` (grouped-by-bookingGuid with `days[]` nested, booking-cohesive pagination, `upperBound` snapshot, server-computed `polarisRef`), and three failure-log endpoints (`POST`/`GET`/`PATCH /confirmedBookings/failures`). New table `rp_export_sync_failures` stores replay recipes for the retry DAG. Cursor column changed from `source_last_updated` (echo-unsafe) to `last_modified_at`. `action` field removed from response — DAGs now classify by `matchStatus` + hour values. Confirmed-bookings pipeline rearchitected as master + 3 partitioned children (routed `(pageNumber - 1) % 3`, `max_active_runs=3` each → peak 9 concurrent) + sync-failure retry DAG. 18 endpoints total. |
