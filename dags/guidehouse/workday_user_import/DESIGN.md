# Guidehouse – Workday User Import Integration
## Integration Design Document

**Spec Reference:** Deltek-Guidehouse Workday Integration v1.1.docx
**Spec Version:** V1.1 (April 6, 2026)
**Spec Authors:** Zehra Sajan, Sumana V.
**Design Date:** 2026-04-14

---

## Table of Contents

1. [Overview](#1-overview)
2. [Integration Architecture](#2-integration-architecture)
3. [DAG Structure](#3-dag-structure)
4. [Input File Specification](#4-input-file-specification)
5. [Field Mapping](#5-field-mapping)
6. [Business Logic](#6-business-logic)
7. [Mapper Requirements](#7-mapper-requirements)
8. [Config & Instance Design](#8-config--instance-design)
9. [Error Handling & Log Design](#9-error-handling--log-design)
10. [SFTP & File Details](#10-sftp--file-details)
11. [Open Questions](#11-open-questions)

---

## 1. Overview

| Attribute | Value |
|---|---|
| **Customer** | Guidehouse Corporation |
| **Account ID** | 678659 |
| **User Count** | ~17,700 |
| **Products** | Workforce Management, Polaris PSA, Time Off Enterprise, TimeBill Plus |
| **Source System** | Workday |
| **Destination** | Replicon |
| **Integration Method** | Flat file CSV (PGP encrypted) via SFTP |
| **Frequency** | Daily (can be multiple times per day) |
| **Integration Type** | Indirect – New |
| **Target Go-Live** | September 1, 2026 (Wave 1) |
| **Replicon Instances** | Guidehouse Inc Sb2, Guidehouse Inc Sb, Guidehouseinc |
| **Primary Key** | Employee ID |
| **SSO** | Email-based SSO |
| **Default Language** | English |

### Phased Go-Live
- **Wave 1:** September 1, 2026
- Subsequent waves TBD
- Guidehouse provides all user data per wave — Replicon does not filter by phase
- Supervisors/PMs belonging to later phases are still included in the feed
- WD Mapper updates required per wave for localization

### Related Integrations
| Integration | Description |
|---|---|
| Time Export | Time export to Costpoint, Peoplesoft, Datalake |
| Project Import | Project and resource assignment from Costpoint and Peoplesoft |

---

## 2. Integration Architecture

### High-Level Flow

```
[Workday] --> CSV.PGP --> [SFTP: /Inbound/Workday/Input]
                                       |
                              [Master DAG - Airflow]
                                       |
         +-----------------------------+------------------------------+
         |                             |                              |
  [Process Groups]        [Process Users - 4 batches]    [Disable Profile]
  - Company Codes         |                   |           (daily: disable
  - Cost Centers        New User         Update User       end-dated users)
  - Locations           Child DAG        Child DAG
  - Employee Types            |
                       [Supervisor Assignment Child]
                              |
                       [Project Resource Child]
                              |
                       [Log Generation Child]
                              |
                  [SFTP: /Inbound/Workday/Logs]
```

### Key Design Decisions

- **File pickup:** SFTP sensor monitors input folder; master DAG polls every 30 seconds with soft-fail timeout to allow retry windows for manual reruns
- **Encryption:** PGP decryption controlled via Airflow Variable (`can_decrypt_file`)
- **Batching:** Users distributed across 4 parallel child DAG batches using `employee_id % 4` modulo
- **Supervisor retry:** Supervisors not found during main processing are queued as Pending log entries and retried after all users complete via a dedicated supervisor child DAG
- **Disable profile:** Separate scheduled DAG runs daily (1 AM) to disable profiles whose end date matches today
- **Log upload:** Consolidated CSV log generated per feed file and uploaded to SFTP Logs folder

---

## 3. DAG Structure

### DAG Inventory

| DAG ID | Description | Max Active Runs |
|---|---|---|
| `guidehouse_workday_user_import_master_{instance}` | Master orchestrator — file pickup, validation, orchestration | 1 |
| `guidehouse_workday_user_import_process_groups_child_{instance}` | Create/validate org groups: company codes, cost centers, locations, employee types | 4 |
| `guidehouse_workday_user_import_process_users_child_{instance}_batch_{1-4}` | Per-user routing — determines new vs update path | 4 |
| `guidehouse_workday_user_import_process_new_users_child_{instance}_batch_{1-4}` | New user creation in Replicon | 4 |
| `guidehouse_workday_user_import_process_update_users_child_{instance}_batch_{1-4}` | Existing user updates in Replicon | 4 |
| `guidehouse_workday_user_import_processs_supervisor_child_{instance}` | Supervisor assignment for queued Pending entries | 4 |
| `guidehouse_workday_user_import_process_new_schedule_child_{instance}` | Auto-create missing office schedules | 4 |
| `guidehouse_workday_user_import_process_projects_child_{instance}` | Add users to admin project resources | 4 |
| `guidehouse_workday_user_import_process_log_generation_child_{instance}` | Consolidate all logs and upload CSV to SFTP | 1 |
| `guidehouse_workday_user_import_disable_profile_master_{instance}` | Scheduled daily — disable users whose end date = today | 1 |

### Master DAG Task Flow

```
new_file_sensor
  |
  +--[No file]--> delete_this_dagrun
  |
  v
is_csv_pgp
  +--[No]--> send_bad_file_format_email
  |
  v
download_file --> archive_file
  |
  v
can_decrypt_file
  +--[Yes]--> decrypt_file --+
  +--[No]--------------------+
                             v
                       get_input_data
                             |
                       load_data (CSV)
                             |
                  create_input_data_collection
                             |
               create_log + process_supervisor_log
                             |
                      has_input_data
                       +--[No]--> send_blank_payload_email
                       |
                       v
              get_valid_data / get_invalid_data
                       |
               log_invalid_users
                       |
         +---------------------------------------------------+
         |          [get_user_prereqs task group]             |
         |  get_enabled_divisions (paged)               |
         |  get_location_details (paged)                     |
         |  get_employeetype_groups_data (paged)             |
         |  get_all_timesheet_period_list (paged)            |
         |  get_user_customfields                             |
         |  get_permission_sets                               |
         |  get_all_payrule_scripts                          |
         |  get_all_policy_sets                               |
         |  get_timesheet_approval_paths                     |
         |  get_all_timezones                                 |
         |  get_all_holiday_calendars                        |
         |  get_all_activity_uris                            |
         |  get_all_user_status_dropdowns                    |
         |  Schedule delta check --> process_new_schedule    |
         +---------------------------------------------------+
                       |
               process_groups --> wait_process_groups
                       |
         +---------------------------------------------------+
         |    [get_updated_user_prereqs task group]          |
         |    Re-fetch locations, employee types,            |
         |    co-costcenters, schedules, project details     |
         +---------------------------------------------------+
                       |
         get_unique_employee_id --> add_row (with record_id)
                       |
         process_each_user (4 parallel batches)
                       |
         gather_user_logs / gather_project_logs
                       |
         get_project_resource_data --> process_project_resource
                       |
         get_supervisorcheck_queued_logs
                       |
              is_supervisorcheck_queued_logs
               +--[Yes]--> process_supervisor_child_dag --+
               +--[No]------------------------------------>+
                                                           v
                                           process_log_generation
                                                           |
                                                   log_to_sumo
                                                           |
                                                   can_fail_dag
```

### Process Users Child DAG Flow (per user)

```
can_run_batch_task
  +--[Yes]--> batch_task (wraps full flow for error capture)
  +--[No]---> process_user_log

process_user_log --> process_project_log --> query_user_data
  --> get_user_payload_data --> get_user_by_empl_id
        |
        +--[has old profile + user found in Replicon]
        |     --> update_existing_user_profile
        |     --> disable_user --> log_old_profile_update
        |
        +--[user found, no old profile]
        |     --> get_effective_user_groups
        |     --> is_user_type_change
        |           +--[Yes]--> update_old_profile_usertype
        |
        v
  if_user_present
    +--[Yes]--> process_update_user --> wait_for_process_update_user
    +--[No]---> process_new_user   --> wait_for_process_new_user
                                              |
                                    catch_and_log_errors
```

---

## 4. Input File Specification

### File Naming Convention

```
User_{ENV}_{YYYYMMDD}_{HHMMSS}.csv.pgp

Examples:
  User_UAT_20250713_100233.csv.pgp
  User_PROD_20250713_100233.csv.pgp
```

**Format:** CSV, comma-separated, force-quoted, UTF-8
**Encryption:** PGP
**Accepted extensions by sensor:** `.csv`, `.pgp`
**Content:** Delta records only — records not present in the file are left unchanged in Replicon

### Input File Header (23 columns, in order)

```
Employee_ID,Login_Name,First_Name,Last_Name,Email,Supervisor_ID,Default_Location,
Employee_Type,Change_Effective_Date,Schedule,Start_Date,Seniority_Date,End_Date,
Job_Code,Job_Description,Pay_Group,Status,Company_Code,Company_Description,
Cost_Center_Code,Cost_Center_Description,Financial_System,Time_Profile_Name
```

### Column Definitions

| # | Column | Required | Replicon Target | Notes |
|---|---|---|---|---|
| 1 | `Employee_ID` | Yes | Employee ID | Primary key. Never changes for a user. |
| 2 | `Login_Name` | Yes | Login Name + SSO Auth ID | Is the email address. Used for SSO. |
| 3 | `First_Name` | Yes | First Name | |
| 4 | `Last_Name` | Yes | Last Name | |
| 5 | `Email` | No | Email | Used for SSO notifications |
| 6 | `Supervisor_ID` | No | T&E Approver / Supervisor | If blank: user created without supervisor; exception logged |
| 7 | `Default_Location` | Yes | Group: Location | Pipe-separated, up to 3 levels. Germany and India always have 3 levels; all others have 2. New locations are NOT auto-created. |
| 8 | `Employee_Type` | Yes | Group: Employee Type | Comma-separated 4-level hierarchy e.g. `Regular,Exempt,Salary,Full-Time`. Drives overtime eligibility, time off policy, and mapper. |
| 9 | `Change_Effective_Date` | Yes | UDF: Change Effective Date | Format: MM/DD/YYYY. Effective date for all group, supervisor, and template changes. |
| 10 | `Schedule` | Yes | Office Schedule | Maps 1:1 to office schedule name in Replicon. Auto-created if missing. |
| 11 | `Start_Date` | Yes | Start Date / Hire Date | Format: MM/DD/YYYY |
| 12 | `Seniority_Date` | No | UDF: Seniority Date | Format: MM/DD/YYYY. Used for time off accruals. |
| 13 | `End_Date` | No | End Date | Format: MM/DD/YYYY. Required when `Status = Terminated`. |
| 14 | `Job_Code` | No | UDF: Job Code | Reporting only. Does not drive any logic. |
| 15 | `Job_Description` | No | UDF: Job Description | Reporting only. |
| 16 | `Pay_Group` | No | UDF: Pay Group | Reporting only. |
| 17 | `Status` | Yes | UDF: Status | Values: `Active`, `OnLeave`, `Terminated` |
| 18 | `Company_Code` | Yes | Group: Company Code | Must pre-exist in Replicon. Exception logged if not found; user skipped. |
| 19 | `Company_Description` | No | — | Reference only. |
| 20 | `Cost_Center_Code` | Yes | Group: Cost Center | Must pre-exist in Replicon. Exception logged if not found; user skipped. |
| 21 | `Cost_Center_Description` | No | — | Reference only. |
| 22 | `Financial_System` | No | Group: Financial System | Values: `CostPoint`, `PeopleSoft`, `India`. Drives project team assignment. |
| 23 | `Time_Profile_Name` | No | UDF: Time Profile Name | Populated only for US Non-Exempt PeopleSoft employees eligible for shift premium. Determines pay rule. Blank for CostPoint users. |

### Mandatory Field Validation

Records missing any of the following are logged as Exception and skipped:

`Employee_ID`, `Login_Name`, `First_Name`, `Last_Name`, `Default_Location`, `Employee_Type`, `Change_Effective_Date`, `Schedule`, `Start_Date`, `Status`, `Company_Code`, `Cost_Center_Code`

> ⚠️ Confirm with Guidehouse whether any `Status` value bypasses mandatory field validation. See [Open Questions](#11-open-questions) #1.

---

## 5. Field Mapping

### New Hire — Replicon User Profile

| Replicon Field | Source | Value / Logic |
|---|---|---|
| First Name | `First_Name` | Direct |
| Last Name | `Last_Name` | Direct |
| Email | `Email` | Direct |
| Employee ID | `Employee_ID` | Direct |
| Language | Hardcoded | English |
| Start Date | `Start_Date` | Direct |
| End Date | — | Left blank on hire |
| Timesheet Effective Date | Logic | Go-live date for existing employees; start of work week (Sunday) for new hires; start of semi-monthly period for India |
| Login Name | `Login_Name` | Direct (same as email) |
| Authentication Type | Hardcoded | SSO |
| Authentication ID | `Login_Name` | Same as login name |
| Login Status | Hardcoded | Enabled |
| Location | `Default_Location` | Pipe-separated hierarchy (empty segments stripped) |
| Employee Type | `Employee_Type` | Comma-separated 4 levels → pipe-separated path in Replicon |
| Company Code | `Company_Code` | Division group membership |
| Cost Center | `Cost_Center_Code` | Division group membership |
| Financial System | `Financial_System` | Division group membership |
| Office Schedule | `Schedule` | 1:1 name match to Replicon office schedule |
| Supervisor | `Supervisor_ID` | Resolved via BulkGetUsers3; see Section 6.4 |
| Products | Hardcoded | Workforce Management, TimeBill Plus, Time Off Enterprise, Polaris PSA |
| Permission Set | Hardcoded | Employee |
| Change Effective Date | `Change_Effective_Date` | UDF |
| Seniority Date | `Seniority_Date` | UDF |
| Job Code | `Job_Code` | UDF |
| Job Description | `Job_Description` | UDF |
| Pay Group | `Pay_Group` | UDF |
| Status | `Status` | UDF |
| Time Profile Name | `Time_Profile_Name` | UDF |
| Timezone | Derived | Country-based lookup — see Section 6.14 |
| Holiday Calendar | Derived | Location-based lookup; effective-dated on transfers — see Section 6.13 |
| Timesheet Template | Mapper | WD sync mapper: location + employee type + financial system |
| Timesheet Approval Path | Mapper | WD sync mapper |
| Timesheet Period | Logic | Weekly (Sun–Sat) for all; semi-monthly for India |
| Pay Rule | Mapper | Location + employee type + `Time_Profile_Name` |
| Work Week | Mapper | WD sync mapper |
| Time Off Types | Mapper | Location + employee type |
| Activities (Work Locations) | Logic | All available activities in Replicon assigned to every user |
| Default Activity | Logic | USA/Canada Non-Exempt (L2): Location Level 2; all others: Location Level 1 |

### Update — Field Behavior

| Field | Behavior on Update |
|---|---|
| First Name, Last Name, Email | Updated from feed |
| Employee ID | **Never updated** |
| Language | **Never updated** |
| Login Name | **Never updated** |
| Authentication Type | **Never updated** |
| Password | **Never updated** |
| Start Date | Updated (retro corrections possible) |
| End Date | Updated for termination only |
| All group memberships | Updated, effective-dated via `Change_Effective_Date` |
| Supervisor | Updated if changed; effective today (date file is processed) |
| Mapper-derived fields | Re-evaluated and applied effective `Change_Effective_Date` |
| Manually assigned permissions (beyond Employee/Supervisor) | **Never overwritten** |

---

## 6. Business Logic

### 6.1 New Hire

1. Search Replicon by `Employee_ID`
2. If not found → create user profile with all attributes from feed and mapper
3. **Timesheet effective date:**
   - Employees in system at go-live → use confirmed go-live date
   - New hires post go-live → start of the work week (Sunday)
   - India employees → start of the semi-monthly period

### 6.2 Rehire

1. User found in Replicon in **disabled / terminated state**:
   - Enable the profile
   - Update start date to new hire date
   - Blank out end date
   - Apply current mapper values for all group assignments
2. ⚠️ If start date is updated, user loses ability to edit historical timesheet data

### 6.3 Modification / Update

1. Search Replicon by `Employee_ID`
2. If found and **enabled**: apply all field updates per feed and mapper
3. All group, supervisor, and template changes are **effective-dated** using `Change_Effective_Date`
4. If a mid-week transfer triggers a new timesheet template or pay rule → applies on the next generated timesheet
5. Future timesheets created before a mid-week transfer must be **manually deleted** by admin

### 6.4 Supervisor Assignment

**Case 1 — Supervisor found, enabled:**
- Check if Supervisor permission set is assigned to the supervisor profile
- If not assigned → assign `Supervisor` permission first
- Set as supervisor of the target user:
  - New user: "Initial Supervisor" (no date range)
  - Existing user: effective today (date file is processed)

**Case 2 — Supervisor found, disabled:**
- If supervisor's end date is in the past → log exception, do NOT enable, do NOT assign
- If end date is not in the past → enable login, then proceed as Case 1

**Case 3 — Supervisor not found:**
- Log exception: "Supervisor not found in Replicon"
- User is still created / updated; no supervisor assignment
- Entry queued as Pending → retried after all users are processed via `processs_supervisor_child` DAG

**Rules:**
- Supervisor permission assigned only if **no supervisor-category permission already exists**
- Elevated permissions (payroll manager, system admin, etc.) assigned manually are **never overwritten**

### 6.5 Termination / Disabling

- If `Status = Terminated` and `End_Date` provided → update End Date on Replicon profile
- **Disable profile DAG** runs daily: disables all profiles where End Date = today
- **On termination:** Zero out all time off type balances as of the termination date (prevents balance carryover on rehire)
- If end date is mid-week: timesheet visible for full week; user cannot enter time after end date
- If end date is retroactive: future timesheets must be manually deleted by admin
- If termination is reversed (end date removed from feed): blank out end date in Replicon; user resumes normal access

### 6.6 Status Handling

| Workday Status | Action in Replicon |
|---|---|
| `Active` | No status change — update profile fields normally |
| `OnLeave` | No status change — user remains active; timesheets populated via import / manual / timekeeper |
| `Terminated` | Update End Date on profile; disable on that date |

### 6.7 Employee Type Change

- Update employee type group effective `Change_Effective_Date`
- Re-evaluate mapper: timesheet template, pay rule, time off types all updated
- If `Time_Profile_Name` is populated but employee becomes Exempt (level 2) → **ignore `Time_Profile_Name`**; pay rule determined by mapper location/employee type logic only

### 6.8 Location Transfer

- Location group updated effective `Change_Effective_Date`
- Holiday calendar, timezone, pay rule, and timesheet template re-derived from new location and updated
- If start date also changes → user loses access to historical timesheet editing

### 6.9 Start Date Changes

- If start date pushed to a later date and timesheets already generated → **manual deletion required** by admin
- If start date is mid-week → full work week timesheet created; user cannot enter time prior to start date

### 6.10 Future Date Terminations

- Future-dated termination records are **not** sent in the Workday feed
- Only current or past-dated terminations are processed

### 6.11 Activity (Work Location) Assignment

- **All users** are assigned to **all available activities** in Replicon
- Employees select physical work location on the timesheet

**Default Activity:**
- **USA and Canada, Non-Exempt (Employee Type Level 2):** default activity = Location Level 2 (e.g., `California`)
- **All other countries:** default activity = Location Level 1 (e.g., `India`)
- Activity name matching is **case-insensitive** (`India` = `INDIA`)

### 6.12 Office Schedule Auto-Creation

- If schedule value from feed does not exist in Replicon → auto-create it
- **Pattern:** Hours equally distributed Monday–Friday
- **Formula:** `daily_hours = weekly_hours / 5`
- **Example:** `22.5` hrs/week → `4.5` hrs/day Mon–Fri; schedule name = `22.5`
- Shift schedule creation / assignment is **out of scope**

### 6.13 Holiday Calendar Assignment

Assigned based on location. Effective-dated on transfers.

| Location Level 1 | Location Level 2 | Location Level 3 | Calendar Name |
|---|---|---|---|
| United States of America | All (except Puerto Rico) | — | United States of America |
| United States of America | Puerto Rico | — | Puerto Rico |
| Canada | All | — | Canada |
| France | All | — | France |
| Netherlands | All | — | Netherlands |
| Lithuania | All | — | Lithuania |
| United Kingdom | All | — | United Kingdom |
| Germany | All | Berlin | Berlin |
| Germany | All | Cologne | Cologne |
| India | All | Chennai | Chennai |
| India | All | Gurgaon | Gurgaon |
| India | All | Trivandrum | Trivandrum |
| India | All | Nagarcoil | Nagarcoil |

**Rules:**
- **Germany and India:** use Location Level 3 for calendar lookup
- **All other countries:** use Location Level 1
- Holiday calendar assigned only if employee is eligible for "Holiday" time off type (per mapper)
- Setting: **"Automatically add/remove holiday bookings" must be UNCHECKED** — holidays populate per timesheet period only

**Holiday Max Entitlement (Full-Time, 40 hrs/week):**

| Location | Level 3 | Max Hours |
|---|---|---|
| United States of America | — | 88 |
| Canada | — | 88 |
| Germany | Berlin | 88 |
| Germany | Cologne | 80 |
| France | — | 72 |
| Lithuania | — | 165 |
| United Kingdom | — | 64 |
| UAE | — | 112 |
| India | Chennai | 72 |
| India | Gurgaon | 64 |
| India | Trivandrum | 104 |
| India | Nagarcoil | 72 |

**Part-Time Proration:** `entitlement = (weekly_hours / 40) x max_entitlement`

**Schedule change mid-year:** A new policy line is added at `Change_Effective_Date` with recalculated entitlement. Max balance threshold for the year remains the full-time maximum.

### 6.14 Timezone Assignment

Assigned automatically based on Location Level 1. Countries with multiple time zones default to Eastern Standard Time.

| Country | Timezone |
|---|---|
| Argentina | (UTC-3:00) Argentina Standard Time |
| Australia | (UTC+10:00) AUS Eastern Standard Time |
| Austria | (UTC+1:00) Central European Standard Time |
| Belgium | (UTC+1:00) Central European Standard Time |
| Brazil | (UTC-4:00) Central Brazilian Standard Time |
| Canada | (UTC-5:00) Eastern Standard Time |
| Chile | (UTC-3:00) SA Eastern Standard Time |
| China | (UTC+8:00) China Standard Time |
| Colombia | (UTC-5:00) Eastern Standard Time |
| Costa Rica | (UTC-6:00) Central Standard Time |
| France | (UTC+1:00) Central European Standard Time |
| Germany | (UTC+1:00) Central European Standard Time |
| Hong Kong | (UTC+8:00) North Asia East Standard Time |
| Hungary | (UTC+1:00) Central European Standard Time |
| Ireland | (UTC+1:00) Central European Standard Time |
| India | (UTC+5:30) India Standard Time |
| Japan | (UTC+9:00) Tokyo Standard Time |
| Lithuania | (UTC+1:00) Central European Standard Time |
| Luxembourg | (UTC+1:00) Central European Standard Time |
| Malaysia | (UTC+8:00) North Asia East Standard Time |
| Mexico | (UTC-6:00) Central Standard Time (Mexico) |
| Netherlands | (UTC+1:00) Central European Standard Time |
| New Zealand | (UTC+12:00) New Zealand Standard Time |
| Peru | (UTC-5:00) Eastern Standard Time |
| Philippines | (UTC+8:00) North Asia East Standard Time |
| Puerto Rico | (UTC-4:00) Atlantic Standard Time |
| Singapore | (UTC+8:00) Singapore Standard Time |
| Spain | (UTC+1:00) Central European Standard Time |
| Switzerland | (UTC+1:00) Central European Standard Time |
| Taiwan | (UTC+8:00) Taipei Standard Time |
| United Kingdom | (UTC+0:00) Greenwich Standard Time |
| Uruguay | (UTC-3:00) Montevideo Standard Time |
| USA | (UTC-5:00) Eastern Standard Time |

### 6.15 Timesheet Period

- **All employees:** Weekly, work week = Sunday to Saturday
- **India employees:** Semi-monthly (exception)

### 6.16 Floating Holiday Logic (USA Only)

**Starting Balance — New Hire:**

| Employee Type | Hire Date | Starting Balance |
|---|---|---|
| Full-time (40 hrs/week) | Before October 1 | 16 hours |
| Full-time (40 hrs/week) | October 1 or later | 8 hours |
| Part-time (< 40 hrs/week) | Any | `(weekly_hours / 40) x 16` hours |

**Annual Entitlement (subsequent years):**
- Full-time: 16 hours per calendar year
- Part-time: `(weekly_hours / 40) x 16` hours

**Schedule change / ineligibility:**
- New policy line added at `Change_Effective_Date` with recalculated entitlement
- Recalculation: `new_entitlement = (new_weekly_hours / 40) x 16`
- Hours already taken count toward the recalculated entitlement; balance may go negative if over-used
- If employee becomes ineligible → disable Floating Holiday leave type; set balance to 0 as of effective date

**Example:**
- Employee works 40 hrs/week Jan 1–Jun 30 → 16 hrs entitlement
- Effective Jul 1 changes to 30 hrs/week → new entitlement = `(30/40) x 16 = 12 hrs`
- If 12 hrs already taken → balance = 0; if fewer taken → balance = 12 − hours taken

### 6.17 Project Team Assignment

- Users added to admin project resources based on `Financial_System` and Employee Type Level 1
- `project_mapper` defines project codes per `(financial_system, employee_type_level_1)` combination
- Users in `PeopleSoft` financial system are also included in the Time Export integration
- Project resource assignment runs after all user create/update operations complete

---

## 7. Mapper Requirements

### 7.1 WD User Sync Mapper (`mappers/user_sync_mapper.py`)

One row per unique combination. Supports exact match, list match, `"All"` wildcard, and `not_in` exclusion. Priority: exact > list > All > not_in.

**Required fields per row:**

| Field | Description | Example |
|---|---|---|
| `Location Level 1` | Country or `"All"` | `"United States of America"` |
| `Location Level 2` | State/region, list, `"All"`, or `{"not_in": [...]}` | `"All"` |
| `Employee Type Level 1` | Top-level employee type | `"Regular"` |
| `Employee Type Level 2` | Second level | `"Exempt"` |
| `Employee Type Level 3` | Third level | `"Salary"` |
| `Employee Type Level 4` | Fourth level | `"Full-Time"` |
| `Financial System` | `"CostPoint"`, `"PeopleSoft"`, `"India"`, or `"All"` | `"All"` |
| `Time Profile Name` | Specific name, `"All"`, or blank | `"BAMA"` |
| `Timesheet Template` | Policy set name in Replicon | `"USA Exempt"` |
| `Timesheet Approval Path` | Approval path name | `"Supervisor"` |
| `Timesheet Period` | Period name | `"Weekly starting on Sunday"` |
| `Pay Rule` | Pay rule script name | `"Weekly 40 Hours"` |
| `Time Types` | Comma-separated time type names | `"Regular, PTO, Holiday"` |
| `Work Week` | Work week definition | `"Sunday to Saturday"` |
| `Holiday Eligible` | Whether to assign a holiday calendar | `true` / `false` |

**V1.1 Mapper Changes (applied per spec):**
- Removed: `[USA] Sick NE`
- Added: `[USA] DTO` for Professional Hourly employees
- Removed: `[USA] PTO PH` for Professional Hourly employees
- Added: `[USA] sick` for Regular Non-Exempt under 20 weekly scheduled hours
- Removed employee type combinations: `Non Exempt Salary Full Time`, `Exempt Hourly Full Time`, `Exempt Hourly Part Time`, `Intern Exempt Salary`, `Non Exempt Salary`, `Exempt Hourly`, `Intern Exempt`

> ⚠️ Full mapper data to be provided by Guidehouse before development. See [Open Questions](#11-open-questions) #4.

### 7.2 Timezone Mapper (`mappers/timezone_mapper.py`)

```python
timezone_mapper = [
    {"country": "United States of America", "timezone": "(UTC-5:00) Eastern Standard Time"},
    {"country": "India",                    "timezone": "(UTC+5:30) India Standard Time"},
    # ... full list per Section 6.14
]
```

### 7.3 Project Mapper (`mappers/project_mapper.py`)

```python
project_mapper = [
    {
        "financial_system": "PeopleSoft",
        "user_type": "Regular",          # Employee_Type Level 1
        "admin_project_code": "..."      # Replicon project code
    },
    # ...
]
```

> ⚠️ Project codes to be provided by Guidehouse. See [Open Questions](#11-open-questions) #5.

### 7.4 Work Week Mapper (`mappers/workweek_mapper.py`)

```python
workweek_mapper = [
    {"value": "sunday", "uri": "urn:replicon:work-week:..."},
    # ...
]
```

---

## 8. Config & Instance Design

### 8.1 `config.py` — Shared Constants

```python
# Region / Environment
region = "us-east-1"
environment = "pre-production"

# Execution
execution_timeout_days = 14
master_dag_interval = 30             # seconds
file_sensor_timeout = 10             # minutes

# Concurrency
max_active_run_master = 1
max_active_runs_process_groups = 4
max_active_runs_process_users = 4
max_active_runs_process_new_users = 4
max_active_runs_process_update_users = 4
max_active_runs_process_supervisor = 4
max_active_runs_process_log_generation = 1
max_active_runs_disable_profile_master = 1
gather_user_logs_timeout_hours = 24

# Parallelism
trigger_parallel_dagrun_count_process_users = 4
trigger_parallel_dagrun_count_process_schedules = 2
PROCESS_USER_BATCH_COUNT = 4

# Defaults
default_language = 'en'

# Timesheet
INDIA_TIMESHEET_PERIOD = 'Semi-Monthly'
STANDARD_WORK_WEEK_START = 'Sunday'

# Licenses
licenses = ["TOE", "WFM", "Polaris PSA"]

# Input file header (23 columns)
input_file_header = (
    'Employee_ID,Login_Name,First_Name,Last_Name,Email,Supervisor_ID,'
    'Default_Location,Employee_Type,Change_Effective_Date,Schedule,Start_Date,'
    'Seniority_Date,End_Date,Job_Code,Job_Description,Pay_Group,Status,'
    'Company_Code,Company_Description,Cost_Center_Code,Cost_Center_Description,'
    'Financial_System,Time_Profile_Name'
)

# UDF fields (used to fetch URIs from Replicon)
CUSTOM_FIELDS = [
    'Change Effective Date', 'Seniority Date', 'Job Code',
    'Job Description', 'Pay Group', 'Status', 'Time Profile Name'
]

# Holiday calendar: countries that use Location Level 3 for lookup
LEVEL3_HOLIDAY_CALENDAR_FOR = ['india', 'germany']

# Default activity: countries where Non-Exempt users get Location Level 2 as default
LEVEL2_DEFAULT_ACTIVITY_COUNTRIES = ['united states of america', 'canada']
NON_EXEMPT_EMPLOYEE_TYPE_LEVEL2 = 'Non-Exempt'

# Multi-timezone countries default to Eastern Standard Time
MULTI_TIMEZONE_DEFAULT = 'Eastern Standard Time'
MULTI_TIMEZONE_COUNTRIES = ['usa', 'united states of america', 'canada']

# Disable profile
disable_master_dag_interval = "0 1 * * *"     # Daily at 1 AM
disable_master_dag_active_runs = 1
user_disable_report_name = "User with end date"
```

### 8.2 Instance Files

| File | Instance | Company Key | Environment |
|---|---|---|---|
| `instances/trial.py` | `dev` | TBD (Guidehouse Inc Sb2) | pre-production |
| `instances/sit.py` | `sit` | TBD (Guidehouse Inc Sb) | pre-production |
| `instances/uat.py` | `uat` | TBD (Guidehouse Inc Sb) | pre-production |
| `instances/prod.py` | `prod` | TBD (Guidehouseinc) | production |

### 8.3 Instance Config Template

```python
from guidehouse.workday_user_import.config import *

region = "us-east-1"
instance = 'uat'
company_key = 'GuidehouseIncSb'              # To be confirmed
replicon_conn_id = 'guidehouseincsb_replicon_rit.workday'
pgp_conn_id = 'guidehouseincsb_replicon_pgp_conn'

sftp_conn_id = 'sftp_guidehouse_uat'         # To be confirmed
input_filepath = '/Inbound/Workday/Input'
archive_filepath = '/Inbound/Workday/Archive'
log_filepath = '/Inbound/Workday/Logs'

tenant_email = ''                            # Guidehouse DL — to be confirmed
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

# DAG IDs
master_dag = f'guidehouse_workday_user_import_master_{instance}'
process_each_user = f'guidehouse_workday_user_import_process_users_child_{instance}'
process_new_users = f'guidehouse_workday_user_import_process_new_users_child_{instance}'
process_update_users = f'guidehouse_workday_user_import_process_update_users_child_{instance}'
processs_supervisor = f'guidehouse_workday_user_import_processs_supervisor_child_{instance}'
process_log_generation = f'guidehouse_workday_user_import_process_log_generation_child_{instance}'
process_groups_dag_id = f'guidehouse_workday_user_import_process_groups_child_{instance}'
process_new_schedule = f'guidehouse_workday_user_import_process_new_schedule_child_{instance}'
process_projects = f'guidehouse_workday_user_import_process_projects_child_{instance}'
process_disable_users = f'guidehouse_workday_user_import_disable_profile_master_{instance}'

# Airflow Variables
can_decrypt_file_var_name = f'guidehouse_workday_user_import_can_decrypt_file_{instance}'
can_run_batch_task = f'guidehouse_workday_user_import_can_run_batch_task_{instance}'
```

---

## 9. Error Handling & Log Design

### 9.1 Log File

- **Format:** CSV
- **Destination:** `/Inbound/Workday/Logs` on SFTP
- **Frequency:** One log file per feed file processed
- **Filename:** `log_{input_filename}_{YYYY-MM-DDTHH-MM-SS}.csv`

### 9.2 Log Columns

| Column | Description |
|---|---|
| `ecid` | DAG run execution correlation ID |
| `employeeid` | Employee ID from feed |
| `loginname` | Login name |
| `firstname` | First name |
| `lastname` | Last name |
| `manager` | Supervisor ID from feed |
| `action` | `Add`, `Update`, `Validation`, `Sync` |
| `status` | `Success`, `Exception`, `Error`, `Skipped` |
| `details` | Human-readable description of outcome |
| `userstatus` | Status field value from feed |
| `co_costcenter` | Cost center description |
| `location` | Location description |

### 9.3 Exception Scenarios

| Scenario | Log Status | User Action |
|---|---|---|
| Mandatory field missing | Exception | Skip user |
| Location not found in Replicon | Exception | Skip user |
| Company Code not found in Replicon | Exception | Skip user |
| Cost Center not found in Replicon | Exception | Skip user |
| No supervisor ID in feed | Exception (info) | Create/update user; no supervisor |
| Supervisor not found in Replicon | Exception | Create/update user; no supervisor; queued for retry |
| Supervisor disabled with past end date | Exception | Create/update user; no supervisor |
| End date before start date | Exception | Skip user |
| User created successfully | Success | — |
| User updated successfully | Success | — |
| User created with partial success | Exception | Logged with detail |
| Unhandled error during processing | Error | Logged with error message |

### 9.4 Email Notifications

| Email | Trigger | Template |
|---|---|---|
| Bad file format | File extension is not `.csv` or `.pgp` | `templates/emails/bad_file_format.html` |
| Blank payload | Input file has zero records | `templates/emails/blank_payload.html` |
| Completion mail | End of successful run | `templates/emails/completion_mail.html` |

### 9.5 Monitoring

DAG run logs pushed to Sumo Logic after log generation via `DagRunLogToSumoOperator` (connection: `sumologic-dagrunlogger`).

---

## 10. SFTP & File Details

| Path | Purpose | Owner |
|---|---|---|
| `/Inbound/Workday/Input` | Guidehouse drops PGP-encrypted input files | Guidehouse |
| `/Inbound/Workday/Archive` | Processed files moved here per run | Replicon |
| `/Inbound/Workday/Logs` | Log CSV uploaded per run; readable by Guidehouse | Replicon |

| Environment | SFTP Host | Username |
|---|---|---|
| UAT | rsftp-useast.replicon.com | TBD — Guidehouse to confirm |
| Production | rsftp-useast.replicon.com | TBD — Guidehouse to confirm |

---

## 11. Open Questions

| # | Question | Impact | Owner |
|---|---|---|---|
| 1 | Is there any `Status` value that should bypass mandatory field validation (e.g., a no-show termination type)? | Validation logic | Guidehouse |
| 2 | Is `Email` truly optional? If blank, what is used for SSO notifications? | User creation | Guidehouse |
| 3 | What are the exact company keys for each Replicon instance (Sb2, Sb, prod)? | Instance config | Guidehouse / Replicon |
| 4 | Full WD sync mapper data required: all combinations of location x employee type x financial system x time profile → timesheet template, pay rule, approval path, time types, holiday eligibility | Mapper file | Guidehouse |
| 5 | What project codes correspond to each `Financial_System` x `Employee_Type Level 1` combination? | `project_mapper.py` | Guidehouse |
| 6 | What are the SFTP usernames for UAT and Production? | Instance config | Guidehouse |
| 7 | What are the tenant notification DL email addresses per environment? | Instance config | Guidehouse |
| 8 | What is the go-live date to use as Timesheet Effective Date for Wave 1 existing employees? | New hire logic | Guidehouse |
| 9 | UAE appears in the holiday entitlement table but not the holiday calendar assignment table. What calendar name applies for UAE? | Holiday logic | Guidehouse |
| 10 | Does `Financial_System` influence timesheet template / pay rule in the mapper, or only project team assignment? | Mapper logic | Guidehouse / Spec |
| 11 | For the disable profile DAG — is zeroing time-off balances on termination handled in the same DAG run or a separate process? | Termination logic | Guidehouse |
| 12 | What are all valid values at each of the 4 Employee Type levels? | Mapper / Validation | Guidehouse |
| 13 | Are there any locations that should be excluded from processing entirely? | Filter logic | Guidehouse |
| 14 | If `Time_Profile_Name` is populated for an Exempt employee, confirm it is fully ignored for pay rule selection. | Mapper logic | Guidehouse / Spec |