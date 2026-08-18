# iPipeline User Import v0 - Comprehensive Design Document

**Version:** 0
**Last Updated:** December 2025
**Integration:** iPipeline → Replicon User Management

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture Overview](#2-system-architecture-overview)
3. [DAG Architecture & Flow](#3-dag-architecture--flow)
4. [Data Flow & Transformation Pipeline](#4-data-flow--transformation-pipeline)
5. [Time Off Policy Assignment Logic (CRITICAL)](#5-time-off-policy-assignment-logic-critical)
6. [Configuration & Mappers](#6-configuration--mappers)
7. [Replicon API Services](#7-replicon-api-services)
8. [Error Handling & Logging](#8-error-handling--logging)
9. [Technical Reference](#9-technical-reference)

---

## 1. Executive Summary

### Purpose
The iPipeline User Import v0 integration automates the creation and maintenance of users in Replicon from iPipeline's HRIS system. It handles:
- New user creation with full configuration
- Existing user updates with change detection
- Time off policy assignment and accrual calculations
- Organizational hierarchy management
- Supervisor relationships
- Daily user disabling based on end dates

### Key Characteristics
- **Schedule:** Polls SFTP every 30 seconds for new files
- **Change Detection:** SHA256 hash-based delta processing
- **Parallel Processing:** Up to 5 concurrent user processing DAGs
- **Multi-Region:** Supports US, UK, Canada, Japan with region-specific rules

---

## 2. System Architecture Overview

### File Structure
```
dags/ipipeline/user_import/
├── master.py                          # Main orchestrator DAG
├── config.py                          # Global configuration
├── instances/
│   └── trial.py                       # Instance-specific config
│
├── Child DAGs:
│   ├── process_user_record_child.py   # Routes to add/update
│   ├── process_add_user_child.py      # Creates new users
│   ├── process_update_user_child.py   # Updates existing users
│   ├── process_supervisor_assignment_child.py
│   ├── process_create_department.py
│   ├── process_create_location.py
│   ├── process_create_employeetype.py
│   ├── process_create_projectrole.py
│   ├── disable_users_master.py
│   ├── disable_users_child.py
│   └── process_log_generation.py
│
├── utils/
│   ├── custom_methods.py              # Business logic & transformations
│   ├── request_payload.py             # API payload builders
│   └── response_filters.py            # API response parsers
│
├── mappers/
│   ├── input_fields_mapper.py         # CSV field mappings
│   ├── defaults_mapper.py             # Default values
│   ├── assignment_rules_mapper.py     # 30-rule assignment matrix
│   ├── permissions_mapper.py          # Role-based permissions
│   ├── time_off_type_mapper.py        # TIME OFF POLICY RULES (34 policies)
│   └── oef_custom_mapper.py           # Custom extension fields
│
└── templates/emails/
    └── import_complete.html           # Email templates
```

### Component Interaction Diagram
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SFTP SERVER                                     │
│   Input: /iPipeline/Dev/Input/*.csv                                         │
│   Archive: /iPipeline/Dev/Archive/                                          │
│   Reference: /iPipeline/Dev/Reference/User_Import_Reference.csv             │
│   Logs: /iPipeline/Dev/Logs/                                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MASTER DAG                                         │
│   ipipeline_user_import_master_trial                                     │
│                                                                              │
│   1. File Sensor → Download CSV → Parse                                     │
│   2. Reference File Comparison (SHA256 hash)                                │
│   3. Organizational Structure Analysis & Creation                           │
│   4. User Business Logic Application                                        │
│   5. Trigger Child DAGs for Each User                                       │
│   6. Log Generation & Email Notification                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
            │  ADD USER    │ │ UPDATE USER  │ │  SUPERVISOR  │
            │  CHILD DAG   │ │  CHILD DAG   │ │  ASSIGNMENT  │
            │              │ │              │ │  CHILD DAG   │
            │ - Create     │ │ - Change     │ │              │
            │   User       │ │   Detection  │ │ - Assign     │
            │ - Assign     │ │ - Update     │ │   pending    │
            │   TimeOff    │ │   User       │ │   supervisors│
            │   Policies   │ │ - Update     │ │              │
            │ - Assign     │ │   TimeOff    │ │              │
            │   Resource   │ │   Policies   │ │              │
            │   Pool       │ │              │ │              │
            └──────────────┘ └──────────────┘ └──────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           REPLICON API                                       │
│   - ImportService2.svc/CreateUserOrApplyModifications                       │
│   - TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser                    │
│   - TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. DAG Architecture & Flow

### Master DAG Flow (Phases)

#### Phase 1: File Detection & Validation
```
new_file_sensor (SFTP) → is_csv? → download_input_file → archive_file → parse_csv
```

#### Phase 2: Change Detection
```
can_use_reference_file?
  └─ YES → download_reference → parse_reference → create_reference_collection
           └─ create_mapped_collection → create_csv_with_hash → identify_changed_records
  └─ NO → process all records
```

#### Phase 3: Organizational Structure Creation
```
analyze_departments_to_create → check_if_need_creation? → trigger child DAGs (parallel)
analyze_locations_to_create → check_if_need_creation? → trigger child DAGs (parallel)
analyze_employeetypes_to_create → check_if_need_creation? → trigger child DAGs (parallel)
analyze_projectroles_to_create → check_if_need_creation? → trigger child DAGs (parallel)
```

#### Phase 4: User Processing
```
For each changed user (parallel, max 5):
  └─ process_user_record_child
       └─ User exists? → UPDATE child DAG
       └─ User new? → ADD child DAG
```

#### Phase 5: Post-Processing
```
filter_pending_supervisor_records → process each (supervisor_assignment_child)
gather_user_logs → format_logs → trigger_log_generation
upload_reference_file
```

---

## 4. Data Flow & Transformation Pipeline

### Input CSV Fields (30+ columns)
| CSV Field | Internal Field | Mandatory Add | Mandatory Update | Updateable |
|-----------|---------------|---------------|------------------|------------|
| Employee ID | employee_id | YES | YES | NO |
| First Name | first_name | YES | NO | YES |
| Last Name | last_name | YES | NO | YES |
| Display Name | display_name | YES | NO | YES |
| Email | email | YES | NO | YES |
| Start Date | start_date | YES | NO | NO |
| End Date | end_date | NO | NO | YES |
| Login Name | login_name | YES | YES | NO |
| Supervisor | supervisor | NO | NO | YES |
| FTE | fte | YES | NO | YES |
| Level | level | YES | NO | YES |
| Title | title | YES | NO | YES |
| Location Level 1 | location_level_1 | YES | NO | YES |
| Location Level 2 | location_level_2 | NO | NO | YES |
| Department Level 1 | department_level_1 | YES | NO | YES |
| Department Level 2 | department_level_2 | NO | NO | YES |
| Employee Schedule | employee_schedule | YES | NO | YES |
| Employee Category | employee_category | YES | NO | YES |
| Scheduled Hours | scheduled_hours | YES | NO | YES |
| HASH | hash_value | YES | YES | NO |

### Business Logic Transformation Pipeline
```
CSV Input
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  apply_ipipeline_business_logic(user_data, config)              │
│                                                                  │
│  1. Location URI Lookup                                         │
│     └─ find_group_uri_by_name_and_path()                        │
│                                                                  │
│  2. Department URI Lookup                                       │
│     └─ Builds hierarchy: RootDept/Level1/Level2                 │
│                                                                  │
│  3. Employee Type Calculation                                   │
│     └─ Hierarchy: Category/Schedule/Type                        │
│                                                                  │
│  4. Assignment Rule Matching (30-rule matrix)                   │
│     └─ get_assignment_rule_from_mapper()                        │
│     └─ Extracts: schedule_type, timesheet_template,             │
│        approval_paths, activities, payrule, holiday_calendar    │
│                                                                  │
│  5. Permission Assignment                                       │
│     └─ Maps title → org role → permissions                      │
│     └─ Schedule manager permissions with restrictions           │
│                                                                  │
│  6. TIME OFF TYPE ASSIGNMENT ← CRITICAL                         │
│     └─ get_timeoff_types_for_assignment()                       │
│     └─ Matches: location, level, employee_category              │
│                                                                  │
│  7. Project Role & Rate Card Lookup                             │
│     └─ Matches user title to project role                       │
└─────────────────────────────────────────────────────────────────┘
    │
    ▼
Calculated User Payload with URIs
```

---

## 5. Time Off Policy Assignment Logic (CRITICAL)

### Overview

The time off policy system is one of the most complex parts of this integration. It determines:
1. **Which time off types** are assigned to each user
2. **Policy accrual calculations** based on FTE and service years
3. **When to update policies** based on changes

### 5.1 Time Off Type Mapper Structure

**Location:** `mappers/time_off_type_mapper.py`
**Total Policies:** 34 unique time off type configurations

#### Mapper Entry Structure
```python
{
    # MATCHING CRITERIA
    "location_level_1": "United States",           # Exact match or "ALL"
    "location_level_2_to_include": [],             # Empty = match all
    "location_level_2_to_exclude": ["California"], # Exclude specific locations
    "employee_levels_to_include": ["M1", "M2", "P1", ...],  # Employee levels
    "employee_levels_to_exclude": [],

    # TIME OFF TYPE DETAILS
    "time_off_type": "USA _Vacation",
    "time_off_type_group": "Vacation/Holiday",
    "time_off_type_approval_path": "iPipeline US",
    "paycode": "VACPY",
    "visible_to_employees": True,                   # Auto-assigned vs HR-managed

    # ACCRUAL CONDITIONS (CRITICAL)
    "accrual_conditions": {
        "tenure_rules": [
            {"min_years": 0, "max_years": 7, "rate": 5, "limitation_hours": 120},
            {"min_years": 7, "max_years": 999, "rate": 6.67, "limitation_hours": 160}
        ],
        "fte_prorated": True,
        "policy_updates_required": True
    },

    # CARRY FORWARD & RESET
    "carry_forward": "",
    "carry_forward_expiry": "",
    "time_off_reset": "1-Jan",
    "leave_type": "Hours"
}
```

### 5.2 Time Off Type Assignment by Region

#### United States
| Time Off Type | Level Filter | Location Filter | Has Accruals |
|--------------|--------------|-----------------|--------------|
| USA _Vacation | M1-M6, P1-P5, S1-S4, E7, E9 | Excludes California | YES |
| USA_California _Vacation | - | California ONLY | NO |
| USA_Illness/Sick | - | All US | YES |
| USA_Personal | - | Excludes California | YES |
| USA_Berevement | - | All US | NO |
| USA_Unpaid_Hourly | - | All US | NO |
| USA_Summer Hours | - | All US | NO |
| Z_USA_Unpaid Leave | - | All US | NO (HR-managed) |
| Z_USA_Short Term Disability | - | All US | NO (HR-managed) |
| Z_USA_Bonding Leave | - | All US | NO (HR-managed) |
| Z_USA_Maternity Leave | - | All US | NO (HR-managed) |

#### United Kingdom
| Time Off Type | Level Filter | Has Accruals |
|--------------|--------------|--------------|
| UK _Holiday | - | YES |
| UK_Illness/Sick | - | YES |
| UK_Time off for Dependants | - | NO |
| UK_Bank Holidays Owed | - | NO |
| UK_Compassionate leave | - | NO (hidden) |
| UK_Flexible Time Off | - | NO |
| UK_Appointment | - | NO |
| Z_UK_Parental Leave | - | NO (HR-managed) |
| Z_UK_Adoptive Leave | - | NO (HR-managed) |
| Z_UK_Antenatal | - | NO (HR-managed) |
| Z_UK_Career Break | - | NO (HR-managed) |
| Z_UK_Paternity Leave | - | NO (HR-managed) |

#### Canada
| Time Off Type | Level Filter | Has Accruals |
|--------------|--------------|--------------|
| Canada_Vacation | - | YES (10-tier) |
| Canada_Personal | - | YES |
| Canada_Berevement | - | NO |
| Canada_Summer Hours | - | NO |
| Z_Canada_Pregnancy/Parental/Maternity Leave | - | NO (HR-managed) |
| Z_Canada_Leave Other | - | NO (HR-managed) |

#### Japan
| Time Off Type | Level Filter | Has Accruals |
|--------------|--------------|--------------|
| Japan_Vacation | - | YES |
| Japan_Berevement | - | NO |

#### Global (ALL Locations)
| Time Off Type | Has Accruals |
|--------------|--------------|
| Global_Volunteer | NO |
| Global_Early Office Closing | NO |
| Global_Jury Duty | NO |

### 5.3 Time Off Assignment Matching Logic

**Function:** `get_timeoff_types_for_assignment()` in `custom_methods.py`

```python
# Matching Algorithm (evaluated for EACH of 34 policies):

for policy in TIME_OFF_TYPE_MAPPER:
    # 1. Location Level 1 Match (Required)
    if policy.location_level_1 != 'ALL' and policy.location_level_1 != user.location_level_1:
        SKIP  # Location doesn't match

    # 2. Location Level 2 Include/Exclude Logic
    if policy.location_level_2_to_include:
        if user.location_level_2 NOT IN policy.location_level_2_to_include:
            SKIP  # User's location not in include list

    if policy.location_level_2_to_exclude:
        if user.location_level_2 IN policy.location_level_2_to_exclude:
            SKIP  # User's location in exclude list

    # 3. Employee Level Include/Exclude Logic
    if policy.employee_levels_to_include:
        if user.level NOT IN policy.employee_levels_to_include:
            SKIP  # User's level not in include list

    if policy.employee_levels_to_exclude:
        if user.level IN policy.employee_levels_to_exclude:
            SKIP  # User's level in exclude list

    # 4. Time Off Type Must Exist in Replicon
    if policy.time_off_type NOT IN replicon_timeoff_types:
        SKIP  # Type not configured in Replicon

    # MATCH FOUND - Add to user's time off types
    calculated_time_off_types[policy.time_off_type] = {
        "uri": replicon_uri,
        "visible_to_employees": policy.visible_to_employees,
        "have_accrual_conditions": bool(policy.accrual_conditions)
    }
```

### 5.4 Accrual Calculation Logic

**Key Function:** `get_timeoff_policy_accruals()` in `custom_methods.py`

#### Accrual Formula
```
For each time off type with accrual_conditions:

1. Calculate FTE Ratio:
   fte_ratio = scheduled_hours / fte_hours
   Example: 32 hours / 40 hours = 0.8 (80% FTE)

2. Calculate Service Years:
   service_years = years between start_date and current_date

3. Find Applicable Tenure Rule:
   for rule in tenure_rules:
       if rule.min_years <= service_years < rule.max_years:
           applicable_rule = rule
           break

4. Calculate Prorated Accruals:
   monthly_accrual = base_rate * fte_ratio
   yearly_entitlement = monthly_accrual * 12
   limitation_hours = base_limitation * fte_ratio

5. Calculate Effective Date for Tier:
   effective_date = start_date + min_years (for future tiers)
```

#### Example: USA Vacation Calculation
```
Employee: John Doe
Start Date: 2020-01-15
Current Date: 2024-12-20
FTE: 40 hours
Scheduled Hours: 32 hours

Step 1: FTE Ratio = 32/40 = 0.8

Step 2: Service Years = 4.9 years

Step 3: Tenure Rule Match
   Rule 1: 0-7 years → rate: 5, limitation: 120
   Rule 2: 7+ years → rate: 6.67, limitation: 160
   → Matches Rule 1 (under 7 years)

Step 4: Prorated Accruals
   Monthly Accrual = 5 * 0.8 = 4 hours/month
   Yearly Entitlement = 4 * 12 = 48 hours/year
   Limitation Hours = 120 * 0.8 = 96 hours

Step 5: Policy Schedule Created:
   {
       "effectiveDate": {"year": 2020, "month": 1, "day": 15},
       "yearly_entitlement": 48,
       "monthly_accrual": 4,
       "limitation_hours": 96
   }
```

### 5.5 Time Off Policy Updates on User Changes

**Key Function:** `build_comprehensive_timeoff_assignments_for_update()` in `custom_methods.py`

#### Change Detection Triggers
```python
# Changes that trigger time off policy updates:
check_fte_or_schedule_or_location_or_level_changes():
    is_fte_changed         # FTE hours changed (e.g., 40 → 32)
    is_scheduled_hours_changed  # Work schedule changed
    is_location_changed    # Location changed (US → UK)
    is_level_changed       # Employee level changed (P1 → P2)
```

#### Update Scenarios Matrix

| Scenario | FTE/Schedule Changed | Location/Level Changed | Action |
|----------|---------------------|----------------------|--------|
| 1 | NO | NO | No update |
| 2 | YES | NO | Add new policy lines to existing types (FTE recalc) |
| 3 | NO | YES | Add/remove time off types with default policies |
| 4 | YES | YES | Both: Add/remove types AND add policy lines |

#### Scenario 2: FTE/Schedule Changes Only
```
User: Jane Smith
Previous FTE: 40 hours → New FTE: 32 hours
Location: United States (unchanged)

1. Get overlapping time off types (already assigned)
   → USA _Vacation, USA_Illness/Sick, USA_Personal

2. For types WITH accrual_conditions:
   → Recalculate accruals with new FTE ratio
   → Add NEW policy line effective TODAY

3. For types WITHOUT accrual_conditions:
   → Keep existing policies unchanged

Result: New policy schedule entry added with today's date
```

#### Scenario 3: Location/Level Changes Only
```
User: Bob Wilson
Previous Location: United States → New Location: United Kingdom
FTE: 40 hours (unchanged)

1. Identify time off type changes:
   REMOVED: USA _Vacation, USA_Illness/Sick, USA_Personal, USA_Berevement...
   ADDED: UK _Holiday, UK_Illness/Sick, UK_Time off for Dependants...

2. For NEW time off types:
   → Get default policies from Replicon
   → Calculate accruals if has accrual_conditions
   → Assign with calculated policies

3. For REMOVED time off types:
   → Disable (not removed, just isTimeOffAllowed = false)

API Calls:
1. PutTimeOffTypeAssignmentsForUser (add new types, disable old)
2. PutUserTimeOffAccountPolicySetSchedule (for each type)
```

#### Scenario 4: Both Changes
```
User: Alice Brown
Previous: US, FTE 40 → New: UK, FTE 32

1. Handle location change:
   → Remove US types, add UK types

2. Handle FTE change:
   → For new UK types with accruals, calculate with 32/40 = 0.8 ratio

Result: UK time off types assigned with prorated accruals
```

### 5.6 Policy Schedule Structure in Replicon

When assigning time off policies, the integration creates this structure:

```python
{
    "timeOffAccount": {
        "userUri": "urn:replicon:user:123",
        "timeOffTypeUri": "urn:replicon:time-off-type:usa-vacation"
    },
    "policySetScheduleEntries": [
        {
            "description": "Effective 0 years of service",
            "effectiveDate": {"year": 2020, "month": 1, "day": 15},
            "policySet": {
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {...},
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {"number": 48}  # Calculated yearly entitlement
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-monthly-amount",
                                "value": {"number": 4}   # Calculated monthly accrual
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:limitation-hours",
                                "value": {"number": 96}  # Calculated limitation
                            }
                        ]
                    }
                ]
            }
        },
        # Future tier (if employee will cross 7-year threshold)
        {
            "description": "Effective 7 years of service",
            "effectiveDate": {"year": 2027, "month": 1, "day": 15},
            "policySet": {
                "timeOffBalanceEventScripts": [
                    {
                        "additionalParameters": [
                            {"keyUri": "...accrual-annual-amount", "value": {"number": 64}},
                            {"keyUri": "...accrual-monthly-amount", "value": {"number": 5.34}},
                            {"keyUri": "...limitation-hours", "value": {"number": 128}}
                        ]
                    }
                ]
            }
        }
    ]
}
```

### 5.7 API Flow for Time Off Assignment

#### New User (Add Child DAG)
```
1. create_new_user
   └─ User created with timeOffTypes in payload (basic assignment)

2. if_timeoff_types_exist_before_create?
   └─ YES → continue

3. get_default_timeoff_policies_for_create
   └─ For each calculated time off type:
      └─ GET GetDefaultTimeOffPolicySetScheduleForTimeOffType

4. calculate_timeoff_policy_accruals_for_create
   └─ Apply FTE and service year calculations
   └─ Transform default policies with calculated values

5. assign_timeoff_policies_to_user
   └─ For each time off type:
      └─ POST PutUserTimeOffAccountPolicySetSchedule
```

#### Existing User (Update Child DAG)
```
1. if_fte_or_schedule_or_location_or_level_changed?
   └─ YES → continue
   └─ NO → skip time off processing

2. get_default_timeoff_policies
   └─ For all calculated time off types (new + overlapping)

3. build_timeoff_types_for_user_update
   └─ Comprehensive logic handling all 4 scenarios

4. if_location_or_level_changed?
   └─ YES → put_timeoff_assignment_for_user (add/remove types)
   └─ NO → skip assignment API

5. assign_timeoff_policies_to_updated_user
   └─ For each time off type with policies:
      └─ POST PutUserTimeOffAccountPolicySetSchedule
```

### 5.8 Key Time Off Related Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `get_timeoff_types_for_assignment()` | custom_methods.py | Determines which types apply to user |
| `get_timeoff_policy_accruals()` | custom_methods.py | Calculates accruals for all types |
| `build_policy_schedule_from_default()` | custom_methods.py | Transforms default policies with FTE |
| `build_timeoff_types_for_user_creation()` | custom_methods.py | Prepares payload for new users |
| `build_comprehensive_timeoff_assignments_for_update()` | custom_methods.py | Handles all update scenarios |
| `get_updated_timeoff_types()` | request_payload.py | Compares current vs new types |
| `check_fte_or_schedule_or_location_or_level_changes()` | request_payload.py | Detects change triggers |
| `put_timeoff_assignment_payload()` | request_payload.py | Builds assignment API payload |
| `create_policy_schedules_for_timeoff_type()` | custom_methods.py | Creates tier-based schedules |

---

## 6. Configuration & Mappers

### 6.1 Assignment Rules Mapper (30 Rules)

**Location:** `mappers/assignment_rules_mapper.py`

Controls: Schedule type, timesheet template, approval paths, activities, payrule, holiday calendar

```python
{
    "location_level_1": "United States",
    "location_level_2_to_include": [],       # All US locations
    "location_level_2_to_exclude": [],
    "department_1_to_include": ["Pro Services"],
    "employee_category_to_include": [],      # All categories

    # Outputs:
    "schedule_type": "Office Schedule",
    "timesheet_template": "Standard Billable Template",
    "timesheet_approval_path": "iPipeline B Supervisor",
    "time_entry_approval_path": "iPipeline B Project Manager",
    "time_off_template": "iPipeline US",
    "time_off_approval": "iPipeline US",
    "holiday_calendar": "Holiday US",
    "payrule": "US Payrule",
    "timesheet_period": "Weekly without crossing months",
    "work_week": "Monday-Sunday",
    "activities": []  # Activity names
}
```

### 6.2 Permissions Mapper

**Location:** `mappers/permissions_mapper.py`

Maps org role code → list of permission names

```python
PERMISSIONS_MAPPER = {
    "Senior Manager": ["Supervisor", "Schedule Manager-Supervisor"],
    "Manager": ["Supervisor", "Project Resource with Reports"],
    "Developer": ["Project Resource with Reports"],
    "Default": ["Project Resource with Reports"]  # Fallback
}
```

### 6.3 OEF Custom Mapper

**Location:** `mappers/oef_custom_mapper.py`

Maps CSV fields to Replicon Object Extension Fields

```python
OEF_FIELD_MAPPER = [
    {"field_name": "level", "oef_name": "Level", "type": "text", "can_update": True},
    {"field_name": "fte", "oef_name": "FTE", "type": "text", "can_update": True},
    {"field_name": "scheduled_hours", "oef_name": "Scheduled Hours", "type": "text", "can_update": True},
    {"field_name": "uksick", "oef_name": "UKSICK", "type": "text", "can_update": True}
]
```

---

## 7. Replicon API Services

### User Management
| Service | Endpoint | Purpose |
|---------|----------|---------|
| BulkGetUsers3 | ImportService1.svc | Lookup user by login/employee ID |
| CreateUserOrApplyModifications | ImportService2.svc | Create or update user |
| GetSupervisorAssignmentDetails | UserService1.svc | Get supervisor info |
| GetEffectiveUserGroupMembership | UserGroupService1.svc | Get current groups |

### Time Off Management
| Service | Endpoint | Purpose |
|---------|----------|---------|
| GetEnabledTimeOffTypes | TimeOffService1.svc | List available types |
| PutTimeOffTypeAssignmentsForUser | TimeOffService1.svc | Assign/remove types |
| GetDefaultTimeOffPolicySetScheduleForTimeOffType | TimeOffPolicyService2.svc | Get default policies |
| PutUserTimeOffAccountPolicySetSchedule | TimeOffPolicyService2.svc | Set policy schedules |

### Organization Management
| Service | Endpoint | Purpose |
|---------|----------|---------|
| CreateDepartmentGroupHierarchyOrApplyModifications | DepartmentGroupService1.svc | Create departments |
| CreateLocationHierarchyOrApplyModifications | LocationService1.svc | Create locations |
| CreateEmployeeTypeGroupHierarchyOrApplyModifications | EmployeeTypeGroupService1.svc | Create employee types |
| CreateProjectRoleOrApplyModifications | ProjectRoleService1.svc | Create project roles |

---

## 8. Error Handling & Logging

### Log Severity Levels
| Level | Description | Example |
|-------|-------------|---------|
| Success | Operation completed fully | "User created successfully" |
| Exception | Partial success with warnings | "User created - Project role not found" |
| Error | Operation failed | "Validation failed: Email missing" |
| Pending | Awaiting future action | "Supervisor from feed - pending assignment" |

### Logged Exception Types
- Missing mandatory fields
- URIs not found (location, department, project role, etc.)
- Supervisor not in system and not in feed
- Time off types not in Replicon
- Resource pool not found
- API errors

---

## 9. Technical Reference

### Key Airflow Variables
| Variable | Purpose | Default |
|----------|---------|---------|
| `ipipeline_user_import_can_run_batch_task_trial` | Enable/disable batch processing | true |
| `ipipeline_user_import_can_use_reference_file_trial` | Enable/disable delta processing | true |
| `dagrun_internal_testing_email` | Success notification recipient | - |
| `dagrun_failure_alert_email` | Error notification recipient | - |

### Performance Settings
| Setting | Value |
|---------|-------|
| Max Active DAG Runs | 1 |
| User Child DAG Parallelism | 5 |
| File Sensor Timeout | 5 minutes (soft fail) |
| Gather Logs Timeout | 2 hours |

### Date Formats
| Format | Usage |
|--------|-------|
| MM/DD/YYYY | Input CSV dates (REP_DATE_FORMAT) |
| YYYY/MM/DD | Internal processing (YMD_DATE_FORMAT) |
| YYYY-MM-DD | Replicon API dates |
| ISO 8601 | Email timestamps |

---

## Appendix A: Complete Time Off Accrual Tiers

### USA Vacation (Non-California)
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| 0-7 years | 5 hours | 120 hours |
| 7+ years | 6.67 hours | 160 hours |

### USA Illness/Sick
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| 0-2 years | 1.66 hours | 40 hours |
| 2+ years | 2 hours | 48 hours |

### UK Holiday
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| All | 15.625 hours | 187.5 hours |

### UK Illness/Sick
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| 0-2 years | 5 hours | 45 hours |
| 2-3 years | 7.5 hours | 90 hours |
| 3+ years | 12.5 hours | 120 hours |

### Canada Vacation (10 Tiers)
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| 0-3 years | 5 hours | 120 hours |
| 3-4 years | 5.66 hours | 136 hours |
| 4-5 years | 6 hours | 144 hours |
| 5-6 years | 6.33 hours | 152 hours |
| 6-7 years | 6.66 hours | 160 hours |
| 7-8 years | 7 hours | 168 hours |
| 8-9 years | 7.33 hours | 176 hours |
| 9-10 years | 7.66 hours | 184 hours |
| 10-11 years | 8 hours | 192 hours |
| 11+ years | 8.33 hours | 200 hours |

### Japan Vacation
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| All | 13.33 hours | 160 hours |

### USA Personal (Non-California)
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| 1+ years | 0.66 hours | 16 hours |

### Canada Personal
| Service Years | Monthly Rate | Annual Limit |
|---------------|--------------|--------------|
| 1+ years | 3.33 hours | 80 hours |

---

*Document generated for iPipeline User Import Integration - Time Off Policy Deep Dive*
