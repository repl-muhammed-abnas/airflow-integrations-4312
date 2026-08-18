# Canada V1 - NONE Fallback Changes

**File:** `user_import_canada_v1/utils/request_payload.py`
**Date:** 2025-12-04
**Branch:** RIT-17381

---

## Summary

Implemented NONE fallback logic for Schedule Type, Holiday Calendar, Payrule, and Timesheet Period when mapper values are not available for International Assignee (IA=1) users.

---

## Changes Made

### 1. Added `NONE_DEFAULT_VALUE` Constant (Line 14)

```python
# Default fallback value for Schedule Type, Holiday Calendar, Payrule, and Timesheet Period
# when mapper value is not available. Update this constant if the name changes in Replicon.
NONE_DEFAULT_VALUE = "NONE"
```

**Purpose:** Centralized constant for the fallback value. If the name changes in Replicon, only this constant needs to be updated.

---

### 2. Added `_is_international_assignee()` Helper Function (Lines 17-28)

```python
def _is_international_assignee(is_ia):
    """
    Check if user is an International Assignee (IA=1).
    Centralized check for consistent IA validation across all functions.

    Args:
        is_ia: The IA value to check (can be int or string)

    Returns:
        True if is_ia equals 1 or '1', False otherwise
    """
    return is_ia in [1, '1']
```

**Purpose:** Centralized helper function for IA checking that:
- Handles both integer (1) and string ('1') values
- Provides single point of change if IA detection logic needs updating
- Improves code readability and maintainability

---

### 3. Updated `_get_schedule_policy_schedule_payload()` - Schedule Type NONE Fallback (Lines 32-83)

**Before:**
```python
def _get_schedule_policy_schedule_payload(dag_run, exception_log):
    # ... schedule_name logic ...
    if schedule_name:
        return [
            {
                "schedulePolicy": {
                    # ...
                    "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                },
                "effectiveDate": null
            }
        ]
```

**After:**
```python
def _get_schedule_policy_schedule_payload(dag_run, exception_log, ia_effective_date=null):
    """
    Get schedule policy payload for user creation/update.

    If schedule_type_uri is not available from mapper, assigns NONE schedule type
    with IA start date as effective date (if IA=1).
    If schedule policy (office_schedule) is not available, keeps it blank (returns []).
    """
    # ... schedule_name logic ...
    if schedule_name:
        # Use mapper schedule_type_uri if available, otherwise use NONE with IA effective date
        schedule_type_uri = dag_run.conf['mapper_data']['schedule_type_uri']
        effective_date = null

        if not schedule_type_uri:
            schedule_type_uri = null  # Will use name lookup instead
            schedule_type_name = NONE_DEFAULT_VALUE
            # Use IA start date as effective date when assigning NONE
            effective_date = ia_effective_date
            exception_log.append(f"Schedule type not available in mapper. Assigning {NONE_DEFAULT_VALUE} schedule type")
        else:
            schedule_type_name = null

        return [
            {
                "schedulePolicy": {
                    # ...
                    "scheduleTypeUri": schedule_type_uri,
                    "scheduleType": {
                        "uri": schedule_type_uri,
                        "name": schedule_type_name
                    } if not schedule_type_uri else null
                },
                "effectiveDate": effective_date
            }
        ]
```

**Changes:**
- Added `ia_effective_date` parameter for IA=1 effective date
- Added NONE fallback logic when `schedule_type_uri` is not available
- Added `scheduleType` object with name lookup for NONE assignment
- Uses IA start date as effective date when assigning NONE

---

### 4. Updated `_get_holiday_calendar_to_assign()` - Holiday Calendar NONE Fallback (Lines 74-94)

**Before:**
```python
def _get_holiday_calendar_to_assign(dag_run):
    if dag_run.conf['mapper_data']["holiday_calendar_uri"]:
        return {
            "uri": dag_run.conf['mapper_data']["holiday_calendar_uri"]['uri'],
            "name": null
        }
    return null
```

**After:**
```python
def _get_holiday_calendar_to_assign(dag_run, exception_log):
    """
    Get holiday calendar payload for user creation.
    If IA=1 and holiday_calendar_uri is not available from mapper, assigns NONE holiday calendar.
    """
    if dag_run.conf['mapper_data']["holiday_calendar_uri"]:
        return {
            "uri": dag_run.conf['mapper_data']["holiday_calendar_uri"]['uri'],
            "name": null
        }

    # If IA=1 and holiday calendar not available from mapper, assign NONE
    is_ia = dag_run.conf['file_data'].get('is_ia')
    if _is_international_assignee(is_ia):
        exception_log.append(f"Holiday calendar not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} holiday calendar")
        return {
            "uri": null,
            "name": NONE_DEFAULT_VALUE
        }

    return null
```

**Changes:**
- Added `exception_log` parameter
- Added IA=1 check for NONE fallback
- Logs exception when NONE is assigned

---

### 5. Updated `_get_timesheet_period_schedule_to_assign()` (Lines 367-398)

**Before:**
```python
def _get_timesheet_period_schedule_to_assign(dag_run):
    timesheet_period_name = dag_run.conf['mapper_data'].get('timesheet_period')
    # ... original logic
```

**After:**
```python
def _get_timesheet_period_schedule_to_assign(dag_run, exception_log, ia_effective_date=null):
    """
    Get timesheet period schedule payload for user creation.
    If IA=1 and no timesheet_period from mapper, assigns NONE timesheet period.
    """
    is_ia = dag_run.conf['file_data'].get('is_ia')
    timesheet_period_name = dag_run.conf['mapper_data'].get('timesheet_period')

    # ... effective date logic ...

    # If IA=1 and no timesheet period from mapper, assign NONE
    if not timesheet_period_name and _is_international_assignee(is_ia):
        timesheet_period_name = NONE_DEFAULT_VALUE
        exception_log.append(f"Timesheet period not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} timesheet period")
        # For IA=1 NONE fallback, use IA effective date
        effective_date = ia_effective_date

    # ... rest of function
```

**Changes:**
- Added `exception_log` parameter
- Added `ia_effective_date` parameter for IA=1 effective date
- Added IA=1 NONE fallback logic
- Uses IA start date as effective date for NONE assignment

---

### 6. Updated `_get_payrule_script_schedule_to_assign()` (Lines 401-434)

**Before:**
```python
def _get_payrule_script_schedule_to_assign(dag_run):
    payrule_name = dag_run.conf['payrule'].get('payrule')
    if payrule_name and dag_run.conf['file_data']['country'] == "Canada" and dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
        return [...]
    return []
```

**After:**
```python
def _get_payrule_script_schedule_to_assign(dag_run, exception_log, ia_effective_date=null):
    """
    Get payrule script schedule payload for user creation.
    If IA=1 and no payrule from mapper, assigns NONE payrule.
    """
    is_ia = dag_run.conf['file_data'].get('is_ia')
    payrule_name = dag_run.conf['payrule'].get('payrule')

    # If IA=1 and no payrule from mapper, assign NONE
    if _is_international_assignee(is_ia) and not payrule_name:
        payrule_name = NONE_DEFAULT_VALUE
        exception_log.append(f"Payrule not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} payrule")
        return [
            {
                "payRuleScript": {
                    "uri": null,
                    "name": payrule_name
                },
                "effectiveDate": ia_effective_date
            }
        ]

    # Original logic: only assign payrule for Canada non-L1/L2
    if payrule_name and dag_run.conf['file_data']['country'] == "Canada" and dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
        return [...]
    return []
```

**Changes:**
- Added `exception_log` parameter
- Added `ia_effective_date` parameter
- Added IA=1 NONE fallback logic (takes priority over original logic)
- Uses IA start date as effective date for NONE assignment

---

### 7. Updated `get_user_creation_payload()` (Lines 437-502)

**Added IA effective date calculation (Lines 441-448):**
```python
# Calculate IA effective date for NONE fallback assignments
is_ia = dag_run.conf['file_data'].get('is_ia')
ia_effective_date = null
if _is_international_assignee(is_ia):
    if dag_run.conf['file_data'].get('ia_start_date'):
        ia_effective_date = get_replicon_date(dag_run.conf['file_data']['ia_start_date'])
    else:
        ia_effective_date = get_todays_date_in_json()
```

**Updated function calls to pass new parameters:**
```python
payload["user"]['holidayCalendar'] = _get_holiday_calendar_to_assign(dag_run, exception_log)
payload["user"]['timesheetPeriodSchedule'] = _get_timesheet_period_schedule_to_assign(dag_run, exception_log, ia_effective_date)
payload["user"]['payRuleScriptSchedule'] = _get_payrule_script_schedule_to_assign(dag_run, exception_log, ia_effective_date)
```

---

### 8. Updated `prepare_update_user_payload_callable()` - Payrule Update (Lines 913-952)

**Added NONE fallback for IA=1 users:**
```python
# Payrule update logic with NONE fallback for IA=1
payrule_name = dag_run.conf['payrule'].get('payrule')

# If IA=1 and no payrule from mapper, assign NONE
if _is_international_assignee(is_ia) and not payrule_name:
    payrule_name = NONE_DEFAULT_VALUE
    logger.append(f"Payrule not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} payrule")
    payload['modifications']['payRulesScheduleModifications'] = {
        "scheduleEntries": [
            {
                "payRuleScript": {
                    "uri": null,
                    "name": payrule_name
                },
                "effectiveDate": ia_effective_date
            }
        ]
    }
    rail.set_result(key="payrule_updated", val="Yes")
elif payrule_name:
    # ... original update logic
```

**Note:** NO `ia_updated` check - applies to ALL IA=1 users regardless of whether IA status changed.

---

### 9. Updated `prepare_update_user_payload_callable()` - Timesheet Period Update (Lines 990-1038)

**Added NONE fallback for IA=1 users:**
```python
# Timesheet Period update logic with NONE fallback for IA=1
timesheet_period_name = dag_run.conf['mapper_data'].get('timesheet_period')

# If IA=1 and no timesheet period from mapper, assign NONE
if _is_international_assignee(is_ia) and not timesheet_period_name:
    timesheet_period_name = NONE_DEFAULT_VALUE
    logger.append(f"Timesheet period not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} timesheet period")
    payload['modifications']['timesheetPeriodScheduleToApply'] = {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": timesheet_period_name
                    },
                    "effectiveDate": ia_effective_date
                }
            ],
            "endDate": null
        }
    }
elif _get_user_details['timesheetPeriodSchedule'] and timesheet_period_name:
    # ... original update logic
```

**Note:** NO `ia_updated` check - applies to ALL IA=1 users regardless of whether IA status changed.

---

### 10. Updated `prepare_update_user_payload_callable()` - Holiday Calendar Update (Lines 1272-1283)

**Added NONE fallback for IA=1 users:**
```python
# Holiday Calendar NONE fallback for update (IA=1 only)
# If IA=1, no holiday calendar from mapper, and user doesn't have NONE, assign NONE
if _is_international_assignee(is_ia) and not dag_run.conf['mapper_data'].get('holiday_calendar_uri'):
    user_holiday_calendar = user_details.get('holidayCalendar', {}).get('displayText', '') if user_details.get('holidayCalendar') else ''
    if user_holiday_calendar != NONE_DEFAULT_VALUE:
        logger.append(f"Holiday calendar not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} holiday calendar")
        payload['modifications']['holidayCalendarToApply'] = {
            "holidayCalendar": {
                "uri": null,
                "name": NONE_DEFAULT_VALUE
            }
        }
```

**Key behavior:**
- Only assigns NONE if user doesn't already have NONE (prevents duplicate assignments)
- Only applies to IA=1 users

---

### 11. Added IA Effective Date Calculation in Update Function (Lines 868-875)

```python
# Get IA info for NONE fallback logic
is_ia = dag_run.conf['file_data'].get('is_ia')
ia_effective_date = null
if _is_international_assignee(is_ia):
    if dag_run.conf['file_data'].get('ia_start_date'):
        ia_effective_date = get_replicon_date(dag_run.conf['file_data']['ia_start_date'])
    else:
        ia_effective_date = get_todays_date_in_json()
```

---

## Key Implementation Decisions

### 1. Holiday Calendar - IA=1 Only
Holiday Calendar NONE fallback only applies to IA=1 users (not all users). This maintains consistency with Payrule and Timesheet Period logic.

### 2. No `ia_updated` Check in Update Functions
NONE fallback on updates applies to ALL IA=1 users, regardless of whether IA status changed. This ensures existing IA=1 users without values also get NONE assigned.

### 3. Centralized `_is_international_assignee()` Helper
Created to avoid code duplication and ensure consistent IA detection across all functions.

---

## Exception Log Messages

The following messages are logged to `exception_log` when NONE fallback is used:

| Scenario | Message |
|----------|---------|
| Holiday Calendar (Create/Update) | "Holiday calendar not available in mapper for IA=1. Assigning NONE holiday calendar" |
| Payrule (Create/Update) | "Payrule not available in mapper for IA=1. Assigning NONE payrule" |
| Timesheet Period (Create/Update) | "Timesheet period not available in mapper for IA=1. Assigning NONE timesheet period" |

---

## Testing Considerations

1. **User Creation (IA=1 without mapper values)**
   - Create an IA=1 user without holiday calendar, payrule, or timesheet period in mapper
   - Verify NONE is assigned for all three fields
   - Verify exception logs contain the appropriate messages

2. **User Update (Existing IA=1 without mapper values)**
   - Update an existing IA=1 user who doesn't have NONE values
   - Verify NONE is assigned for missing fields
   - Verify user who already has NONE doesn't get duplicate assignment

3. **Non-IA Users**
   - Verify non-IA users (IA=0 or no IA value) do NOT get NONE assigned
   - Verify original logic still works for non-IA users
