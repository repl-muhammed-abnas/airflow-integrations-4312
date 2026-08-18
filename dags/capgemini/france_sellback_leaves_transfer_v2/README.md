# Capgemini France Sellback Leaves Transfer V2

## Overview
This is V2 of the France Sellback Leaves Transfer workflow, updated for 2026 to handle the new time-off type naming conventions.

## Key Changes from V1

### 1. Time-Off Type Mapping Updates
**Updated for 2026 year requirements:**
- RTT Salarié types now use "2025" naming (for year 2026 processing)
- JNT Salarié types now use "2025" naming (for year 2026 processing)
- CET destination types remain the same

**New Mapping (`mappers/transfer_timeoff_types.py`):**
```python
timeoff_types = {
    "[FRA] A - RTT Salarié 2025 (UES Capgemini)": "[FRA] A - CET (UES Capgemini)",
    "[FRA] A - RTT Salarié 2025 (UES Altran)": "[FRA] A - CET (UES Altran)",
    "[FRA] A - JNT Salarié 2025 (UES Altran)": "[FRA] A - CET (UES Altran)"
}
```

**Previous V1 Mapping:**
```python
timeoff_types = {
    "[FRA] A - RTT Salarié (UES Capgemini)": "[FRA] A - CET (UES Altran)",
    "[FRA] A - RTT Salarié 2024 (UES Altran)": "[FRA] A - CET (UES Altran)",
    "[FRA] A - JNT Salarié 2024 (UES Altran)": "[FRA] A - CET (UES Altran)"
}
```

### 2. Enhanced Error Handling
**Added validation check for unmapped time-off types:**
- New task: `is_timeoff_mapping_exists` - validates that a mapping exists before processing
- New logging: `log_timeoff_mapping_not_found` - records exceptions when no mapping is found
- Prevents processing of time-off types that aren't in the mapper

**Flow Enhancement:**
```
create_log 
  → is_timeoff_mapping_exists
    → [Yes] → get_credit_to_timeoff (proceed as normal)
    → [No] → log_timeoff_mapping_not_found → catch_and_log_errors
```

This prevents failures when:
- Old time-off types (e.g., 2024 variants) appear in the report
- New time-off types are added to Replicon but not yet mapped
- Manual testing with unmapped time-off types

### 3. Updated DAG Configuration
**All instance files updated with V2 naming:**
- `can_run_batch_task_var_name` → includes `_v2` suffix
- `master_dagid` → includes `_v2` suffix  
- `assign_policy_child_dagid` → includes `_v2` suffix

**Start date updated:**
- V1: `start_date=datetime(2024, 12, 1, tz=config.time_zone)`
- V2: `start_date=datetime(2026, 1, 1, tz=config.time_zone)`

## Workflow Architecture

### Master DAG (`master.py`)
1. Runs the France Sell Back Leaves Transfer report
2. Filters for "Sell Back" event types
3. Retrieves time-off policy script URIs
4. Triggers parallel child DAG runs (one per sellback record)
5. Gathers logs from all child runs
6. Generates CSV log file and uploads to SFTP
7. Sends completion email

### Child DAG (`assign_policy_to_user.py`)
For each sellback record:
1. Validates mapping exists for source time-off type
2. Retrieves destination CET time-off type URI
3. Validates CET type is enabled in Replicon
4. Checks if user has CET type assigned
5. Updates user's CET balance by adding sellback amount
6. Configures yearly reset policy (resets to 0 on Nov 30)
7. Logs success/exception/error status

## Configuration Files

### Instances
- `instances/dev.py` - CapgeminiDev environment
- `instances/sit.py` - CapgeminiSIT environment  
- `instances/uat.py` - CapgeminiUAT environment
- `instances/production.py` - Capgemini production environment

### Common Settings
- **Schedule**: Daily at 1 AM UTC
- **Report**: "France Sell Back Leaves Transfers V1" (same as V1)
- **Connections**: Same Replicon and SFTP connections as V1
- **Parallel Processing**: 10 concurrent child DAG runs
- **Timeout**: 14 days for batch operations

## Date Range Capability
Can be triggered with custom date ranges via DAG run configuration:
```json
{
  "start_date": "01/15/2026",
  "end_date": "01/15/2026"
}
```

**Format**: MM/DD/YYYY
**Default**: Previous day's data

## Deployment Strategy

### Recommended Approach:
1. **Deploy V2 to Dev/SIT first** - Test with 2025-named time-off types
2. **Run parallel with V1 in UAT** - Compare results during transition period
3. **Disable V1 in Production** - Once V2 is validated
4. **Deploy V2 to Production** - Effective January 1, 2026

### Coexistence Notes:
- V1 and V2 use different Airflow variable names (with `_v2` suffix)
- Both can run simultaneously without conflicts
- Separate DAG IDs prevent collision

## Monitoring

### Log Structure
CSV logs uploaded to: `/Internal/France_RTT_Sellback_Leaves_Transfer/Logs`

**Columns:**
- Username
- Employee ID  
- Sell Back Source Time Off Type
- Sell Back Amount
- Sell Back To Time Off Type
- Status (Success/Exception/Error)
- Comments
- RunID

### Email Notifications
**Success:** Sent to tenant and internal logs email
**Exceptions:** Sent to tenant and internal logs email  
**Errors:** Sent to tenant and alert email (escalation)

## Testing Checklist

### Dev/SIT Testing:
- [ ] Verify 2025 RTT Salarié (UES Capgemini) → CET (UES Capgemini)
- [ ] Verify 2025 RTT Salarié (UES Altran) → CET (UES Altran)
- [ ] Verify 2025 JNT Salarié (UES Altran) → CET (UES Altran)
- [ ] Test unmapped time-off type handling (should log exception)
- [ ] Test disabled CET type handling (should log exception)
- [ ] Test user without CET policy (should log exception)
- [ ] Verify log file generation and SFTP upload
- [ ] Verify email notifications

### UAT Testing:
- [ ] Run V1 and V2 in parallel with same date range
- [ ] Compare log outputs for consistency
- [ ] Verify V2 properly handles 2025 variants
- [ ] Confirm V1 handles any remaining 2024 variants

## Support

**For issues or questions:**
- Contact: capgeminisupportreplicon@deltek.com
- Support Portal: https://support.deltek.com

## Version History

### V2 (January 2026)
- Updated time-off type mappings for 2026
- Added validation for unmapped time-off types
- Enhanced error handling and logging
- Updated start date to 2026

### V1 (December 2024)
- Initial implementation
- Support for 2024 time-off types
