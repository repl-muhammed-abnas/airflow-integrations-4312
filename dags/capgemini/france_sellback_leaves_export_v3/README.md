# Capgemini France Sellback Leaves Export V3

## Overview
This is V3 of the France Sellback Leaves Export workflow, updated for 2026 to handle the new time-off type naming conventions.

## Key Changes from V2

### 1. Time-Off Type Code Mappings Updates
**Updated for 2026 year requirements:**
- RTT Salarié types now use "2025" naming (for year 2026 processing)
- JNT Salarié types now use "2025" naming (for year 2026 processing)
- CET types have updated SAP codes (CE9 for Capgemini, CE6 for Altran)

**New Mapping (`mappers/codes_on_timeoffs.py`):**
```python
codes_to_export = [
    {
        "timeoff_type_name": "[FRA] A - RTT Salarié 2025 (UES Capgemini)",
        "ZYOQ_CODCON": "RS9",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "SL"
    },
    {
        "timeoff_type_name": "[FRA] A - RTT Salarié 2025 (UES Altran)",
        "ZYOQ_CODCON": "RS6",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "SL"
    },
    {
        "timeoff_type_name": "[FRA] A - JNT Salarié 2025 (UES Altran)",
        "ZYOQ_CODCON": "JS6",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "SL"
    },
    {
        "timeoff_type_name": "[FRA] A - CET (UES Altran)",
        "ZYOQ_CODCON": "CE6",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "PM"
    },
    {
        "timeoff_type_name": "[FRA] A - CET (UES Capgemini)",
        "ZYOQ_CODCON": "CE9",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "PM"
    }
]
```

**Previous V2 Mapping:**
```python
codes_to_export = [
    {
        "timeoff_type_name": "[FRA] A - RTT Salarié (UES Capgemini)",
        "ZYOQ_CODCON": "RS6",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "SL"
    },
    {
        "timeoff_type_name": "[FRA] A - RTT Salarié 2024 (UES Altran)",
        "ZYOQ_CODCON": "RS6",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "SL"
    },
    {
        "timeoff_type_name": "[FRA] A - JNT Salarié 2024 (UES Altran)",
        "ZYOQ_CODCON": "JS6",
        "ZYOQ_TYPAJU": "S",
        "ZYOQ_MOTIFA": "PM"
    },
    ...
]
```

### 2. SAP Export Code Changes

**Key Changes:**
- **CET (UES Capgemini)**: Code changed from `CE6` → `CE9`
- **RTT Salarié (UES Capgemini)**: Code changed from `RS6` → `RS9`
- All time-off types now use year 2025 naming

### 3. Updated DAG Configuration
**All instance files updated with V3 naming:**
- `can_run_batch_task_var_name` → includes `_v3` suffix
- `master_dagid` → includes `_v3` suffix  
- `export_child_dagid` → includes `_v3` suffix

**Start date updated:**
- V2: `start_date=datetime(2025, 1, 1)`
- V3: `start_date=datetime(2026, 1, 1)`

## Workflow Architecture

### Master DAG (`master.py`)
1. Runs the France Sell Back Leaves Export report
2. Processes report data and creates collection
3. Filters for valid employee IDs with "Sell Back" events
4. Separates time-off types by adjustment type:
   - **SL (Sell Back)**: RTT Salarié and JNT Salarié types
   - **PM (Payment)**: CET types
5. Triggers two child DAG runs (one for SL, one for PM)
6. Each child generates separate exports

### Child DAG (`export_child.py`)
For each adjustment type (SL or PM):
1. Queries filtered sellback data for the specific adjustment type
2. Transforms data into SAP format with proper codes
3. Writes CSV file with fixed-width format
4. Uploads to S3 (unencrypted backup)
5. Encrypts with PGP
6. Uploads encrypted file to SFTP
7. Sends completion email

### Data Transformation
Each row is formatted as a fixed-width string:
```
000000000CAP{employee_id}{spaces}*FZYOQ{spaces}30{ZYOQ_CODCON}{year}0000{date}{ZYOQ_TYPAJU}000{date}{ZYOQ_MOTIFA}{amount}{timeoff_type_name}
```

**Example Output:**
```
000000000CAP00012345                        *FZYOQ                  30RS920260000002026-01-15S0002026-01-15SL-00500[FRA] A - RTT Salarié 2025 (UES Capgemini)
```

## Configuration Files

### Instances
- `instances/dev.py` - CapgeminiDev environment
- `instances/sit.py` - CapgeminiSIT environment  
- `instances/uat.py` - CapgeminiUAT environment
- `instances/production.py` - Capgemini production environment

### Common Settings
- **Schedule**: Daily at 1 AM UTC
- **Report**: "France Sell Back Leaves Export V1" (same as V2)
- **Connections**: Same Replicon, SFTP, and PGP connections as V2
- **S3 Bucket**: replicon.integration_eu_s3_bucket
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

## Export Files

### File Naming Convention
- **SL Export**: `Rep_CET_SL_FRA_{timestamp}.txt.pgp`
- **PM Export**: `Rep_CET_PM_FRA_{timestamp}.txt.pgp`

### File Locations
**SFTP**: `/Outbound/France_RTT_CET_Sellback_Leaves_Export/Input`
**S3**: `{CompanyKey}/Outbound/France_RTT_CET_Sellback_Leaves_Export/Input`

## Deployment Strategy

### Recommended Approach:
1. **Deploy V3 to Dev/SIT first** - Test with 2025-named time-off types
2. **Validate SAP code changes** - Ensure CE9 and RS9 codes work in SAP
3. **Run parallel with V2 in UAT** - Compare exports during transition
4. **Coordinate with SAP team** - Confirm they're ready for new codes
5. **Disable V2 in Production** - Once V3 is validated
6. **Deploy V3 to Production** - Effective January 1, 2026

### Coexistence Notes:
- V2 and V3 use different Airflow variable names (with `_v3` suffix)
- Both can run simultaneously without conflicts
- Separate DAG IDs prevent collision
- Use different file prefixes if running parallel (optional)

## Monitoring

### Success Indicators
- Two child DAG runs complete (SL and PM)
- Two encrypted files uploaded to SFTP
- Two backup files in S3
- Two completion emails sent

### Email Notifications
**Empty Export**: When no records exist for that adjustment type
**Valid Export**: When records are successfully exported

**Email Recipients:**
- **To**: Capgemini support team
- **BCC**: Deltek internal logs email

## Testing Checklist

### Dev/SIT Testing:
- [ ] Verify RTT Salarié 2025 (UES Capgemini) exports with RS9 code
- [ ] Verify RTT Salarié 2025 (UES Altran) exports with RS6 code
- [ ] Verify JNT Salarié 2025 (UES Altran) exports with JS6 code
- [ ] Verify CET (UES Capgemini) exports with CE9 code (PM type)
- [ ] Verify CET (UES Altran) exports with CE6 code (PM type)
- [ ] Check SL export file format and content
- [ ] Check PM export file format and content
- [ ] Verify S3 uploads for both files
- [ ] Verify SFTP uploads for both encrypted files
- [ ] Test empty export scenario (no data)
- [ ] Verify email notifications

### UAT Testing:
- [ ] Run V2 and V3 in parallel with same date range
- [ ] Compare SL export outputs (V2 vs V3)
- [ ] Compare PM export outputs (V2 vs V3)
- [ ] Validate SAP codes with SAP team
- [ ] Confirm CE9 code works for Capgemini CET
- [ ] Confirm file naming matches expectations

### SAP Integration Testing:
- [ ] Import V3 SL file into SAP test environment
- [ ] Import V3 PM file into SAP test environment
- [ ] Verify RS9 code processes correctly for Capgemini
- [ ] Verify CE9 code processes correctly for Capgemini
- [ ] Confirm balances update correctly in SAP
- [ ] Test with various sellback amounts

## SAP Export Format Details

### Fixed-Width Fields
| Field | Position | Length | Description | Example |
|-------|----------|--------|-------------|---------|
| Prefix | 1-9 | 9 | Always "000000000" | 000000000 |
| Employee ID | 10-20 | 11 | CAP + 8-digit padded ID | CAP00012345 |
| Spaces | 21-44 | 24 | Blank spaces | (spaces) |
| Marker | 45-46 | 2 | Always "*F" | *F |
| Constant | 47-50 | 4 | Always "ZYOQ" | ZYOQ |
| Spaces | 51-52 | 2 | Blank spaces | (spaces) |
| Spaces | 53-72 | 20 | Blank spaces | (spaces) |
| Version | 73 | 1 | Always "3" | 3 |
| Zero | 74 | 1 | Always "0" | 0 |
| SAP Code | 75-77 | 3 | ZYOQ_CODCON from mapper | RS9 |
| Year | 78-81 | 4 | Current year | 2026 |
| Zeros | 82-85 | 4 | Always "0000" | 0000 |
| Date1 | 86-95 | 10 | Sellback date YYYY-MM-DD | 2026-01-15 |
| Type | 96 | 1 | ZYOQ_TYPAJU (always "S") | S |
| Zeros | 97-99 | 3 | Always "000" | 000 |
| Date2 | 100-109 | 10 | Sellback date YYYY-MM-DD | 2026-01-15 |
| Reason | 110-111 | 2 | ZYOQ_MOTIFA (SL or PM) | SL |
| Amount | 112-117 | 6 | Negative amount * 100 | -00500 |
| TimeOffType | 118+ | Variable | Full time-off type name | [FRA] A - RTT... |

### Amount Encoding
- Amount is multiplied by 100 and formatted as negative
- Example: 5 days → `-00500`
- Example: 10.5 days → `-01050`

## Support

**For issues or questions:**
- Contact: capgeminisupportreplicon@deltek.com
- Support Portal: https://support.deltek.com

## Troubleshooting

### Common Issues

**Issue**: No files generated
- Check if report has data for the date range
- Verify time-off types match mapper exactly
- Check batch task variable is set to 'true'

**Issue**: Wrong SAP codes in export
- Verify mapper configuration for your instance
- Check time-off type names match exactly (including year)
- Ensure mapper is imported correctly in instance file

**Issue**: SFTP upload fails
- Verify SFTP connection is active
- Check file path permissions
- Validate PGP encryption succeeded

**Issue**: S3 upload fails
- Verify AWS connection and bucket permissions
- Check bucket name variable is set correctly
- Ensure bucket exists in correct region (eu-central-1)

## Version History

### V3 (January 2026)
- Updated time-off type mappings for 2026 (2025 year suffix)
- Changed CET (UES Capgemini) SAP code from CE6 → CE9
- Changed RTT Salarié (UES Capgemini) SAP code from RS6 → RS9
- Updated start date to 2026

### V2 (January 2025)
- Updated mappings for 2025 time-off types
- Enhanced error handling
- Improved logging

### V1 (December 2024)
- Initial implementation
- Support for 2024 time-off types
