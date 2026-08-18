"""
Custom utility methods for T-Systems Clock In/Out Export integration.
"""
import json
import pendulum
import rail

def generate_export_filename(prefix, company_code, timestamp_format):
    """
    Generate export filename with timestamp.
    
    Format: ClockInOut_REPLICON_OUT_6205_20250605_100512.json
    
    Args:
        prefix: File prefix (e.g., 'ClockInOut_REPLICON_OUT')
        company_code: Company code (e.g., '6205')
        timestamp_format: Timestamp format string
        
    Returns:
        str: Generated filename
    """
    timestamp = pendulum.now().strftime(timestamp_format)
    return f"{prefix}_{company_code}_{timestamp}.json"

def create_json_export(records, filename, config):
    """
    Create JSON export file with proper structure.
    
    Args:
        records: List of processed clock records
        filename: Export filename
        config: Configuration object
        
    Returns:
        str: JSON content as string artifact
    """
    export_data = {
        'FileName': filename.replace('.json', ''),
        'Exporting System': config.exporting_system,
        'Integration Name': config.integration_name,
        'Company Code': config.company_code,
        'Time Stamp': pendulum.now().strftime(config.timestamp_format),
        'Exported Data': config.export_data_type,
        'Clock InOut': records
    }
    
    # Convert to JSON with proper formatting
    return json.dumps(export_data, indent=2, ensure_ascii=False)

def get_work_type(record):
    """
    Get the mapped work type based on the record's work type fields.
    Tries fields in priority order as per tech spec:
    1. WorkType HR200 (primary)
    2. WorkType HR200 Tarif (secondary) 
    3. WorkType HR200 Tariffrei (tertiary)
    Returns code value if found in report against the work type, otherwise returns None.
    """
    # Try each work type field in priority order
    if record.get('worktype_hr200'):
        return record.get('worktype_hr200_code')
    elif record.get('worktype_hr200_tarif'):
        return record.get('worktype_hr200_tarif_code')
    elif record.get('worktype_hr200_tariffrei'):
        return record.get('worktype_hr200_tariffrei_code')
    return None

def process_clock_records():
    report_data = rail.load_all_records(rail.result('query_valid_records'))
    return list(map(lambda record:{
        'CID':int(record['employee_id']) if record['employee_id'].isdigit() else
              record['employee_id'],
        'SAP Personal Number': int(record['personal_number'].replace('.', '').split(',')[0]) if record['personal_number'] else None,
        'Attendance Date': record['entry_date'],
        'Clock In': f"{record['clock_in'].split(':')[0].zfill(2)}:{record['clock_in'].split(':')[1].zfill(2)}" if ':' in record['clock_in'] else record['clock_in'],
        'Clock Out': '24:00' if record['clock_out'] == '23:59:59' else f"{record['clock_out'].split(':')[0].zfill(2)}:{record['clock_out'].split(':')[1].zfill(2)}" if ':' in record['clock_out'] else record['clock_out'],
        'Number of Hours': "{:05.2f}".format(float(record['hours'].replace(',', '.'))) if record['hours'] else "00.00",
        'Work Type': get_work_type(record),
    }, report_data
    ))
