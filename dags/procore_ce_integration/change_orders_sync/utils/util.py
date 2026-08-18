import rail
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from procore_ce_integration.change_orders_sync.config import procore_webhook_fmt
from procore_ce_integration.change_orders_sync.utils.constants import APPROVED, RFC_FIELD_LIMITS


def extract_ce_code(origin_id):
    """Extract CE code from origin_id (format: CE_CODE)"""
    if origin_id and str(origin_id).startswith('CE_'):
        return str(origin_id)[3:]
    return None

def validate_field_length(value: str, field_name: str) -> str:
    """Validate field value length and raise error if exceeds CE field length limits."""
    if not value:
        return ''

    max_length = RFC_FIELD_LIMITS.get(field_name)

    if not max_length:
        return str(value)

    value_str = str(value).strip()

    if len(value_str) <= max_length:
        return value_str

    # Only description should be truncated, others should error
    if field_name == 'description':
        return value_str[:max_length]

    # For critical fields (jobnum, rfcnum, phasenum, catnum), raise error
    raise ValueError(f"{field_name} exceeds maximum length of {max_length}: '{value_str}' (length: {len(value_str)})")

def is_resource_ready(resource):
    if not resource:
        return False
    return resource.get('status') == APPROVED or bool(resource.get('custom_field'))

def is_sync_custom_field_present(resource, custom_field_key):
    if not custom_field_key:
        return False
    return custom_field_key in (resource.get('custom_fields') or {})

def parse_webhook_timestamp(timestamp_str, default_year=1900):
    try:
        return datetime.strptime(timestamp_str, procore_webhook_fmt)
    except (ValueError, TypeError):
        return datetime(default_year, 1, 1)

def parse_iso(timestamp_str):
    try:
        return datetime.fromisoformat(
            timestamp_str.replace('Z', '+00:00')
        )
    except (ValueError, TypeError):
        raise ValueError(f'Could not parse time: {timestamp_str}')

def get_change_event_duration(timestamp_str):
    if not timestamp_str:
        raise ValueError(
            "bulk_sync_start_time is required and must be an ISO-8601 timestamp "
            "(e.g. 2026-07-01T00:00:00Z)"
        )
    timestamp = datetime.fromisoformat(
        timestamp_str.replace('Z', '+00:00')
    )

    start_time = timestamp - timedelta(minutes=1)
    end_time = datetime.now(timezone.utc)

    return (
        f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        f"..."
        f"{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )

def format_date_for_ce(date_value) -> Optional[str]:
    """Format date for CE import."""
    if not date_value:
        return None

    if isinstance(date_value, datetime):
        return date_value.strftime('%Y-%m-%d')

    date_str = str(date_value).strip()
    if not date_str:
        return None

    # If already in YYYY-MM-DD format, return as-is
    if len(date_str) >= 10 and date_str[4] == '-' and date_str[7] == '-':
        return date_str[:10]

    # Try parsing various formats
    for fmt in ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
        try:
            parsed = datetime.strptime(
                date_str[:19] if 'T' in date_str else date_str, fmt)
            return parsed.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def generate_budget_revision_xml(
    phase_groups: Dict,
    job_code: str,
    config,
    cost_type_map: Dict[str, int] = None,
    event_metadata: Dict = None
) -> str:
    """
    Generate RFC XML for change order sync from phase-grouped data.

    Args:
        phase_groups: Dictionary of phase-grouped data with revenue and cost_types
        job_code: CE job code
        config: Configuration object
        cost_type_map: Mapping of cost type references to CE cost type codes
        event_metadata: Change event metadata (number, title, description, created_at)

    Returns:
        Pretty-printed RFC XML string
    """
    event_metadata = event_metadata or {}
    root = ET.Element('import', attrib={'type': 'rfc'})
    rfc = ET.SubElement(root, 'rfc')

    # Job number
    jobnum_elem = ET.SubElement(rfc, 'jobnum')
    jobnum_elem.text = validate_field_length(job_code, 'jobnum')

    # RFC number (use event number)
    event_number = event_metadata.get('number', '')
    if event_number:
        rfcnum_elem = ET.SubElement(rfc, 'rfcnum')
        rfcnum_elem.text = validate_field_length(event_number, 'rfcnum')

    # RFC date (use event created date)
    created_at = event_metadata.get('created_at', '')
    if created_at:
        formatted_date = format_date_for_ce(created_at)
        if formatted_date:
            rfcdate_elem = ET.SubElement(rfc, 'rfcdate')
            rfcdate_elem.text = formatted_date

    # Description (use event title)
    title = event_metadata.get('title', '')
    if title:
        desc_elem = ET.SubElement(rfc, 'description')
        desc_elem.text = validate_field_length(title, 'description')

    # Type
    type_elem = ET.SubElement(rfc, 'type')
    type_elem.text = config.rfc_type

    # Notes (event description)
    description = event_metadata.get('description', '')
    if description:
        notes_elem = ET.SubElement(rfc, 'notes')
        notes_elem.text = str(description).strip()

    updated_at = event_metadata.get('updated_at', '')
    if updated_at:
        formatted_approval_date = format_date_for_ce(updated_at)
        if formatted_approval_date:
            approvaldate_elem = ET.SubElement(rfc, 'approvaldate')
            approvaldate_elem.text = formatted_approval_date

    approved = event_metadata.get('approved', False)
    if approved:
        approved_elem = ET.SubElement(rfc, 'approved')
        approved_elem.text = 'true'

    approved_by = event_metadata.get('approved_by', '')
    if approved_by:
        approvedby_elem = ET.SubElement(rfc, 'approvedby')
        approvedby_elem.text = validate_field_length(approved_by, 'approvedby')

    change_order_num = event_metadata.get('change_order_num', '')
    if change_order_num:
        changeordernum_elem = ET.SubElement(rfc, 'changeordernum')
        changeordernum_elem.text = validate_field_length(change_order_num, 'changeordernum')

    invalid_cost_types = []
    cost_type_map = cost_type_map or {}

    if phase_groups:
        costcodes_elem = ET.SubElement(rfc, 'costcodes')

        for phase_key, phase_data in phase_groups.items():
            total_budget = sum(ct['amount'] for ct in phase_data['cost_types'].values())
            if not config.allow_zero_amounts and phase_data['revenue'] == 0 and total_budget == 0:
                continue

            costcode_elem = ET.SubElement(costcodes_elem, 'costcode')

            cc_jobnum_elem = ET.SubElement(costcode_elem, 'jobnum')
            cc_jobnum_elem.text = validate_field_length(job_code, 'jobnum')

            if phase_data['phase_num']:
                phasenum_elem = ET.SubElement(costcode_elem, 'phasenum')
                phasenum_elem.text = validate_field_length(phase_data['phase_num'], 'phasenum')

            if phase_data['cat_num']:
                catnum_elem = ET.SubElement(costcode_elem, 'catnum')
                catnum_elem.text = validate_field_length(phase_data['cat_num'], 'catnum')

            # Add contract amount (revenue) - SUMMED at phase level
            contractamt_elem = ET.SubElement(costcode_elem, 'contractamt')
            contractamt_elem.text = str(phase_data['revenue'])

            # Add budgets element with multiple budget children (one per cost type)
            if phase_data['cost_types']:
                budgets_elem = ET.SubElement(costcode_elem, 'budgets')

                for cost_type_ref, budget_data in phase_data['cost_types'].items():
                    if cost_type_ref in cost_type_map:
                        cost_type_code = cost_type_map[cost_type_ref]
                        budget_elem = ET.SubElement(budgets_elem, 'budget')

                        number_elem = ET.SubElement(budget_elem, 'number')
                        number_elem.text = str(cost_type_code)

                        budget_quantity = budget_data['quantity'] if budget_data['quantity'] > 0 else 1
                        hours_elem = ET.SubElement(budget_elem, 'hours')
                        hours_elem.text = str(budget_quantity)

                        cost_elem = ET.SubElement(budget_elem, 'cost')
                        cost_elem.text = str(budget_data['amount'])
                    else:
                        invalid_cost_types.append(cost_type_ref)

    rail.set_result(invalid_cost_types, key='invalid_cost_types')

    # Convert to pretty-printed XML string
    xml_str = ET.tostring(root, encoding='unicode')

    # Pretty print
    dom = minidom.parseString(xml_str)
    pretty_xml = dom.toprettyxml(indent='  ')

    # Remove extra blank lines
    lines = [line for line in pretty_xml.split('\n') if line.strip()]
    return '\n'.join(lines)
