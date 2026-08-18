import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from rail import set_result
from procore_ce_integration.subcontract_change_order_sync.utils.constants import RFC_FIELD_LIMITS
from procore_ce_integration.job_structure_sync.utils.constants import WBSType
from procore_ce_integration.initial_setup_sync.shared_utils import parse_wbs_flat_code


def truncate_field(value: str, field_name: str) -> str:
    if not value:
        return ''

    max_length = RFC_FIELD_LIMITS.get(field_name)

    if not max_length:
        return str(value)

    value_str = str(value).strip()

    if len(value_str) <= max_length:
        return value_str

    if field_name == 'description' and max_length > 3:
        return value_str[:max_length - 3] + '...'

    return value_str[:max_length]


def format_date_for_ce(date_value) -> Optional[str]:
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


def generate_rfc_xml(cop_data: Dict, line_items: List[Dict], job_code: str,
                     wbs_type: str, config, cost_type_map: Dict[str, int] = None) -> str:
    root = ET.Element('import', attrib={'type': 'rfc'})
    rfc = ET.SubElement(root, 'rfc')

    jobnum_elem = ET.SubElement(rfc, 'jobnum')
    jobnum_elem.text = truncate_field(job_code, 'jobnum')

    cop_id = cop_data['id']
    if cop_id:
        rfcnum_elem = ET.SubElement(rfc, 'rfcnum')
        rfcnum_elem.text = truncate_field(str(cop_id), 'rfcnum')

    created_at = cop_data['created_at']
    if created_at:
        formatted_date = format_date_for_ce(created_at)
        if formatted_date:
            rfcdate_elem = ET.SubElement(rfc, 'rfcdate')
            rfcdate_elem.text = formatted_date

    due_date = cop_data.get('due_date')
    if due_date:
        formatted_due_date = format_date_for_ce(due_date)
        if formatted_due_date:
            respondby_elem = ET.SubElement(rfc, 'respondbydate')
            respondby_elem.text = formatted_due_date

    title = cop_data.get('title', '')
    if title:
        desc_elem = ET.SubElement(rfc, 'description')
        desc_elem.text = truncate_field(title, 'description')

    type_elem = ET.SubElement(rfc, 'type')
    type_elem.text = config.rfc_type

    description = cop_data.get('description', '')
    invalid_cost_types = []
    if description:
        notes_elem = ET.SubElement(rfc, 'notes')
        notes_elem.text = str(description).strip()

    if line_items:
        costcodes_elem = ET.SubElement(rfc, 'costcodes')
        cost_type_map = cost_type_map or {}

        for line_item in line_items:
            amount = float(line_item.get('amount', 0) or 0)
            if not config.allow_zero_amounts and amount == 0:
                continue
            quantity = float(line_item.get('quantity', 1) or 1)
            if quantity == 0:
                quantity = 1

            costcode_elem = ET.SubElement(costcodes_elem, 'costcode')

            cc_jobnum_elem = ET.SubElement(costcode_elem, 'jobnum')
            cc_jobnum_elem.text = truncate_field(job_code, 'jobnum')

            wbs_code_obj = line_item['wbs_code']
            cost_code_str = wbs_code_obj.get(
                'flat_code', '') if isinstance(wbs_code_obj, dict) else ''

            cost_type_ref = ''
            if cost_code_str:
                phase_num, cat_num, cost_type_ref = parse_wbs_flat_code(
                    cost_code_str, line_item.get('cost_code'), wbs_type)

                if phase_num:
                    phasenum_elem = ET.SubElement(costcode_elem, 'phasenum')
                    phasenum_elem.text = truncate_field(phase_num, 'phasenum')

                if cat_num:
                    catnum_elem = ET.SubElement(costcode_elem, 'catnum')
                    catnum_elem.text = truncate_field(cat_num, 'catnum')

            # Add budget if cost type exists and maps to valid code
            if cost_type_ref:
                if cost_type_ref in cost_type_map:
                    cost_type_code = cost_type_map[cost_type_ref]
                    budgets_elem = ET.SubElement(costcode_elem, 'budgets')
                    budget_elem = ET.SubElement(budgets_elem, 'budget')

                    number_elem = ET.SubElement(budget_elem, 'number')
                    number_elem.text = str(cost_type_code)

                    hours_elem = ET.SubElement(budget_elem, 'hours')
                    hours_elem.text = str(quantity)

                    cost_elem = ET.SubElement(budget_elem, 'cost')
                    cost_elem.text = str(amount)
                else:
                    invalid_cost_types.append(cost_type_ref)

    set_result(invalid_cost_types, key='invalid_cost_types')
    # Convert to pretty-printed XML string
    xml_str = ET.tostring(root, encoding='unicode')

    # Pretty print
    dom = minidom.parseString(xml_str)
    pretty_xml = dom.toprettyxml(indent='  ')

    # Remove extra blank lines
    lines = [line for line in pretty_xml.split('\n') if line.strip()]
    return '\n'.join(lines)
