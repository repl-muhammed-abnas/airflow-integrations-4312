from datetime import datetime
import json


def convert_ce_date_to_procore(date_str, ce_format='%m/%d/%y', procore_format='%Y-%m-%d'):
    if not date_str or str(date_str).strip() == '':
        return None
    try:
        parsed_date = datetime.strptime(str(date_str).strip(), ce_format)
        return parsed_date.strftime(procore_format)
    except ValueError:
        # Try alternative format with 4-digit year
        try:
            parsed_date = datetime.strptime(str(date_str).strip(), '%m/%d/%Y')
            return parsed_date.strftime(procore_format)
        except ValueError:
            return None


def clean_currency(value):
    try:
        return str(value or '0.00').replace('$', '').replace(',', '').strip()
    except (ValueError, TypeError):
        return '0.00'


def is_non_zero_value(value):
    if not value:
        return False
    try:
        return float(value) != 0.0
    except (ValueError, TypeError):
        return False


def build_flat_code(phase, category, cost_type):
    phase = (phase or '').strip()
    category = (category or '').strip()
    cost_type = (cost_type or '').strip()
    if not cost_type:
        return None

    if phase and category:
        return f"{phase}-{category}.{cost_type}"
    elif category:
        return f"{category}.{cost_type}"
    elif phase:
        return f"{phase}.{cost_type}"
    else:
        return cost_type


def parse_budget_line_items(budget_line_items_str):
    # Parse budget_line_items from JSON string or list
    if isinstance(budget_line_items_str, str):
        try:
            return json.loads(budget_line_items_str)
        except (json.JSONDecodeError, TypeError):
            return []
    return budget_line_items_str if isinstance(budget_line_items_str, list) else []


def build_phase_category_code(phase, category):
    # Build phase-category code without cost type (e.g., "A-1", "1", "A")
    phase = (phase or '').strip()
    category = (category or '').strip()

    if phase and category:
        return f"{phase}-{category}"
    elif category:
        return category
    elif phase:
        return phase
    else:
        return None


def aggregate_cost_budgets_by_flat_code(rfcs):
    # Aggregate cost_budget amounts by flat_code across all RFCs, returns {flat_code: total}
    cost_budget_amounts = {}

    for rfc in rfcs:
        budget_line_items = parse_budget_line_items(rfc.get('budget_line_items', '[]'))
        for budget_item in budget_line_items:
            flat_code = budget_item.get('flat_code', '')
            cost_budget = float(budget_item.get('cost_budget', 0) or 0)
            if flat_code:
                cost_budget_amounts[flat_code] = cost_budget_amounts.get(flat_code, 0) + cost_budget

    return cost_budget_amounts


def aggregate_contract_amounts_by_phase_category(rfcs):
    # Aggregate contract amounts by phase-category across all RFCs, returns {phase_category: {details}}
    revenue_aggregation = {}

    for rfc in rfcs:
        budget_line_items = parse_budget_line_items(rfc.get('budget_line_items', '[]'))

        for budget_item in budget_line_items:
            phase = budget_item.get('phase', '')
            category = budget_item.get('category', '')
            contract_amount = float(budget_item.get('contract_amount', 0) or 0)

            phase_category = build_phase_category_code(phase, category)
            if not phase_category:
                continue

            if phase_category not in revenue_aggregation:
                revenue_aggregation[phase_category] = {
                    'phase_code': phase,
                    'category_code': category,
                    'phase_name': budget_item.get('phase_name', ''),
                    'category_name': budget_item.get('category_name', ''),
                    'total_amount': 0.0
                }

            revenue_aggregation[phase_category]['total_amount'] += contract_amount

    return revenue_aggregation


def create_adjustment_line_item(ref_counter, amount, comment, description, wbs_code_id):
    # Create standardized budget change adjustment line item
    return {
        "adjustment_number": ref_counter,
        "amount": float(amount),
        "calculation_strategy": "manual",
        "comment": comment,
        "description": description,
        "uom": "",
        "quantity": "",
        "to_from": "To",
        "type": "change_event",
        "unit_cost": "",
        "wbs_code_id": wbs_code_id,
        "ref": ref_counter
    }


def create_revenue_wbs_code_definition(flat_code, phase_code, category_code, revenue_cost_type):
    # Create WBS code definition for revenue codes
    return {
        'flat_code': flat_code,
        'phase_code': phase_code,
        'category_code': category_code,
        'cost_type': revenue_cost_type
    }

def build_cop_origin_id(job, rfc):
    return f'CE_COP_{job}_{rfc}'

def build_pco_origin_id(job, rfc, flat_code=None):
    origin_id = f'CE_PCO_{job}_{rfc}'
    if flat_code:
        return origin_id + f'_contract_{flat_code}'
    return origin_id
