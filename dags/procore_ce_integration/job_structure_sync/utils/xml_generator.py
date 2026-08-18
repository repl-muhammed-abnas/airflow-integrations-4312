import xml.etree.ElementTree as ET
from procore_ce_integration.job_structure_sync.config import FIELD_CHAR_LIMITS, PHASE_LIMITS, CATEGORY_LIMITS, address_max_lines
from procore_ce_integration.job_structure_sync.utils.constants import WBSType
# pylint: disable = too-many-branches, unused-variable


def generate_job_xml(project_data, cost_codes_data, wbs_type, contract_by_category=None):
    validation_errors = []
    project_validation = validate_field_limits(
        project_data, FIELD_CHAR_LIMITS, entity_type="Project", identifier_field="project_number")
    validation_errors.append(project_validation)
    if project_validation.get('errors'):
        return {
            'xml': None,
            'logs': [project_validation]
        }
    # Create root element
    import_element = ET.Element("import", type="job")
    job_element = ET.SubElement(import_element, "job")

    # Basic job information
    if project_data.get("project_number"):
        id_elem = ET.SubElement(job_element, "id")
        id_elem.text = str(project_data["project_number"])

    if project_data.get("name"):
        name_elem = ET.SubElement(job_element, "name")
        name_elem.text = str(project_data["name"])[:FIELD_CHAR_LIMITS['name'][0]]

    # Address structure
    if project_data.get('address1') or project_data.get('city') or project_data.get('state_code') or project_data.get('zip'):
        address_elem = ET.SubElement(job_element, "address")

        # Add all address lines present
        for i in range(1, address_max_lines + 1):
            address_key = f'address{i}'
            if project_data.get(address_key):
                line_elem = ET.SubElement(address_elem, "line")
                line_elem.text = str(project_data[address_key])
        line_elem = ET.SubElement(address_elem, "line")
        line_elem.text = str(project_data['zip']) #Adding it as extra line since CE ignores the last line

        if project_data.get('city'):
            city_elem = ET.SubElement(address_elem, "city")
            city_elem.text = str(project_data['city'])

        if project_data.get('state_code'):
            state_elem = ET.SubElement(address_elem, "state")
            state_elem.text = str(project_data['state_code'])

        if project_data.get('zip'):
            zip_elem = ET.SubElement(address_elem, "zip")
            zip_elem.text = str(project_data['zip'])

    status = "active" if project_data.get('active', True) else "inactive"
    status_elem = ET.SubElement(job_element, "status")
    status_elem.text = status

    # Dates
    if project_data.get('start_date'):
        dateopen_elem = ET.SubElement(job_element, "dateopen")
        dateopen_elem.text = str(project_data['start_date'])

    if project_data.get('completion_date'):
        datedue_elem = ET.SubElement(job_element, "datedue")
        datedue_elem.text = str(project_data['completion_date'])

    # Add contractbycategory flag only when the value is known; omit to let CE use its own default
    if contract_by_category is not None:
        contractbycategory_elem = ET.SubElement(job_element, "contractbycategory")
        contractbycategory_elem.text = 'true' if contract_by_category else 'false'

    # Items section - WBS structure based on wbs_type
    if cost_codes_data:
        items_elem = ET.SubElement(job_element, "items")

        if wbs_type == WBSType.JOB_CAT:
            # Job/Cat: Use top level as Category
            errors = _generate_categories_et(items_elem, cost_codes_data)
            if errors:
                validation_errors.extend(errors)
        else:
            # Other types: Use top level as Phase
            errors = _generate_phases_et(items_elem, cost_codes_data)
            if errors:
                validation_errors.extend(errors)

        # Remove items element if no children were added (all skipped due to validation)
        if len(items_elem) == 0:
            job_element.remove(items_elem)

    # Convert ElementTree to string with proper formatting
    ET.indent(import_element, space="    ")
    xml_str = ET.tostring(
        import_element, encoding='unicode', xml_declaration=True)

    return {
        'xml': xml_str,
        'logs': validation_errors
    }


def _generate_categories_et(items_elem, cost_codes_data):
    validation_errors = []
    for parent_code, cost_code_info in cost_codes_data.items():
        cc_validation = validate_field_limits(
            cost_code_info, CATEGORY_LIMITS, entity_type="Category", identifier_field="code")
        validation_errors.append(cc_validation)
        if cc_validation.get('errors'):
            continue
        # Parent as category
        category_elem = ET.SubElement(items_elem, "category")

        id_elem = ET.SubElement(category_elem, "id")
        id_elem.text = str(cost_code_info["code"])

        if cost_code_info.get("name"):
            name_elem = ET.SubElement(category_elem, "name")
            name_elem.text = str(cost_code_info["name"])[:CATEGORY_LIMITS['name'][0]]

        if "contractamount" in cost_code_info:
            contractamount_elem = ET.SubElement(
                category_elem, "contractamount")
            contractamount_elem.text = str(cost_code_info["contractamount"])

        if cost_code_info.get("budgets"):
            _generate_budgets_et(category_elem, cost_code_info['budgets'])

    return validation_errors


def _generate_phases_et(items_elem, cost_codes_data):
    validation_errors = []
    for parent_code, cost_code_info in cost_codes_data.items():
        cc_validation = validate_field_limits(
            cost_code_info, PHASE_LIMITS, entity_type="Phase", identifier_field="code")
        validation_errors.append(cc_validation)
        if cc_validation.get('errors'):
            continue
        # Parent as phase
        phase_elem = ET.SubElement(items_elem, "phase")

        id_elem = ET.SubElement(phase_elem, "id")
        id_elem.text = str(cost_code_info["code"])

        if cost_code_info.get("name"):
            name_elem = ET.SubElement(phase_elem, "name")
            name_elem.text = str(cost_code_info["name"])[:PHASE_LIMITS['name'][0]]

        if "contractamount" in cost_code_info:
            contractamount_elem = ET.SubElement(phase_elem, "contractamount")
            contractamount_elem.text = str(cost_code_info["contractamount"])

        # Children as categories within the phase
        if cost_code_info.get('children'):
            categories_elem = ET.SubElement(phase_elem, "categories")

            for child in cost_code_info['children']:
                child_cc_validation = validate_field_limits(
                    child, CATEGORY_LIMITS, entity_type="Category", identifier_field="code")
                validation_errors.append(child_cc_validation)
                if child_cc_validation.get('errors'):
                    continue
                category_elem = ET.SubElement(categories_elem, "category")

                child_id_elem = ET.SubElement(category_elem, "id")
                child_id_elem.text = str(child["code"])

                if child.get("name"):
                    child_name_elem = ET.SubElement(category_elem, "name")
                    child_name_elem.text = str(child["name"])[:CATEGORY_LIMITS['name'][0]]

                if "contractamount" in child:
                    contractamount_elem = ET.SubElement(
                        category_elem, "contractamount")
                    contractamount_elem.text = str(child["contractamount"])

                if child.get("budgets"):
                    _generate_budgets_et(category_elem, child['budgets'])

    return validation_errors


def _generate_budgets_et(category_elem, budgets):
    budgets_elem = ET.SubElement(category_elem, "budgets")

    for budget in budgets:
        budget_elem = ET.SubElement(budgets_elem, "budget")
        budget_number_elem = ET.SubElement(budget_elem, "number")
        budget_number_elem.text = str(budget["number"])

        budget_hours_elem = ET.SubElement(budget_elem, "hours")
        budget_hours_elem.text = str(budget["hours"])

        budget_cost_elem = ET.SubElement(budget_elem, "cost")
        budget_cost_elem.text = str(budget["cost"])


def validate_field_limits(data_dict, field_limits, entity_type="Unknown", identifier_field=None):
    errors = []
    warnings = []

    for field_name, value in data_dict.items():
        if value and field_name in field_limits:
            limit_info = field_limits[field_name]
            if isinstance(limit_info, tuple):
                limit, bypass_flag = limit_info
            else:
                # Backward compatibility for old format
                limit, bypass_flag = limit_info, False

            if limit and len(str(value)) > limit:
                message = f"Field '{field_name}' exceeds {limit} character limit (actual: {len(str(value))})"
                if bypass_flag:
                    data_dict[field_name] = str(value)[:limit]
                    warnings.append(message)
                else:
                    errors.append(message)

    return {
        "errors": errors,
        "warnings": warnings,
        "type": entity_type,
        "identifier": data_dict.get(identifier_field) if identifier_field else ''
    }
