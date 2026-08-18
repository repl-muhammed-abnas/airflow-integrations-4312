# UK&I CSC Mapper Module
# Main mapper functions for user assignment matching using restrictions pattern

from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.mapper.company_code_mapper import DXC_COMPANY_CODES_ES

def evaluate_restriction(user_data, restriction):
    field = restriction.get("field")
    operator = restriction.get("operator")
    values = restriction.get("values")
    # Get the actual value from user data
    actual_value = user_data.get(field)
    if actual_value is None:
        return False
    
    # Convert to string for comparison
    actual_value = str(actual_value)
    
    # Handle different operators
    if operator == "equal":
        return actual_value == values
    elif operator == "in":
        if isinstance(values, list):
            return actual_value in values
        return actual_value == values
    elif operator == "not_in":
        if isinstance(values, list):
            return actual_value not in values
        return actual_value != values
    elif operator == "less_than":
        try:
            return float(actual_value) < float(values)
        except:
            return False
    elif operator == "greater_equal":
        try:
            return float(actual_value) >= float(values)
        except:
            return False
    
    return False


def get_user_assignments(user_data, DXC_ASSIGNMENT_MAPPER):
    for assignment in DXC_ASSIGNMENT_MAPPER:
        # Check if all restrictions are satisfied
        restrictions = assignment.get("restrictions", [])
        all_satisfied = True
        
        for restriction in restrictions:
            if not evaluate_restriction(user_data, restriction):
                all_satisfied = False
                break
        
        if all_satisfied:
            # Return copy without restrictions
            result = {k: v for k, v in assignment.items() if k != "restrictions"}
            return result
    
    return None


def validate_company_code(company_code):
    return str(company_code) in DXC_COMPANY_CODES_ES


def get_timeoffs_for_user_data(user_data):
    assignment = get_user_assignments(user_data)
    if assignment:
        return assignment.get("timeoffs", [])
    
    return []
