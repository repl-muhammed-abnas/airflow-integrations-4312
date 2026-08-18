

# mapping_criteria field -> key on the user object built by get_user_obj_from_cospoint.
CRITERIA_FIELD_TO_USER_KEY = {
    'employee_status': 'employeestatus',        # S_EMPL_STATUS_CD (API)
    'country': 'servicecenter',                 # COUNTRY_CD (API)
    'account': 'costcenter',                    # ACCT_ID (cost center)
    'employee_type': 'employeetype',            # S_EMPL_TYPE_CD
    'general_labor_category': 'generalLabercategory',  # GENL_LAB_CAT_CD (department)
    'labor_location': 'location',               # LAB_LOC_CD (location)
    'organizations': 'division',                # ORG_ID (division)
}


def _norm(value):
    return value.strip() if isinstance(value, str) else value


def _row_matches(mapping_criteria, user):
    for field, allowed_values in mapping_criteria.items():
        if not allowed_values:
            continue  # empty list = wildcard
        user_key = CRITERIA_FIELD_TO_USER_KEY.get(field)
        if user_key is None:
            raise ValueError(
                f"Unknown mapping_criteria field '{field}'. "
                f"Valid fields: {sorted(CRITERIA_FIELD_TO_USER_KEY)}")
        user_value = _norm(user.get(user_key))
        if user_value not in [str(v).strip() for v in allowed_values]:
            return False
    return True


def matches_mapper(user, mapper):
    """Flow-level filter: True if the user matches any row's mapping_criteria."""
    if not mapper:
        return True
    return any(_row_matches(row.get('mapping_criteria', {}), user) for row in mapper)


def find_mapper_row(user, mapper):
    """Return the first row whose mapping_criteria the user matches (assignment),
    falling back to the first row."""
    for row in mapper or []:
        if _row_matches(row.get('mapping_criteria', {}), user):
            return row
    return mapper[0] if mapper else None
