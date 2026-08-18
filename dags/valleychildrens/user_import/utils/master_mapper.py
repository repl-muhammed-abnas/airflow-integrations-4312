

_IDENTIFIER_KEY = 'identifier(_department|company|employee_type|_c_me_entitlement)'

def _identifier_matches(row_identifier, department, company, employee_type, cme_entitlement):
    parts = (row_identifier or '').split('|')
    if len(parts) != 4:
        return False
    user_parts = [department, company, employee_type, cme_entitlement]
    for row_part, user_part in zip(parts, user_parts):
        if row_part == 'ALL':
            continue
        if (row_part or '').strip() != (user_part or '').strip():
            return False
    return True

def lookup_value(mapper, field, department, company, employee_type, cme_entitlement):
    matches = []
    for row in (mapper or []):
        if (row.get('field') or '').strip() != field:
            continue
        ident = row.get(_IDENTIFIER_KEY) or ''
        if _identifier_matches(ident, department, company, employee_type, cme_entitlement):
            specificity = sum(1 for p in ident.split('|') if p != 'ALL')
            matches.append((specificity, row.get('value')))
    if not matches:
        return None
    matches.sort(key=lambda t: -t[0])
    return matches[0][1]

def lookup_all_fields(mapper, department, company, employee_type, cme_entitlement):
    out = {}
    for row in (mapper or []):
        field = (row.get('field') or '').strip()
        if not field or field.startswith('﻿'):
            continue
        ident = row.get(_IDENTIFIER_KEY) or ''
        if _identifier_matches(ident, department, company, employee_type, cme_entitlement):
            specificity = sum(1 for p in ident.split('|') if p != 'ALL')
            existing = out.get(field)
            if existing is None or specificity > existing[0]:
                out[field] = (specificity, row.get('value'))
    return {k: v[1] for k, v in out.items()}

