import rail


def parse_group_option(option):
    return {
        'name': option['cells'][0].get('textValue', ''),
        'code': option['cells'][1].get('textValue', ''),
        'uri': option['cells'][2].get('uri', '')
    }

def build_hierarchical_options(ce_unions, existing_options, root_dept_uri, root_department):
    parent_options = []
    for union in ce_unions:
        union_name = union.get('name', '')
        union_code = union.get('code', '')

        existing_union_uri = rail.find_first_by_attr_and_get_attr(
            existing_options, 'code', union_code, 'uri'
        )

        parent_options.append({
            'name': union_name,
            'code': union_code,
            'parent': root_department,
            'parent_uri': root_dept_uri,
            'parent_code': None,
            'is_parent': True,
            'uri': existing_union_uri
        })

    union_name_mapping = {
        opt['code']: opt['name']
        for opt in parent_options
    }

    child_options = []
    for union in ce_unions:
        union_code = union.get('code', '')
        parent_union_name = union_name_mapping.get(union_code, union.get('name', ''))
        parent_union_uri = rail.find_first_by_attr_and_get_attr(
            existing_options, 'code', union_code, 'uri'
        )

        for local in union.get('union_locals', []):
            child_options.append({
                'name': local.get('name', ''),
                'code': local.get('code', ''),
                'parent': parent_union_name,
                'parent_uri': parent_union_uri,
                'parent_code': union_code,
                'is_parent': False
            })

    return parent_options, child_options, union_name_mapping


def calculate_sync_operations(ce_all_options, existing_options, root_department):
    root_dept = rail.find_first_by_attr_and_get_attr(existing_options, 'name', root_department)
    root_dept_code = root_dept.get('code') if root_dept else root_department
    ce_codes = {opt['code'] for opt in ce_all_options}

    options_to_disable = [
        option for option in existing_options
        if (option['name'] != root_department and
            option['code'] != root_dept_code and
            option['code'] not in ce_codes)
    ]

    enriched_options = []
    for option in ce_all_options:
        existing_by_code = rail.find_first_by_attr_and_get_attr(
            existing_options, 'code', option['code']
        )
        existing_by_name = None
        if not existing_by_code:
            existing_by_name = rail.find_first_by_attr_and_get_attr(
                existing_options, 'name', option['name']
            )

        existing_option = existing_by_code or existing_by_name
        existing_uri = existing_option.get('uri') if existing_option else None

        enriched_options.append({
            'name': option['name'],
            'code': option['code'],
            'parent': option.get('parent'),
            'parent_uri': option.get('parent_uri'),
            'parent_code': option.get('parent_code'),
            'is_parent': option.get('is_parent', False),
            'uri': existing_uri
        })

    return options_to_disable, enriched_options
