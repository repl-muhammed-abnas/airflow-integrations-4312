import rail
from dxctechnology.c1_wbs_import_v1 import request_payload
null = None


def map_list_data_to_companycode_list(response):
    company_code_list = []
    data = response.json()['d']
    if data['rows']:
        company_code_list = list(map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "fullpath": " | ".join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))),
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
            "parenturi": row['cells'][1]['cellCollection'][0]['uri']
        }, data['rows']))
    return company_code_list


def map_non_contractor_employeetype_groups(response):
    data = response.json()['d']
    return list(map(lambda item: {
        "name": item['displayText'],
        "uri": item['uri'],
        "status": "Yes"
    }, filter(lambda item: item['displayText'].lower() != "contractor", data)))


def map_user_based_on_empid(response):
    data = response.json()['d']
    conf = request_payload.get_dag_run_conf()
    user_cell_index = rail.find_index_by_attr(
        data['header'], 'uri', "urn:replicon:user-list-column:user")
    emp_type_group_cell_index = rail.find_index_by_attr(
        data['header'], 'uri', "urn:replicon:user-list-column:employee-type-group")
    employee_id_cell_index = rail.find_index_by_attr(
        data['header'], 'uri', "urn:replicon:user-list-column:employee-id")
    enabled_cell_index = rail.find_index_by_attr(
        data['header'], 'uri', "urn:replicon:user-list-column:enabled")
    end_date_cell_index = rail.find_index_by_attr(
        data['header'], 'uri', "urn:replicon:user-list-column:end-date")

    # pylint: disable=too-many-arguments
    def get_cell_value(
            collection,
            search_cell_index,
            search_cell_key,
            conf_search_key,
            attr_cell_index,
            attr_key):
        if conf[conf_search_key]:
            filter_data = list(
                filter(
                    lambda x: x['cells'][search_cell_index].get(
                        search_cell_key, None) == conf[conf_search_key],
                    collection))
            if len(filter_data) > 1:
                return 'multiple-entry'
            if len(filter_data) == 1:
                return filter_data[0]['cells'][attr_cell_index].get(attr_key, None)

        return None

    rows = data['rows']
    responsible_person_field = 'PersonResponsibleNumber' if request_payload.is_wbs_project(
    ) else 'SOPersonResponsible'
    applicant_field = 'WBSOwner2Number' if request_payload.is_wbs_project(
    ) else 'SOPartnerWBSOwner2'

    return {
        'useruri': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            responsible_person_field,
            user_cell_index,
            'uri'),
        'username': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            responsible_person_field,
            user_cell_index,
            'textValue'),
        'userstatus': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            responsible_person_field,
            enabled_cell_index,
            'boolValue'),
        'useremployeegroup': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            responsible_person_field,
            emp_type_group_cell_index,
            'textValue'),
        'userenddate': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            responsible_person_field,
            end_date_cell_index,
            'dateValue') if get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            responsible_person_field,
            end_date_cell_index,
            'textValue') else None,
        'comanageruri': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            applicant_field,
            user_cell_index,
            'uri'),
        'comanagername': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            applicant_field,
            user_cell_index,
            'textValue'),
        'comanagerstatus': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            applicant_field,
            enabled_cell_index,
            'boolValue'),
        'comanageremployeegroup': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            applicant_field,
            emp_type_group_cell_index,
            'textValue'),
        'comanagerenddate': get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            applicant_field,
            end_date_cell_index,
            'dateValue') if get_cell_value(
            rows,
            employee_id_cell_index,
            'textValue',
            applicant_field,
            end_date_cell_index,
            'textValue') else None
    }
