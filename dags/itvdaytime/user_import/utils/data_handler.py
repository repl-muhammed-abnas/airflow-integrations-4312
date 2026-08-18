import rail

null_urn = "urn:replicon:list-type:null"


def get_value(data, index, pluck_key):
    if data[index]['dataType'] == null_urn:
        return None
    return data[index].get(pluck_key)


def get_search_user_data_handler(response, dag_run, caller=None):
    response = response['rows']
    if not response:
        return []

    employee_number = str(dag_run.conf['line_manager']) if caller else str(
        dag_run.conf['employee_number'])
    return list(filter(lambda item: item['employee_id'] == employee_number,
                       list(map(lambda record: {
                            "login_name": get_value(record['cells'], 0, 'textValue'),
                            "user_uri": get_value(record['cells'], 0, "uri"),
                            "employee_id": get_value(record['cells'], 1, 'textValue'),
                            # json format
                            "start_date": get_value(record['cells'], 2, 'dateValue'),
                            # json format
                            "end_date": get_value(record['cells'], 3, 'dateValue'),
                            "status": get_value(record['cells'], 4, 'textValue')
                            }, response))
                       )
                )


def get_required_permission_sets(response):
    required_permission_sets = [
        'Supervisor', 'Standard User - Staff', 'Standard User - Freelance']
    ret_data = []
    not_found_permission_sets = []
    for permission_set in required_permission_sets:
        value = rail.find_first_by_attr_and_get_attr(
            response, 'displayText', permission_set)
        if value:
            ret_data.append({
                "name": value['displayText'],
                "uri": value['uri']
            })
            continue

        if not value:
            not_found_permission_sets.append(permission_set)

    if not ret_data:
        raise Exception(
            f"Required permissions sets: {required_permission_sets} are not available")

    if not_found_permission_sets:
        raise Exception(
            f"Few permissions sets {not_found_permission_sets} not found")

    return ret_data


def get_timeoff_types(response):
    return list(map(
        lambda x: {
            "name": x["displayText"].replace("  ", " "),
            "uri": x["uri"]
        }, response))


def get_timeoff_types_details(response):
    measurement_unit_uri_hours = "urn:replicon:time-off-measurement-unit:hours"
    measurement_unit_uri_days = "urn:replicon:time-off-measurement-unit:work-days"

    def get_timoff_type_unit(item):
        if item['measurementUnitUri'] == measurement_unit_uri_hours:
            return "hours"
        if item['measurementUnitUri'] == measurement_unit_uri_days:
            return "days"

        return "invalid"

    return list(map(
        lambda item: {
            "name": item["displayText"].replace("  ", " "),
            "uri": item["uri"],
            "display_format_uri": item["timeOffDisplayFormatUri"],
            "measurement_unit_uri": item["measurementUnitUri"],
            "is_day_or_hour": get_timoff_type_unit(item)
        }, response))


def get_required_custom_fields(response):
    required_custom_fields = ['Assignment ID', 'Primary Contact Number',
                              'Fusion Department Name', 'Job Title', 'Assignment ID Enter Date',
                              'Fusion Location', 'Contract Type']
    ret_data = []
    not_found_custom_fields = []

    for custom_field in required_custom_fields:
        value = rail.find_first_by_attr_and_get_attr(
            response, 'displayText', custom_field)
        if value:
            ret_data.append({
                "name": value['displayText'],
                "uri": value['uri'],
                "enabled": value['isEnabled'],
                "type": value['type']['displayText']
            })
            continue

        if not value:
            not_found_custom_fields.append(custom_field)

    if not ret_data:
        raise Exception(
            f"Required Custom fields {required_custom_fields} are not found")

    if not_found_custom_fields:
        raise Exception(
            f"Few custom fields {not_found_custom_fields} not found")

    return ret_data


def get_all_user_custom_fields_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))


def get_all_drop_down_options_filter(response):
    if not response:
        return []
    return list(map(lambda data: {
        "name": data['displayText'],
        "uri": data['uri'],
        'enabled': data['isEnabled']
    }, response))
