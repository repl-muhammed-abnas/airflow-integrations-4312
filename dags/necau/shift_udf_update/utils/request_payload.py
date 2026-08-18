import rail

from necau.shift_udf_update.utils.python_callable_method import get_default_schedule_name
null = None

def get_all_shift_data():
    return {
            "page": 1,
            "pagesize": 10000,
            "columnUris": [
                "urn:replicon:shift-list-column:name",
                "urn:replicon:shift-list-column:is-enabled",
                "urn:replicon:shift-list-column:shift"
            ],
            "sort": [],
            "filterExpression": null,
            }

def get_custom_field_data():
    return {
            "customField": {
                "uri": rail.result('get_autoschedule_assignment_uri'),
                "name": null,
                "groupUri": null
            }
        }

def get_request_payload():
    data = {
            "customFieldUri": rail.result('get_autoschedule_assignment_uri'),
            "customFieldDropDownOptionUris": rail.result('get_final_dropdown_list')
            }
    return data

def get_default_office_schedule_uri(default_office_schedule_name):
    # pylint: disable=line-too-long
    default_drop_down_option = rail.result("get_custom_field_details")
    default_drop_down_option_uri = default_drop_down_option['defaultDropDownValue']['uri'] if 'defaultDropDownValue' in default_drop_down_option else None
    return default_drop_down_option_uri if default_drop_down_option_uri else get_default_schedule_name(default_office_schedule_name)

def get_request_payload_to_update_dropdown_default_value():
    return {
        "customFieldUri": rail.result('get_autoschedule_assignment_uri'),
        "customFieldDropDownOptionUri": rail.result('get_default_office_schedule_uri')
    }
