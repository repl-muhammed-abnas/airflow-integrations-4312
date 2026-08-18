import rail
from macquariegroup.recovery_reconciliation.utils.custom_methods import get_str_date
from macquariegroup.recovery_reconciliation.mapper.recovery_field_mapper import recovery_field_mapper


def get_required_divisions(response):
    unique_groups_from_feed = list(filter(bool, map(lambda x: x['unique_groups'], rail.load_all_records(
        rail.result('get_unique_groups_from_feed')))))

    result = list(filter(lambda x: x['name'].lower() in unique_groups_from_feed, map(
        lambda group: {
            "name": group['displayText'],
            'uri': group['uri']
        }, response
    )))

    if len(result) == len(unique_groups_from_feed):
        return result

    # pylint: disable=line-too-long
    raise Exception(
        f"Feed file groups not found in Replicon Instance. unique feed file groups: {unique_groups_from_feed}, groups found in replicon: {[item['name'] for item in result]}")


def get_holiday_date_list(response):

    if not response:
        return []

    res = []
    for date in response:
        res.append(get_str_date(date['date'], is_dict=True))

    return res

def get_timesheet_uris(response):
    data= list(map(lambda item: {
    'uri': item['cells'][0]['uri']
    },response['rows']))
    return [x['uri'] for x in data if x['uri']]

def get_timesheet_details_payload(dag_run):
    effective_date = list(filter(lambda item: item['employee_type'] == dag_run.conf['employee_type'], recovery_field_mapper))
    return {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:timesheet-list-column:timesheet",
                    "urn:replicon:timesheet-list-column:due-date"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:timesheet-list-filter:due-date"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "dateRange": {
                                    "startDate": effective_date[0]['timesheet_period_assignment']
                                }
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uri": dag_run.conf['user_uri']
                            }
                        }
                    }
                }
            }
