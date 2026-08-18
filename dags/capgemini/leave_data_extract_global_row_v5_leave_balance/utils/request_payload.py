import pendulum
import rail


def page_handler(request, response):
    if len(response['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_all_location_hierarchy_data():
    """Get payload for LocationListService1.svc/GetHierarchyData without text filter."""
    return {
        "page": 1,
        "pagesize": 1000,
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
        ]
    }


def get_leave_bal_report_filters(time_zone):
    current_year = pendulum.now(time_zone).year
    enabled_filters = rail.result('get_report_details')['filterConfiguration']['enabledFilters']

    datefilter = rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', "AsOfDateFilter", 'uri')
    current_location_filter_uri = rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', "CurrentLocationFilter", 'uri')

    start_date = pendulum.datetime(current_year, 1, 1).strftime("%m/%d/%Y")
    end_date = pendulum.now(time_zone).strftime("%m/%d/%Y")

    all_location_uris = rail.result('get_location_uris_for_country')

    filter_values = [
        {"reportFilterUri": datefilter, "value": "DateRange"},
        {"reportFilterUri": datefilter, "value": end_date},
        {"reportFilterUri": datefilter, "value": start_date}
    ]

    for uri in all_location_uris:
        if uri:
            filter_values.append({
                "reportFilterUri": current_location_filter_uri,
                "value": uri.split(':')[-1] if ':' in uri else uri
            })

    return filter_values


def get_report_parameters(time_zone):
    return {
        "reportParameters": [{
            "reportUri": rail.result('get_report_details')["uri"],
            "filterValues": get_leave_bal_report_filters(time_zone),
            "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }]
    }
