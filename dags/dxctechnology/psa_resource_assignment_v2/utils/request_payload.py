import rail

null = None


def get_process_wbs_bulk(item):
    """
    Payload for processing a WBS with all its users in bulk.
    Each item contains a WBS and all associated users.
    """
    return {
        'wbs': item['wbs'],
        'users': item['users']  # List of all users to be assigned to this WBS
    }


def get_process_child_wbs_bulk(item, dag_run):
    """
    Payload for processing child WBS with users needing child assignments.
    """
    # Get users needing child WBS from the categorization result
    users_needing_child = rail.result('categorize_bulk_users')['users_needing_child_wbs']

    return {
        'wbs': item.split(" - ")[0].strip(),
        'users': users_needing_child,
        'parentWbs': dag_run.conf['wbs'],
        'wbs_log': rail.result('create_log')
    }


def get_name_uri_employeeid_status(item, selection):
    """
    Get specific attribute for an employee from active users list.
    """
    ia_perner_id = rail.find_first_by_attr_and_get_attr(
        rail.result("get_active_user"), "employeeid", item['PERN'], selection)
    if bool(ia_perner_id):
        return ia_perner_id
    return null


def get_child_wbs_payload(dag_run):
    """
    Create payload for querying child WBS projects.
    """
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            rail.result('get_all_columns')[0]['uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": rail.result('get_all_filter_defination')[0]['uri']
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run.conf['wbs'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }