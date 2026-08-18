import rail


def is_required_custom_fields_present():
    if rail.result('get_client_data')['isActive']:
        custom_field_data = get_required_client_custom_fields()
        time_and_material = custom_field_data['time_and_material'][0]['text']
        fixed_bid = custom_field_data['fixed_bid'][0]['text']
        return bool(time_and_material or fixed_bid)
    return False


def get_required_client_custom_fields():
    custom_field = rail.result("get_client_data")

    time_and_material_custom_field = list(filter(lambda x: x['displayText'] == "T&M Phases", map(lambda row: {
        'uri': row['customField']['uri'],
        'displayText': row['customField']['displayText'],
        'text': row['text']
    }, custom_field['customFields'])))

    fixed_bid_custom_field = list(filter(lambda x: x['displayText'] == "Fixed Bid Phases", map(lambda row: {
        'uri': row['customField']['uri'],
        'displayText': row['customField']['displayText'],
        'text': row['text']
    }, custom_field['customFields'])))

    return {
        "time_and_material": time_and_material_custom_field,
        "fixed_bid": fixed_bid_custom_field
    }


def get_unique_project_list(project_data):
    return list(x['Projectname'] for x in project_data)


def final_data_list(row):
    if not row:
        return []

    replicon_projects = rail.result("get_all_project_data")
    return {
        "Client": row['Client'] if row['Client'] else None,
        "Clienturi": row['Clienturi'] if row['Clienturi'] else None,
        "Projectname": row['Projectname'] if row['Projectname'] else None,
        "Key": row['Key'] if row['Key'] else None,
        "Summary": row['Summary'] if row['Summary'] else None,
        "Customer": row['Customer'] if row['Customer'] else None,
        "Wing": row['Wing'] if row['Wing'] else None,
        "Billingtype": row['Billingtype'] if row['Billingtype'] else None,
        'Repliconprojectname': rail.find_first_by_attr_and_get_attr(
            replicon_projects, 'Projectname', row['Projectname'], 'Projectname', default=""),
        'Repliconprojecturi': rail.find_first_by_attr_and_get_attr(
            replicon_projects, 'Projectname', row['Projectname'], 'Projecturi', default=""),
        'Repliconprojectstatus': rail.find_first_by_attr_and_get_attr(
            replicon_projects, 'Projectname', row['Projectname'], 'Projectstatus', default=""),
        'Repliconprojectstartdate': rail.find_first_by_attr_and_get_attr(
            replicon_projects, 'Projectname', row['Projectname'], 'Projectstartdate', default=""),
        'Repliconprojectenddate': rail.find_first_by_attr_and_get_attr(
            replicon_projects, 'Projectname', row['Projectname'], 'Projectenddate', default=""),
        'Issuetype': row['Issuetype'] if row['Issuetype'] else None,
        'Parentjira': row['Parentjira'] if row['Parentjira'] else None,
        'Epicid': row['Epicid'] if row['Epicid'] else None
    }


def valid_task_data_to_process(dag_run, data):
    if data[0]['Taskname'] == dag_run.conf['Key']:
        if data[0]['Taskstatus'].lower() != "true":
            return True
    return False
