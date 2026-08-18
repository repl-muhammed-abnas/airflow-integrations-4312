from datetime import datetime
import rail

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def do_final_list(hdr_labour_type_task_id, itm_labour_type_task_id):
    hdr_labour_types = get_data_from_document(
        rail.result(hdr_labour_type_task_id))
    itm_labour_types = get_data_from_document(
        rail.result(itm_labour_type_task_id))
    return list(
        map(lambda x: {
            'wbs': x['wbs'],
            'labourtypes': x['hdrlabortype'] if x.get('hdrlabortype') else x['itmlabourtype'],
            'startdate': x['hdrstartdate'] if x.get('hdrstartdate') else x['itmstartdate'],
            'enddate': x['hdrenddate'] if x.get('hdrenddate') else x['itmenddate'],
            'description': x['description']
        }, hdr_labour_types + itm_labour_types)
    )


def get_input_combined_list(billing_rates_wbs_task_id):
    all_billing_rates_wbs = get_data_from_document(
        rail.result(billing_rates_wbs_task_id))
    return list(
        map(lambda x: {
            'wbs': x['wbs'],
            'labourtypes': x['labourtypes'],
            'startdate': datetime.strptime(x['startdate'], '%Y%m%d').strftime("%m/%d/%Y"),
            'enddate': datetime.strptime(x['enddate'], '%Y%m%d').strftime("%m/%d/%Y") if x['enddate']
            else datetime(9999, 1, 31).strftime("%m/%d/%Y"),
            'description': x['description']
        }, all_billing_rates_wbs)
    )


def get_assignable_billing_rates(dag_run):
    billing_rates_queried = get_data_from_document(
        rail.result('query_billing_rates_for_wbs'))
    input_combined_list = get_data_from_document(
        dag_run.conf['input_combined_data'])
    return list(
        map(lambda x: {
            'name': x['labourtypes'],
            'startdate': min(list(map(lambda item: datetime.strptime(
                item['startdate'], '%m/%d/%Y'), filter(
                    lambda item: item['wbs'] == item['wbs'] and item['labourtypes'] == x['labourtypes'] and item['startdate'],
                    input_combined_list)))).strftime('%m/%d/%Y'),
            'enddate': max(list(map(lambda item: datetime.strptime(
                item['enddate'], '%m/%d/%Y'), filter(
                    lambda item: item['wbs'] == item['wbs'] and item['labourtypes'] == x['labourtypes'] and item['enddate'],
                    input_combined_list)))).strftime('%m/%d/%Y')
        }, billing_rates_queried)
    )


def get_labour_type_blobs(project_uri, join_collection, labour_types_blob_collection):
    final_labour_type_blobs = []
    if rail.result(labour_types_blob_collection):
        return get_data_from_document(rail.result(labour_types_blob_collection))
    if rail.result(join_collection):
        jsonValue = get_data_from_document(rail.result(join_collection))
        final_labour_type_blobs = list(
            map(lambda x: {
                'wbsUri': project_uri,
                'wbsName': x['wbs'],
                'labourType': x['labourtype'],
                'labourTypeUri': x['labourtypeuri'],
                'startDate': x['startdate'],
                'endDate': x['enddate']
            }, jsonValue))
    return final_labour_type_blobs


def get_billing_rates_to_assign_in_blob(
        assignable_billing_rates, project_response_task_id):

    billing_rates_for_wbs = get_billing_rates_to_assign_for_wbs(
        assignable_billing_rates)

    project_billing_rates_display_text = get_project_billing_rates_display_text(
        project_response_task_id)

    return list(map(
        lambda x: {
            'displayText': x['displayText'],
            'name': x['name'],
            'uri': x['uri'],
            'availableinproject': "Yes" if len(
                project_billing_rates_display_text) > 0 and x['displayText'] in project_billing_rates_display_text else "No",
            'requiredtoassign': "Yes"
        }, billing_rates_for_wbs))


def get_billing_rates_to_assign_for_wbs(assignable_billing_rates):
    dag_run_conf = get_dag_run_conf()
    billing_rates_assign_by_name = [
        item['name'].strip() for item in get_data_from_document(
            rail.result(assignable_billing_rates))]
    replicon_billing_rates = dag_run_conf['billing_rates_from_replicon']
    billing_rates_assign_for_project = [
        x for x in replicon_billing_rates if x['name'] in billing_rates_assign_by_name]

    return billing_rates_assign_for_project


def get_project_billing_rates_display_text(project_response_task_id):
    billing_rates_display_text = []
    if rail.result(project_response_task_id)['results'][0]:
        billing_rates = rail.result(project_response_task_id)['results'][0].get(
            'timeAndMaterials', []).get('projectBillingRates', [])
        billing_rates_display_text = [item['billingRate']['displayText']
                                      for item in billing_rates] if billing_rates else []
    return billing_rates_display_text


def get_project_billing_rates_to_assign(
        wbs, billing_rates_to_assign, project_uri, billing_rates_assign_for_project):

    billing_rates_to_assign = get_data_from_document(
        rail.result(billing_rates_to_assign))

    return list(
        map(lambda x: {
            'wbsUri': project_uri,
            'wbsName': wbs,
            'labourType': x['displayText'],
            'labourTypeUri': x['uri'],
            'startDate': rail.find_first_by_attr_and_get_attr(billing_rates_to_assign, 'name', x['name'], 'startdate'),
            'endDate': rail.find_first_by_attr_and_get_attr(billing_rates_to_assign, 'name', x['name'], 'enddate')
        }, rail.result(billing_rates_assign_for_project)))


def get_unique_billing_rate_names_by_attr(
        attribute, value):

    billing_rates_by_name = [x['name'] for x in rail.result(
        'billing_rates_to_assign_in_blob') if x['name'] and x[attribute] == value]

    return list(set(billing_rates_by_name))


def get_billing_rate_to_assign_compass(billingrates):
    billing_rates_queried = get_data_from_document(billingrates)
    return list(
        map(lambda x: {
            'name': x['name'],
            # convention for labour type startdate
            'taskassignmentstartdate': x['startdate'],
            # convention for labour type enddate
            'taskassignmentenddate': x['enddate'],
            'blanklabortype': 'No' if x['name'] else 'Yes'
        }, billing_rates_queried)
    )


def get_billing_rates_to_assign_in_compass_blob(project_data):
    dag_run_conf = get_dag_run_conf()
    project_billing_rates_display_text = get_project_billing_rates_display_text(
        project_data)
    result = list(map(
        lambda x: {
            'displayText': x['displayText'],
            'name': x['name'],
            'uri': x['uri'],
            'availableinproject': "Yes" if len(project_billing_rates_display_text) > 0 and x['displayText'] in project_billing_rates_display_text else "No",
            'requiredtoassign': "Yes" if rail.find_first_by_attr_and_get_attr(rail.result('get_billing_rates_to_asssign_compass'), 'name', x['name']) else "No"
        }, dag_run_conf['billingratesinreplicon']))
    return [i for i in result if i['requiredtoassign'] == 'Yes']


def get_project_billing_rates_to_assign_compass(billing_rates_to_assign, project_uri, wbs):
    data = get_data_from_document(rail.result(
        'query_billing_rates_to_assign_validated'))
    return list(
        map(lambda x: {
            'wbsUri': project_uri,
            'wbsName': wbs,
            'labourType': x['displayText'],
            'labourTypeUri': x['uri'],
            'startDate': datetime.strptime(rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentstartdate'),
                                           '%m/%d/%Y').strftime("%m/%d/%Y")
            if rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentstartdate') else null,
            'endDate': datetime.strptime(rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentenddate'),
                                         '%m/%d/%Y').strftime("%m/%d/%Y")
            if rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentenddate') else null
        }, rail.result(billing_rates_to_assign))
    )


def get_project_billing_rates_to_assign_compass_2(billing_rates_to_assign, project_uri, wbs):
    data = get_data_from_document(rail.result(
        'query_billing_rates_to_assign_validated'))
    return list(
        map(lambda x: {
            'wbsUri': project_uri,
            'wbsName': wbs,
            'labourType': x['displayText'],
            'labourTypeUri': x['uri'],
            'startDate': str(datetime.strptime(rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentstartdate'),
                                               '%m/%d/%Y').strftime("%m/%d/%Y"))
            if rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentstartdate') else null,
            'endDate': str(datetime.strptime(rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentenddate'),
                                             '%m/%d/%Y').strftime("%m/%d/%Y"))
            if rail.find_first_by_attr_and_get_attr(data, 'name', x['name'], 'taskassignmentenddate') else null
        }, rail.result(billing_rates_to_assign))
    )


def get_unique_billing_rate(attribute, value):
    billing_rates_by_name = [x['name'] for x in rail.result(
        'billing_rates_to_assign_in_compass_blob') if x['name'] and x[attribute] == value]
    return list(set(billing_rates_by_name))


def test(date_validation_update):
    jsonValue = get_data_from_document(rail.result(date_validation_update))
    return list(
        map(lambda x: {
            'name': x['name'],
            'taskassignmentstartdate': x['taskassignmentstartdate'],
            'taskassignmentenddate': x['taskassignmentenddate'],
            'blanklabortype': x['blanklabortype']
        }, jsonValue)
    )


def get_labour_type_blobs_compass(project_uri, doc_result, labour_types_blob):
    if rail.result(labour_types_blob):
        return get_data_from_document(rail.result(labour_types_blob))
    if rail.result(doc_result):
        jsonValue = get_data_from_document(rail.result(doc_result))
        return list(
            map(lambda x: {
                'wbsUri': project_uri,
                'wbsName': x['wbs'],
                'labourType': x['labourtype'],
                'labourTypeUri': x['labourtypeuri'],
                'startDate': x['startdate'],
                'endDate': x['enddate']
            }, jsonValue))
    return []
