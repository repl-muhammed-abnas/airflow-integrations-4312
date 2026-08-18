
def get_valid_wbs(response, dag_run):
    if not response.json()['d']:
        return []
    response = response.json()['d']['rows']
    return list(
        filter(lambda x: x['parent_wbs_name'] == dag_run.conf['wbs'],
               map(lambda item:
                   {
                       'child_wbs_uri': item['cells'][0]['uri'],
                       'child_wbs_name': item['cells'][0].get('textValue'),
                       'parent_wbs_name': item['cells'][1].get('textValue'),
                   }, response))
    )


def get_descendant_task_details_filter(response):
    response = response.json()['d']
    if not response:
        return []

    return list(
        map(lambda x: {
            "task_name": x['task']['name'],
            "task_uri": x['task']['uri'],
            "task_code": x['task']['code'],
            "task_start_date": x['task'].get('timeEntryDateRange', {}).get('startDate'),
            "task_end_date": x['task'].get('timeEntryDateRange', {}).get('endDate')
        }, response)
    )
