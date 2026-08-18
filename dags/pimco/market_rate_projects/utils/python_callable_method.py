import rail


def get_task_heirarchy():
    data = rail.load_all_records(rail.result('query_task_to_be_updated'))
    return list(map(lambda item: {
        'target': {
            'uri': item['taskuri']
        },
        'taskModificationToApply': {
            'isClosed': not item['taskstatus'] == 'Open',
            'isTimeEntryAllowed': True,
            'customFieldsToApply': [{
                'customField': {
                    'uri': rail.result('get_custom_field_list')[0]['orguri']
                },
                'dropDownOption':{
                    'name': 'No'
                }
            }]
        }
    }, data))


def get_task_heirarchy_details(dag_run):
    taskdetails=rail.result('get_task_details')
    def get_task_uri(item):
        result = []
        result = list(filter(lambda x: x['taskname']==item['taskname'] and x['taskcode']==item['taskcode'],map(lambda ele:{
            'taskname': ele['name'],
            'taskcode': ele['code'],
            'taskuri' : ele['uri']
        },taskdetails)))
        return result[0]['taskuri'] if result else result

    data = dag_run.conf['taskdata']
    return list(map(lambda item: {
        'target': {
            'uri': get_task_uri(item)
        },
        'taskModificationToApply': {
            'isClosed': not item['taskstatus'] == 'Open',
            'isTimeEntryAllowed': True,
            "estimatedCostToApply": {
                "value": {
                    "amount": item['marketrate'],
                    "currency": {
                        "uri": dag_run.conf['currencyuri']
                    }
                }
            }
        },
    }, data))
