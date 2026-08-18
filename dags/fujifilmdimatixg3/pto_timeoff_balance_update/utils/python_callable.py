import rail

def get_paid_timeoff_uri():
    return {
                "page": "1",
                "pageSize": "10000",
                "timeOffTypeSearch": {
                    "textSearch": {
                    "queryText": "Paid Time Off",
                    "searchInDisplayText": "1",
                    "searchInName": "0",
                    "searchInDescription": "0"
                    }
                }
            }


def check_user_for_emp_id(dag_run):
    return  {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:login-name"
                ],
                "filterExpression": {
                    "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                    "value": {
                        "text": dag_run.conf['employeeid']
                    }
                    }
                }
            }

null_urn = "urn:replicon:list-type:null"

def get_value(data, index, pluck_key):
    if data[index]['dataType'] == null_urn:
        return None
    return data[index].get(pluck_key)


def get_user_data_from_empid_data_handler(response, dag_run):
    response = response['rows']
    if not response:
        return []

    employeeid = dag_run.conf['employeeid']
    return list(filter(lambda item: item['employeeid'] == employeeid,
                       list(map(lambda record: {
                            "uri": get_value(record['cells'], 1, "uri"),
                            "employeeid": get_value(record['cells'], 0, 'textValue')
                            }, response))
                       )
                )


def check_paid_time_off_script_uri():
    data = rail.result('get_user_time_off_policy')
    if len(data) > 0:
        if data[0]['policySet']['timeOffBalanceEventScripts']:
            return data[0]['policySet']['timeOffBalanceEventScripts'][0]['script']['uri']
    return None


def pto_update(dag_run):
    data = rail.result('get_user_data_from_empid')[0]['uri']

    script_uri = check_paid_time_off_script_uri()
    if not script_uri:
        script_uri = dag_run.conf['starting_balance_set_to_script_uri']

    return {
        "timeOffAccount": {
            "userUri": data,
            "timeOffTypeUri": dag_run.conf['pto_uri']
        },
        "policySetScheduleEntries": [
            {
            "effectiveDate": {
                "year": int(dag_run.conf['effectivedate'].split('/')[2]),
                "month": int(dag_run.conf['effectivedate'].split('/')[0]),
                "day": int(dag_run.conf['effectivedate'].split('/')[1])
            },
            "description": f"Effective from { dag_run.conf['effectivedate'] }",
            "policySet": {
                "timeOffBalanceEventScripts": [
                {
                    "scriptTarget": {
                        "uri": script_uri
                    },
                    "additionalParameters": [
                    {
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                        "number": dag_run.conf['balance']
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                        "value": {
                        "number": "20"
                        }
                    }
                    ]
                }
                ]
            }
            }
        ]
    }
