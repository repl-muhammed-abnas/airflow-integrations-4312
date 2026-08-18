from datetime import datetime

null=None

def get_replicon_date(date_str, date_format='%d/%m/%Y'):
    if not date_str:
        return None

    try:
        _date = datetime.strptime(date_str, date_format)
        return {
            'year': _date.year,
            'month': _date.month,
            'day': _date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_put_task_payload(dag_run):
    return{
        "project": {
            "name": dag_run.conf['wbs']
        },
        "task": {
            "target": {
            "name": dag_run.conf['task2']  ,
            "parent": {
                    "name": dag_run.conf['task1']

            }
            },
            "name": dag_run.conf['task2']  ,
            "code": dag_run.conf['code']  ,
            "timeEntryDateRange": {
            "startDate": get_replicon_date(dag_run.conf['startdate']),
            "endDate": get_replicon_date(dag_run.conf['enddate']),
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "isClosed": "false",
            "customFieldValues": [
            {
                "customField": {
                "uri": "urn:replicon-tenant:dc2477cce42c427d8f3d41f43c3f1288:user-defined-field:ae2ff810-2cbb-401d-a76b-962c1bd4a7db",
                "name": null,
                "groupUri": "urn:replicon:object-type:task"
                },
                "dropDownOption": {
                "uri": "urn:replicon-tenant:dc2477cce42c427d8f3d41f43c3f1288:custom-field-option:34294e6d-6eba-442f-9b51-15711debd926"
                }
            }
            ]
        }
        }
