from uuid import uuid4
import rail
null = None


def get_model_task():
    return {
        "projects": [
            {
                "uri": null,
                "name": "PIMCO Model Task",
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def process_market_rate_update_child_conf(item):
    taskdata = rail.load_all_records(rail.result('query_task_to_be_updated'))
    taskdatalist = list(map(lambda x: {
        'taskcode': x['taskcode'],
        'taskname': x['taskname'],
        'taskstatus': x['taskstatus'],
        'taskfullpath': x['taskfullpath'],
        'marketrate': float(x['marketrate'].replace('USD$', '').replace(',', ''))
    }, taskdata))
    return{
        'projectname': item['projectname'],
        'projecturi': item['projecturi'],
        'taskdata': taskdatalist,
        'currencyuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies'), 'displayText', 'USD$', 'uri')
    }


def get_custom_field_list():
    return {
        "page": "1",
        "pagesize": "500",
        "columnUris": [
            "urn:replicon:task-custom-field-list-column:task-custom-field"
        ],
        "sort": [],
        "filterExpression": null
    }


def update_custom_field():
    return {
        "project": {
            "uri": rail.result('get_pimco_model_task_details')[0]['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "taskHierarchy": rail.result('get_task_heirarchy'),
        "taskModificationOptionUri": 'urn:replicon:task-modification-option:save',
        "unitOfWorkId": str(uuid4())
    }


def update_project_task_market_rate(dag_run):
    return{
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "taskHierarchy": rail.result('get_task_heirarchy_details'),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_task_details(dag_run):
    return{
      "pageIndex": "1",
        "pageSize": "10000",
        "projectUris": [dag_run.conf['projecturi']],
        "taskDataInclusionOptionUris": []
    }
