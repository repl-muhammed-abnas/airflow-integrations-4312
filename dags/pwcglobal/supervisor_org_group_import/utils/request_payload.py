# pylint:disable = too-many-statements
from datetime import datetime
from uuid import uuid4
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_replicon_date(input_date):
    return rail.parse_date(input_date, DATE_FORMAT)

def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def page_handler(request, result_resp):
    if len(result_resp['rows']) > 0:
        request['page'] += 1
        return request
    return null

def get_costcenter_hierarchy_payload():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:cost-center-list-column:full-path",
            "urn:replicon:cost-center-list-column:effectively-enabled"
        ],
        "filterExpression": null,
        "hierarchyListDataOptionUris": []
    }


def get_target(parents):
    if not parents:
        return null
    parent = {}
    for level in parents.split('|'):
        parent = {
            'name': level,
            'parent': parent if parent else null
        }
    return {
        'parent': parent
    }

def get_add_conf(dag_run):
    hierarchy = []
    parents = dag_run.conf['parents']
    child = dag_run.conf['child']
    for level in child.split('|'):
        target = get_target(parents)
        hierarchy.append({
            'target':target,
            'modificationToApply':{
                'name': level,
                'isEnabled': 'true'
            }
        })
        parents += f"|{level}" if parents else level
    return {
        "hierarchy": hierarchy,
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }