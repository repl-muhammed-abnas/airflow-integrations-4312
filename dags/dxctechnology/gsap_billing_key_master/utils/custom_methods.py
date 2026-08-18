from datetime import datetime
import rail

null = None


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_replicon_date(date_str, date_format='%Y%m%d'):
    if not date_str:
        return null
    # date format in 20060401
    try:
        _date = datetime.strptime(date_str, date_format)
        return {
            'year': _date.year,
            'month': _date.month,
            'day': _date.day
        }
    except:  # pylint: disable=bare-except
        return null


def get_userlist(project_details):
    data = rail.result(project_details)
    return list(
        map(lambda x: {
            'uri': x,
            'assignmentstartdate': null,
            'assignmentenddate': null
        }, data)
    )


def set_dag_run_conf_ancestry(ancestry, context):
    if len(ancestry) > 5:
        context['dag_run'].conf['_ancestry'] = ancestry[:5] + [ancestry[-1]]
        return []
    context['dag_run'].conf['_ancestry'] = ancestry
    return []

def get_process_unique_wbs_conf_reprocess(item, context):
    _ = set_dag_run_conf_ancestry(item['properties']['_ancestry'], context)
    # increased the reprocess count by 1
    item['properties']['reprocess_count'] = int(item['properties'].get('reprocess_count', 0)) + 1
    return {
        **item['properties']
        }
