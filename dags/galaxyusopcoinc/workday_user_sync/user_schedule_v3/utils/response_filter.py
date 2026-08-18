from galaxyusopcoinc.workday_user_sync.user_schedule_v3.utils import request_payload


def map_user_response(response):
    data = response.json()['d']['rows']
    return list(filter(lambda x: x['cells'][2]['textValue'] == request_payload.get_dag_run_conf()['employee_id'], data))


def map_schedule_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == request_payload.get_dag_run_conf()['replicon_schedule_type'], data))
