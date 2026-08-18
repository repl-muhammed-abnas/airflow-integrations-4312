from dxctechnology.time_export_webhook_ackn.utils import custom_method


def map_time_export(response):
    time_export_id = custom_method.get_dag_run_conf(
    )['webhook']['data']['timeExportID'].split("|")[0]
    data = response.json()['d']['rows']
    result = list(filter(lambda x: x['cells']
                  [0]['textValue'] == time_export_id, data))
    return result
