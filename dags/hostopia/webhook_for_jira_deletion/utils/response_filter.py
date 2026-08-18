import rail


def get_task_data(response):
    name = rail.result("get_triggered_data")["taskname"]
    data = response.json()['d']
    if not data:
        return []

    return list(filter(lambda x: x['Taskname'] == name, list(map(lambda item: {
        "Taskname": item['task']['name'],
        "Taskuri": item['task']['uri'],
    }, data))))
