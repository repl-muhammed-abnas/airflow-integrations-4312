

def get_expense_code_value(response, dag_run):
    expense_codes = dag_run.conf['item']['ExpenseCodes']
    return list(map(lambda row: row['expenseCode']['uri'], filter(lambda x: x['expenseCode']['name'] in expense_codes, response)))


def get_task_value(response, dag_run):
    return [x for x in get_task_from_input(dag_run) if x not in get_task_from_project(response)]


def get_task_from_input(dag_run):
    return list(map(lambda row: row, dag_run.conf['item']['Projects']))


def get_task_from_project(response):
    if response:
        return [{
            'ProjName': x['name'],
            'ProjCode': x['code']

        }for x in response]
    return []
