import rail


def build_time_activity_payload(dag_run):
    hours_minutes = rail.result('calculate_hours_minutes')
    pay_item_id = rail.result('find_pay_item_id')
    entry_date = dag_run.conf.get('entry_date', '').replace('/', '-')
    return {
        'NameOf': 'Employee',
        'EmployeeRef': {'value': str(rail.result('get_qbo_employee_id'))},
        'Hours': hours_minutes['hours'],
        'Minutes': hours_minutes['minutes'],
        'PayrollItemRef': {'value': pay_item_id},
        'TxnDate': entry_date
    }