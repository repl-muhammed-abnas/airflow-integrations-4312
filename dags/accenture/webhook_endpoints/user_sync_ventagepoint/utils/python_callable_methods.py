import rail


def filter_unprocessed_rows():
    rows = rail.result('fetch_grid_rows') or []
    processed_markers = {'Y', '1', 'y', 'true', 'True', 'TRUE'}
    return [
        row for row in rows
        if str(row.get('CustProcessed', '')).strip() not in processed_markers
        and row.get('CustProcessed') not in (True, 1)
    ]


def normalize_employee_id(employee_id):
    s = str(employee_id or '').lstrip('0') or '0'
    return s.zfill(6)


def make_get_child_conf(vantagepoint_conn_id):
    def get_child_conf(dag_run, item):
        return {
            **item,
            'udic_uid': dag_run.conf['webhook']['data']['UID'],
            'custname': dag_run.conf['webhook']['data'].get('CUSTNAME'),
            'custnumber': dag_run.conf['webhook']['data'].get('CUSTNUMBER'),
            'vantagepoint_conn_id': vantagepoint_conn_id,
            'employee': normalize_employee_id(item.get('CustEmployeeId')),
        }
    return get_child_conf


def build_parent_processed_payload(dag_run):
    data = dag_run.conf['webhook']['data']
    return {
        'UDIC_UID': data['UID'],
        'CustName': data.get('CUSTNAME'),
        'CustNumber': data.get('CUSTNUMBER'),
        'CustProcessed': 'Y',
        '_originalValues': {'CustProcessed': 'N'},
    }
