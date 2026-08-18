from datetime import datetime
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


def get_employee_lookup_filter(dag_run):
    employee_id = normalize_employee_id(dag_run.conf.get('CustEmployeeId', ''))
    return f'?filterHash[0][name]=Employee&filterHash[0][value]={employee_id}'


def _base_employee_fields(c):
    payload = {
        'Employee': normalize_employee_id(c.get('CustEmployeeId')),
        'LastName': c.get('CustLastName'),
        'FirstName': c.get('CustFirstName'),
        'MiddleName': c.get('CustMiddleName'),
        'PreferredName': c.get('CustPreferredName'),
        'EMail': c.get('CustEmail'),
        'EmployeeCompany': c.get('CustEmployeeCompany'),
        'JobCostRate': c.get('CustJobCostRate'),
        'JCOvtPct': c.get('CustJCOvtPct'),
        'JCSpecialOvtPct': c.get('CustJCSpecialOvtPct'),
        'HoursPerDay': c.get('CustHoursPerDay'),
        'HireDate': c.get('CustHireDate') or None,
        'TerminationDate': c.get('CustTerminationDate') or None,
        'Status': c.get('CustStatus'),
        'Org': c.get('CustOrg'),
        'Supervisor': c.get('CustSupervisor'),
        'DefaultLC1': c.get('CustDefaultLC1'),
        'JobCostType': c.get('CustJobCostType'),
        'CustFLSAStatus': c.get('CustCustFLSAStatus'),
        'CustFTPTSTATUS': c.get('CustCustFTPTSTATUS'),
        'Address1': c.get('CustAddressLine1'),
        'Address2': c.get('CustAddressLine2'),
        'City': c.get('CustCity'),
        'State': c.get('CustState'),
        'ZIP': c.get('CustZIP'),
        'Country': c.get('CustCountry'),
    }
    home_company = c.get('CustHomeCompany')
    if home_company:
        payload['HomeCompany'] = home_company
    return {k: v for k, v in payload.items() if v is not None and v != ''}


def _bank_deposit_fields(c):
    bank_id = c.get('CustBankKeys', '')
    account = c.get('CustBankAccountNumber', '')
    if not bank_id or not account:
        return None
    account_type_map = {'Savings': 'S', 'Checking': 'C', 'Money Market': 'M'}
    raw_type = c.get('CustAccountType', '')
    account_type = account_type_map.get(raw_type, raw_type[:1] if raw_type else 'C')
    return {
        'BankID': bank_id,
        'Account': account,
        'AccountType': account_type,
        'Status': 'A',
        'Employee': c.get('CustEmployeeId'),
        'EmployeeCompany': c.get('CustEmployeeCompany'),
        'Override': 'N',
        'exMethod': 'A',
    }


def _merge_direct_deposits(existing, incoming):
    merged = []
    matched = False
    for rec in existing:
        if not matched and rec.get('AccountType') == incoming.get('AccountType'):
            merged.append({**rec, **incoming, 'Seq': rec.get('Seq')})
            matched = True
        else:
            merged.append(rec)
    if not matched:
        next_seq = max((rec.get('Seq') or 0 for rec in existing), default=0) + 1
        merged.append({**incoming, 'Seq': next_seq})
    return merged


def get_direct_deposit_filters(dag_run):
    company = dag_run.conf.get('CustEmployeeCompany', '')
    if not company:
        return ''
    return f'?EmployeeCompany={company}&inDialog=false&Company={company}'


def get_create_payload(dag_run):
    payload = _base_employee_fields(dag_run.conf)
    deposit = _bank_deposit_fields(dag_run.conf)
    if deposit:
        payload['EMDirectDeposit'] = [{**deposit, 'Seq': 1}]
    return payload


def get_update_payload(dag_run):
    payload = _base_employee_fields(dag_run.conf)
    payload.pop('Employee', None)
    payload.pop('EmployeeCompany', None)
    deposit = _bank_deposit_fields(dag_run.conf)
    if deposit:
        existing = rail.result('lookup_direct_deposits') or []
        payload['EMDirectDeposit'] = _merge_direct_deposits(existing, deposit)
    return payload


def make_build_status_payload(employee_integration_table):
    def build_status_payload(error_message, dag_run):
        record = {
            '_transType': 'U',
            'UDIC_UID': dag_run.conf.get('udic_uid'),
            'Seq': dag_run.conf.get('Seq'),
            'CustProcessed': 'Y',
            'CustProcessedDate': datetime.utcnow().isoformat(),
            'CustMessage': f'Failed: {error_message}'[:500] if error_message else 'Processed successfully',
        }
        return {employee_integration_table: [record]}
    return build_status_payload


def get_error_summary(error_message):
    return {'error': f'Employee sync child failed — {error_message}'}
