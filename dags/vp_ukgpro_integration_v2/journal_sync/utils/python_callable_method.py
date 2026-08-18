"""
Common utility methods for VP UKG Pro Journal Sync integration.
"""
# pylint: disable=invalid-name,missing-function-docstring
# pylint: disable=too-many-locals,line-too-long
import json
from datetime import datetime
import rail
from airflow.models import Variable


def capture_cd_error(error_message):
    """
    Catch task for cash_disbursement_dag.
    error_message is injected via op_args using
    {{ get_error_message() }} Jinja macro.
    Always RETURNS (never raises) to keep the DAG run as SUCCESS.
    If create_cash_disbursement succeeded, CD was created -
    email failure only, not a CD error.
    If create_cash_disbursement failed or was skipped, report
    the error so sync time is not updated.
    """
    try:
        cd_result = rail.result('create_cash_disbursement')
        if cd_result:
            return None
    except Exception:  # pylint: disable=broad-except
        pass
    if error_message:
        return {'error': error_message}
    return None


def capture_main_error():
    """
    Catch task for main_dag.
    Reads gathered child dag errors and raises if any found,
    so main_dag fails and sync time is NOT updated (triggers retry).
    """
    child_errors = rail.result('gather_cd_errors') or []
    errors = [e for e in child_errors if e and e.get('error')]
    if errors:
        error_messages = '\n'.join(e['error'] for e in errors)
        raise RuntimeError(
            f"Cash disbursement errors:\n{error_messages}"
        )


def format_payroll_data_method():
    flat_row = rail.result('process_each_record')
    wbs_mapping_data = rail.result('fetch_mapping_data')

    companyCode = flat_row.get('companyCode')
    orglevel2 = flat_row.get('orgLevel2Code')
    orglevel3 = flat_row.get('orgLevel3Code')
    orglevel4 = flat_row.get('orgLevel4Code')

    payee = 'UKG Pro'
    bankCode = flat_row.get('CheckAddModeCode')
    checkNo = flat_row.get('CheckNumber')
    transDate = flat_row.get('PayDate')
    batch = flat_row.get('Id')
    employee_number = flat_row.get('EmployeeNumber')
    first_name = flat_row.get('NameFirst') or ''
    last_name = flat_row.get('NameLast') or ''
    employee_name = f"{first_name} {last_name}".strip() or None

    def normalize(value):
        """Treat null / empty / '-' / 'none' as 'All'"""
        if not value or str(value).strip().lower() in (
            '-', 'none', 'null'
        ):
            return 'All'
        return str(value).strip()

    def find_wbs_match(
        payrollCode, companyCode, orglevel2, orglevel3, orglevel4
    ):
        """
        Priority-based lookup:
          P1 - exact match on all 3 orglevels
          P2 - orglevel2 + orglevel3 match, orglevel4 = 'All'
          P3 - orglevel2 match, orglevel3 = 'All', orglevel4 = 'All'
          P4 - full fallback: all orglevels = 'All'
        payrollCode + companyCode always required.
        """
        ol2 = normalize(orglevel2)
        ol3 = normalize(orglevel3)
        ol4 = normalize(orglevel4)

        priority_checks = [
            # P1 - most specific
            {'orglevel2': ol2, 'orglevel3': ol3, 'orglevel4': ol4},
            # P2
            {'orglevel2': ol2, 'orglevel3': ol3, 'orglevel4': 'All'},
            # P3
            {'orglevel2': ol2, 'orglevel3': 'All', 'orglevel4': 'All'},
            # P4 - full fallback
            {'orglevel2': 'All', 'orglevel3': 'All', 'orglevel4': 'All'},
        ]

        for criteria in priority_checks:
            for row in wbs_mapping_data:
                if (
                    row.get('payrollCode') == payrollCode and
                    row.get('company') == companyCode and
                    normalize(row.get('orglevel2'))
                    == criteria['orglevel2'] and
                    normalize(row.get('orglevel3'))
                    == criteria['orglevel3'] and
                    normalize(row.get('orglevel4'))
                    == criteria['orglevel4']
                ):
                    return row  # return first match at highest priority

        return None  # no match found at any priority level

    valid_records = []
    invalid_records = []

    payCode = flat_row.get('payCode')
    description = flat_row.get('payCodeDescription')
    raw_amount = flat_row.get('amount')

    if flat_row.get('type') == 'Deduction':
        amount = str(-float(raw_amount)) if raw_amount is not None else None
    else:
        amount = raw_amount

    matched_row = find_wbs_match(
        payCode, companyCode, orglevel2, orglevel3, orglevel4
    )

    if matched_row:
        valid_records.append({
            'payrollCode': payCode,
            'companyCode': companyCode,
            'orglevel2': orglevel2,
            'orglevel3': orglevel3,
            'orglevel4': orglevel4,
            'employeeNumber': employee_number,
            'employeeName': employee_name,
            'BankCode': bankCode,
            'Payee': payee,
            'CheckNo': checkNo,
            'TransDate': transDate,
            'WBS1': matched_row.get('project'),
            'WBS2': matched_row.get('phase'),
            'WBS3': matched_row.get('task'),
            'Account': matched_row.get('account'),
            'Amount': amount,
            'DetailDescription': description,
            'Batch': batch
        })
    else:
        # no match - mirror valid_records structure, WBS fields as None
        invalid_records.append({
            'payrollCode': payCode,
            'companyCode': companyCode,
            'orglevel2': orglevel2,
            'orglevel3': orglevel3,
            'orglevel4': orglevel4,
            'employeeNumber': employee_number,
            'employeeName': employee_name,
            'BankCode': bankCode,
            'Payee': payee,
            'CheckNo': checkNo,
            'TransDate': transDate,
            'WBS1': None,
            'WBS2': None,
            'WBS3': None,
            'Account': None,
            'Amount': amount,
            'DetailDescription': description,
            'Batch': batch,
            'wbs_error': (
                f"No WBS match found for payrollCode '{payCode}'"
                f" and companyCode '{companyCode}'"
            )
        })

    return {
        "valid_records": valid_records,
        "invalid_records": invalid_records
    }


def aggregate_resolved_records_method():
    """
    Flatten each resolved record's Earnings and Deductions
    into individual rows, then aggregate by (companyCode,
    CheckAddModeCode, orgLevel2Code, orgLevel3Code,
    orgLevel4Code, payCode, type), summing amounts per group.
    Preserves all fields required by format_payroll_data_method.
    """
    records = rail.result('fetch_resolved_records')

    # Step 1 - Flatten Earnings and Deductions into individual rows
    flat_rows = []
    for record in records:
        parent_fields = {
            'companyCode': record.get('companyCode'),
            'CheckAddModeCode': record.get('CheckAddModeCode'),
            'orgLevel2Code': record.get('orgLevel2Code'),
            'orgLevel3Code': record.get('orgLevel3Code'),
            'orgLevel4Code': record.get('orgLevel4Code'),
            'CheckNumber': record.get('CheckNumber'),
            'PayDate': record.get('PayDate'),
            'Id': record.get('Id'),
            'EmployeeNumber': record.get('EmployeeNumber'),
            'NameFirst': record.get('NameFirst'),
            'NameLast': record.get('NameLast'),
        }
        for earning in record.get('Earnings') or []:
            flat_rows.append({
                **parent_fields,
                'payCode': earning.get('EarningCode'),
                'type': 'Earning',
                'amount': earning.get('EarningCurrentAmount'),
                'payCodeDescription': earning.get('EarningDescription'),
            })
        for deduction in record.get('Deductions') or []:
            flat_rows.append({
                **parent_fields,
                'payCode': deduction.get('DeductionCode'),
                'type': 'Deduction',
                'amount': deduction.get('EmployeeDeductionAmount'),
                'payCodeDescription': deduction.get('DeductionDescription'),
            })

    # Step 2 - Group by key and sum amounts
    groups = {}
    group_indices = {}

    for idx, row in enumerate(flat_rows):
        key = (
            row.get('companyCode'),
            row.get('CheckAddModeCode'),
            row.get('orgLevel2Code'),
            row.get('orgLevel3Code'),
            row.get('orgLevel4Code'),
            row.get('payCode'),
            row.get('type'),
            row.get('PayDate'),
        )
        if key not in groups:
            groups[key] = {
                'companyCode': row.get('companyCode'),
                'CheckAddModeCode': row.get('CheckAddModeCode'),
                'orgLevel2Code': row.get('orgLevel2Code'),
                'orgLevel3Code': row.get('orgLevel3Code'),
                'orgLevel4Code': row.get('orgLevel4Code'),
                'payCode': row.get('payCode'),
                'type': row.get('type'),
                'PayDate': row.get('PayDate'),
                'payCodeDescription': row.get('payCodeDescription'),
                '_amount_sum': 0.0,
                # Non-grouping fields - first occurrence wins
                'CheckNumber': row.get('CheckNumber'),
                'Id': row.get('Id'),
                'EmployeeNumber': row.get('EmployeeNumber'),
                'NameFirst': row.get('NameFirst'),
                'NameLast': row.get('NameLast'),
            }
            group_indices[key] = []
        groups[key]['_amount_sum'] += float(row.get('amount') or 0)
        group_indices[key].append(idx)

    # Step 3 - Build final output list
    aggregated = []
    for key, group in groups.items():
        indices = group_indices[key]
        group['amount'] = str(group.pop('_amount_sum'))
        group['source_count'] = len(indices)
        group['source_indices'] = indices
        aggregated.append(group)

    return aggregated


def aggregate_artifacts_method():
    """
    Reads all CD artifacts from fetch_artifact_references, groups them by
    (BankCode + TransDate), merges cdDetail entries per group, reassigns
    Seq/P key/Batch, recalculates Total and Sum total, writes one new
    artifact per group, and returns the list of new artifact names.
    """
    artifact_names = rail.result('fetch_artifact_references') or []

    # Step 1 - Fetch actual CD record data from each artifact
    # Skip empty artifacts produced when all records had no WBS match
    records = []
    for name in artifact_names:
        record = json.loads(rail.read_artifact(name))
        if not record:
            continue
        records.append(record)

    # Step 2 - Group records by (BankCode + TransDate)
    groups = {}
    group_order = []  # preserve insertion order

    for record in records:
        cdmaster = record.get('cdMaster') or {}
        bank_code = cdmaster.get('Bank code', '').replace(' ', '')
        trans_date = cdmaster.get('Trans date', '').replace(' ', '')
        key = (bank_code, trans_date)
        if key not in groups:
            groups[key] = []
            group_order.append(key)
        groups[key].append(record)

    # Step 3 - Merge each group into one aggregated record
    aggregated_artifact_names = []

    for key in group_order:
        group_records = groups[key]
        bank_code, trans_date = key
        new_batch = f"UKGPro_{bank_code}_{trans_date.replace('-', '')}"
        first = group_records[0]

        # Scalar fields from first record; Batch and Description use new format
        aggregated = {
            'Batch': new_batch,
            'Description': new_batch,
            'Recurring': first.get('Recurring'),
            'End date': first.get('End date'),
            'Diff total': first.get('Diff total'),
            'Default bank': first.get('Default bank'),
            'Default bank description': first.get('Default bank description'),
            'Bank currency code': first.get('Bank currency code'),
            'Selected': first.get('Selected'),
            'Posted': first.get('Posted'),
            'Creator': first.get('Creator'),
            'Period': first.get('Period'),
            'Company': first.get('Company'),
            'cdMaster': dict(first.get('cdMaster') or {}),
            'notifierEmail': first.get('notifierEmail'),
        }
        aggregated['cdMaster']['Batch'] = new_batch

        # Merge cdDetail entries from every record in the group
        merged_details = []
        for record in group_records:
            merged_details.extend(record.get('cdDetail') or [])

        # Reassign Seq, P key, Batch and Check no on every detail entry
        cd_master_check_no = aggregated['cdMaster'].get('Check no', '')
        for seq, detail in enumerate(merged_details):
            detail['Seq'] = seq
            detail['P key'] = f"{new_batch}_{str(seq).rjust(4, '0')}"
            detail['Batch'] = new_batch
            detail['Check no'] = cd_master_check_no

        aggregated['cdDetail'] = merged_details

        # Recalculate Total and Sum total across all merged detail amounts
        total = round(
            sum(float(d.get('Amount') or 0) for d in merged_details)
        )
        aggregated['Total'] = total
        aggregated['Sum total'] = total

        # Write aggregated record as a new artifact and collect its name
        artifact_name = rail.write_json_artifact(aggregated)
        aggregated_artifact_names.append(artifact_name)

    return aggregated_artifact_names


def format_cash_disbursement_data(notification_email):
    """Format parsed CSV data and store in XCom

    Args:
        notification_email: Email address for notifications
    """
    parsed_data = rail.result('format_payroll_data').get('valid_records') or []
    if not parsed_data:
        return {}
    first_record = parsed_data[0]
    batch_name = "UKGPro" + first_record['Batch'].replace(" ", "")
    trans_date = datetime.strptime(
        first_record['TransDate'].rstrip('Z'), "%Y-%m-%dT%H:%M:%S"
    )

    formatted_data = {
        "Batch": batch_name,
        "Description": batch_name,
        "Recurring": "N",
        "End date": trans_date.strftime("%Y-%m-%d"),
        "Total": sum(int(float(data["Amount"])) for data in parsed_data),
        "Sum total": sum(int(float(data["Amount"])) for data in parsed_data),
        "Diff total": "",
        "Default bank": first_record['BankCode'],
        "Default bank description": "",
        "Bank currency code": "",
        "Selected": "N",
        "Posted": "N",
        "Creator": "DELTEK_API",
        "Period": trans_date.strftime("%Y%m"),
        "Company": first_record['companyCode'],
        "cdMaster": {
            "Batch": batch_name,
            "Check no": first_record['CheckNo'],
            "Bank code": first_record['BankCode'],
            "Trans date": trans_date.strftime("%Y-%m-%d"),
            "Payee": first_record['Payee'],
            "Posted": "N",
            "Seq": "",
            "Currency code": "",
            "Currency exchange override method": None,
            "Currency exchange override date": None,
            "Currency exchange override rate": None,
            "Status": "N",
            "Authorized by": "",
            "Reject reason": "",
            "Mod user": "",
            "Mod date": "",
            "Diary": "",
            "Diary no": ""
        },
        "cdDetail": [{
            "Batch": batch_name,
            "Check no": data.get('CheckNo', ""),
            "P key": batch_name + "_" + str(index).rjust(4, "0"),
            "Seq": index,
            "Description": data.get("DetailDescription", ""),
            "WBS 1": data.get("WBS1", ""),
            "WBS 2": data.get("WBS2", ""),
            "WBS 3": data.get("WBS3", ""),
            "Account": data.get("Account", ""),
            "Net amount": "",
            "Amount": data.get("Amount", ""),
            "Currency exchange override rate": "",
            "Link company": ""
        } for index, data in enumerate(parsed_data)],
        "notifierEmail": notification_email
    }

    return formatted_data


def get_wbs_mapping_data(instance):
    """
    Returns WBS mapping data for journal sync, read from the per-instance
    Airflow Variable 'vp_ukgpro_journal_sync_v2_wbs_mapping_{instance}'.
    Maps earning code + company + org levels to project/phase/task/account.
    Raises if the Variable is unset or empty so misconfigured runs fail
    loudly instead of silently posting financials against demo data.
    """
    variable_key = f'vp_ukgpro_journal_sync_v2_wbs_mapping_{instance}'
    variable_data = Variable.get(
        variable_key,
        default_var=None,
        deserialize_json=True
    )
    if not variable_data:
        raise RuntimeError(
            f"WBS mapping Airflow Variable '{variable_key}' is not set "
            f"or empty. Configure per-instance WBS mapping before running "
            f"vp_ukgpro_journal_sync_v2 for instance '{instance}'."
        )
    return variable_data


def build_cd_request_body():
    """Build request body for cash disbursement creation"""
    conf = rail.result('read_cash_disbursement_data')

    bank_currency_code = None
    banks = rail.result('get_all_banks_from_vp')
    default_bank_code = conf.get("Default bank")

    for bank in banks:
        if bank.get("Code") == default_bank_code:
            bank_currency_code = bank.get("AccountCurrencyCode")
            break  # stop after first match

    request_body = {
        "Batch": conf.get('Batch'),
        "Description": conf.get('Description'),
        "Recurring": conf.get('Recurring'),
        "EndDate": conf.get('End date'),
        "Total": conf.get('Total'),
        "SumTotal": conf.get('Sum total'),
        "DiffTotal": conf.get('Diff total'),
        "DefaultBank": default_bank_code,
        "DefaultBankDescription": conf.get('Default bank description'),
        "BankCurrencyCode": bank_currency_code,
        "Selected": conf.get('Selected'),
        "Posted": conf.get('Posted'),
        "Creator": conf.get('Creator'),
        "Period": conf.get('Period'),
        "Company": conf.get('Company'),
        "cdMaster": [{
            "Batch": conf.get('cdMaster').get('Batch'),
            "CheckNo": conf.get('cdMaster').get('Check no'),
            "BankCode": conf.get('cdMaster').get('Bank code'),
            "TransDate": conf.get('cdMaster').get('Trans date'),
            "Payee": conf.get('cdMaster').get('Payee'),
            "Posted": conf.get('cdMaster').get('Posted'),
            "Seq": conf.get('cdMaster').get('Seq'),
            "CurrencyCode": bank_currency_code,
            "CurrencyExchangeOverrideMethod": conf.get('cdMaster').get(
                'Currency exchange override method'
            ),
            "CurrencyExchangeOverrideDate": conf.get('cdMaster').get(
                'Currency exchange override date'
            ),
            "CurrencyExchangeOverrideRate": conf.get('cdMaster').get(
                'Currency exchange override rate'
            ),
            "Status": conf.get('cdMaster').get('Status'),
            "AuthorizedBy": conf.get('cdMaster').get('Authorized by'),
            "RejectReason": conf.get('cdMaster').get('Reject reason'),
            "ModUser": conf.get('cdMaster').get('Mod user'),
            "ModDate": conf.get('cdMaster').get('Mod date'),
            "Diary": conf.get('cdMaster').get('Diary'),
            "DiaryNo": conf.get('cdMaster').get('Diary no')
        }],
        "cdDetail": [{
            "Batch": data.get('Batch'),
            "CheckNo": data.get('Check no'),
            "PKey": data.get('P key'),
            "Seq": data.get('Seq'),
            "Description": data.get('Description'),
            "WBS1": data.get('WBS 1'),
            "WBS2": data.get('WBS 2'),
            "WBS3": data.get('WBS 3'),
            "Account": data.get('Account'),
            "NetAmount": data.get('Net amount'),
            "Amount": data.get('Amount'),
            "CurrencyExchangeOverrideRate": data.get(
                'Currency exchange override rate'
            ),
            "LinkCompany": data.get('Link company')
        } for data in conf.get('cdDetail')]
    }

    cd_master = request_body["cdMaster"][0]

    override_keys = (
        "CurrencyExchangeOverrideMethod",
        "CurrencyExchangeOverrideDate",
        "CurrencyExchangeOverrideRate"
    )
    for key in override_keys:
        if cd_master.get(key) is None:
            cd_master.pop(key, None)

    return request_body


def is_cd_already_exists_error():
    """
    Extract error from failed task and check if it's duplicate batch error
    """
    ti = rail.get_current_context()["ti"]
    error_data = ti.xcom_pull(
        task_ids='create_cash_disbursement', key='error'
    )
    error_message = "Unknown error occurred"
    if error_data and isinstance(error_data, dict):
        error_message = error_data.get(
            'exc_message', 'Unknown error occurred'
        )
    ti.xcom_push(key='error_message', value=error_message)
    is_duplicate = (
        "already exists and cannot be added" in error_message.lower()
    )

    return is_duplicate
