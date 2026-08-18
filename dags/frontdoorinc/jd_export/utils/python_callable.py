# pylint: disable=too-many-statements
from pendulum import timezone, now, datetime
from functools import lru_cache
from rail import find_first_by_attr_and_get_attr, result, load_all_records, write_json_artifact, load_json_artifact
from frontdoorinc.jd_export.mapper.frontdoorinc_jd_export_mapper import frontdoorinc_jd_export_master_mapper

def get_dag_run_conf():
    start_date = now(timezone("America/Chicago")).subtract(months=1)
    return {
        "start_date": datetime(start_date.year, start_date.month, 1, tz='America/Chicago').strftime("%m/%d/%Y"),
        "end_date": datetime(start_date.year, start_date.month, start_date.days_in_month, tz='America/Chicago').strftime("%m/%d/%Y")
    }

def get_ondemand_conf(dag_run):
    day = 1
    month = int(dag_run.conf['month'])
    year = int(dag_run.conf['year'])
    email = dag_run.conf['email']
    start_date = datetime(day=day, month=month, year=year)
    end_date = datetime(day=start_date.days_in_month, month=month, year=year)
    return {
        "start_date": start_date.strftime("%m/%d/%Y"),
        "end_date": end_date.strftime("%m/%d/%Y"),
        "email":email
    }

def get_report_input_value():
    date = now(timezone("America/Chicago"))
    return {
        "filename":"frontdoorinc_innotas_" + date.strftime("%Y_%m_%dT%H_%M_%S") + ".csv",
        "filename2":"frontdoorinc_je_" + date.strftime("%Y_%m_%dT%H_%M_%S") + ".csv",
        "dag_trigger_time": date.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    }

def get_value_for_contractor_and_full_time():
    response = result('get_enabled_employee_type_groups')
    return {
        "contractor":find_first_by_attr_and_get_attr(response, 'displayText', 'Contractor', 'uri').split(":")[-1],
        "full_time":find_first_by_attr_and_get_attr(response, 'displayText', 'Full Time', 'uri').split(":")[-1]
    }

def get_opexdebitaccount(field, item):
    if item['Project Cost Center Code']:
        value = list(filter(lambda x: x['field'] == field and x['employeetype'] == item['Employee Type'], frontdoorinc_jd_export_master_mapper))
        return str(item['Project Cost Center Code']) + "." + value[0]['code'] if value else "."
    return 0

def get_opexcreditaccount(field, item):
    value = list(filter(lambda x: x['field'] == field and x['employeetype'] == item['Employee Type'], frontdoorinc_jd_export_master_mapper))
    return str(item['Employee Cost Center Code']) + "." + value[0]['code'] if value else "."

def get_wipaccount_debit_or_capitalcreditaccount(field, item):
    if item['CapEx/OpEx'] != "OpEx" and item['SaaS Solution'] and item['SaaS Solution'] == "No":
        value = list(filter(lambda x: x['field'] == field and x['employeetype'] == item['Employee Type'], frontdoorinc_jd_export_master_mapper))
        return str(item['Project Cost Center Code']) + "." + value[0]['code'] if value else 0
    return 0


def get_wipdebitamount_or_capitalcreditamount(item):
    return item['Total cost'] if item['CapEx/OpEx'] != "OpEx" and item['SaaS Solution'] and item['SaaS Solution'] == "No" else 0

def get_saasimplaccountwipddebitaccount(field, item):
    if item['CapEx/OpEx'] != "OpEx" and item['SaaS Solution'] and item['SaaS Solution'] == "Yes" and item['Project Capitalizable'] == "Yes":
        value = list(filter(lambda x: x['field'] == field, frontdoorinc_jd_export_master_mapper))
        return str(item['Project Cost Center Code']) + "." + value[0]['code'] if value else 0
    return 0

def get_saaswipdebitamount(item):
    return item['Total cost'] if item['CapEx/OpEx'] != "OpEx" and item['SaaS Solution'] and \
        item['SaaS Solution'] == "Yes" and item['Project Capitalizable'] == "Yes" else 0

def get_saascapitalcreditaccount(field, item):
    if item['CapEx/OpEx'] != "OpEx" and item['SaaS Solution'] and item['SaaS Solution'] == "Yes" and item['Project Capitalizable'] == "Yes":
        value = list(filter(lambda x: x['field'] == field and x['employeetype'] == item['Employee Type'], frontdoorinc_jd_export_master_mapper))
        return str(item['Project Cost Center Code']) + "." + value[0]['code'] if value else 0
    return 0

def get_naffundedcreditamount(item):
    return item['Total cost'] if item['CapEx/OpEx'] != "OpEx" and item['SaaS Solution'] and \
        item['SaaS Solution'] == "Yes" and item['Project Capitalizable'] == "Yes" else 0

def get_export_list1(item):
    return [
        item['Project Code'],
        item['IT Financial Budget ID'],
        item['Project Name'],
        item['Company'],
        item['Company Code'],
        item['Employee ID'],
        item['User Name'],
        item['Employee Cost Center Code'],
        item['Employee Type'],
        item['Job Profile Name'],
        item['User Supervisor Name (Current)'],
        item['Total Hrs'],
        item['Hourly Cost Amount'],
        item['Total cost'],
        item['Approval Status'],
        item['Entry Date'],
        item['Month (Entry Date)'],
        item['Project Capitalizable'],
        item['SaaS Solution'],
        item['Project Cost Center Code'],
        item['Finance Department'],
        get_opexdebitaccount("Opex Debit Account", item),
        item['Total cost'],
        get_opexcreditaccount("Opex Credit Account", item),
        item['Total cost'],
        get_wipaccount_debit_or_capitalcreditaccount("WIP Account (Debit)", item),
        get_wipdebitamount_or_capitalcreditamount(item),
        get_wipaccount_debit_or_capitalcreditaccount("Capital Credit Account", item),
        get_wipdebitamount_or_capitalcreditamount(item),
        get_saasimplaccountwipddebitaccount("SaaS Impl Account WIP Debit Account", item),
        get_saaswipdebitamount(item),
        get_saascapitalcreditaccount("SaaS-Capital Credit Account", item),
        get_naffundedcreditamount(item),
        0,
        0,
        0,
        0,
    ]

def get_opexdebittorestructuringgross(field, item, opexdebit_account):
    if item['opexdebitaccount'] not in [0, '0', False, None]:
        if item['opexdebitaccount'] == opexdebit_account:
            for value in frontdoorinc_jd_export_master_mapper:
                if value['field'] == field and value['employeetype'] == 'Full Time':
                    return str(value['code'])
        else:
            for value in frontdoorinc_jd_export_master_mapper:
                if value['field'] == field and value['employeetype'] == item['employeetype']:
                    return str(value['code'])
    return 0

def get_export_list2(item, opexdebit_account):
    return [
            item['projectid'],
            item['itfinancialbudgetid'],
            item['projectname'],
            item['companyname'],
            item['companyid'],
            item['employeeid'],
            item['username'],
            item['employeecostcentercode'],
            item['employeetype'],
            item['jobprofilename'],
            item['supervisor'],
            item['total'],
            item['hourlycost'],
            item['totalcost'],
            item['approvalstatus'],
            item['timesheetentrydate'],
            item['timesheetentrymonth'],
            item['projectcapexeligible'],
            item['saasolution'],
            item['projectcostcentercode'],
            item['financialdepartment'],
            item['opexdebitaccount'],
            item['opexdebitamount'],
            item['opexcreditaccount'],
            item['opexcreditamount'],
            item['wipaccount(debit)'],
            item['wipdebitamount'],
            item['capitalcreditaccount'],
            item['capitalcreditamount'],
            item['saasimplaccountwipddebitaccount'],
            item['saaswipdebitamount'],
            item['saascapitalcreditaccount'],
            item['naffundedcreditamount'],
            get_opexdebittorestructuringgross("OPEX Debit To Restructuring Gross Allocation", item, opexdebit_account),
            item['opexrestrucutingallocationdebit'],
            item['opexcredittorelieveit'],
            item['opexrestructuringallocationcredit'],
    ]

def get_export_list3(item):
    return [
            item['projectid'],
            item['itfinancialbudgetid'],
            item['projectname'],
            item['companyname'],
            item['companyid'],
            item['employeeid'],
            item['username'],
            item['employeecostcentercode'],
            item['employeetype'],
            item['jobprofilename'],
            item['supervisor'],
            item['total'],
            item['hourlycost'],
            item['totalcost'],
            item['approvalstatus'],
            item['timesheetentrydate'],
            item['timesheetentrymonth'],
            item['projectcapexeligible'],
            item['saasolution'],
            item['projectcostcentercode'],
            item['financialdepartment'],
            item['opexdebitaccount'],
            item['opexdebitamount'],
            item['opexcreditaccount'],
            item['opexcreditamount'],
            item['wipaccount(debit)'],
            item['wipdebitamount'],
            item['capitalcreditaccount'],
            item['capitalcreditamount'],
            item['saasimplaccountwipddebitaccount'],
            item['saaswipdebitamount'],
            item['saascapitalcreditaccount'],
            item['naffundedcreditamount'],
            item['opexdebittorestructuringgross'],
            0 if str(item['opexdebittorestructuringgross']) == "0" else item['opexcreditaccount'],
            0 if str(item['opexdebittorestructuringgross']) == "0" else item['opexcreditaccount'],
            0 if str(item['opexdebittorestructuringgross']) == "0" else item['opexcreditamount'],
    ]

def get_request_body_payroll_download_batch(dag_run):
    enablefilter = result('get_report_details')['filterConfiguration']['enabledFilters']
    return {"reportParameters": [
                {
                    "reportUri": result('get_report_details')['uri'],
                    "filterValues": [
                        {
                            "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'EntryDateFilter', 'uri'),
                            "value": None
                        },
                        {
                            "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'EntryDateFilter', 'uri'),
                            "value": dag_run.conf['start_date']
                        },
                        {
                            "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'EntryDateFilter', 'uri'),
                            "value": dag_run.conf['end_date']
                        },
                        {
                            "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'ApprovalStatusFilter', 'uri'),
                            "value": "2"
                        },
                        {
                            "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'CurrentEmployeeTypeGroupFilter', 'uri'),
                            "value": result('get_value_for_contractor_and_full_time').get('contractor')
                        },
                        {
                            "reportFilterUri": find_first_by_attr_and_get_attr(enablefilter, 'displayText', 'CurrentEmployeeTypeGroupFilter', 'uri'),
                            "value": result('get_value_for_contractor_and_full_time').get('full_time')
                        }
                    ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]
        }


@lru_cache
def get_all_records(jelist1):
    return load_all_records(jelist1)

def get_amount_finance_dept(dag_run):
    response = load_all_records(dag_run.conf['jelist1'])
    total = 0.0
    finance_dept = ''
    for item in response:
        if item['businessunitobjsub'] == dag_run.conf['businessunitobjsub'] and \
        item['projectname'] == dag_run.conf['projectname'] and \
        item['projectcode'] == dag_run.conf['projectcode']:
            amount = item['amount']
            if len(amount) == 1 and not amount.isdigit():
                amount = 0.0
            elif "," in amount:
                amount = amount.split(",")[0]
            total += float(amount) if amount else 0.0
            if not finance_dept:
                finance_dept = item['financedepartment']
    return {
        "total_amount": total,
        "financedepartment": finance_dept
    }
