from functools import lru_cache
from rail import result, load_all_records

def get_naf_funded_credit_amount(item):
    naffundedcreditamount = str(item["naffundedcreditamount"])
    if naffundedcreditamount in ("0.00", "0"):
        return "$ " + naffundedcreditamount
    return "-$ " + naffundedcreditamount

def get_capital_credit_amount(item):
    capitalcreditamount = str(item["capitalcreditamount"])
    if capitalcreditamount in ("0.00", "0"):
        return "$ " + capitalcreditamount
    return "-$ " + capitalcreditamount

def get_opex_credit_amount(item):
    opexcreditamount = str(item["opexcreditamount"])
    if opexcreditamount in ("0.00", "0"):
        return "$ " + opexcreditamount
    return "-$ " + opexcreditamount

def get_innotas_file_csv_row_data(item):
    row_data = [
        '""' if not item["projectid"] else item["projectid"],
        '""' if not item["itfinancialbudgetid"] else item["itfinancialbudgetid"],
        '""' if not item["projectname"] else item["projectname"],
        '""' if not item["companyname"] else item["companyname"],
        '""' if not item["companyid"] else item["companyid"],
        '""' if not item["employeeid"] else item["employeeid"],
        '""' if not item["username"] else item["username"],
        '""' if not item["employeecostcentercode"] else item["employeecostcentercode"],
        '""' if not item["employeetype"] else item["employeetype"],
        '""' if not item["jobprofilename"] else item["jobprofilename"],
        '""' if not item["supervisor"] else item["supervisor"],
        '""' if not item["total"] else item["total"],
        '""' if not item["hourlycost"] else item["hourlycost"],
        "$ " + str(item["totalcost"]),
        '""' if not item["approvalstatus"] else item["approvalstatus"],
        '""' if not item["timesheetentrydate"] else item["timesheetentrydate"],
        '""' if not item["timesheetentrymonth"] else item["timesheetentrymonth"],
        '""' if not item["projectcapexeligible"] else item["projectcapexeligible"],
        '""' if not item["saasolution"] else item["saasolution"],
        '""' if not item["projectcostcentercode"] else item["projectcostcentercode"],
        '""' if not item["financialdepartment"] else item["financialdepartment"],
        '0' if not item["opexdebitaccount"] else item["opexdebitaccount"],
        "$ " + str(item["opexdebitamount"]),
        '0' if not item["opexcreditaccount"] else item["opexcreditaccount"],
        get_opex_credit_amount(item),
        '0' if not item["wipaccount(debit)"] else item["wipaccount(debit)"],
        "$ " + str(item["wipdebitamount"]),
        '0' if not item["capitalcreditaccount"] else item["capitalcreditaccount"],
        get_capital_credit_amount(item),
        '0' if not item["saasimplaccountwipddebitaccount"] else item["saasimplaccountwipddebitaccount"],
        "$ " + str(item["saaswipdebitamount"]),
        '0' if not item["saascapitalcreditaccount"] else item["saascapitalcreditaccount"],
        get_naf_funded_credit_amount(item)
    ]
    return row_data

def get_je_file_csv_row_data(item):
    row_data = [
        item["projectid"],
        item["itfinancialbudgetid"],
        item["projectname"],
        item["companyname"],
        item["companyid"],
        item["employeeid"],
        item["username"],
        item["employeecostcentercode"],
        item["employeetype"],
        item["jobprofilename"],
        item["supervisor"],
        item["total"],
        item["hourlycost"],
        item["totalcost"],
        item["approvalstatus"],
        item["timesheetentrydate"],
        item["timesheetentrymonth"],
        item["projectcapexeligible"],
        item["saasolution"],
        item["projectcostcentercode"],
        item["financialdepartment"],
        item["opexdebitaccount"],
        item["opexdebitamount"],
        item["opexcreditaccount"],
        "-" + str(item['opexcreditamount']),
        item["wipaccount(debit)"],
        item["wipdebitamount"],
        item["capitalcreditaccount"],
        "-" + str(item['capitalcreditamount']),
        item["saasimplaccountwipddebitaccount"],
        item["saaswipdebitamount"],
        item["saascapitalcreditaccount"],
        "-" + str(item['naffundedcreditamount']),
        item['opexdebittorestructuringgross'],
        item['opexrestrucutingallocationdebit'],
        item['opexcredittorelieveit'],
        item['opexrestructuringallocationcredit']
    ]
    return row_data

def get_summary_list(dag_run):
    amount_financedept = result("get_amount_financedept")
    return {
                "businessunitobjsub": dag_run.conf.get('businessunitobjsub'),
                "amount": amount_financedept['total_amount'],
                "projectname": dag_run.conf.get('projectname'),
                "projectcode": dag_run.conf.get('projectcode'),
                "financedepartment": amount_financedept['financedepartment'],
            }

def get_create_je_file_csv_data(item):
    return [
        item['properties']['businessunitobjsub'],
        "0.0" if item['properties']['amount'] == "0" else item['properties']['amount'],
        '""' if not item['properties']['projectname'] else item['properties']['projectname'],
        '""' if not item['properties']['projectcode'] else item['properties']['projectcode'],
        item['properties']['businessunitobjsub'] if len(str(item['properties']['businessunitobjsub'])) < 9 else item['properties']['businessunitobjsub'][-9:],
        "0" if "-" in str(item['properties']['amount']) else "1",
        '""' if not item['properties']['financedepartment'] else item['properties']['financedepartment']
    ]
