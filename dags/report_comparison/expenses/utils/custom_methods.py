import csv
import io
from dataclasses import field
import json
from datetime import datetime, timedelta
import rail


def generate_test_report():
    results = rail.result("comparison_report")

    total_workbook = (
        len(results["perfect_matches"])
        + len(results["real_mismatches"])
        + len(results["only_in_workbook"])
    )
    total_maconomy = (
        total_workbook
        - len(results["only_in_workbook"])
        + len(results["only_in_maconomy"])
    )

    # Calculate percentages
    perfect_pct = (
        (len(results["perfect_matches"]) / total_workbook * 100)
        if total_workbook > 0
        else 0
    )
    case_pct = (
        (len(results["case_only_differences"]) / total_workbook * 100)
        if total_workbook > 0
        else 0
    )
    mismatch_pct = (
        (len(results["real_mismatches"]) / total_workbook * 100)
        if total_workbook > 0
        else 0
    )
    missing_pct = (
        (len(results["only_in_maconomy"]) / total_maconomy * 100)
        if total_maconomy > 0
        else 0
    )

    report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # Summary Statistics Section
    writer.writerow(["Workbook vs Maconomy Expense Comparison Report"])
    writer.writerow(["Report Generated", report_time])
    writer.writerow([])

    # Overall Statistics
    writer.writerow(["SUMMARY STATISTICS"])
    writer.writerow(["Metric", "Count", "Percentage"])
    writer.writerow(["Total Workbook Expenses Records", total_workbook, "100.00%"])
    writer.writerow(
        [
            "Total Maconomy Expenses Records",
            total_maconomy,
            (
                f"{(total_maconomy/total_workbook*100):.2f}%"
                if total_workbook > 0
                else "N/A"
            ),
        ]
    )
    writer.writerow(
        [
            "Perfect Matches",
            len(results["perfect_matches"]),
            f"{perfect_pct:.2f}%",
        ]
    )
    
    writer.writerow(
        [
            "Real Data Mismatches",
            len(results["real_mismatches"]),
            f"{mismatch_pct:.2f}%",
        ]
    )
    writer.writerow(
        [
            "Only in Maconomy",
            len(results["only_in_maconomy"]),
            f"{missing_pct:.2f}%",
        ]
    )
    only_workbook_pct = (
    (len(results["only_in_workbook"]) / total_workbook * 100)
    if total_workbook > 0
    else 0
    )
    writer.writerow(["Only in Workbook", len(results["only_in_workbook"]), f"{only_workbook_pct:.2f}%"])
    writer.writerow([])
    # Real Mismatches Details - One row per employee
    writer.writerow(["REAL DATA MISMATCHES DETAILS"])

    # Collect all unique fields that have mismatches
    all_mismatch_fields = set()
    for mismatch in results["real_mismatches"]:
        for diff in mismatch["differences"]:
            all_mismatch_fields.add(diff["field"])

    # Sort fields for consistent column order
    sorted_fields = sorted(all_mismatch_fields)

    # Build header row
    header = ["Remark2"]
    for field in sorted_fields:
        header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(header)

    for mismatch in results["real_mismatches"]:
        row = [
            mismatch.get("remark2", ""),
        ]

        # Create a dict of field differences for easy lookup
        diff_dict = {diff["field"]: diff for diff in mismatch["differences"]}

        # Add values for each field in order
        for field in sorted_fields:
            if field in diff_dict:
                row.append(diff_dict[field]["workbook"])
                row.append(diff_dict[field]["maconomy"])
            else:
                row.append("")
                row.append("")

        writer.writerow(row)
    writer.writerow([])

    # Only in Maconomy
    writer.writerow(["Expenses ONLY IN MACONOMY"])
    writer.writerow(["Remark2", "EmployeeNumber", "VendorNumber"])
    for missing in results["only_in_maconomy"]:
        writer.writerow([missing.get("remark2", ""), missing.get("employeenumber", ""), missing.get("vendornumber", "")])
    writer.writerow([])

    # Only in Workbook
    writer.writerow(["Expenses ONLY IN WORKBOOK"])
    writer.writerow(["Remark2", "VendorNumber"])
    for wb_only in results["only_in_workbook"]:
        writer.writerow([wb_only.get("remark2", ""), wb_only.get("vendornumber", "")])
    writer.writerow([])

    # Case-Only Differences Summary
    writer.writerow(["CASE-ONLY DIFFERENCES SUMMARY"])

    # Collect all unique fields that have case differences
    all_case_fields = set()
    for case_diff in results["case_only_differences"]:
        for diff in case_diff["differences"]:
            all_case_fields.add(diff["field"])

    # Sort fields for consistent column order
    sorted_case_fields = sorted(all_case_fields)

    # Build header row
    case_header = ["Remark2"]
    for field in sorted_case_fields:
        case_header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(case_header)

    for case_diff in results["case_only_differences"]:
        row = [case_diff.get("remark2", "")]
        diff_dict = {diff["field"]: diff for diff in case_diff["differences"]}
        for field in sorted_case_fields:
            if field in diff_dict:
                row.append(diff_dict[field]["workbook"])
                row.append(diff_dict[field]["maconomy"])
            else:
                row.append("")
                row.append("")
        writer.writerow(row)

    writer.writerow([])

    # Field-Level Mismatch Analysis
    writer.writerow(["FIELD-LEVEL MISMATCH ANALYSIS"])
    writer.writerow(["Field Name", "Number of Mismatches"])
    field_counts = {}
    for mismatch in results["real_mismatches"]:
        for diff in mismatch["differences"]:
            field_counts[diff["field"]] = field_counts.get(diff["field"], 0) + 1

    for field, count in sorted(field_counts.items(), key=lambda x: x[1], reverse=True):
        writer.writerow([field, count])
    data = csv_buffer.getvalue()
    return rail.write_artifact(data)


def comparison_details():

    maconomy_data = rail.result("maconomy_combined_data")
    workbook_data = rail.result("workbook_data_python")
    purchases_vat_mapper = rail.result("get_purchases_vat_mapper")
    
    results = {
        "perfect_matches": [],
        "case_only_differences": [],
        "real_mismatches": [],
        "only_in_maconomy": [],
        "only_in_workbook": [],
    }

    compare_fields = [
        "remark2",
        "entrydate",
        "jobnumber",
        "taskname",
        "currency",
        "financevatcode",
        "amountcurrency",
        "linenumber",
        "vendornumber",
    ]

    for remark2, mac_details in maconomy_data.items():
        if remark2 not in workbook_data:
            results["only_in_maconomy"].append(
                {
                    "remark2": remark2,
                    "employeenumber": mac_details.get("employeenumber", ""),
                    "vendornumber": mac_details.get("vendornumber", ""),
                }
            )
            continue

        wb_details = workbook_data[remark2]

        if "financevatcode" in wb_details:
            wb_details["financevatcode"] = purchases_vat_mapper.get(
                str(wb_details["financevatcode"]), ""
            )

        mismatched_fields = []
        case_only_fields = []

        for field in compare_fields:
            mac_val = mac_details.get(field)
            wb_val = wb_details.get(field)

            if field == "amountcurrency":
                mac_val_str = mac_val
                wb_val_str = wb_val
            else:
                mac_val_str = str(mac_val) if mac_val is not None else ""
                wb_val_str = str(wb_val) if wb_val is not None else ""

            if mac_val_str != wb_val_str:
                if field != "amountcurrency" and str(mac_val_str).lower() == str(wb_val_str).lower():
                    case_only_fields.append(
                        {
                            "field": field,
                            "workbook": wb_val_str,
                            "maconomy": mac_val_str,
                        }
                    )
                else:
                    mismatched_fields.append(
                        {
                            "field": field,
                            "workbook": wb_val_str,
                            "maconomy": mac_val_str,
                        }
                    )

        # Categorize the expenses
        if len(mismatched_fields) == 0 and len(case_only_fields) == 0:
            results["perfect_matches"].append({"remark2": remark2})
        elif len(mismatched_fields) == 0 and len(case_only_fields) > 0:
            results["case_only_differences"].append(
                {
                    "remark2": remark2,
                    "employeenumber": mac_details.get("employeenumber", ""),
                    "vendornumber": mac_details.get("vendornumber", ""),
                    "differences": case_only_fields,
                }
            )
        else:
            results["real_mismatches"].append(
                {
                    "remark2": remark2,
                    "employeenumber": mac_details.get("employeenumber", ""),
                    "vendornumber": mac_details.get("vendornumber", ""),
                    "differences": mismatched_fields,
                }
            )

    # Find records only in Workbook
    for remark2, wb_details in workbook_data.items():
        if remark2 not in maconomy_data:
            results["only_in_workbook"].append(
                {
                    "remark2": remark2,
                    "vendornumber": wb_details.get("vendornumber", ""),
                }
            )

    return results

def normalize_date(date_str):
    if not date_str or date_str in ["", "System.DateTime"]:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str.split("T")[0], fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return date_str

def process_workbook_data():
    workbook_response = rail.result("workbook_data_api")
    result = {}
    for i in workbook_response[0]:
        entry_date = normalize_date(i["EntryDate"])
        if not is_within_last_six_months(entry_date):
            continue
        result[i["Remark2"]] = {
            "remark2": i["Remark2"],
            "vendornumber": i["VendorNumber"],
            "entrydate": entry_date,
            "jobnumber": i["JobNumber"],
            "taskname": i["TaskName"],
            "currency": i["Currency"],
            "financevatcode": i["FinanceVATCode"],
            "amountcurrency": i["AmountCurrency"],
            "linenumber": i["LineNumber"],
        }
 
    return result

 
def process_maconomy_expenses_data():
    maconomy_response = rail.result("maconomy_data")
    result = {}
    for i in maconomy_response["panes"]["filter"]["records"]:
        data = i["data"]
        entry_date = normalize_date(data["entrydate"])
        if not is_within_last_six_months(entry_date):
            continue
        result[data["remark2"]] = {
            "remark2": data["remark2"],
            "employeenumber": data["employeenumber"],
            "entrydate": entry_date,
            "jobnumber": data["jobnumber"],
            "taskname": data["taskname"],
            "currency": data["currency"].upper() if data["currency"] else "",
            "financevatcode": data["financevatcode"],
            "linenumber": data["linenumber"],
            "amountcurrency": (
                data["amountcurrency"] / 100
                if data["amountcurrency"]
                else 0
            ),
        }

    return result


def process_maconomy_employee_data():
    maconomy_response = rail.result("maconomy_emp_data")
    result = {}
    for i in maconomy_response["panes"]["filter"]["records"]:
        data = i["data"]
        result[data["employeenumber"]] = {
            "vendornumber": data["vendornumber"],
        }
    return result


def combine_maconomy_data():
    expenses_data = rail.result("maconomy_expenses_data")
    employee_data = rail.result("maconomy_employee_data")

    combined_data = {}

    for key, details in expenses_data.items():
        new_details = details.copy()

        employeenumber = details.get("employeenumber")
        if employeenumber and employeenumber in employee_data:
            new_details["vendornumber"] = employee_data[employeenumber].get("vendornumber", "")
        else:
            new_details["vendornumber"] = ""

        combined_data[key] = new_details

    return combined_data

def is_within_last_six_months(date_str):
    if not date_str:
        return False
    try:
        entry_date = datetime.strptime(date_str, "%Y-%m-%d")
 
        today = datetime.today()
        six_months_ago = today - timedelta(days=180) 
        return entry_date >= six_months_ago
    except Exception:
        return False


def process_purchases_vat_data():
    workato_response = json.loads(rail.result("workato_purchases_vat_api"))
    purchases_vat_mapper = {}
    for i in workato_response:
        purchases_vat_mapper[i["data"]["WorkBookCreditorTaxID"]] = i["data"]["MaconomyGLTaxCode"]
    return purchases_vat_mapper

