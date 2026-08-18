import csv
import json
from datetime import datetime
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

    import io

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # Summary Statistics Section
    writer.writerow(["Workbook vs Maconomy Employee Comparison Report"])
    writer.writerow(["Report Generated", report_time])
    writer.writerow([])

    # Overall Statistics
    writer.writerow(["SUMMARY STATISTICS"])
    writer.writerow(["Metric", "Count", "Percentage"])
    writer.writerow(["Total Workbook Employees", total_workbook, "100.00%"])
    writer.writerow(
        [
            "Total Maconomy Employees",
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
    # writer.writerow(
    #     [
    #         "Case-Only Differences",
    #         len(results["case_only_differences"]),
    #         f"{case_pct:.2f}%",
    #     ]
    # )
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
    writer.writerow(["Only in Workbook", len(results["only_in_workbook"]), "N/A"])
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
    header = ["Employee Number", "Name", "Email"]
    for field in sorted_fields:
        header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(header)

    # Write one row per employee with all mismatches
    for mismatch in results["real_mismatches"]:
        row = [
            mismatch["employee_number"],
            mismatch["name"],
            mismatch.get("email", "N/A"),
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
    writer.writerow(["EMPLOYEES ONLY IN MACONOMY"])
    writer.writerow(["Employee Number", "Name", "Company", "Email"])
    for missing in results["only_in_maconomy"]:
        writer.writerow(
            [
                missing["employee_number"],
                missing["name"],
                missing.get("company", "N/A"),
                missing.get("email", "N/A"),
            ]
        )
    writer.writerow([])

    # Only in Workbook
    writer.writerow(["EMPLOYEES ONLY IN WORKBOOK"])
    writer.writerow(["Employee Number", "Name", "Company"])
    for wb_only in results["only_in_workbook"]:
        writer.writerow(
            [
                wb_only["employee_number"],
                wb_only["name"],
                wb_only.get("company", "N/A"),
            ]
        )
    writer.writerow([])

    # Case-Only Differences Summary - One row per employee
    writer.writerow(["CASE-ONLY DIFFERENCES SUMMARY"])

    # Collect all unique fields that have case differences
    all_case_fields = set()
    for case_diff in results["case_only_differences"]:
        for diff in case_diff["differences"]:
            all_case_fields.add(diff["field"])

    # Sort fields for consistent column order
    sorted_case_fields = sorted(all_case_fields)

    # Build header row
    case_header = ["Employee Number", "Name"]
    for field in sorted_case_fields:
        case_header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(case_header)

    # Write one row per employee with all case differences
    # for case_diff in results["case_only_differences"]:
    #     row = [case_diff["employee_number"], case_diff["name"]]

    #     # Create a dict of field differences for easy lookup
    #     diff_dict = {diff["field"]: diff for diff in case_diff["differences"]}

    #     # Add values for each field in order
    #     for field in sorted_case_fields:
    #         if field in diff_dict:
    #             row.append(diff_dict[field]["workbook"])
    #             row.append(diff_dict[field]["maconomy"])
    #         else:
    #             row.append("")
    #             row.append("")

        # writer.writerow(row)
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
    """
    Optimized comparison of Maconomy and Workbook employee data
    Categorizes differences and generates summary statistics
    """
    maconomy_data = rail.result("maconomy_employee_data")
    workbook_data = rail.result("workbook_data_python")
    employee_department_mapper = rail.result("get_employee_department_mapper")
    application_access_role_mapper = rail.result("get_applicationaccessrole_mapper")
    business_unit_mapper = rail.result("get_business_mapper")
    position_mapper = rail.result("get_position_mapper")
    # Initialize categorized results
    results = {
        "perfect_matches": [],
        "case_only_differences": [],
        "real_mismatches": [],
        "only_in_maconomy": [],
        "only_in_workbook": [],
    }

    # Fields to compare
    compare_fields = [
        "blocked",
        "companynumber",
        "costprice",
        "dateemployed",
        "dateendemployment",
        "electronicmailaddress",
        "employeepopup3",
        "employeepopup4",
        "entityname",
        "jobpricegroupnumber",
        "mustusetimesheets",
        "name1",
        "primaryemployeecategorynumber",
        "remark2",
        "remark5",
        "specification1name",
        "statistic1",
        "statistic2",
        "statistic3",
        "substitute1",
        "substitute3",
        "superioremployee",
        "vendornumber",
    ]

    # Compare Maconomy records
    processed = 0
    for emp_num, mac_details in maconomy_data.items():
        processed += 1
        if emp_num not in workbook_data:
            results["only_in_maconomy"].append(
                {
                    "employee_number": emp_num,
                    "name": mac_details.get("name1", "N/A"),
                    "company": mac_details.get("companynumber", "N/A"),
                    "email": mac_details.get("electronicmailaddress", "N/A"),
                }
            )
            continue
        default_access = "employee_without_access"
        wb_details = workbook_data[emp_num]
        if wb_details["jobpricegroupnumber"] in position_mapper:
            wb_details["jobpricegroupnumber"] = position_mapper[
                wb_details["jobpricegroupnumber"]
            ]
        if wb_details["entityname"] in employee_department_mapper:
            wb_details["entityname"] = employee_department_mapper[
                wb_details["entityname"]
            ]
        if wb_details["employeepopup3"] in application_access_role_mapper:
            wb_details["employeepopup3"] = application_access_role_mapper[
                wb_details["employeepopup3"]
            ]
        if mac_details["specification1name"] in business_unit_mapper:
            mac_details["specification1name"] = business_unit_mapper[
                mac_details["specification1name"]
            ]
        # Compare all fields
        mismatched_fields = []
        case_only_fields = []

        for field in compare_fields:
            mac_val = mac_details.get(field)
            wb_val = wb_details.get(field)

            # Convert None to empty string for comparison
            mac_val_str = str(mac_val) if mac_val is not None else ""
            wb_val_str = str(wb_val) if wb_val is not None else ""

            if mac_val_str != wb_val_str:
                # Check if it's only a case difference
                if mac_val_str.lower() == wb_val_str.lower():
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

        # Categorize the employee
        if len(mismatched_fields) == 0 and len(case_only_fields) == 0:
            results["perfect_matches"].append(
                {
                    "employee_number": emp_num,
                    "name": mac_details.get("name1", "N/A"),
                }
            )
        elif len(mismatched_fields) == 0 and len(case_only_fields) > 0:
            results["case_only_differences"].append(
                {
                    "employee_number": emp_num,
                    "name": mac_details.get("name1", "N/A"),
                    "differences": case_only_fields,
                }
            )
        else:
            results["real_mismatches"].append(
                {
                    "employee_number": emp_num,
                    "name": mac_details.get("name1", "N/A"),
                    "email": mac_details.get("electronicmailaddress", "N/A"),
                    "differences": mismatched_fields,
                }
            )

    # Find employees only in Workbook
    for emp_num, wb_details in workbook_data.items():
        if emp_num not in maconomy_data:
            results["only_in_workbook"].append(
                {
                    "employee_number": emp_num,
                    "name": wb_details.get("name1", "N/A"),
                    "company": wb_details.get("companynumber", "N/A"),
                }
            )

    return results


def process_maconomy_employee_data():
    """Process Maconomy employee data"""
    maconomy_response = rail.result("maconomy_data")

    result = dict(
        map(
            lambda i: (
                i["data"]["employeenumber"],
                {
                    "blocked": i["data"]["blocked"],
                    "changeddate": i["data"]["changeddate"],
                    "companynumber": i["data"]["companynumber"],
                    "costprice": i["data"]["costprice"],
                    "dateemployed": (
                        datetime.strftime(
                            datetime.strptime(i["data"]["dateemployed"], "%Y-%m-%d"),
                            "%d/%m/%Y",
                        )
                        if i["data"]["dateemployed"]
                        else ""
                    ),
                    "dateendemployment": (
                        datetime.strftime(
                            datetime.strptime(
                                i["data"]["dateendemployment"], "%Y-%m-%d"
                            ),
                            "%d/%m/%Y",
                        )
                        if i["data"]["dateendemployment"]
                        else ""
                    ),
                    "electronicmailaddress": i["data"]["electronicmailaddress"],
                    "employeepopup3": i["data"]["employeepopup3"],
                    "employeepopup4": i["data"]["employeepopup4"],
                    "entityname": i["data"]["entityname"],
                    "jobpricegroupnumber": i["data"]["jobpricegroupnumber"],
                    "mustusetimesheets": i["data"]["mustusetimesheets"],
                    "name1": i["data"]["name1"],
                    "primaryemployeecategorynumber": i["data"][
                        "primaryemployeecategorynumber"
                    ],
                    "remark2": i["data"]["remark2"],
                    "remark5": i["data"]["remark5"],
                    "specification1name": i["data"]["specification1name"],
                    "statistic1": i["data"]["statistic1"],
                    "statistic2": i["data"]["statistic3"],
                    "statistic3": i["data"]["statistic2"],
                    "substitute1": i["data"]["substitute1"],
                    "substitute3": i["data"]["substitute3"],
                    "superioremployee": i["data"]["superioremployee"],
                    "vendornumber": i["data"]["vendornumber"],
                },
            ),
            maconomy_response["panes"]["filter"]["records"],
        )
    )
    return result


def process_workbook_data():
    """Process workbook API response data"""

    workbook_response = rail.result("workbook_data_api")
    result = dict(
        map(
            lambda i: (
                i["employeenumber"],
                {
                    "blocked": i["blocked"],
                    "changeddate": i["changeddate"],
                    "companynumber": i["companynumber"],
                    "costprice": i["costprice"],
                    "dateemployed": (
                        datetime.strftime(
                            datetime.strptime(
                                i["dateemployed"].split("T")[0], "%Y-%m-%d"
                            ),
                            "%d/%m/%Y",
                        )
                        if i["dateemployed"] and i["dateemployed"] != "System.DateTime"
                        else ""
                    ),
                    "dateendemployment": (
                        datetime.strftime(
                            datetime.strptime(
                                i["dateendemployment"].split("T")[0], "%Y-%m-%d"
                            ),
                            "%d/%m/%Y",
                        )
                        if i["dateendemployment"]
                        and i["dateendemployment"] != "System.DateTime"
                        else ""
                    ),
                    "electronicmailaddress": i["electronicmailaddress"],
                    "employeepopup3": i["employeepopup3"],
                    "employeepopup4": i["employeepopup4"],
                    "entityname": i["entityname"],
                    "jobpricegroupnumber": i["jobpricegroupnumber"],
                    "mustusetimesheets": i["mustusetimesheets"],
                    "name1": i["name1"],
                    "primaryemployeecategorynumber": i["primaryemployeecategorynumber"],
                    "remark2": i["remark2"],
                    "remark5": i["remark5"],
                    "specification1name": i["specification1name"],
                    "statistic1": i["statistic1"],
                    "statistic2": i["statistic3"],
                    "statistic3": i["statistic2"],
                    "substitute1": i["substitute1"],
                    "substitute3": i["substitute3"],
                    "superioremployee": i["superioremployee"],
                    "vendornumber": i["vendornumber"],
                },
            ),
            workbook_response[0],
        )
    )
    return result


def process_employee_department_data():
    workato_response = json.loads(rail.result("workato_employee_department_api"))
    employee_department_mapper = {}
    for i in workato_response:
        employee_department_mapper[i["data"]["WorkBook"]] = i["data"]["Maconomy"]
    return employee_department_mapper


def process_applicationaccessrole_data():
    workato_response = json.loads(rail.result("workato_applicationaccessrole_api"))
    employee_department_mapper = {}
    for i in workato_response:
        employee_department_mapper[i["data"]["WorkBookId"]] = i["data"][
            "MaconomyRestDescription"
        ]
    return employee_department_mapper


def process_business_unit_data():
    workato_response = json.loads(rail.result("workato_business_unit"))
    employee_department_mapper = {}
    for i in workato_response:
        employee_department_mapper[i["data"]["MaconomyCodeNumber"]] = i["data"][
            "WorkBookUserInterfaceName"
        ]
    return employee_department_mapper


def process_position_data():
    workato_response = json.loads(rail.result(task_id="workato_position"))
    employee_department_mapper = {}
    for i in workato_response:
        employee_department_mapper[i["data"]["WorkBookId"]] = i["data"]["MaconomyID"]
    return employee_department_mapper
