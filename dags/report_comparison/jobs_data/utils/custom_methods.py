import csv
import json
from datetime import datetime
import rail


def bool_to_int(value):
    """Convert various boolean-like values to 1 or 0.

    Accepts booleans, numeric values, and common truthy/falsey strings.
    Returns 1 for truthy values, 0 otherwise.
    """
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None or value == "":
        return 0
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("true", "1", "yes", "y", "t"):
            return 1
        return 0
    try:
        return 1 if int(value) != 0 else 0
    except Exception:
        return 0


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
    writer.writerow(["Workbook vs Maconomy jobs Comparison Report"])
    writer.writerow(["Report Generated", report_time])
    writer.writerow([])

    # Overall Statistics
    writer.writerow(["SUMMARY STATISTICS"])
    writer.writerow(["Metric", "Count", "Percentage"])
    writer.writerow(["Total Workbook Jobs", total_workbook, "100.00%"])
    writer.writerow(
        [
            "Total Maconomy Jobs",
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

    # Real Mismatches Details - One row per job
    writer.writerow(["REAL DATA MISMATCHES DETAILS"])

    # Collect all unique fields that have mismatches
    all_mismatch_fields = set()
    for mismatch in results["real_mismatches"]:
        for diff in mismatch["differences"]:
            all_mismatch_fields.add(diff["field"])

    # Sort fields for consistent column order
    sorted_fields = sorted(all_mismatch_fields)

    # Build header row
    header = ["Text2"]
    for field in sorted_fields:
        header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(header)

    # Write one row per job with all mismatches
    for mismatch in results["real_mismatches"]:
        row = [
            mismatch["Text2"]
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
    writer.writerow(["JOBS ONLY IN MACONOMY"])
    writer.writerow(["Text2"])
    for missing in results["only_in_maconomy"]:
        writer.writerow(
            [
                missing["Text2"]
            ]
        )
    writer.writerow([])

    # Only in Workbook
    writer.writerow(["JOBS ONLY IN WORKBOOK"])
    writer.writerow(["Text2"])
    for wb_only in results["only_in_workbook"]:
        writer.writerow(
            [
                wb_only["Text2"]
            ]
        )
    writer.writerow([])

    # Case-Only Differences Summary - One row per job
    writer.writerow(["CASE-ONLY DIFFERENCES SUMMARY"])

    # Collect all unique fields that have case differences
    all_case_fields = set()
    for case_diff in results["case_only_differences"]:
        for diff in case_diff["differences"]:
            all_case_fields.add(diff["field"])

    # Sort fields for consistent column order
    sorted_case_fields = sorted(all_case_fields)

    # Build header row
    case_header = ["Text2","Jobname"]
    for field in sorted_case_fields:
        case_header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(case_header)

    # Write one row per job with all case differences
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

    # Write the CSV with a UTF-8 BOM (utf-8-sig) so spreadsheet apps (Excel)
    # auto-detect UTF-8 and render characters like '£' correctly instead of
    # mojibake ('Â£'). bytes(data, 'utf-8-sig') requires data to be a str.
    data = csv_buffer.getvalue()
    with rail.new_artifact() as artifact:
        artifact.file.write(bytes(data, 'utf-8-sig'))
        artifact.file.flush()
        return artifact.name


def comparison_details():
    """
    Optimized comparison of Maconomy and Workbook jobs data
    Categorizes differences and generates summary statistics
    """
    maconomy_data = rail.result("maconomy_jobs_data")
    workbook_data = rail.result("workbook_data_python")
    employee_department_mapper = rail.result("get_employee_department_mapper")
    dimensionfeetype_mapper = rail.result("get_dimensionfeetype_mapper")
    business_unit_mapper = rail.result("get_business_mapper")
    Dimension_income_risk_mapper = rail.result("get_dimension_income_risk_mapper")
    vccp_company_mapper = rail.result("get_vccp_company_mapper")
    specification6_mapper = rail.result("get_specification6_data")
    reverse_vccp_mapper = {wb: mac for mac, wb in vccp_company_mapper.items()}
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
            "JobGroup",
            "CustomerNumber",
            "LocationName",
            "ProjectName",
            "StartingDate",
            "ExpectedEndingDate",
            "SalesPersonNumber",
            "JobName",
            "Text2",
            "Text5",
            "Text6",
            "Text7",
            "Text8",
            "Text9",
            "JobPriceList",
            "Specification1Name",
            "CompanyNumber",
            "ProjectManagerNumber",
            "Specification5Name",
            "Specification6Name",
            "Specification10Name",
            "Currency",
            "BlockedForAmountRegistrations",
            "Popup3",
    ]

    # Compare Maconomy records
    processed = 0
    for job_id, mac_details in maconomy_data.items():
        processed += 1
        if job_id not in workbook_data:
            results["only_in_maconomy"].append(
                {
                    "Text2": job_id,
                    "JobName": mac_details.get("JobName", "")
                
                }
            )
            continue
        default_access = "employee_without_access"
        wb_details = workbook_data[job_id]
        if wb_details["LocationName"] in employee_department_mapper :
            wb_details["LocationName"] = employee_department_mapper[
                wb_details["LocationName"]
            ]
        if mac_details["ProjectName"] in dimensionfeetype_mapper:
            mac_details["ProjectName"] = dimensionfeetype_mapper[
                mac_details["ProjectName"]
            ]
        # Transform Workbook Specification1Name using mapper
        original_spec1_wb = wb_details["Specification1Name"]
        if original_spec1_wb:
            # Ensure consistent string comparison
            original_spec1_wb_str = str(original_spec1_wb).strip()
            lower_business_unit_mapper = {k.lower(): v for k, v in business_unit_mapper.items()}
            if original_spec1_wb_str.lower() in lower_business_unit_mapper:
                wb_details["Specification1Name"] = lower_business_unit_mapper[original_spec1_wb_str.lower()]
            #     print(f"Mapped Workbook Spec1: '{original_spec1_wb_str}' → '{wb_details['Specification1Name']}'")
            # else:
            #     print(f"WARNING: Workbook Specification1Name '{original_spec1_wb_str}' not found in business_unit_mapper")

        if mac_details["Specification5Name"] in Dimension_income_risk_mapper:
            mac_details["Specification5Name"] = Dimension_income_risk_mapper[
                mac_details["Specification5Name"]
            ]

        # Look up raw WorkBook CompanyNumber in the mapper's WorkBook field,
        # pluck the equivalent Maconomy company number, then normalise both
        # sides to that value so the comparison treats them as matched.
        # If not found in the mapper, fall back to the raw values from each system.
        wb_company_raw = str(wb_details.get("CompanyNumber", "")).strip()
        mac_company_raw = str(mac_details.get("CompanyNumber", "")).strip()
        mac_from_wb_lookup = reverse_vccp_mapper.get(wb_company_raw)
        if mac_from_wb_lookup:
            wb_details["CompanyNumber"] = mac_from_wb_lookup
            mac_details["CompanyNumber"] = mac_company_raw
        else:
            wb_details["CompanyNumber"] = wb_company_raw
            mac_details["CompanyNumber"] = mac_company_raw
    
        # Replace Maconomy Specification6Name with description from Specification6 API
        spec6_mac = mac_details.get("Specification6Name", "")
        if spec6_mac and spec6_mac in specification6_mapper:
            mac_details["Specification6Name"] = specification6_mapper[spec6_mac]

        # Compare all fields
        mismatched_fields = []
        case_only_fields = []

        for field in compare_fields:
            mac_val = mac_details.get(field)
            wb_val = wb_details.get(field)

            # Convert None to empty string; keep originals for the report
            mac_val_str = str(mac_val).strip() if mac_val is not None else ""
            wb_val_str = str(wb_val).strip() if wb_val is not None else ""

            # Normalize for comparison only (case- and space-insensitive for text-like fields)
            mac_val_cmp = mac_val_str
            wb_val_cmp = wb_val_str
            if field == "JobGroup" or field == "JobName" or field == "JobPriceList" or field.startswith("Specification") or field == "LocationName" or field == "ProjectName" or field == "Currency" or field == "Popup3" or field == "ProjectManagerNumber" or field.startswith("Text"):
                mac_val_cmp = mac_val_str.upper().replace(" ", "_")
                wb_val_cmp = wb_val_str.upper().replace(" ", "_")

            if mac_val_cmp != wb_val_cmp:
                # Check if it's only a case difference
                if mac_val_cmp.upper() == wb_val_cmp.upper():
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

        # Rename Specification6Name to Specification6Description in report records
        for diff in mismatched_fields:
            if diff["field"] == "Specification6Name":
                diff["field"] = "Specification6Description"
                break
        for diff in case_only_fields:
            if diff["field"] == "Specification6Name":
                diff["field"] = "Specification6Description"
                break

        # Categorize the job
        if len(mismatched_fields) == 0 and len(case_only_fields) == 0:
            results["perfect_matches"].append(
                {
                    "Text2": job_id,
                }
            )
        elif len(mismatched_fields) == 0 and len(case_only_fields) > 0:
            results["case_only_differences"].append(
                {
                    "Text2": job_id,
                    "differences": case_only_fields,
                }
            )
        else:
            results["real_mismatches"].append(
                {
                    "Text2": job_id,
                    "differences": mismatched_fields,
                }
            )

    # Find jobs only in Workbook
    for job_id, wb_details in workbook_data.items():
        if job_id not in maconomy_data:
            results["only_in_workbook"].append(
                {
                    "Text2": job_id,
                }
            )

    return results


def process_maconomy_jobs_data():
    """Process Maconomy jobs data"""
    maconomy_response = rail.result("maconomy_data")

    result = dict(
        map(
            lambda i: (
                i["data"]["text2"],
                {
                    "JobGroup": i["data"]["jobgroup"],
                    "CustomerNumber": i["data"]["customernumber"],
                    "LocationName": i["data"]["locationname"],
                    "ProjectName": i["data"]["projectname"],
                    "StartingDate": (
                        datetime.strftime(
                            datetime.strptime(i["data"]["startingdate"], "%Y-%m-%d"),
                            "%d/%m/%Y",
                        )
                        if i["data"]["startingdate"]
                        else ""
                    ),
                    "ExpectedEndingDate": (
                        datetime.strftime(
                            datetime.strptime(
                                i["data"]["expectedendingdate"], "%Y-%m-%d"
                            ),
                            "%d/%m/%Y",
                        )
                        if i["data"]["expectedendingdate"]
                        else ""
                    ),
                    "SalesPersonNumber": i["data"]["salespersonnumber"],
                    "JobName": i["data"]["jobname"],
                    "Text2": i["data"]["text2"],
                    "Text5": i["data"]["text5"],
                    "Text6": i["data"]["text6"],
                    "Text7": i["data"]["text7"],
                    "Text8": i["data"]["text8"],
                    "Text9": i["data"]["text9"],
                    "JobPriceList": i["data"]["jobpricelist"],
                    "Specification1Name": str(i["data"].get("specification1name", "")).strip() if i["data"].get("specification1name") else "",
                    "CompanyNumber": str(i["data"].get("companynumber", "")).strip() if i["data"].get("companynumber") else "",
                    "ProjectManagerNumber": i["data"]["projectmanagernumber"],
                    "Specification5Name": i["data"]["specification5name"],
                    "Specification6Name": i["data"]["specification6name"],
                    "Specification10Name": i["data"]["specification10name"],
                    "Currency": i["data"]["currency"],
                    "BlockedForAmountRegistrations": bool_to_int(i["data"].get("blockedforamountregistrations")),
                    "Popup3": i["data"]["popup3"],
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
                i.get("Text2", ""),
                {
                    "JobGroup": i.get("JobGroup", ""),
                    "CustomerNumber": i.get("CustomerNumber", ""),
                    "LocationName": i.get("LocationName", ""),
                    "ProjectName": i.get("ProjectName", ""),
                    "StartingDate": (
                        datetime.strftime(
                            datetime.strptime(
                                i.get("StartingDate", "").split("T")[0], "%Y-%m-%d"
                            ),
                            "%d/%m/%Y",
                        )
                        if i.get("StartingDate") and i.get("StartingDate") != "System.DateTime"
                        else ""
                    ),
                    "ExpectedEndingDate": (
                        datetime.strftime(
                            datetime.strptime(
                                i.get("ExpectedEndingDate", "").split("T")[0], "%Y-%m-%d"
                            ),
                            "%d/%m/%Y",
                        )
                        if i.get("ExpectedEndingDate") and i.get("ExpectedEndingDate") != "System.DateTime"
                        else ""
                    ),
                    "SalesPersonNumber": i.get("SalesPersonNumber", ""),
                    "JobName": i.get("JobName", ""),
                    "Text2": i.get("Text2", ""),
                    "Text5": i.get("Text5", ""),
                    "Text6": i.get("Text6", ""),
                    "Text7": i.get("Text7", ""),
                    "Text8": i.get("Text8", ""),
                    "Text9": i.get("Text9", ""),
                    "JobPriceList": i.get("JobPriceList", ""),
                    "Specification1Name": str(i.get("Specification1Name", "")).strip() if i.get("Specification1Name") else "",
                    "CompanyNumber": str(i.get("Company Number", "")).strip() if i.get("Company Number") else "",
                    "ProjectManagerNumber": i.get("ProjectManagerNumber", ""),
                    "Specification5Name": i.get("Specification5Name", ""),
                    "Specification6Name": i.get("Specification6Name", ""),
                    "Specification10Name": i.get("Specification10Name", ""),
                    "Currency": i.get("Currency", ""),
                    "BlockedForAmountRegistrations": bool_to_int(i.get("BlockedForAmountRegistrations", "")),
                    "Popup3": i.get("Popup3", "")
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
        employee_department_mapper[i["data"]["WorkBookDepartmentName"]] = i["data"]["Maconomy"]
    return employee_department_mapper


def process_dimensionfeetype_data():
    workato_response = json.loads(rail.result("workato_dimensionfeetype_api"))
    dimensionfeetype_mapper = {}
    for i in workato_response:
        dimensionfeetype_mapper[i["data"]["MaconomyCodeNumber"]] = i["data"]["WorkBookUserInterfaceName"]
    return dimensionfeetype_mapper


def process_business_unit_data():
    workato_response = json.loads(rail.result("workato_dimension_business_unit_api"))
    business_unit_mapper = {}
    for i in workato_response:
        # Convert both to string to ensure consistent lookup
        wb_id = str(i["data"]["WorkBookID"]).strip()
        mac_code = str(i["data"]["MaconomyCodeNumber"]).strip()
        business_unit_mapper[wb_id] = mac_code
    print(f"Business Unit Mapper created with {len(business_unit_mapper)} entries")
    print(f"Sample entries: {dict(list(business_unit_mapper.items())[:5])}")
    return business_unit_mapper


def process_income_risk_data():
    workato_response = json.loads(rail.result(task_id="workato_income_risk_api"))
    Dimension_income_risk_mapper = {}
    for i in workato_response:
        Dimension_income_risk_mapper[i["data"]["MaconomyCodeNumber"]] = i["data"]["WorkBookUserInterfaceName"]
    return Dimension_income_risk_mapper

def process_vccp_company_data():
    workato_response = json.loads(rail.result(task_id="workato_vccp_company_api"))
    vccp_company_mapper = {}
    for i in workato_response:
        # Convert both to string to ensure consistent lookup
        mac_val = str(i["data"]["Maconomy"]).strip()
        wb_val = str(i["data"]["WorkBook"]).strip()
        vccp_company_mapper[mac_val] = wb_val
    # print(f"VCCP Company Mapper created with {len(vccp_company_mapper)} entries")
    # print(f"Sample entries: {dict(list(vccp_company_mapper.items())[:5])}")
    return vccp_company_mapper


def process_specification6_data():
    """Build a lookup of specification6name -> description from Maconomy Specification6 API"""
    spec6_response = rail.result("maconomy_specification6_api")
    result = {}
    for record in spec6_response["panes"]["filter"]["records"]:
        spec6name = record["data"].get("specification6name", "")
        description = record["data"].get("description", "")
        if spec6name:
            result[spec6name] = description
    return result
