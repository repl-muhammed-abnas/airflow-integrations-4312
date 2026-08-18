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
    only_workbook_pct = (
    (len(results["only_in_workbook"]) / total_workbook * 100)
    if total_workbook > 0
    else 0
    )
    report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    import io

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # Summary Statistics Section
    writer.writerow(["Workbook vs Maconomy Prospect Comparison Report"])
    writer.writerow(["Report Generated", report_time])
    writer.writerow([])

    # Overall Statistics
    writer.writerow(["SUMMARY STATISTICS"])
    writer.writerow(["Metric", "Count", "Percentage"])
    writer.writerow(["Total Workbook Prospects", total_workbook, "100.00%"])
    writer.writerow(
        [
            "Total Maconomy Prospects",
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

    writer.writerow(["Only in Workbook", len(results["only_in_workbook"]), f"{only_workbook_pct:.2f}%",])
    writer.writerow([])

    # Real Mismatches Details - One row per prospects
    writer.writerow(["REAL DATA MISMATCHES DETAILS"])

    # Collect all unique fields that have mismatches
    all_mismatch_fields = set()
    for mismatch in results["real_mismatches"]:
        for diff in mismatch["differences"]:
            all_mismatch_fields.add(diff["field"])

    # Sort fields for consistent column order
    sorted_fields = sorted(all_mismatch_fields)

    # Header for Mismatches
    header = ["name1"]
    for field in sorted_fields:
        header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(header)

    # Rows for Mismatches
    for mismatch in results["real_mismatches"]:
        row = [
            mismatch["name"]
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
    writer.writerow(["PROSPECTS ONLY IN MACONOMY"])
    writer.writerow(["name1"]) 
    for missing in results["only_in_maconomy"]:
        writer.writerow(
            [
                missing["name"],
            ]
        )
    writer.writerow([])

    # Only in Workbook
    writer.writerow(["PROSPECTS ONLY IN WORKBOOK"])
    writer.writerow(["name1"])
    for wb_only in results["only_in_workbook"]:
        writer.writerow(
            [
                wb_only["name"]
            ]
        )
    writer.writerow([])

    # --- Field-Level Mismatch Analysis ---
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

def normalize(value):
    if value is None:
        return ""
    value = str(value)
    value = value.strip()
    value = value.replace("’", "'")   # normalize curly apostrophe
    return value

def comparison_details():
    """
    Optimized comparison of Maconomy and Workbook prospects data
    Categorizes differences and generates summary statistics
    """
    
    workato_mapper=rail.result("get_prospect_database_name_mapper")
    maconomy_data = rail.result("maconomy_prospect_data")
    workbook_data = rail.result("workbook_data_python")
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
        "country",
        "customergroup",
        "customerremark3",
        "customerremark4",
        "electronicmailaddress",
        "employeenumber1",
        "employeenumber2",
        "employeenumber4",
        "name1",
        "name2",
        "name3",
        "postaldistrict",
        "selectedoption2",
        "selectedoption3",
        "selectedoption4",
        "selectedoption5",
        # "selectedoption6",
        "specification2name",
        "telephone",
        "zipcode",
    ]


    # Compare Maconomy records
    processed = 0
    for prospect_name, mac_details in maconomy_data.items():
        processed += 1
        if prospect_name not in workbook_data:
            results["only_in_maconomy"].append(
                {
                    "name": prospect_name,
                }
            )
            continue
            
        wb_details = workbook_data[prospect_name]
        if str(wb_details["customergroup"]) in workato_mapper:
            wb_details["customergroup"] = workato_mapper[
                str(wb_details["customergroup"])
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

        # Categorize the prospect
        if len(mismatched_fields) == 0 and len(case_only_fields) == 0:
            results["perfect_matches"].append(
                {
                    "name": prospect_name,
                }
            )
        elif len(mismatched_fields) == 0 and len(case_only_fields) > 0:
            results["case_only_differences"].append(
                {
                    "name": prospect_name,
                    "differences": case_only_fields,
                }
            )
        else:
            results["real_mismatches"].append(
                {
                    "name": prospect_name,
                    "differences": mismatched_fields,
                }
            )

    # Find prospects only in Workbook
    for prospect_name, wb_details in workbook_data.items():
        if prospect_name not in maconomy_data:
            results["only_in_workbook"].append(
                {
                    "name": prospect_name,
                }
            )

    return results


def process_maconomy_prospect_data():
    """Process Maconomy prospect data"""
    maconomy_response = rail.result("maconomy_data")

    result = dict(
        map(
            lambda i: (
                normalize(i["data"]["name1"]),
                {
                    "name2": normalize(i["data"]["name2"]),
                    "name3": normalize(i["data"]["name3"]),
                    "telephone": normalize(i["data"]["telephone"]),
                    "customergroup": normalize(i["data"]["customergroup"]),
                    "country": normalize(i["data"]["country"]),
                    "zipcode": normalize(i["data"]["zipcode"]),
                    "postaldistrict": normalize(i["data"]["postaldistrict"]),
                    "customerremark3": normalize(i["data"]["customerremark3"]),
                    "specification2name": normalize(i["data"]["specification2name"]),
                    "customerremark4": normalize(i["data"]["customerremark4"]),
                    "selectedoption2": normalize(i["data"]["selectedoption2"]),
                    "selectedoption3": normalize(i["data"]["selectedoption3"]),
                    "selectedoption4": normalize(i["data"]["selectedoption4"]),
                    "selectedoption5": normalize(i["data"]["selectedoption5"]),
                    # "selectedoption6": normalize(i["data"]["selectedoption6"]),
                    "employeenumber1": normalize(i["data"]["employeenumber1"]),
                    "employeenumber2": normalize(i["data"]["employeenumber2"]),
                    "employeenumber4": normalize(i["data"]["employeenumber4"]),
                    "electronicmailaddress": normalize(i["data"]["electronicmailaddress"]),
                }
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
                normalize(i["Name1"]),
                {
                    "name2": normalize(i["Name2"]),
                    "name3": normalize(i["Name3"]),
                    "telephone": normalize(i["Telephone"]),
                    "customergroup": normalize(i["CustomerGroup"]),
                    "country": normalize("_".join(i["Country"].lower().split())),
                    "zipcode": normalize(i["ZipCode"]),
                    "postaldistrict": normalize(i["PostalDistrct"]),
                    "customerremark3": normalize(i["CustomerRemark3"]),
                    "specification2name": normalize(i["Specification2Name"]),
                    "customerremark4": normalize(i["CustomerRemark4"]),
                    "selectedoption2": normalize(i["SelectedOption2"]),
                    "selectedoption3": normalize(i["SelectedOption3"]),
                    "selectedoption4": normalize(i["SelectedOption4"]),
                    "selectedoption5": normalize(i["SelectedOption5"]),
                    # "selectedoption6": normalize(i["SelectedOption6"]),
                    "employeenumber1": normalize(i["EmployeeNumber1"]),
                    "employeenumber2": normalize(i["EmployeeNumber2"]),
                    "employeenumber4": normalize(i["EmployeeNumber4"]),
                    "electronicmailaddress": normalize(i["ElectronicMailAddress"]),
                },
            ),

            filter(
                lambda i: i.get("ActiveStatus") is True,
                workbook_response[0],
            ),
        )
    )

    return result



def process_prospect_data():
    workato_response = json.loads(rail.result("workato_user_interface_name_api"))
    prospect_mapper = {}
    for i in workato_response:
        prospect_mapper[i["data"]["WorkBookID"]] = i["data"]["MaconomyDatabaseName"]
    return prospect_mapper

