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
    writer.writerow(["Workbook vs Maconomy Rate Cards Comparison Report"])
    writer.writerow(["Report Generated", report_time])
    writer.writerow([])

    # Overall Statistics
    writer.writerow(["SUMMARY STATISTICS"])
    writer.writerow(["Metric", "Count", "Percentage"])
    writer.writerow(["Total Workbook Rate Cards", total_workbook, "100.00%"])
    writer.writerow(
        [
            "Total Maconomy Rate Cards",
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

    # Real Mismatches Details - One row per rate card
    writer.writerow(["REAL DATA MISMATCHES DETAILS"])

    # Collect all unique fields that have mismatches
    all_mismatch_fields = set()
    for mismatch in results["real_mismatches"]:
        for diff in mismatch["differences"]:
            all_mismatch_fields.add(diff["field"])

    # Sort fields for consistent column order
    sorted_fields = sorted(all_mismatch_fields)

    # Header for Mismatches
    header = ["jobpricelistname","description"]
    for field in sorted_fields:
        header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(header)

    # Rows for Mismatches
    for mismatch in results["real_mismatches"]:
        row = [
            mismatch["jobpricelistname"],
            mismatch["description"]
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
    writer.writerow(["RATE CARDS ONLY IN MACONOMY"])
    writer.writerow(["jobpricelistname","description"]) 
    
    for missing in results["only_in_maconomy"]:
        writer.writerow(
            [
                missing["jobpricelistname"],
                missing["description"]
            ]
        )
    writer.writerow([])

    # Only in Workbook
    writer.writerow(["RATE CARDS ONLY IN WORKBOOK"])
    writer.writerow(["jobpricelistname","description"])
    for wb_only in results["only_in_workbook"]:
        writer.writerow(
            [
                wb_only["jobpricelistname"],
                wb_only["description"]
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
    Optimized comparison of Maconomy and Workbook rate card data
    Categorizes differences and generates summary statistics
    """
    
    maconomy_data = rail.result("maconomy_rate_card_data")
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
         "currency",
    ]


    # Compare Maconomy records
    processed = 0
    for jobprice_name, mac_details in maconomy_data.items():
        processed += 1
        if jobprice_name not in workbook_data:
            results["only_in_maconomy"].append(
                {
                    "jobpricelistname": mac_details.get("jobpricelistname", "N/A"),
                    "description": mac_details.get("description", "N/A"),
                }
            )
            continue

        # Compare all fields
        mismatched_fields = []
        case_only_fields = []
        wb_details = workbook_data[jobprice_name]
        for field in compare_fields:
            mac_val = mac_details.get(field)
            wb_val = wb_details.get(field)

            # Convert None to empty string for comparison
            mac_val_str = str(mac_val) if mac_val is not None else ""
            wb_val_str = str(wb_val) if wb_val is not None else ""

            mac_val_str = normalize(mac_val)
            wb_val_str = normalize(wb_val)

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

        # Categorize the Rate Cards
        if len(mismatched_fields) == 0 and len(case_only_fields) == 0:
            results["perfect_matches"].append(
                {
                    "jobpricelistname": mac_details.get("jobpricelistname", "N/A"),
                    "description": mac_details.get("description", "N/A"),
                }
            )
        elif len(mismatched_fields) == 0 and len(case_only_fields) > 0:
            results["case_only_differences"].append(
                {
                    "jobpricelistname": mac_details.get("jobpricelistname", "N/A"),
                    "description": mac_details.get("description", "N/A"),
                    "differences": case_only_fields,
                }
            )
        else:
            results["real_mismatches"].append(
                {
                    "jobpricelistname": mac_details.get("jobpricelistname", "N/A"),
                    "description": mac_details.get("description", "N/A"),
                    "differences": mismatched_fields,
                }
            )

    # Find Rate Cards only in Workbook
    for jobprice_name, wb_details in workbook_data.items():
        if jobprice_name not in maconomy_data:
            results["only_in_workbook"].append(
                {
                    "jobpricelistname": wb_details.get("jobpricelistname", "N/A"),
                    "description": wb_details.get("description", "N/A"),
                }
            )

    return results


def process_maconomy_rate_card_data():
    """Process Maconomy rate card data"""
    maconomy_response = rail.result("maconomy_data")

    result = {}

    for record in maconomy_response["panes"]["filter"]["records"]:
        data = record["data"]
        key = (
            normalize(data.get("jobpricelistname"))
            + "_"
            + normalize(data.get("description"))
        )

        issue = data.get("issue", 0)

        if key not in result or issue > result[key]["issue"]:
            result[key] = {
                "jobpricelistname": data.get("jobpricelistname"),
                "description": data.get("description"),
                "currency": data.get("currency"),
                "issue": issue, 
            }
    return result


def process_workbook_data():
    """Process workbook API response data"""

    workbook_response = rail.result("workbook_data_api")
    result = dict(
    map(
        lambda i: (
            normalize(i["JobPriceListName"])+"_"+normalize(i["Description"]),
            {
                "jobpricelistname": i.get("JobPriceListName"),
                "description": i.get("Description"),
                "currency": i.get("Currency")
                },
            ),
            workbook_response[0],
        )
    )

    return result



