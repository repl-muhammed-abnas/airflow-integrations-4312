import csv
import json
from datetime import datetime
import rail

# Fields to compare
COMPARE_FIELDS = [
    "customerremark3",
    "customerremark2",
    "activestatus",
    "paymentterms",
    "jobpricelist",
    "currency",
    "name2",
    "name3",
    "country",
    "zipcode",
    "postaldistrict",
    "telephone",
    "electronicmailaddress",
    "customergroup",
    "customerpopup3",
    "customerpopup4",
    "specification1name",
    "specification10name",
]

def normalize(value):
    if value is None:
        return ""
    value = str(value)
    value = value.strip()
    value = value.replace("’", "'")   # normalize curly apostrophe
    return value
 

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
    writer.writerow(["Workbook vs Maconomy Customer Comparison Report"])
    writer.writerow(["Report Generated", report_time])
    writer.writerow([])

    # Overall Statistics
    writer.writerow(["SUMMARY STATISTICS"])
    writer.writerow(["Metric", "Count", "Percentage"])
    writer.writerow(["Total Workbook Customers", total_workbook, "100.00%"])
    writer.writerow(
        [
            "Total Maconomy Customers",
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
 
    writer.writerow(["Only in Workbook", len(results["only_in_workbook"]), f"{only_workbook_pct:.2f}%",])

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
    header = ["CustomerRemark2"]
    for field in sorted_fields:
        header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    print(header)
    writer.writerow(header)

    # Write one row per employee with all mismatches
    for mismatch in results["real_mismatches"]:
        row = [
            mismatch["customerremark2"]
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
    writer.writerow(["CUSTOMER ONLY IN MACONOMY"])
    writer.writerow(["CustomerRemark2"])
    for mac_only in results["only_in_maconomy"]:
        writer.writerow(
            [
                mac_only["customerremark2"]
            ]
        )
    writer.writerow([])

    # Only in Workbook
    writer.writerow(["CUSTOMER ONLY IN WORKBOOK"])
    writer.writerow(["CustomerRemark2"])
    for wb_only in results["only_in_workbook"]:
        writer.writerow(
            [
                wb_only["customerremark2"]
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
    case_header = ["CustomerRemark2", "Name"]
    for field in sorted_case_fields:
        case_header.extend([f"{field} (Workbook)", f"{field} (Maconomy)"])
    writer.writerow(case_header)

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
    maconomy_data = rail.result("maconomy_customer_data")
    workbook_data = rail.result("workbook_data_python")
    customer_department_mapper = rail.result("get_workato_mac_wb_paymentterms_mapper")
    position_mapper = rail.result("get_workato_mac_wb_industry_mapper")
    business_unit_mapper = rail.result("get_workato_mac_wb_business_unit_mapper")
    
    # Mapper for activestatus: Workbook boolean to Maconomy status
    activestatus_mapper = {
        "true": "active",
        "false": "inactive"
    }
    
    # Initialize categorized results
    results = {
        "perfect_matches": [],
        "case_only_differences": [],
        "real_mismatches": [],
        "only_in_maconomy": [],
        "only_in_workbook": [],
    }

    # Compare Maconomy records
    processed = 0
    for remark, mac_details in maconomy_data.items():
        processed += 1
        if remark not in workbook_data:
            # Collect all compare fields for this customer
            only_mac_record = {"customerremark2": remark}
            for field in COMPARE_FIELDS:
                only_mac_record[field] = mac_details.get(field, "")
            results["only_in_maconomy"].append(only_mac_record)
            continue
        default_access = "employee_without_access"
        wb_details = workbook_data[remark]
        
        # Apply activestatus mapper to Workbook data
        wb_activestatus = wb_details.get("activestatus", "").lower()
        if wb_activestatus in activestatus_mapper:
            wb_details["activestatus"] = activestatus_mapper[wb_activestatus]
        
        if  wb_details["customergroup"] in position_mapper:
            wb_details["customergroup"] = position_mapper[str(wb_details["customergroup"])]

        if wb_details["paymentterms"] in customer_department_mapper:
            wb_details["paymentterms"] = customer_department_mapper[
                wb_details["paymentterms"]
            ]

        if wb_details["specification1name"] in business_unit_mapper:
            wb_details["specification1name"] = business_unit_mapper[
                wb_details["specification1name"]
            ]
            
        # Compare all fields
        mismatched_fields = []
        case_only_fields = []

        for field in COMPARE_FIELDS:
            mac_val = mac_details.get(field)
            wb_val = wb_details.get(field)

            # Normalize values for comparison
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

        # Categorize the customer
        if len(mismatched_fields) == 0 and len(case_only_fields) == 0:
            results["perfect_matches"].append(
                {
                    "customerremark2": remark
                }
            )
        elif len(mismatched_fields) == 0 and len(case_only_fields) > 0:
            results["case_only_differences"].append(
                {
                    "customerremark2": remark,
                    "differences": case_only_fields,
                }
            )
        else:
            results["real_mismatches"].append(
                {
                    "customerremark2": remark,
                    "differences": mismatched_fields,
                }
            )

    # Find customers only in Workbook
    for remark, wb_details in workbook_data.items():
        if remark not in maconomy_data:
            results["only_in_workbook"].append(
                {
                    "customerremark2": remark
                }
            )

    return results


def process_maconomy_customer_data():
    """Process Maconomy customer data"""
    maconomy_response = rail.result("maconomy_data")

    result = dict(
        map(
            lambda i: (
                normalize(i["data"]["customerremark2"]),
                {
                    "customerremark3": normalize(i["data"]["customerremark3"]),
                    "customerremark2": normalize(i["data"]["customerremark2"]),
                    "activestatus": normalize(i["data"]["activestatus"]),
                    "paymentterms": normalize(i["data"]["paymentterms"]),
                    "jobpricelist": normalize(i["data"]["jobpricelist"]),
                    "currency": normalize(i["data"]["currency"]),
                    "name2": normalize(i["data"]["name2"]),
                    "name3": normalize(i["data"]["name3"]),
                    "country": normalize(i["data"].get("country")),
                    "zipcode": normalize(i["data"].get("zipcode")),
                    "postaldistrict": normalize(i["data"].get("postaldistrict")),
                    "telephone": normalize(i["data"].get("telephone")),
                    "electronicmailaddress": normalize(i["data"].get("electronicmailaddress")),
                    "customergroup": normalize(i["data"].get("customergroup")),
                    "customerpopup3": normalize(i["data"].get("customerpopup3")),
                    "customerpopup4": normalize(i["data"].get("customerpopup4")),
                    "specification1name": normalize(i["data"].get("specification1name")),
                    "specification10name": normalize(i["data"].get("specification10name")),
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
                normalize(i["CustomerRemark2"]),
                {
                    "customerremark3": normalize(i["CustomerRemark3"]),
                    "customerremark2": normalize(i["CustomerRemark2"]),
                    "activestatus": normalize(i["ActiveStatus"]),
                    "paymentterms": normalize(i["PaymentTerms"]),
                    "jobpricelist": normalize(i["JobPriceList"]),
                    "currency": normalize(i["Currency"]),
                    "name2": normalize(i["Name2"]),
                    "name3": normalize(i["Name3"]),
                    "country": normalize("_".join(i["Country"].lower().split())),
                    "zipcode": normalize(i["ZipCode"]),
                    "postaldistrict": normalize(i["PostalDistrict"]),
                    "telephone": normalize(i["Telephone"]),
                    "electronicmailaddress": normalize(i["ElectronicEmailAddress"]),
                    "customergroup": normalize(i["CustomerGroup"]),
                    "customerpopup3": normalize(i["CustomerPopup3"]),
                    "customerpopup4": normalize(i["CustomerPopup4"]),
                    "specification1name": normalize(i["Specification1Name"]),
                    "specification10name": normalize(i["Specification10Name"]),
                },
            ),
            workbook_response[0],
        )
    )
    return result

def process_mac_wb_paymentterms_data():
    workato_response = json.loads(rail.result("workato_mac_wb_paymentterms_api"))
    customer_department_mapper = {}
    for i in workato_response:
        customer_department_mapper[i["data"]["WorkBookId"]] = i["data"]["MaconomyRest"]
    return customer_department_mapper

def process_mac_wb_business_unit_data():
    workato_response = json.loads(rail.result("workato_mac_wb_business_unit_api"))
    businss_unit_mapper = {}
    for i in workato_response:
        businss_unit_mapper[i["data"]["WorkBookUserInterfaceName"]] = i["data"]["MaconomyCodeNumber"]
    return businss_unit_mapper

def workato_mac_wb_industry_data():
    workato_response = json.loads(rail.result("workato_mac_wb_industry_api"))
    customer_department_mapper = {}
    for i in workato_response:
        customer_department_mapper[i["data"]["WorkBookID"]] = i["data"]["MaconomyDatabaseName"]
    return customer_department_mapper


