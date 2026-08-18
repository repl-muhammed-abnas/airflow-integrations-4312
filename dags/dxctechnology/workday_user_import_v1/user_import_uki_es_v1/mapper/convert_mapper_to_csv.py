import csv
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from es_assignment_mapper_v2 import DXC_ASSIGNMENT_MAPPER

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "es_assignment_mapper_v2.csv")

RESTRICTION_FIELDS = [
    "additional_job_classification",
    "company_code",
    "location",
    "work_shift",
    "fte_percent",
]

CSV_HEADERS = [
    "rule_order",
    "timesheet_period",
    "work_week",
    "timesheet_template",
    "payrule",
    "timeoff_approval_path",
    "timesheet_approval_path",
    "R.additional_job_classification",
    "R.additional_job_classification_op",
    "R.company_code",
    "R.company_code_op",
    "R.location",
    "R.location_op",
    "R.work_shift",
    "R.work_shift_op",
    "R.fte_percent",
    "R.fte_percent_op",
]


def extract_restrictions(restrictions):
    values = {}
    operators = {}
    for r in restrictions:
        values[r["field"]] = r["values"]
        operators[r["field"]] = r["operator"]
    return values, operators


def build_rows(entry):
    restriction_vals, restriction_ops = extract_restrictions(
        entry.get("restrictions", [])
    )

    base = {
        "timesheet_period": entry.get("timesheet_period", ""),
        "work_week": entry.get("work_week", ""),
        "timesheet_template": entry.get("timesheet_template", ""),
        "payrule": entry.get("payrule", ""),
        "timeoff_approval_path": entry.get("timeoff_approval_path", ""),
        "timesheet_approval_path": entry.get("timesheet_approval_path", ""),
        "R.additional_job_classification": restriction_vals.get(
            "additional_job_classification", "NOT APPLICABLE"
        ),
        "R.additional_job_classification_op": restriction_ops.get(
            "additional_job_classification", "NOT APPLICABLE"
        ),
        "R.location": restriction_vals.get("location", "NOT APPLICABLE"),
        "R.location_op": restriction_ops.get("location", "NOT APPLICABLE"),
        "R.work_shift": restriction_vals.get("work_shift", "NOT APPLICABLE"),
        "R.work_shift_op": restriction_ops.get("work_shift", "NOT APPLICABLE"),
        "R.fte_percent": restriction_vals.get("fte_percent", "NOT APPLICABLE"),
        "R.fte_percent_op": restriction_ops.get("fte_percent", "NOT APPLICABLE"),
    }

    company_codes = restriction_vals.get("company_code", "NOT APPLICABLE")
    # After exploding list into rows, the effective operator is always "equal"
    company_code_op = "equal"

    if isinstance(company_codes, list):
        rows = []
        for code in company_codes:
            row = {
                **base,
                "R.company_code": code,
                "R.company_code_op": company_code_op,
            }
            rows.append(row)
        return rows

    base["R.company_code"] = company_codes
    base["R.company_code_op"] = restriction_ops.get("company_code", "NOT APPLICABLE")
    return [base]


def main():
    all_rows = []
    for rule_idx, entry in enumerate(DXC_ASSIGNMENT_MAPPER, start=1):
        rows = build_rows(entry)
        for row in rows:
            row["rule_order"] = rule_idx
        all_rows.extend(rows)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Written {len(all_rows)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
