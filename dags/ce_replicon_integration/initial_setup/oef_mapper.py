import copy

# Format of an item in mapper:
# {
#     "id" - Unique identifier for the field
#     "name" - Name of the field to be displayed in UI. Keep this as well unique.
#     "type" - Type of OEF (text/dropdown/number)
#     "bind" - List of entities to which this field is bound (user/project/timesheet)
#     "input" - Key in the API response that gives value for this field (optional)
#     "options" - List of options for dropdown type. This is optional and only meant for static option which are not fetched from API.
# }

oef_mapper = [
    {
        "id": "useridentify",
        "name": "userIdentify",
        "type": "text",
        "bind": ["user"],
        "input": "uuid"
    },
    {
        "id": "companyuuid",
        "name": "Company_uuid",
        "type": "text",
        "bind": ["user", "project"],
        "input": "company_uuid"
    },
    {
        "id": "paytype",
        "name": "Pay Type",
        "type": "text",
        "bind": ["user"],
        "input": "pay_interval"
    },
    {
        "id": "unionworkerclass",
        "name": "Worker Class",
        "type": "dropdown",
        "bind": ["user"],
        "input": "class"
    },
    {
        "id": "payrolldate",
        "name": "Payroll_Date",
        "type": "text",
        "bind": ["project"],
        "input": "payroll_date"
    },
    {
        "id": "wbstype",
        "name": "wbs_type",
        "type": "text",
        "bind": ["project"],
        "input": "wbs_type"
    },
    {
        "id": "amount",
        "name": "Amount",
        "type": "number",
        "bind": ["timesheet"],
        "input": "amount"
    },
    {
        "id": "workpayment",
        "name": "Work Payment",
        "type": "dropdown",
        "bind": ["timesheet"],
        "input": None,
        "options": ["Per Diem", "Bonus", "Non-tax"]
    }
]


def get_oefs_with_required_name(required_oefs):
    oef_mapper_copy = copy.deepcopy(oef_mapper)
    filtered_oefs = [item for item in oef_mapper_copy if item.get('id') in required_oefs]

    for item in filtered_oefs:
        new_name = required_oefs.get(item['id'])
        if new_name is not None:
            item['name'] = new_name

    return filtered_oefs
