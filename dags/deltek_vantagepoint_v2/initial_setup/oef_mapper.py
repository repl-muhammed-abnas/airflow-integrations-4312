import copy
#format of an item in mapper
# {
#     "id" - Unique Identifier for the field.
#     "name" - Name of the field to be displayed in UI. Keep this as well unique.
#     "input" - Key in the api response that gives value for this field.
#     "type" - Type of OEF(text/dropdown/number)
#     "bind" - List of entities to which this field is bound(user/timesheet/project)
#     "options" - List of options for dropdown type. This is optional and only meant for static option which are not fetched from API.
# }
oef_mapper = [
    {
        "id": "organization",
        "name": "Organization",
        "input": "OrganizationName",
        "type": "text",
        "bind": ["user"]
    },
    {
        "id": "laborcategory",
        "name": "Labor Category",
        "input": "BillingCategory",
        "type": "dropdown",
        "bind": ["user", "timesheet"]
    },
    {
        "id": "laborcodelevel1",
        "name": "Labor Code Level 1",
        "code": "Labor Code Level 1",
        "input": "DefaultLC1",
        "type": "dropdown",
        "bind": ["user", "timesheet"]
    },
    {
        "id": "laborcodelevel2",
        "name": "Labor Code Level 2",
        "code": "Labor Code Level 2",
        "input": "DefaultLC2",
        "type": "dropdown",
        "bind": ["user", "timesheet"]
    },
    {
        "id": "laborcodelevel3",
        "name": "Labor Code Level 3",
        "code": "Labor Code Level 3",
        "input": "DefaultLC3",
        "type": "dropdown",
        "bind": ["user", "timesheet"]
    },
    {
        "id": "laborcodelevel4",
        "name": "Labor Code Level 4",
        "code": "Labor Code Level 4",
        "input": "DefaultLC4",
        "type": "dropdown",
        "bind": ["user", "timesheet"]
    },
    {
        "id": "laborcodelevel5",
        "name": "Labor Code Level 5",
        "code": "Labor Code Level 5",
        "input": "DefaultLC5",
        "type": "dropdown",
        "bind": ["user", "timesheet"]
    },
    {
        "id": "laborcodecombined",
        "name": "Labor Codes",
        "input": None,
        "type": "dropdown",
        "bind": ["timesheet"]
    },
    {
        "id": "allowlcupdate",
        "name": "Allow LC Update",
        "input": "ChangeDefaultLC",
        "type": "text",
        "bind": ["user"]
    },
    {
        "id": "yearsotherfirms",
        "name": "YearsOtherFirms",
        "input": "YearsOtherFirms",
        "type": "number",
        "bind": ["user"]
    },
    {
        "id": "prioryearsfirm",
        "name": "PriorYearsFirm",
        "input": "PriorYearsFirm",
        "type": "number",
        "bind": ["user"]
    },
    {
        "id": "projectsupervisor",
        "name": "Project Supervisor",
        "input": "Supervisor",
        "type": "text",
        "bind": ["project"]
    },
    {
        "id": "projectprincipal",
        "name": "Project Principal",
        "input": "Principal",
        "type": "text",
        "bind": ["project"]
    },
    {
        "id": "state",
        "name": "State",
        "input": "State",
        "type": "text",
        "bind": ["user"]
    },
    {
        "id": "country",
        "name": "Country",
        "input": "Country",
        "type": "text",
        "bind": ["user"]
    },
    {
        "id": "locale",
        "name": "Locale",
        "input": "Locale",
        "type": "text",
        "bind": ["user"]
    },
    {
        "id": "tkgroup",
        "name": "Timesheet Group",
        "input": "TKGroup",
        "type": "text",
        "bind": ["user"],
    },
    {
        "id": "workdistribution",
        "name": "WorkDistribution",
        "input": None,
        "type": "dropdown",
        "bind": ["timesheet"],
        "options": ["Regular Time","Over Time"],
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
