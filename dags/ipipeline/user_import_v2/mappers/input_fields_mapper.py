"""
iPipeline User Import - Input Fields Mapper

Field mappings between actual iPipeline CSV column names and internal processing field names.
Maps the exact CSV headers to standardized internal field names for processing.
"""

# CSV to Internal Field Mappings - EXACT from actual input CSV headers with validation flags
INPUT_FIELDS = [
    {
        "csv_field": "Employee ID",
        "field_name": "employee_id",
        "mandatory_add": True,
        "mandatory_update": True,
        "updateable": False
    },
    {
        "csv_field": "First Name",
        "field_name": "first_name",
        "mandatory_add": True,
        "mandatory_update": True,
        "updateable": True
    },
    {
        "csv_field": "Last Name",
        "field_name": "last_name",
        "mandatory_add": False,
        "mandatory_update": True,
        "updateable": True
    },
    {
        "csv_field": "Display Name",
        "field_name": "display_name",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Email",
        "field_name": "email",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Start Date",
        "field_name": "start_date",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": False
    },
    {
        "csv_field": "End Date",
        "field_name": "end_date",
        "mandatory_add": False,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Login Name",
        "field_name": "login_name",
        "mandatory_add": True,
        "mandatory_update": True,
        "updateable": False
    },
    {
        "csv_field": "Authentication Type",
        "field_name": "authentication_type",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Authentication ID",
        "field_name": "authentication_id",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": False
    },
    {
        "csv_field": "Supervisor",
        "field_name": "supervisor",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Language",
        "field_name": "language",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": False
    },
    {
        "csv_field": "FTE",
        "field_name": "fte",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Level",
        "field_name": "level",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Title",
        "field_name": "title",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Location Level 1",
        "field_name": "location_level_1",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Location Level 2",
        "field_name": "location_level_2",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Employee Schedule",
        "field_name": "employee_schedule",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Department Level 1",
        "field_name": "department_level_1",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Department Level 2",
        "field_name": "department_level_2",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Employee Category",
        "field_name": "employee_category",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Scheduled Hours",
        "field_name": "scheduled_hours",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "ELT",
        "field_name": "elt",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "UKSICK",
        "field_name": "uksick",
        "mandatory_add": False,
        "mandatory_update": False,
        "updateable": False
    },
    {
        "csv_field": "Transfer Date",
        "field_name": "transfer_date",
        "mandatory_add": False,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Employee Type",
        "field_name": "employee_type",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Paygroup",
        "field_name": "paygroup",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": False
    },
    {
        "csv_field": "Project",
        "field_name": "project",
        "mandatory_add": False,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "Seniority Years",
        "field_name": "seniority_level",
        "mandatory_add": True,
        "mandatory_update": False,
        "updateable": True
    },
    {
        "csv_field": "HASH",
        "field_name": "hash_value",
        "mandatory_add": True,
        "mandatory_update": True,
        "updateable": True
    }
]
