"""
iPipeline User Import - OEF (Object Extension Field) and Custom Field Mapper

OEF and custom field mappings for iPipeline user data based on XYZ Timekeeper integration spec.
Maps source ABC candidate fields to XYZ user OEFs and custom fields.

FIELD MAPPING FROM CSV:
======================
Source System: ABC (Candidate data)
Target System: XYZ (User data)
Integration Type: Flat File (CSV, daily)

OEF TEXT FIELDS:
- FTE: Employee classification (FTE/Part Time)
- Level: Organizational level code (D1, D2, etc.)
- Title: Job title/position name
- Employee Category: Hourly/Salaried classification
- Scheduled Hours: Weekly scheduled hours (e.g., 40)
- ELT: Employee lookup text (First Name + Last Name)
- UKSICK: UK sick leave category (A/B/C, special update rules)
- Transfer Date: Employee transfer date
- Employee Type: Regular/Temp classification
- Paygroup: Payroll group assignment
- Project: Project assignment
- HASH: Data integrity hash for efficient processing

CUSTOM FIELDS:
- Date of Employment: Employee start date mapping
"""

# Object Extension Field Mappings for XYZ Timekeeper
OEF_FIELDS_MAPPER = [
    {
        'field_name': 'fte',
        'oef_name': 'FTE',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'level',
        'oef_name': 'Level',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'title',
        'oef_name': 'Title',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'scheduled_hours',
        'oef_name': 'Scheduled Hours',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'elt',
        'oef_name': 'ELT',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'uksick',
        'oef_name': 'UKSICK',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'transfer_date',
        'oef_name': 'Transfer Date',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'paygroup',
        'oef_name': 'Paygroup',
        'type': 'text',
        'can_update': False
    },
    {
        'field_name': 'project',
        'oef_name': 'Project',
        'type': 'text',
        'can_update': True
    },
    {
        'field_name': 'seniority_level',
        'oef_name': 'Seniority Years',
        'type': 'text',
        'can_update': True
    }
]
