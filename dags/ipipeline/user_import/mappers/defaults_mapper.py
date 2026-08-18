"""
iPipeline User Import - Defaults Mapper

Default values for user creation and fallback values.
Following tsystems pattern.
"""

DEFAULTS_MAPPER = {
    'authentication_type': 'SSO',
    'last_name_fallback': '.',
    'default_fte_hours': 40.0,
    'default_schedule_hours': 40.0,
    'supervisor_permission': 'Supervisor',
    'root_department': 'iPipeline',
    'default_org_role': 'Default',
    'default_permission': 'Project Resource with Reports',
    'schedule_manager_supervisor_permission': 'Schedule Manager-Supervisor',
    'schedule_manager_not_supervisor_permission': 'Schedule Manager- Not Supervisor'
}