from deltek_vantagepoint.initial_setup.oef_mapper import get_oefs_with_required_name
from deltek_vantagepoint.initial_setup.config import *
region = 'us-east-1'
environment = 'pre-production'
instance = "integrationtest"
company_key = 'integrationtest'
replicon_conn_id = f'vp_{company_key}_replicon_conn'
deltek_vantagepoint_conn_id = f'vp_{company_key}_vp_conn'

execution_timeout_days = 14
can_run_batch_task_var_name = f'Vantagepoint_initial_setup_can_run_batch_task_{instance}'
child_dag_max_active_runs = 3

root_department = 'Company'
replicon_export_file_format_name = 'Vantagepoint TimeData Export'

timesheet_field_oef_name_for_lc = 'Labor Codes'

# Required OEFs format - "id": "preferred caption" or None to keep the default caption
oefs = get_oefs_with_required_name({
    "organization": None,
    "laborcategory": None,
    "laborcodelevel1": "Office",
    "laborcodelevel2": "Role",
    "laborcodelevel3": "Workplace",
    "laborcodelevel4": "Shift",
    "laborcodelevel5": "Staff",
    "laborcodecombined": timesheet_field_oef_name_for_lc,
    "yearsotherfirms": None,
    "prioryearsfirm": None,
    "allowlcupdate": None,
    "state": None,
    "country": None,
    "locale": None,
    "projectsupervisor": None,
    "projectprincipal": None,
    "workdistribution": None,
    "tkgroup": None
})

groups = [
    {
        "id": "homecompany",
        "name": "Office Company",
        "plural": "Office Companies",
        "input": "HomeCompany",
        "assignby": "code",
        **default_group_configs.get('servicecenter')
    },
    {
        "id": "paytype",
        "name": "Pay Type",
        "plural": "Pay Types",
        "input": "PayType",
        "assignby": "name",
        **default_group_configs.get('costcenter')
    }
]

usersync_filter_var = f'deltek_vantagepoint_usersync_filter_{company_key}'
