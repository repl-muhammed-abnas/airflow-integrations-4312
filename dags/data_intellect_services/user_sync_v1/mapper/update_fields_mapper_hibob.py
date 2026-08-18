employee_details_update_fields = [
    {
        "path": "/root/firstName",
        "value": "firstname"
    },
    {
        "path": "/root/surname",
        "value": "lastname"
    },
    {
        "path": "/root/displayName",
        "value": "displayname"
    },
    {
        "path": "/work/startDate",
        "value": "startdate"
    },
    {
        "path": "/internal/terminationDate",
        "value": "enddate"
    },
    {
        "path": "/internal/status",
        "value": "status"
    }
]

employee_work_update_fields = ["effectiveDate", "siteId", "title", "reportsTo", "customColumns", "department"]

employee_contract = ["contract", "type", "effectiveDate"]

tenant_wide_log_columns = ["id","employee_id","action","type","firstname","lastname","displayname","email","startdate","enddate","status",
                        "is_manager","effective_date","team","workstream","site","supervisor","supervisor_emp_id","title",
                        "primary_role","resource_pool","department","contract","emp_type","effectivedateemptype","effectivedateemptype",
                        "timestamp"]
