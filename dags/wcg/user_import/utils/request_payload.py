import rail

null = None
false = False
true = True


def get_task_state(task_id):
    """
    Get the current state of a task in the current DAG run.
    Used by supervisor child DAG to check task states for conditional logic.
    """
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_create_user_payload(dag_run, config):
    conf = dag_run.conf

    hire_date_str = conf.get("hire_date", "")
    hire_date_parsed = rail.parse_date(hire_date_str, config.REP_DATE_FORMAT) if hire_date_str else null
    end_date_parsed = rail.parse_date(conf.get("release_date"), config.REP_DATE_FORMAT) if conf.get("release_date") else null

    custom_field_values = []

    if conf.get("subsidiary_field_uri"):
        custom_field_values.append({
            "customField": {
                "uri": conf.get("subsidiary_field_uri")
            },
            "dropDownOption": {
                "name": conf.get("subsidiary")
            }
        })

    if conf.get("employeeid"):
        custom_field_values.append({
            "customField": {
                "uri": conf.get("netsuite_internal_id_oef_uri")
            },
            "text": conf.get("employeeid")
        })
    
    if conf.get("middlename"):
        custom_field_values.append({
            "customField": {
                "uri": conf.get("middle_name_oef_uri"),
            },
            "text": conf.get("middlename")
        })

    return {
        "user": {
            "target": {
                "loginName": conf.get("email")
            },
            "firstname": conf.get("firstname"),
            "lastname": conf.get("lastname"),
            "emailAddress": conf.get("email"),
            "employeeId": conf.get("adp_employee_id") if conf.get("adp_employee_id") else null,
            "department": {
                "uri": conf.get("department_uri"),
            } if conf.get("department_uri") else null,
            "workWeekStartDayUri": "urn:replicon:day-of-week:sunday",
            "employmentDateRange": {
                "startDate": hire_date_parsed,
                "endDate": end_date_parsed,
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:replicon"
                ],
                "isLoginEnabled": "1",
                "loginName": conf.get("email"),
                "password": config.defaults_mapper_data.get("default_password", "Track@Time!232")
            },
            "permissionSets": [
                {
                    "name": config.defaults_mapper_data.get("default_permission", "Project Resource")
                }
            ],
            "policySets": [
                {
                    "name": config.defaults_mapper_data.get("default_timesheet_template", "WCG Timesheet Template")
                }
            ],
            "employeeType": {
                "name": conf.get("employee_type", config.defaults_mapper_data.get("default_employee_type", "Regular Employee"))
            },
            "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
            "timesheetApprovalPath": {
                "name": "Supervisor"
            },
            "timeZone": {
                "uri": "urn:replicon:time-zone:america-new-york",
            },
            "locationSchedule": [{
                "location": {
                    "name": conf.get("location"),
                }
            }] if conf.get("location") else [],
            "customFieldValues": custom_field_values
        }
    }
