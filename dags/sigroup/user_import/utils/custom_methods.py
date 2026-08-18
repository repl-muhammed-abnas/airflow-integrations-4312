from datetime import datetime
import functools
import json
from dateutil.relativedelta import relativedelta
import rail
null = None
MMDDYYY="%m/%d/%Y"

MAPPER_FIELD_ALIASES = {
    "TimeOff Types": "Time Off Types",
    "WorkWeek": "Work Week",
}
MAPPER_BLANK_VALUE = "(Blank)"
# Only the fields that have a matching clear_* task in process_update_user. Adding a
# field here without its clear_* task silently does nothing.
# Timesheet approval path is deliberately NOT here: UpdateApprovalPathForUser rejects a
# null approvalPathUri ("No URI Provided"), there is no unassign endpoint for it, and no
# user in the instance has a blank approval path - it is effectively mandatory.
MAPPER_POLICY_FIELDS = ("timesheetperiod",)


def normalize_mapper_rows(rows):
    """The mapper is maintained in a spreadsheet, so Field/Value carry stray tabs
    and spaces. Every Replicon lookup is an exact displayText match, so an
    un-stripped name silently resolves to None and the assignment is skipped."""
    normalized = []
    for row in rows or []:
        field = (row.get("Field") or "").strip()
        value = row.get("Value")
        if isinstance(value, str):
            value = "|".join(part.strip() for part in value.split("|")).strip()
        normalized.append({
            **row,
            "Field": MAPPER_FIELD_ALIASES.get(field, field),
            "Value": value
        })
    return normalized


def find_mapper_rows(mapper, key):
    """The legal employer keys use mixed case "All" in the location slot while the
    derived lookup key uses "ALL", so an exact dict lookup misses them entirely."""
    if key in mapper:
        return mapper[key]
    target = key.upper()
    for candidate, rows in mapper.items():
        if candidate.upper() == target:
            return rows
    return null


def get_blank_mapper_fields(mapper_config):
    """A mapper value of "(Blank)" or an explicitly empty string means the field must
    be unassigned in Replicon, not left at whatever the user already had. A field with
    no mapper row at all resolves to None and is deliberately left untouched."""
    return [key for key in MAPPER_POLICY_FIELDS
            if isinstance((mapper_config or {}).get(key), str)
            and mapper_config[key].strip() in ("", MAPPER_BLANK_VALUE)]


def get_valid_activity_names(dag_run):
    """Activities are sent to Replicon by name. A single name that does not exist
    fails the whole PutUser3 call with 400 Bad Request, so drop unknown names the
    same way every other mapper-driven lookup does."""
    activities = dag_run.conf.get("get_all_activities") or []
    valid_names = []
    for name in (dag_run.conf.get("activity") or "").split("|"):
        name = name.strip()
        if not name:
            continue
        if rail.find_first_by_attr_and_get_attr(activities, "displayText", name, "uri"):
            valid_names.append(name)
        else:
            # not configured in this instance - skip it instead of failing the
            # whole PutUser3 call with a 400
            pass
    return valid_names


def get_effective_group_value(response, collection, attribute):
    """GetEffectiveUserGroupMembership omits or empties a collection when the user
    has no membership for it, so indexing [0] blindly raises IndexError."""
    entries = (response or {}).get(collection) or []
    if not entries:
        return null
    return ((entries[0] or {}).get(attribute) or {}).get(attribute)


def get_group_display_text(group):
    return (group or {}).get("displayText")


def get_custom_fields_data(response):
    custom_field_list = list(map(lambda i: {
        i["displayText"]: i["uri"]}, response))
    custom_fields = {}
    for i in custom_field_list:
        for k, v in i.items():
            custom_fields[k] = v
    return custom_fields

def get_user_config_for_mapper_values(dag_run):
    return {
        "useruri": rail.result("bulk_get_users")["uri"] if rail.result("bulk_get_users") else "",
        "loginname": dag_run.conf["loginname"],
        "scheduletype": rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Schedule Type",
            "Value"),
        "authenticationtype":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Authentication Type",
            "Value"),
        "holidaycalendar":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Holiday Calender",
            "Value"),
        "workweek":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Work Week",
            "Value"),
        "timeofftypes":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Time Off Types",
            "Value"),
        "timeoffapproval":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Timeoff Approval",
            "Value"),
        "timeofftemplate":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Time Off Template",
            "Value"),
        "punchentrypolicy":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Punch Entry Policy",
            "Value"),
        "timesheetperiod":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Timesheet Period",
            "Value"),
        "timesheetapproval":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Timesheet Approval",
            "Value"),
        "timesheettemplate":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Timesheet Template",
            "Value"),
        "payrule": rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Payrule",
            "Value"),
        "status": rail.result("bulk_get_users")["status"] if rail.result("bulk_get_users") else "",
        "activity":  rail.find_first_by_attr_and_get_attr(
            rail.result("aggregate_mapping_values"),
            "Field",
            "Activity",
            "Value")
    }


def get_shift_user_config(dag_run):
    shift_mapper_values = rail.result("get_shift_mapper_values")
    return {
        **dag_run.conf,
        "lookuptable": dag_run.conf["lookuptable"],
        # NB: do not overwrite "location" with locationcode here - locationcode is already
        # resolved to a uri, and if_location_update compares "location" against the group's
        # displayText, so a uri makes every run look like a location change.
        "department": dag_run.conf["department"].split("|")[-1],
        "actioneffectivedate": dag_run.conf["actioneffectivedate"] if
        dag_run.conf["actioneffectivedate"]
        else datetime.strftime(datetime.today(), MMDDYYY),
        **get_shift_schedule_type_uri(shift_mapper_values, dag_run),
        **get_shift_auth_type(shift_mapper_values),
        "workweek": rail.find_first_by_attr_and_get_attr(
            shift_mapper_values,
            "Field",
            "Work Week",
            "Value"
        ) or "urn:replicon:day-of-week:monday",
        "timeofftypes": rail.find_first_by_attr_and_get_attr(
            shift_mapper_values,
            "Field",
            "Time Off Types",
            "Value"
        ),
        "timeoffapproval": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_approval_paths_timeoff"],
            "displayText",
            rail.find_first_by_attr_and_get_attr(
                shift_mapper_values,
                "Field",
                "Timeoff Approval",
                "Value"
            ),
            "uri"
        ),
        "timeofftemplate": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_policy_sets"],
            "displayText",
            rail.find_first_by_attr_and_get_attr(
                shift_mapper_values,
                "Field",
                "Time Off Template",
                "Value"
            ),
            "uri"
        ),
        "holidaycalendar": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_holiday_calendars"],
            "displayText",
            rail.find_first_by_attr_and_get_attr(
                shift_mapper_values,
                "Field",
                "Holiday Calender",
                "Value"
            ),
            "uri"
        ),
        "timesheetperiod": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_timesheet_period_list"],
            "textValue",
            rail.find_first_by_attr_and_get_attr(
                shift_mapper_values,
                "Field",
                "Timesheet Period",
                "Value"
            ),
            "uri"
        ),
        "timesheetapproval": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_approval_paths_timesheet"],
            "displayText",
            rail.find_first_by_attr_and_get_attr(
                shift_mapper_values,
                "Field",
                "Timesheet Approval",
                "Value"
            ),
            "uri"
        ),
        **get_shift_punch_entry_policy(shift_mapper_values, dag_run),
        **get_shift_timesheet_template(shift_mapper_values,dag_run),
        **get_shift_payrule(shift_mapper_values, dag_run),
        "supervisorpermissionuri": dag_run.conf["supervisorpermissionuri"],
        "manufacturing":  dag_run.conf.get("get_manufacturing_custom_field_dropdown"),
        "coefficientlevel": dag_run.conf.get("get_coefficient_level_custom_field_dropdown"),
        "elderlyallowance":  dag_run.conf.get("get_elderly_allowance_custom_field_dropdown"),
        "apprentice":  dag_run.conf.get("get_apprentice_custom_field_dropdown"),
        "timecode":  dag_run.conf.get("get_timecode_custom_field_dropdown"),
        "cbaappendix":  dag_run.conf.get("get_cba_appendix_custom_field_dropdown"),
        "istariffemployee": dag_run.conf.get("get_tariff_employee_custom_field_dropdown"),
        "tariffclassification": dag_run.conf.get("get_tariff_classification_custom_field_dropdown"),
        "stepinformation": dag_run.conf.get("get_step_information_custom_field_dropdown"),
        "workleader": dag_run.conf.get("get_work_leader_custom_field_dropdown"),
        **get_shift_activity(shift_mapper_values, dag_run),
        "mapperblankfields": [],
        "businessunitcodeforpayrule": dag_run.conf["businessunitcode"],
        "startingbalancesettouri":dag_run.conf["startingbalancesettouri"],
        "preventbalanceoverdrawuri":dag_run.conf["preventbalanceoverdrawuri"],
        "useruri": rail.result("create_user_config_for_mapper_values")["useruri"] if rail.result("create_user_config_for_mapper_values")else null
    }


def get_shift_activity(shift_mapper_values,dag_run):
    activity = list(filter(lambda i: i["Field"] == "Activity" and
                           i["Shift"] == dag_run.conf["shift"] and
                           i["Employeetype"] == dag_run.conf["employee_type"], shift_mapper_values))
    if activity:
        return {
            "activity": activity[0]["Value"]
        }
    return {
        "activity": null
    }


def get_shift_payrule(shift_mapper_values, dag_run):
    payrule = list(filter(lambda i: i["Field"] == "Payrule" and
                          i["Shift"] == dag_run.conf["shift"] and
                          i["Employeetype"] == dag_run.conf["employee_type"], shift_mapper_values))
    if payrule:
        return {
            "payrule": rail.find_first_by_attr_and_get_attr(
                dag_run.conf["get_all_payrule_scripts"],
                "displayText",
                payrule[0]["Value"],
                "uri"
            )
        }
    return {
        "payrule": null
    }


def get_shift_timesheet_template(shift_mapper_values, dag_run):
    timesheet_template = list(filter(lambda i: i["Field"] == "Timesheet Template" and
                                     i["Shift"] == dag_run.conf["shift"] and
                                     i["Employeetype"] == dag_run.conf["employee_type"], shift_mapper_values))
    if timesheet_template:
        return {
            "timesheettemplate": rail.find_first_by_attr_and_get_attr(
                dag_run.conf["get_all_policy_sets"],
                "displayText",
                timesheet_template[0]["Value"],
                "uri"
            )
        }
    return {
        "timesheettemplate": null
    }


def get_shift_punch_entry_policy(shift_mapper_values, dag_run):
    punch_entry_policy = list(filter(lambda i: i["Field"] == "Punch Entry Policy" and
                                     i["Shift"] == dag_run.conf["shift"] and
                                     i["Employeetype"] == dag_run.conf["employee_type"], shift_mapper_values))
    if punch_entry_policy:
        return {
            "punchentrypolicy": rail.find_first_by_attr_and_get_attr(
                dag_run.conf["get_all_policy_sets"],
                "displayText",
                punch_entry_policy[0]["Value"],
                "uri"
            )
        }
    return {
        "punchentrypolicy": null
    }


def get_shift_auth_type(shift_mapper_values):
    auth_type = rail.find_first_by_attr_and_get_attr(
        shift_mapper_values,
        "Field",
        "Authentication Type",
        "Value"
    )
    if not auth_type or (auth_type and auth_type == "SSO"):
        return {
            "authenticationtype":  "urn:replicon:user-authentication-type:sso"
        }
    return {
        "authenticationtype":  "urn:replicon:user-authentication-type:replicon"
    }


def get_shift_schedule_type_uri(shift_mapper_values,dag_run):
    shift_type = list(filter(lambda i: i["Field"] == "Schedule Type" and
                             i["Shift"] == dag_run.conf["shift"] and
                             i["Employeetype"] == dag_run.conf["employee_type"], shift_mapper_values))
    if shift_type:
        if "Shift" in shift_type[0]["Value"]:
            return {
                "scheduletypeuri": "urn:replicon:schedule-type:shift",
                "scheduletype": shift_type[0]["Value"]
            }
        return {
            "scheduletypeuri": rail.find_first_by_attr_and_get_attr(
                dag_run.conf["get_all_office_schedules"],
                "displayText",
                shift_type[0]["Value"],
                "uri"
            ),
            "scheduletype": shift_type[0]["Value"]
        }
    return {
        "scheduletypeuri": null,
        "scheduletype": null
    }


def get_user_config(dag_run):
    shift_mapper_values = rail.result("create_user_config_for_mapper_values")
    return {
        **dag_run.conf,
        "lookuptable": dag_run.conf["lookuptable"],
        "employee_type": dag_run.conf["get_employee_type_custom_field_dropdown"],
        "department": dag_run.conf["department"].split("|")[-1],
        "actioneffectivedate": dag_run.conf["actioneffectivedate"] if
        dag_run.conf["actioneffectivedate"]
        else datetime.strftime(datetime.today(), MMDDYYY),
        **get_schedule_type_uri(shift_mapper_values, dag_run),
        **get_auth_type(shift_mapper_values),
        "holidaycalendar": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_holiday_calendars"],
            "displayText",
            shift_mapper_values["holidaycalendar"],
            "uri"
        ) if shift_mapper_values.get("holidaycalendar","") else null,
        "workweek": shift_mapper_values["workweek"] or "urn:replicon:day-of-week:monday",
        "timeofftypes": shift_mapper_values["timeofftypes"],
        "timeoffapproval": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_approval_paths_timeoff"],
            "displayText",
            shift_mapper_values["timeoffapproval"],
            "uri"
        ),
        "timeofftemplate": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_policy_sets"],
            "displayText",
            shift_mapper_values["timeofftemplate"],
            "uri"
        ),
        "punchentrypolicy": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_policy_sets"],
            "displayText",
            shift_mapper_values["punchentrypolicy"],
            "uri"
        ) if shift_mapper_values["punchentrypolicy"] else null,
        "timesheetperiod": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_timesheet_period_list"],
            "textValue",
            shift_mapper_values["timesheetperiod"],
            "uri"
        ) if shift_mapper_values["timesheetperiod"] else null,
        "timesheetapproval": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_approval_paths_timesheet"],
            "displayText",
            shift_mapper_values["timesheetapproval"],
            "uri"
        ) if shift_mapper_values["timesheetapproval"] else null,
        "timesheettemplate": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_policy_sets"],
            "displayText",
            shift_mapper_values["timesheettemplate"],
            "uri"
        ) if shift_mapper_values["timesheettemplate"] else null,
        "payrule": rail.find_first_by_attr_and_get_attr(
            dag_run.conf["get_all_payrule_scripts"],
            "displayText",
            shift_mapper_values["payrule"],
            "uri"
        ) if shift_mapper_values["payrule"] else null,
        "supervisorpermissionuri": dag_run.conf["supervisorpermissionuri"],
        "manufacturing":  dag_run.conf.get("get_manufacturing_custom_field_dropdown"),
        "coefficientlevel": dag_run.conf.get("get_coefficient_level_custom_field_dropdown"),
        "elderlyallowance":  dag_run.conf.get("get_elderly_allowance_custom_field_dropdown"),
        "apprentice":  dag_run.conf.get("get_apprentice_custom_field_dropdown"),
        "timecode":  dag_run.conf.get("get_timecode_custom_field_dropdown"),
        "cbaappendix":  dag_run.conf.get("get_cba_appendix_custom_field_dropdown"),
        "istariffemployee": dag_run.conf.get("get_tariff_employee_custom_field_dropdown"),
        "tariffclassification": dag_run.conf.get("get_tariff_classification_custom_field_dropdown"),
        "stepinformation": dag_run.conf.get("get_step_information_custom_field_dropdown"),
        "activity": shift_mapper_values["activity"] if shift_mapper_values["activity"] else null,
        "mapperblankfields": get_blank_mapper_fields(shift_mapper_values),
        "workleader": dag_run.conf.get("get_work_leader_custom_field_dropdown"),
        "businessunitcodeforpayrule": dag_run.conf["businessunitcode"],
        "startingbalancesettouri":dag_run.conf["startingbalancesettouri"],
        "preventbalanceoverdrawuri":dag_run.conf["preventbalanceoverdrawuri"],
        "useruri": rail.result("create_user_config_for_mapper_values")["useruri"] if rail.result("create_user_config_for_mapper_values")else null
    }


def get_auth_type(shift_mapper_values):
    auth_type = shift_mapper_values["authenticationtype"]
    if not auth_type or (auth_type and auth_type == "SSO"):
        return {
            "authenticationtype":  "urn:replicon:user-authentication-type:sso"
        }
    return {
        "authenticationtype":  "urn:replicon:user-authentication-type:replicon"
    }


def get_schedule_type_uri(shift_mapper_values, dag_run):
    shift_type = shift_mapper_values["scheduletype"]
    if shift_type:
        if "Shift" in shift_type:
            return {
                "scheduletypeuri": "urn:replicon:schedule-type:shift",
                "scheduletype": shift_type
            }
        return {
                "scheduletypeuri": rail.find_first_by_attr_and_get_attr(
                    dag_run.conf["get_all_office_schedules"],
                    "displayText",
                    shift_type,
                    "uri"
                ),
                "scheduletype": shift_type
            }
    return {
                "scheduletypeuri": null,
                "scheduletype": null
            }


def get_excpetion_logs(dag_run):
    msg = ""
    if not dag_run.conf["scheduletypeuri"] and dag_run.conf["scheduletype"]:
        msg += "Office schedule not assigned since " + \
            dag_run.conf["scheduletype"] + " not available in Replicon |"
    if not dag_run.conf["holidaycalendar"]:
        msg += "Holiday calendar " + \
            str(dag_run.conf["holidaycalendar"]) + " not avaiilble in Replicon |"
    if not dag_run.conf["timesheettemplate"]:
        msg += "TImesheet template not assigned since " + \
            str(dag_run.conf["timesheettemplate"]) + "not available in Replicon"
    if not dag_run.conf["initialsupervisorloginname"]:
        msg += "Supervisor not assigned as the the Initial Supervisor was not present in the input file."


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **dict(log['properties'].items()),**{"ecid":log.get("ecid")}
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['Status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['Status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['Status'] == 'Exception', final_log_records))))

    return final_log_records


def is_policy_line_before(policy_entry, end_date):
    """True when a time off policy line takes effect before end_date (dd/mm/yyyy).
    The initial policy line comes back with effectiveDate null, so it always precedes
    the termination date - dereferencing it raises 'NoneType' is not subscriptable."""
    effective = (policy_entry or {}).get("effectiveDate")
    if not effective:
        return True
    effective_date = str(effective["day"]) + "/" + \
        str(effective["month"]) + "/" + str(effective["year"])
    return datetime.strptime(effective_date, "%d/%m/%Y") < \
        datetime.strptime(end_date, "%d/%m/%Y")


def get_current_schedule_uri():
    """Each schedulePolicies entry carries its own schedule-policy-entry uri, which is
    not the schedule itself. Comparing the mapper's office-schedule uri against it always
    reports a change, so the schedule gets re-applied on every run and the user collects a
    duplicate dated entry. Compare against the office schedule (or the shift type)."""
    policies = rail.result("bulk_get_users")["schedulepolicies"] or []
    if not policies:
        return null
    entry = policies[-1] or {}
    return (entry.get("officeSchedule") or {}).get("uri") or entry.get("scheduleTypeUri")


def is_future_termination(dag_run):
    return datetime.strptime(dag_run.conf["enddate"], MMDDYYY).date() > datetime.today().date()


def is_rate_changed(feed_amount, existing_schedule):
    # payrollRateSchedule/costRateSchedule come back as a list of schedule entries,
    # so the last entry is the one currently in effect
    entry = existing_schedule
    if isinstance(entry, list):
        entry = entry[-1] if entry else null
    existing_amount = ((entry or {}).get("hourlyRate") or {}).get("amount")
    if existing_amount is None:
        return True
    try:
        return float(feed_amount) != float(existing_amount)
    except (TypeError, ValueError):
        return str(feed_amount) != str(existing_amount)


def get_user_basic_attribute_update(dag_run):
    firstname, lastname, emailaddress, displayname = null, null, null, null
    attribute_change = 0
    if dag_run.conf["firstname"] != rail.result("bulk_get_users")["firstname"]:
        firstname = dag_run.conf["firstname"]
        attribute_change = 1
    if dag_run.conf["lastname"] != rail.result("bulk_get_users")["lastname"]:
        lastname = dag_run.conf["lastname"]
        attribute_change = 1
    if dag_run.conf["emailaddress"] != rail.result("bulk_get_users")["emailaddress"]:
        emailaddress = dag_run.conf["emailaddress"]
        attribute_change = 1
    if dag_run.conf["displayname"] != rail.result("bulk_get_users")["displayname"]:
        displayname = dag_run.conf["displayname"]
        attribute_change = 1
    if attribute_change == 1:
        return {
            "user": {
                "uri": dag_run.conf["useruri"],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "modifications": {
                "userDetailsToApply": {
                    "firstName": firstname,
                    "lastName": lastname,
                    "emailAddress": {
                        "emailAddress": emailaddress
                    } if emailaddress else null,
                    "language": null,
                    "employmentDateRange": null,
                    "employmentStartDate": null,
                    "employmentEndDate": null,
                    "employeeId": null,
                    "displayNameParameter": {
                        "displayName": displayname
                    }if displayname else null
                },
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    return null


def get_user_custom_fields_text_update(dag_run):
    custom_fields_text = ["action", "status", "shift", "cloudpay_paycode",]
    custom_field_to_apply = []
    for i in custom_fields_text:
        if i in dag_run.conf and dag_run.conf[i] != rail.result("get_custom_field_values").get(i):
            custom_field_to_apply.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": dag_run.conf[i],
                "date": null,
                "dropDownOption": null,
                "number": null
            })
    if custom_field_to_apply:
        return {
            "user": {
                "uri": dag_run.conf["useruri"],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "modifications": {
                "customFieldValuesToApply": custom_field_to_apply,
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    return null


def get_user_custom_date_fields_update(dag_run):
    custom_fields_dates = ["actioneffectivedate",
                           "workinglifestartdate", "ptoservicedate"]
    custom_field_to_apply = []
    for i in custom_fields_dates:
        if i in dag_run.conf and dag_run.conf[i] != rail.result("get_custom_field_values").get(i):
            custom_field_to_apply.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": rail.parse_date(dag_run.conf[i], "%m/%d/%Y"),
                "dropDownOption": null,
                "number": null
            })
    if custom_field_to_apply:
        return {
            "user": {
                "uri": dag_run.conf["useruri"],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "modifications": {
                "customFieldValuesToApply": custom_field_to_apply,
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    return null


def get_user_custom_dropdown_fields_update(dag_run):
    custom_field_dropdown = ["locationstate", "locationcity", "employee_type",
                             "manufacturing", "coefficientlevel", "elderlyallowance",
                             "apprentice", "timecode", "cbaappendix", "istariffemployee",
                             "tariffclassification", "stepinformation", "workleader"]
    custom_field_to_apply = []
    for i in custom_field_dropdown:
        if i in dag_run.conf and dag_run.conf[i] != rail.result("get_custom_field_values").get(i):
            custom_field_to_apply.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": null,
                "dropDownOption": {
                    "uri": dag_run.conf[i]
                },
                "number": null
            })
    if custom_field_to_apply:
        return {
            "user": {
                "uri": dag_run.conf["useruri"],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "modifications": {
                "customFieldValuesToApply": custom_field_to_apply,
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    return null

def get_user_custom_field_exception(dag_run):
    msg = ""
    custom_field_dropdown = ["locationstate", "locationcity", "employee_type",
                             "manufacturing", "coefficientlevel", "elderlyallowance",
                             "apprentice", "timecode", "cbaappendix", "istariffemployee",
                             "tariffclassification", "stepinformation", "workleader"]
    for i in custom_field_dropdown:
        if i in dag_run.conf and not dag_run.conf[i+"uri"]:
            msg += i + "value" + dag_run.conf[i] + "not available in replicon"
    return msg

def get_custom_field_drop_down_request(existing_record_values,new_record_values, customfield):
    dropdown_request = []
    existing_dropdown_records = rail.load_all_records(existing_record_values)

    dropdown_request = list(map(lambda i: {
        "target": {
            "uri": i["uri"],
            "name": i["displayText"]
        },
        "name": i["displayText"],
        "isEnabled": i["isEnabled"]

    }, existing_dropdown_records))

    new_dropdown_records = rail.load_all_records(new_record_values)

    dropdown_request.extend(list(map(lambda i: {
        "target": {
            "uri": null,
            "name": i[customfield]
        },
        "name": i[customfield],
        "isEnabled": int(True)

    }, new_dropdown_records)))

    return dropdown_request

def get_timecode_drop_down_request():
    timecode = []
    existing_timecode_records = rail.load_all_records(
        rail.result("create_timecode_existing_values")
    )
    timecode = list(map(lambda i: {
        "target": {
            "uri": i["uri"],
            "name": i["displayText"]
        },
        "name": i["displayText"],
        "isEnabled": i["isEnabled"]

    }, existing_timecode_records))

    new_timecode_records = rail.load_all_records(
        rail.result("query_new_timecode_lower_drop_down")
    )

    timecode.extend(list(map(lambda i: {
        "target": {
            "uri": null,
            "name": i["timecode"]
        },
        "name": i["timecode"],
        "isEnabled": int(True)

    }, new_timecode_records)))
    return timecode

def get_time_off_to_retain():
    existing_timeoff = []
    for i in rail.result("get_timeoff_types_data"):
        if rail.find_first_by_attr_and_get_attr(
            rail.result("get_timeoff_type_policy"),
            "name",
            i["name"],
            "uri"):
            existing_timeoff.append(i["uri"])
    return existing_timeoff

def get_new_timeoff_types():
    new_timeoff = []
    for i in rail.result("get_timeoff_types_data"):
        if i["name"] is not None and not rail.find_first_by_attr_and_get_attr(
            rail.result("get_timeoff_type_policy"),
            "name",
            i["name"],
            "uri"):
            new_timeoff.append(i["uri"])
    return new_timeoff

def get_action_date_to_consider(dag_run):
    date_to_consider = ""
    if dag_run.conf["actioneffectivedate"]:
        date_to_consider = datetime.strftime(
            datetime.strptime(dag_run.conf["actioneffectivedate"],MMDDYYY), MMDDYYY)
    else:
        date_to_consider = datetime.strftime(datetime.today(),"%m/%d%Y")
    return date_to_consider

def get_tenure(dag_run):
    date_to_consider = datetime.strptime(get_action_date_to_consider(dag_run),MMDDYYY)
    startdate = datetime.strptime(dag_run.conf["startdate"],MMDDYYY)

    return relativedelta(date_to_consider, startdate).years


def get_timeoff_policies_for_user():
    policy_doc = rail.find_first_by_attr_and_get_attr(
        rail.result("get_timeoff_type_policy"),
        "timeOffType.uri",
        rail.result(["process_policies_for_rehire"]),
        "policySetSchedule"
    )
    policies_list=[]
    for i in policy_doc:
        for j in i:
            effective_date = j["effectiveDate"]["day"] + "/"+j["effectiveDate"]["month"] + "/" + j["effectiveDate"]["year"]
            if datetime.strftime(datetime.strptime(effective_date, "%d/%m/%Y")) <\
                rail.result("get_action_date_to_consider"):
                policies_list.append(j)
    return policies_list

def get_offset_count_policies(dag_run):
    tenure = rail.result("get_year_of_service")
    policy_list = []
    new_policy_list = []
    for i in rail.result("get_default_timeoff_policies_schedule"):
        if i["startOffset"]["offsetValue"] <= tenure:
            policy_list.append({
                "description": "Policy added as of "+dag_run.conf["actioneffectivedate"],
                "effectiveDate": rail.parse_date(datetime.strftime(
                    datetime.strptime(dag_run.conf["startdate"], MMDDYYY) +
                    relativedelta(months=+12*int(i["startOffset"]["offsetValue"])),MMDDYYY), MMDDYYY),
                "policySet": i["policySet"],
                "offsetValue": i["startOffset"]["offsetValue"]
            })
        else:
            new_policy_list.append({
                "description": "Policy added as of "+dag_run.conf["actioneffectivedate"],
                "effectiveDate": rail.parse_date(datetime.strftime(
                    datetime.strptime(dag_run.conf["startdate"], MMDDYYY) +
                    relativedelta(months=+12*i["startOffset"]["offsetValue"]),MMDDYYY), MMDDYYY),
                "policySet": i["policySet"]
            })
        new_policy_list.extend(policy_list)
    return new_policy_list

def get_all_new_policy_data(dag_run):
    policy_list = []
    if rail.result("get_offset_count_list"):
        first =  rail.result("get_offset_count_list")[0]
        policy_list.append({
            "description": "Effective as of " + dag_run.conf["actioneffectivedate"],
            "effectiveDate": rail.parse_date(dag_run.conf["actioneffectivedate"], MMDDYYY),
            "policySet":first["policySet"]
        })

        data = rail.result("get_offset_count_list")[1:]
        for i in data:
            policy_list.append({
            "description": "Effective as of " + i["effectiveDate"]["day"] + "/" + i["effectiveDate"]["month"] + "/" + i["effectiveDate"]["year"],
            "effectiveDate": i["effectiveDate"],
            "policySet":i["policySet"]
        })
    return policy_list

def get_custom_fields():
    custom_field_values = {}
    for i in rail.result("bulk_get_users")["customfieldvalues"]:
        if not isinstance(i, dict):
            continue
        # dropdown values live in dropDownOption, not text, and the feed side holds
        # the option uri - compare like for like. dropDownOption comes back either
        # as an object or as the uri string itself.
        option = i.get("dropDownOption")
        if isinstance(option, dict):
            value = option.get("uri")
        elif option:
            value = option
        elif i.get("number") is not None:
            value = i.get("number")
        else:
            value = i.get("text")
        custom_field_values.update({
                    i['customField']['displayText'].lower().replace(" ", ""): value
            })
    return custom_field_values

def get_user_update_groups_exceptions(dag_run):
    msg = ""
    groups = ["department", "paygroup", "businessunit",
               "location","financecostcenter", "legalemployer"]
    for i in groups:
        if dag_run.conf[i] and not dag_run.conf[i+"code"]:
            msg += i + dag_run.conf[i] + "not present in replicon"
    return msg

def get_user_update_schedule_exceptions(dag_run):
    msg = ""
    if not dag_run.conf["hourlypayratecurrency"] or not dag_run.conf["hourlypayeffectivedate"]:
        msg += "Hourly Pay rate not updated as the required currency/effective date is not avalaible."
    if not dag_run.conf["hourlycostcurrency"] or not dag_run.conf["hourlycosteffectivedate"]:
        msg += "Hourly Cost not updated as the required currency/effective date is not avalaible."
    if dag_run.conf["scheduletypeuri"] != get_current_schedule_uri():
        msg += "Schedule " + dag_run.conf["scheduletype"] +" not available in Replicon"
    if rail.result("create_timeoffchange_variable")["value"] and not dag_run["timeofftypes"]:
        msg += "Time off assignment not updated as the required Location/Employee type is not available in the current mapper."
    if not dag_run.conf["workweek"]:
        msg += "User updated  with basic configurations - The required values for the Location" + dag_run.conf["location"] + " and "\
             + dag_run.conf["employee_type"] +"is not available in the Integration's mapper."
    return msg


def get_existing_policies_list(dag_run):
    end_date = dag_run.conf["enddateday"] + "/" + dag_run.conf["enddatemonth"] + "/" + dag_run.conf["enddateyear"]
    timeoff_policy_list = []
    for i in dag_run.conf["policyset"]:
        if is_policy_line_before(i, end_date):
            timeoff_policy_list.append(i)
    timeoff_policy_list.extend(rail.result("create_new_policy_line"))
    return json.loads(json.dumps(
        timeoff_policy_list, ensure_ascii=False).replace(
            '"description": null', '"description": "effective"').replace(
        '"script"', '"scriptTarget"'))

def get_stop_accrual_policy_line(dag_run):
    """Policy line effective on the termination date that stops further accruals
    (no balance event scripts) and blocks any further balance usage."""
    return {
        "effectiveDate": rail.parse_date(dag_run.conf["enddate"], "%d/%m/%Y"),
        "description": "Added by integration on " + dag_run.conf["enddate"].replace("/", "-"),
        "policySet": {
            "timeOffBalanceEventScripts": [],
            "timeOffValidationScripts": [
                {
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                                "number": 0
                            }
                        }
                    ],
                    "script": {
                        "description": "Do not allow the user's time off balance to go below the overdraw threshold.",
                        "name": "Prevent balance overdraw",
                        "uri": dag_run.conf["preventbalanceoverdrawuri"]
                    }
                }
            ]
        }
    }


def get_new_payout_policy_line(dag_run):
    return [
          {
            "description": "Added by Integration on" + dag_run.conf["enddateday"] + "-"+\
                  dag_run.conf["enddatemonth"] + "-" + dag_run.conf["enddateyear"],
            "effectiveDate": {
              "day": dag_run.conf["enddateday"],
              "month": dag_run.conf["enddatemonth"],
              "year": dag_run.conf["enddateyear"]
            },
            "policySet": {
              "timeOffBalanceEventScripts": [
                {
                  "additionalParameters": [
                    {
                      "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                      "value": {
                        "number": dag_run.conf["newschedulebalance"]
                      }
                    }
                  ],
                  "script": {
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To",
                    "uri": dag_run.conf["startingbalancesettouri"]
                  }
                }
              ],
              "timeOffValidationScripts": [
                {
                  "additionalParameters": [
                    {
                      "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                      "value": {
                        "number": 0
                      }
                    }
                  ],
                  "script": {
                    "description": "Do not allow the user's time off balance to go below the overdraw threshold.",
                    "name": "Prevent balance overdraw",
                    "uri": dag_run.conf["preventbalanceoverdrawuri"]
                  }
                }
              ]
            }
          },
        ]

@functools.lru_cache(maxsize=128)
def get_valid_user_config():
    return {
        "get_all_office_schedules": rail.result("get_all_office_schedules"),
        "get_all_holiday_calendars":rail.result("get_all_holiday_calendars"),
        "get_all_approval_paths_timeoff":rail.result("get_all_approval_paths_timeoff"),
        "get_all_policy_sets":rail.result("get_all_policy_sets"),
        "get_timesheet_period_list":rail.result("get_timesheet_period_list"),
        "get_all_approval_paths_timesheet":rail.result("get_all_approval_paths_timesheet"),
        "get_all_payrule_scripts":rail.result("get_all_payrule_scripts"),
        "get_all_activities":rail.result("get_all_activities"),
        "startingbalancesettouri":rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_off_balance_scripts"),
            "displayText",
            "Starting Balance Set To",
            "uri"),
        "preventbalanceoverdrawuri":rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_validation_scripts"),
            "displayText",
            "Prevent balance overdraw",
            "uri"),
        "workinglifestartdateuri": rail.result("get_all_customfields")["Working Life Start Date"],
        "actionuri": rail.result("get_all_customfields")["Action"],
        "actioneffectivedateuri": rail.result("get_all_customfields")["Action Effective Date"],
        "statusuri": rail.result("get_all_customfields")["Status"],
        "locationcityuri": rail.result("get_all_customfields")["Location City"],
        "locationstateuri": rail.result("get_all_customfields")["Location State"],
        "employee_typeuri": rail.result("get_all_customfields")["EMPLOYEE_TYPE"],
        "shifturi": rail.result("get_all_customfields")["SHIFT"],
        "cloudpay_paycodeuri": rail.result("get_all_customfields")["CLOUDPAY_PAYCODE"],
        "manufacturinguri": rail.result("get_all_customfields")["Manufacturing"],
        "coefficientleveluri": rail.result("get_all_customfields")["Coefficient Level"],
        "elderlyallowanceuri": rail.result("get_all_customfields")["Elderly Allowance"],
        "apprenticeuri": rail.result("get_all_customfields")["Apprentice"],
        "timecodeuri": rail.result("get_all_customfields")["Time Code"],
        "cbaappendixuri": rail.result("get_all_customfields")["CBA Appendix"],
        "istariffemployeeuri": rail.result("get_all_customfields")["Is Tariff Employee"],
        "tariffclassificationuri": rail.result("get_all_customfields")["Tariff Classification"],
        "stepinformationuri": rail.result("get_all_customfields")["Step Information"],
        "fteuri": rail.result("get_all_customfields")["FTE"],
        "ptoservicedateuri": rail.result("get_all_customfields")["PTO Service Date"],
        "workleaderuri": rail.result("get_all_customfields")["Work Leader"],
        "adminmodifieduri": rail.result("get_all_customfields")["Admin Modified"],
        "adminmodified":rail.find_first_by_attr_and_get_attr(
            rail.result("get_admin_modified_custom_field_dropdown"),
            "displayText",
            "No",
            "uri"
        ),
        "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permissionset"),
            "displayText",
            "Supervisor",
            "uri"
        )
    }

def get_disable_timeoff_types():
    disable_timeoff = []
    for i in rail.result("get_timeoff_type_policy"):
        if i["name"] and not i["uri"] in rail.result("get_new_timeoff_type_from_mapper"):
            disable_timeoff.append(i)
    return disable_timeoff

def get_all_user_config(item):
    return {
        **item,
        "lookuptable":rail.result("create_sigroup_user_import_log"),
        "get_employee_type_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_employee_type_custom_field_dropdown"),
            "displayText",
            item.get("employee_type",""),
            "uri"
        ),
        "get_manufacturing_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_manufacturing_custom_field_dropdown"),
            "displayText",
            item.get("manufacturing",""),
            "uri"),
        "get_coefficient_level_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_coefficient_level_custom_field_dropdown"),
            "displayText",
            item.get("coefficientlevel",""),
            "uri"),
        "get_elderly_allowance_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_elderly_allowance_custom_field_dropdown"),
            "displayText",
            item.get("elderlyallowance",""),
            "uri"),
        "get_apprentice_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_apprentice_custom_field_dropdown"),
            "displayText",
            item.get("apprentice",""),
            "uri"),
        "get_timecode_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_timecode_custom_field_dropdown"),
            "displayText",
            item.get("timecode",""),
            "uri"),
        "get_cba_appendix_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_cba_appendix_custom_field_dropdown"),
            "displayText",
            item.get("cbaappendix",""),
            "uri"),
        "get_tariff_employee_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_tariff_employee_custom_field_dropdown"),
            "displayText",
            item.get("istariffemployee",""),
            "uri"),
        "get_tariff_classification_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_tariff_classification_custom_field_dropdown"),
            "displayText",
            item.get("tariffclassification",""),
            "uri"),
        "get_step_information_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_step_information_custom_field_dropdown"),
            "displayText",
            item.get("stepinformation",""),
            "uri"),
        "get_work_leader_custom_field_dropdown":rail.find_first_by_attr_and_get_attr(
            rail.result("get_work_leader_custom_field_dropdown"),
            "displayText",
            item.get("workleader",""),
            "uri"
        ),
        "financecostcenter_code":item["financecostcentercode"],
        "legalemployer_code": item["legalemployercode"],
        "paygroup_code": item["paygroupcode"],
        "businessunit_code": item["businessunitcode"],
        "location_code": item["locationcode"],
        "location_state": item["locationstate"],
        "location_city": item["locationcity"],
        "department_code": item["departmentcode"],
        "financecostcentercode": rail.find_first_by_attr_and_get_attr(
            rail.result("get_finance_cost_centers"),
            "code",
            item["financecostcentercode"],
            "uri"
            ) if item["financecostcentercode"] else null,
        "legalemployercode": rail.find_first_by_attr_and_get_attr(
            rail.result("get_legal_employers"),
            "code",
            item["legalemployercode"],
            "uri"
            ) if item["legalemployercode"] else null,
        "paygroupcode": rail.find_first_by_attr_and_get_attr(
            rail.result("get_employee_type_paygroups"),
            "code",
            item["paygroupcode"],
            "uri"
            ) if item["paygroupcode"] else null,
        "businessunitcode": rail.find_first_by_attr_and_get_attr(
            rail.result("get_business_units"),
            "code",
            item["businessunitcode"],
            "uri"
            ) if item["businessunitcode"] else null,
        "locationcode": rail.find_first_by_attr_and_get_attr(
            rail.result("get_location_schedule"),
            "code",
            item["locationcode"],
            "uri"
            ) if item["locationcode"] else null,
        "locationstate": rail.find_first_by_attr_and_get_attr(
            rail.result("get_state_custom_field_dropdown"),
            "displayText",
            item["locationstate"],
            "uri"
            ),
        "locationcity": rail.find_first_by_attr_and_get_attr(
            rail.result("get_city_custom_field_dropdown"),
            "displayText",
            item["locationcity"],
            "uri"
            ),
        "departmentcode": rail.find_first_by_attr_and_get_attr(
            rail.result("get_department_group"),
            "code",
            item[
            "departmentcode"].split("|")[-1],
            "uri"
            ) if item["departmentcode"] else null,
        "timezone": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timezones"),
            "displayText",
            item["timezone"],
            "uri"
            ),
        "hourlypayratecurrency": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_currencies"),
            "name",
            item["hourlypayratecurrency"],
            "uri"),
        "hourlycostcurrency": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_currencies"),
            "name",
            item["hourlycostcurrency"],
            "uri"),
        **get_valid_user_config()

    }
