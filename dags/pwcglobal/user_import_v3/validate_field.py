import re
from pwcglobal.user_import_v3 import request_payload

email_regex = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
required = True

field_config_add = {
    # entry = tuple ( isrequired=Boolean, (optional)custom message - str,method)
    "companycode": (required, None),
    "country": (required, None),
    "emailaddress": (required, lambda x: 'Email address not present in payload' if not x['emailaddress'] \
                     else 'Email not updated since email field received incorrect format' \
                     if x['emailaddress'] and not re.fullmatch(email_regex, x['emailaddress']) else False),
    "employeeid": (required, None),
    "employeetype": (required, None),
    "enddate": (not required, lambda x: 'Incorrect date format received for Enddate' \
                if x['enddate'] and not request_payload.get_replicon_date(x['enddate']) else False),
    "firstname": (required, None),
    "holidaycalendar": (not required, lambda x: 'Holiday calendar not assigned since blank value received' \
                        if not x['holidaycalendar'] else False),
    "holidaycalenderuri": (not required, lambda x: f'Holiday calendar {x["holidaycalendar"]} not available in Replicon'
                           if x["holidaycalendar"] and not x['holidaycalenderuri'] else False),

    "homeofficelocation": (required, None),
    "isloginenabled": (required, None),
    "lastname": (required, None),
    "legalentity": (required, None),
    "loscode": (not required, None),
    "toil": (not required, None),
    "ftepercent": (not required, None),
    "ftepercenteffectivedate": (not required, None),
    "linemanagerpartyid":(not required, lambda x: 'Line manager not assigned since the Line manager is not provided' \
                   if not x['linemanagerpartyid'] else False),
    "payrule": (not required, None),
    "loginname": (required, None),
    "prefix": (not required, None),
    "grade": (required, None),
    "profilestatus": (not required, None),
    "resourcerole": (not required, None),
    "scheduletype": (not required, lambda x: 'Schedule type is not assigned since blank value received' \
                     if not x['scheduletype'] else False),
    "startdate":  (required, lambda x: 'Incorrect date format received for Startdate' \
                   if not request_payload.get_replicon_date(x['startdate']) else False),
    "supervisor": (not required, lambda x: 'Supervisor not assigned since the Supervisor ID is not provided' \
                   if not x['supervisor'] else
                   'Supervisor not assigned since the Supervisor partyID and user party ID are the same'
                   if x['supervisor'] and '||' in x['supervisor'] and \
                   x['supervisor'].split('||')[0] == x['supervisor'].split('||')[1] else False),
    "timesheetapprovalpath": (not required, lambda x: 'Timesheet approval path not assigned since blank value received' \
                              if not x['timesheetapprovalpath'] else False),
    "timeentryapprovalpath": (not required, lambda x: 'Timeentry approval path System Approval assigned since blank value received' \
                              if not x['timeentryapprovalpath'] else False),
    "timesheettemplate": (not required, lambda x: 'Timesheet template not assigned since blank value received'\
                          if not x['timesheettemplate'] else False),
    "timezone": (not required, None),
    "timesheetperiodtype": (not required, lambda x: 'Timesheet period type is not assigned since blank value received' \
                            if not x['timesheetperiodtype'] else False),
    "workdayid": (not required, lambda x: 'Workdayid is not assigned since blank value received' \
                  if not x['workdayid'] else False),
    "workweek": (not required, None),
    "timeoffpolicyuri": (not required, None),
    "employeetypegroupuri": (required, lambda x: f'Employee type {x["employeetype"]} not available or is disabled in Replicon'
                             if x["employeetype"] and not x["employeetypegroupuri"] else False),

    "timeofftypeuri": (not required, None),
    "toiltimeofftypeuri": (not required, None),
    "companycodegroupuri": (required, lambda x: f'Company code {x["companycode"]} not available in Replicon'
                            if x["companycode"] and not x["companycodegroupuri"] else False),

    "legalentitygroupuri": (required, lambda x: f'Legal entity {x["legalentity"]} not available in Replicon'
                            if x["legalentity"] and not x["legalentitygroupuri"] else False),
    "countriesgroupuri": (required, lambda x: f'Country {x["country"]} not available in Replicon'
                          if x["country"] and not x["countriesgroupuri"] else False),

    "timesheettemplateuri": (not required, lambda x: f'Timesheet template {x["timesheettemplate"]} not available in Replicon'
                             if x["timesheettemplate"] and not x["timesheettemplateuri"] else False),


    "timesheetapprovalpathuri": (not required, lambda x: f'Timesheet approval path {x["timesheetapprovalpath"]} not available in Replicon'
                                 if x["timesheetapprovalpath"] and not x["timesheetapprovalpathuri"] else False),

    "timezoneuri": (not required, lambda x: f'timezone {x["timezone"]} not available in Replicon'
                    if x["timezone"] and not x["timezoneuri"] else False),

    "supervisorlegalentityuri": (not required, lambda x: f'supervisor legal entity {x["supervisor"]} not available in Replicon'
                                 if x["supervisor"] and not x["supervisorlegalentityuri"] else False),

    "scheduleuri": (required, lambda x: f'schedule type {x["scheduletype"]} not available in Replicon'
                    if x["scheduletype"] and not x["scheduleuri"] else False),

    "gradedropdownuri": (required, lambda x: f'Grade {x["grade"]} not available in Replicon'
                         if x["grade"] and not x["gradedropdownuri"] else False),

    "profilestatusdropdownuri": (not required, lambda x: f'profile status {x["profilestatus"]} not available in Replicon'
                                 if x["profilestatus"] and not x["profilestatusdropdownuri"] else False),

    "permissionseturi": (required, lambda x: f'Permissoin set  {x["adduserpermission"]} not available in Replicon'
                         if not x["permissionseturi"] and x["adduserpermission"] else False),

    "timeentryapprovalpathuri": (not required, lambda x: f'Timeentry approval path {x["timeentryapprovalpath"]} not available in Replicon'
                                 if x["timeentryapprovalpath"] and not x["timeentryapprovalpathuri"] else False),
    "payruleuri": (not required, lambda x: f'payrule {x["payrule"]} not available in Replicon'
                                 if x["payrule"] and not x["payruleuri"] else False),
    "zerotimeuserpermissionseturi": (not required, lambda x: f'{x["zerotimepermission"]} not available in Replicon'
                                 if not x["zerotimeuserpermissionseturi"] and x["zerotimepermission"]\
                                      else False),
}


field_config_update = {
    "emailaddress": (not required, lambda x: 'Email not updated since email received is incorrect format'
                     if x['emailaddress'] and not re.fullmatch(email_regex, x['emailaddress']) else False),
    "enddate": (not required, lambda x: 'Enddate not updated since Enddate received is incorrect format'
                if x['enddate'] and not request_payload.get_replicon_date(x['enddate']) else False),
    "holidaycalenderuri": (not required, lambda x: f'Holiday calendar not updated sinnce Holiday calendar {x["holidaycalendar"]} not available in Replicon'
                           if x["holidaycalendar"] and not x['holidaycalenderuri'] else False),
    "loginname": (required, None),
    "grade": (not required, None),
    "startdate":  (not required, lambda x: 'Startdate not updated since Startdate received is incorrect format'
                   if not request_payload.get_replicon_date(x['startdate']) else False),
    "supervisor": (not required, lambda x:
                   'Supervisor not assigned since the Supervisor partyID and user party ID are the same'
                   if x['supervisor'] and '||' in x['supervisor'] and
                   x['supervisor'].split('||')[0] == x['supervisor'].split('||')[1] else False),
    "employeetypegroupuri": (not required, lambda x:
                             f'Employee type not updated since Employee type {x["employeetype"]} not available or is disabled in Replicon'
                             if x["employeetype"] and not x["employeetypegroupuri"] else False),
    "companycodegroupuri": (not required, lambda x: f'Company code not updated since Company code {x["companycode"]} not available in Replicon'
                            if x["companycode"] and not x["companycodegroupuri"] else False),

    "legalentitygroupuri": (not required, lambda x: f'Legal entity not updated since Legal entity {x["legalentity"]} not available in Replicon'
                            if x["legalentity"] and not x["legalentitygroupuri"] else False),
    "countriesgroupuri": (not required, lambda x: f'Country not updated since Country {x["country"]} not available in Replicon'
                          if x["country"] and not x["countriesgroupuri"] else False),

    "timesheettemplateuri": (not required, lambda x:
                             f'Timesheet template not updated since Timesheet template {x["timesheettemplate"]} not available in Replicon'
                             if x["timesheettemplate"] and not x["timesheettemplateuri"] else False),

    "timesheetapprovalpathuri": (not required, lambda x:
                                 f'Timesheet approval path not updated since Timesheet approval path {x["timesheetapprovalpath"]} not available in Replicon'
                                 if x["timesheetapprovalpath"] and not x["timesheetapprovalpathuri"] else False),

    "timezoneuri": (not required, lambda x: f'timezone not updated since timezone {x["timezone"]} not available in Replicon'
                    if x["timezone"] and not x["timezoneuri"] else False),

    "supervisorlegalentityuri": (not required, lambda x: f'supervisor not updated since supervisor legal entity {x["supervisor"]} not available in Replicon'
                                 if x["supervisor"] and not x["supervisorlegalentityuri"] else False),

    "scheduleuri": (not required, lambda x: f'Office Schedule type not updated since Office Schedule type {x["scheduletype"]} not available in Replicon'
                    if x["scheduletype"] and not x["scheduleuri"] else False),

    "gradedropdownuri": (required, lambda x: f'Grade not updated since grade {x["grade"]} not available in Replicon'
                         if x["grade"] and not x["gradedropdownuri"] else False),

    "profilestatusdropdownuri": (not required, lambda x: f'profile status not updated since profile status {x["profilestatus"]} not available in Replicon'
                                 if x["profilestatus"] and not x["profilestatusdropdownuri"] else False),

    "permissionseturi": (not required, lambda x: f'Permission set not updated since Permission set  {x["adduserpermission"]} not available in Replicon'
                         if not x["permissionseturi"] and x["adduserpermission"] else False),
    "toiltimeofftypeuri": (not required, None),
    "timeentryapprovalpathuri": (not required, lambda x: f'Timeentry approval path {x["timeentryapprovalpath"]} not available in Replicon'
                                 if x["timeentryapprovalpath"] and not x["timeentryapprovalpathuri"] else False),
    "payruleuri": (not required, lambda x: f'payrule {x["payrule"]} not available in Replicon'
                                 if x["payrule"] and not x["payruleuri"] else False),
    "zerotimeuserpermissionseturi": (not required, lambda x: f'{x["zerotimepermission"]} not available in Replicon'
                                 if not x["zerotimeuserpermissionseturi"] and x["zerotimepermission"]\
                                      else False),

}


def validate_field(field_config):
    data = request_payload.get_conf()
    errors = []
    for field_name in data:
        if field_name in field_config:
            (is_required, custom_message) = field_config[field_name]
            field_value = data[field_name]
            error = None
            if custom_message and callable(custom_message):
                error = custom_message(data)
            elif is_required and not field_value:
                error = f'{field_name} is not present in payload'
            if error:
                errors.append(
                    {'field_name': field_name, 'log_type': 'Exception' if is_required else 'Warning', 'message': error})

    return errors
