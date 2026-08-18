from datetime import datetime
from os.path import getsize
import re
from uuid import uuid4
import rail
from dxctechnology.cwf_user_profile.user_profile_sync.mapper.dxc_cwf_location_list import cwf_location_list


null = None


def do_has_file_content():
    with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
        return getsize(artifact.local_filename) > 0


def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_replicon_date(date_str):
    if not date_str:
        return null
    try:
        date = datetime.fromisoformat(date_str)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return null


def create_costcenter_in_replicon(item):
    return {
        'modifications': {
            'name': item['afmcostcenter'],
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def get_all_employeetypes_payload():
    return {
        'page': 1,
        'pagesize': 1000000,
        'columnUris': [
            'urn:replicon:employee-type-group-list-column:employee-type-group',
            'urn:replicon:employee-type-group-list-column:full-path'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:employee-type-group-list-filter:text'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': 'Contractor'
                }
            }
        }
    }


def get_userprofile_child_dag_conf(item):

    filtered_mapper = list(filter(lambda x: x['workertype'] == item['workertype'] and x['financesystem']
                           == item['financesystem'], rail.result('get_mapper_data'))) if rail.result('get_mapper_data') else []

    if not filtered_mapper:
        workertype_exception = 'Required Worker type is not present in mapper' if len(
            [x for x in rail.result('get_mapper_data') if x['workertype'] == item['workertype']]) == 0 else ''
        financesystem_exception = 'Required Finance System is not present in mapper' if len(
            [x for x in rail.result('get_mapper_data') if x['financesystem'] == item['financesystem']]) == 0 else ''
        skipped_message = ','.join(
            [workertype_exception, financesystem_exception])

        return {**dict(item.items()), **{'skip_user': 'yes', 'skipped_message': skipped_message}}

    employee_type = rail.find_first_by_attr_and_get_attr(
        filtered_mapper, 'type', 'Employee Type', 'value')
    timesheet_template = rail.find_first_by_attr_and_get_attr(
        filtered_mapper, 'type', 'Timesheet Template', 'value')
    work_week_mapper = rail.find_first_by_attr_and_get_attr(
        filtered_mapper, 'type', 'Work Week', 'value').split('|')
    time_entry_approval_path = rail.find_first_by_attr_and_get_attr(
        filtered_mapper, 'type', 'Time Entry Approval Path', 'value')

    activities = [x['value']
                  for x in filtered_mapper if x['type'] == 'Activity']
    activity_uris = [x['uri'] for x in rail.result(
        'get_all_activities') if x['name'] in activities] if activities else []

    authentication = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'Authentication', 'value').split('|')
    end_user_permission = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'End User Permission', 'value')
    supervisor_permission = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'Supervisor User Permission', 'value')
    supervisor_end_user_permission = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'Supervisor End User Permission', 'value')
    product_details = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'Product', 'value').split('|')
    language_details = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'Language', 'value').split('|')

    return {
        **dict(item.items()),
        **{
            'skip_user': 'no',
            'timetracking': item['timetracking'].lower() if item['timetracking'] else null,
            'employee_type': employee_type,
            'office_schedule': rail.find_first_by_attr_and_get_attr(filtered_mapper, 'type', 'Office Schedule', 'value'),
            'authentication': authentication[0].strip(),
            'activities': ','.join(activities) if activities else null,
            'timesheet_approval_path': rail.find_first_by_attr_and_get_attr(filtered_mapper, 'type', 'Timesheet Approval Path', 'value'),
            'timesheet_template': timesheet_template,
            'work_week': work_week_mapper[0].strip(),
            'work_week_uri': work_week_mapper[-1].strip(),
            'activity_uris': ','.join(activity_uris) if activity_uris else null,
            'authentication_uri': authentication[-1].strip(),
            'employee_type_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetypes'), 'fullpath', employee_type, 'uri'),
            'finance_system_uri': rail.result('get_financesystem_workertype_customfield')['financesystemuri'],
            'finance_system_value_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_financesystem_customfield_dropdowns'), 'displayText', item['financesystem'], 'uri'),
            'worker_type_uri': rail.result('get_financesystem_workertype_customfield')['workertypeuri'],
            'worker_type_value_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_workertype_customfield_dropdowns'), 'displayText', item['workertype'], 'uri'),
            'contractenddate': item['contractenddate'] if item['contractenddate'] else null,
            'end_user_permission_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionsets'), 'name', end_user_permission, 'uri'),
            'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionsets'), 'name', supervisor_permission, 'uri'),
            'product': product_details[0].strip().split(', ') if product_details[0].strip() else null,
            'product_uri': product_details[-1].strip().split(', ') if product_details[-1].strip() else null,
            'language': language_details[0].strip(),
            'language_uri': language_details[-1].strip(),
            'timesheet_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_policysets'), 'name', timesheet_template, 'uri'),
            'supervisor_end_user_permission': supervisor_end_user_permission,
            'supervisor_end_user_permission_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionsets'), 'name', supervisor_end_user_permission, 'uri'),
            'supervisor_permission': supervisor_permission,
            'end_user_permission_name': end_user_permission,
            'company_parent_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_companycodes'), 'displayText', item['companycode'], 'uri'),
            'timesheet_period': rail.find_first_by_attr_and_get_attr(
                filtered_mapper, 'type', 'Timesheet Period', 'value'),
            'timeentry_approval_path': time_entry_approval_path.split('|')[-1].strip() if time_entry_approval_path else null,
            'perner_udf_uri': rail.result('get_financesystem_workertype_customfield')['pernerudfuri']
        }}


def validate_email(email_address):
    email_regex = re.compile(
        r'^([a-zA-Z0-9_.+-])+\@(([a-zA-Z0-9-])+\.)+([a-zA-Z0-9]{2,4})+$')
    return 'Email id is not available' if not email_address \
        else 'Email not in valid format' \
        if email_address and not re.fullmatch(email_regex, email_address) else False


def get_search_user_param(dag_run):
    is_empid_present = rail.result('is_employeeid_present')
    login_name_check = (is_empid_present ==
                        'get_user_data_from_loginname') if is_empid_present else False
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:login-name',
            'urn:replicon:user-list-column:employee-id'
        ],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name' if login_name_check else 'urn:replicon:user-list-filter:text'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': dag_run.conf['emailaddress'] if login_name_check else dag_run.conf['hpid']
                }
            }
        }
    }


def get_adduser_updateuser_conf(dag_run, item):

    path_to_search = 'loginname'
    attribute_value_to_search = dag_run.conf['emailaddress']
    user_search_task_id = 'get_user_data_from_loginname'

    if rail.result('is_employeeid_not_unique') and rail.result('is_employeeid_not_unique') == 'update_userprofile_child_dag_emp_id':
        path_to_search = 'employeeid'
        attribute_value_to_search = dag_run.conf['hpid']
        user_search_task_id = 'get_user_data_from_employeeid'

    return {
        **{k: v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')},
        **{
            'user_uri': rail.find_first_by_attr_and_get_attr(rail.result(user_search_task_id), path_to_search, attribute_value_to_search, 'uri'),
            'log': rail.result('create_log')
        }
    }


def get_schedule_policy(office_schedule):
    if office_schedule:
        return [
            {
                'schedulePolicy': {
                    'name': office_schedule,
                    'officeSchedule': {
                        'name': office_schedule
                    },
                    'scheduleTypeUri': 'urn:replicon:schedule-type:office-schedule'
                }
            }
        ]
    return [
        {
            'schedulePolicy': {
                'name': '7.5 hours/day, Fri, Sa off',
                'officeSchedule': {
                    'name': '7.5 hours/day, Fri, Sa off'
                },
                'scheduleTypeUri': 'urn:replicon:schedule-type:office-schedule'
            }
        }
    ]


# pylint:disable=too-many-arguments
def get_custom_field_values(
        workertype_uri, workertype_value_uri, financesystem_uri, financesystem_value_uri, pernerudf_uri=null, hpid=null):

    custom_field_values = [{
        'customField': {
            'uri': pernerudf_uri
        },
        'text': hpid
    }] if pernerudf_uri else []
    if workertype_value_uri:
        custom_field_values.append({
            'customField': {
                'uri': workertype_uri
            },
            'dropDownOption': {
                'uri': workertype_value_uri
            }
        })
    if financesystem_value_uri:
        custom_field_values.append({
            'customField': {
                'uri': financesystem_uri
            },
            'dropDownOption': {
                'uri': financesystem_value_uri
            }
        })
    return custom_field_values


def get_put_user_payload(dag_run, should_add_emailaddress):

    cwf_location_name = rail.find_first_by_attr_and_get_attr(
        cwf_location_list, 'country', dag_run.conf['afmcostcenter'][0:2], 'name') if dag_run.conf.get(
        'afmcostcenter') and dag_run.conf.get('financesystem') == 'ES' else null

    return {
        'user': {
            'target': {
                'loginName': dag_run.conf['emailaddress']
            },
            'firstname': dag_run.conf['firstname'],
            'lastname': dag_run.conf['lastname'],
            'emailAddress': dag_run.conf['emailaddress'] if should_add_emailaddress else null,
            'employeeId': dag_run.conf['hpid'],
            'schedulePolicySchedule': get_schedule_policy(dag_run.conf['office_schedule']),
            'workWeekStartDayUri': dag_run.conf['work_week_uri'] if dag_run.conf['work_week_uri'] else null,
            'employmentDateRange': {
                'startDate': rail.result('get_contract_startdate_enddate')['start_date'],
                'endDate': rail.result('get_contract_startdate_enddate')['end_date']
            },
            'securityConfiguration': {
                'enabledAuthenticationTypeUris': [
                    dag_run.conf['authentication']
                ],
                'isLoginEnabled': 'false' if dag_run.conf['timetracking'].lower() == 'no' else 'true',
                'loginName': dag_run.conf['emailaddress'],
                'SSOName': dag_run.conf['emailaddress']
            },
            'permissionSets': [{
                'uri': dag_run.conf['end_user_permission_uri']
            }] if dag_run.conf['end_user_permission_uri'] else null,
            'policySets': [{
                'uri': dag_run.conf['timesheet_uri']
            }] if dag_run.conf['timesheet_uri'] else null,
            'timesheetApprovalPath': {
                'name': dag_run.conf['timesheet_approval_path']
            } if dag_run.conf['timesheet_approval_path'] else null,
            'customFieldValues': get_custom_field_values(dag_run.conf['worker_type_uri'], dag_run.conf['worker_type_value_uri'],
                                                         dag_run.conf['finance_system_uri'], dag_run.conf['finance_system_value_uri'],
                                                         dag_run.conf['perner_udf_uri'], dag_run.conf['hpid']),
            'assignedActivities': list(map(lambda x: {
                'name': x
            }, list(set(dag_run.conf['activities'].split(','))))) if dag_run.conf['activities'] else [],
            'locationSchedule': [
                {
                    'location': {
                        'name': cwf_location_name
                    }
                }
            ] if cwf_location_name else null,
            'divisionSchedule': [
                {
                    'division': {
                        'uri': dag_run.conf['company_parent_uri'],
                    }
                }
            ] if dag_run.conf['company_parent_uri'] else null,
            'costCenterSchedule': [
                {
                    'costCenter': {
                        'name': dag_run.conf['afmcostcenter']
                    },
                }
            ] if dag_run.conf['afmcostcenter'] else null,
            'employeeTypeGroupSchedule': [
                {
                    'employeeTypeGroup': {
                        'uri': dag_run.conf['employee_type_uri'],
                    }
                }
            ] if dag_run.conf['employee_type_uri'] else null,
            'timesheetPeriodSchedule': [
                {
                    'timesheetPeriod': {
                        'uri': null,
                        'name': dag_run.conf['timesheet_period']
                    },
                    'effectiveDate': null
                }
            ] if dag_run.conf['timesheet_period'] else null,
            'displayNameParameter': {
                'displayName': f"{dag_run.conf['lastname']}, {dag_run.conf['firstname']} {dag_run.conf['hpid']} {dag_run.conf['emailaddress']}"
            }
        }
    }


def get_data_for_supervisor_payload(dag_run):
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:login-name',
            'urn:replicon:user-list-column:employee-id',
            'urn:replicon:user-list-column:enabled'
        ],
        'filterExpression': {
            'leftExpression': {
                'leftExpression': {
                    'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                },
                'operatorUri': 'urn:replicon:filter-operator:text-search',
                'rightExpression': {
                    'value': {
                        'text': dag_run.conf['manageremail']
                    }
                }
            },
            'operatorUri': 'urn:replicon:filter-operator:and',
            'rightExpression': {
                'leftExpression': {
                    'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                },
                'operatorUri': 'urn:replicon:filter-operator:text-search',
                'rightExpression': {
                    'value': {
                        'text': dag_run.conf['managerid']
                    }
                }
            }
        }
    }


def is_enddate_less_than_startdate(start_date, end_date, fmt=null):

    if fmt:
        return datetime.fromisoformat(end_date) < datetime.strptime(
            f"{start_date['year']}-{start_date['month']}-{start_date['day']}+0000", fmt) if end_date else False
    return datetime.fromisoformat(end_date) < datetime.fromisoformat(start_date) if end_date else False


def get_update_employee_date_range_payload(user_uri, end_date, start_date=null):
    return {
        'userUri': user_uri,
        'dateRange': {
            'startDate': get_replicon_date(start_date) if start_date else rail.result('bulk_get_user')['userDetails']['employmentDateRange']['startDate'],
            'endDate': get_replicon_date(end_date) if end_date else null
        }
    }


def get_attribute_to_update(attribute_value, attribute_keys):
    attribute_value_from_userdetails = rail.result(
        'bulk_get_user').get(attribute_keys[0], {}).get(attribute_keys[1])

    if attribute_value_from_userdetails:
        if attribute_value and attribute_value.lower() != attribute_value_from_userdetails.lower():
            return attribute_value
    else:
        return attribute_value

    return null


def supress_none(val):
    return '' if val is null else val


def get_displayname(firstname_to_apply, lastname_to_apply, employeeid_to_apply, emailaddress_to_apply):
    # pylint:disable=line-too-long
    return f"{supress_none(lastname_to_apply)}, {supress_none(firstname_to_apply)} {supress_none(employeeid_to_apply)} {supress_none(emailaddress_to_apply)}"


# pylint:disable=too-many-arguments
def get_custom_field_values_to_update(
        financesystem, workertype, workertype_uri, workertype_value_uri, financesystem_uri, financesystem_value_uri, bulkgetuser_customfields):

    financesystem_value_to_add = null
    workertype_value_to_add = null

    user_financesystem_customfield = rail.find_first_by_attr_and_get_attr(
        bulkgetuser_customfields, 'customField.displayText', 'Finance System (CWF)', 'text')
    user_workertype_customfield = rail.find_first_by_attr_and_get_attr(
        bulkgetuser_customfields, 'customField.displayText', 'Worker Type', 'text')

    if financesystem_value_uri and financesystem.lower() != (
            user_financesystem_customfield.lower() if user_financesystem_customfield else null):
        financesystem_value_to_add = financesystem_value_uri
    if workertype_value_uri and workertype.lower() != (
            user_workertype_customfield.lower() if user_workertype_customfield else null):
        workertype_value_to_add = workertype_value_uri

    return get_custom_field_values(workertype_uri, workertype_value_to_add, financesystem_uri, financesystem_value_to_add)


def get_costcenter_schedule_to_update(afm_costcenter, user_costcenters):
    current_user_costcenter = user_costcenters[0] if user_costcenters else null
    current_user_costcenter_displayname = current_user_costcenter[
        'costCenter']['costCenter']['displayText'] if current_user_costcenter and current_user_costcenter.get(
            'costCenter') else null
    if not current_user_costcenter_displayname or (
            current_user_costcenter_displayname and current_user_costcenter_displayname.lower() != afm_costcenter.lower()):
        return {
            'userCostCenterScheduleModificationOptionUri': 'urn:replicon:schedule-modification-option:update-schedule-over-date-range',
            'updateCostCenterScheduleOverDateRange': {
                'replacementCostCenterScheduleEntries': [
                    {
                        'costCenter': {
                            'name': afm_costcenter
                        },
                        'effectiveDate': get_today_date()
                    }
                ]
            }
        }

    return null


def get_division_schedule_to_update(company_code, company_parent_uri, user_divisions):
    current_user_division = user_divisions[0] if user_divisions else null
    current_user_division_displayname = current_user_division[
        'division']['division']['displayText'] if current_user_division and current_user_division.get(
            'division') else null
    if not current_user_division_displayname or (
            current_user_division_displayname and current_user_division_displayname.lower() != company_code.lower()):
        return {
            'userDivisionScheduleModificationOptionUri': 'urn:replicon:schedule-modification-option:update-schedule-over-date-range',
            'updateDivisionScheduleOverDateRange': {
                'replacementDivisionScheduleEntries': [
                    {
                        'division': {
                            'uri': company_parent_uri
                        },
                        'effectiveDate': get_today_date()
                    }
                ]
            }
        }

    return null


def get_location_schedule_to_update(financesystem, afmcostcenter, user_locations, user_costcenters):
    current_user_location = user_locations[0] if user_locations else null
    current_user_location_displayname = current_user_location['location']['location']['displayText'] if current_user_location and current_user_location.get(
        'location') else null
    if financesystem == 'C1' and current_user_location_displayname:
        return {
            'userLocationScheduleModificationOptionUri': 'urn:replicon:schedule-modification-option:update-schedule-over-date-range',
            'updateLocationScheduleOverDateRange': {
                'replacementLocationScheduleEntries': [
                    {
                        'location': {
                            'name': 'No Location'
                        },
                        'effectiveDate': rail.result('get_effective_group_date')
                    }
                ]
            }
        }
    if financesystem == 'ES' and afmcostcenter:
        current_user_costcenter = user_costcenters[0] if user_costcenters else null
        current_user_costcenter_displayname = current_user_costcenter[
            'costCenter']['costCenter']['displayText'] if current_user_costcenter and current_user_costcenter.get(
                'costCenter') else null
        if not current_user_costcenter_displayname or (
                current_user_costcenter_displayname and current_user_costcenter_displayname.lower() != afmcostcenter.lower()):
            cwf_location_name = rail.find_first_by_attr_and_get_attr(
                cwf_location_list, 'country', afmcostcenter[0:2], 'name')
            if not current_user_location_displayname or (
                    current_user_location_displayname and current_user_location_displayname != cwf_location_name):
                return {
                    'userLocationScheduleModificationOptionUri': 'urn:replicon:schedule-modification-option:update-schedule-over-date-range',
                    'updateLocationScheduleOverDateRange': {
                        'replacementLocationScheduleEntries': [
                            {
                                'location': {
                                    'name': cwf_location_name
                                },
                                'effectiveDate': rail.result('get_effective_group_date')
                            }
                        ]
                    }
                }

    return null


def get_timesheetperiod_schedule_to_update(timesheet_period, timesheetperiod_schedules, user_start_date):

    least_day_diff = null
    if timesheetperiod_schedules:
        current_timesheetperiod_schedules = []
        for item in timesheetperiod_schedules:
            effective_datetime = datetime.strptime(
                f"{item['effectiveDate']['year']}/{item['effectiveDate']['month']}/{item['effectiveDate']['day']}", '%Y/%m/%d') if item.get(
                    'effectiveDate', {}).get('day') else datetime.strptime(
                f"{user_start_date['year']}/{user_start_date['month']}/{user_start_date['day']}", '%Y/%m/%d')
            day_diff = datetime.now() - effective_datetime
            current_timesheetperiod_schedules.append({
                'effective_date': effective_datetime,
                'display_text': item['timesheetPeriod']['displayText'],
                'day_diff': day_diff
            })
        least_day_diff = min(
            current_timesheetperiod_schedules, key=lambda x: x['day_diff'])
    if not least_day_diff or (least_day_diff and least_day_diff['display_text'] != timesheet_period):
        return {
            'userTimesheetPeriodScheduleModificationOptionUri': 'urn:replicon:schedule-modification-option:update-schedule-over-date-range',
            'updateTimesheetPeriodScheduleOverDateRange': {
                'replacementTimesheetPeriodScheduleEntries': [
                    {
                        'timesheetPeriod': {
                            'name': timesheet_period
                        },
                        'effectiveDate': rail.result('get_effective_group_date')
                    }
                ]
            }
        }
    return null


def apply_user_modifications2(dag_run, should_add_emailaddress):
    emailaddress_bulk_get_user = rail.result(
        'bulk_get_user')['userDetails']['emailAddress']
    loginname_bulk_get_user = rail.result(
        'bulk_get_user')['securityConfiguration']['loginName']

    firstname_to_apply = get_attribute_to_update(
        dag_run.conf['firstname'], ('userDetails', 'firstName')) if dag_run.conf['firstname'] else null

    lastname_to_apply = get_attribute_to_update(
        dag_run.conf['lastname'], ('userDetails', 'lastName')) if dag_run.conf['lastname'] else null

    employeeid_to_apply = get_attribute_to_update(
        dag_run.conf['hpid'], ('userDetails', 'employeeId')) if dag_run.conf['hpid'] else null

    timesheetapprovalpath_to_apply = get_attribute_to_update(
        dag_run.conf['timesheet_approval_path'], ('timesheetApprovalPath', 'displayText')) if dag_run.conf['timesheet_approval_path'] else null

    emailaddress_to_apply = dag_run.conf[
        'emailaddress'] if should_add_emailaddress and (not emailaddress_bulk_get_user or dag_run.conf[
            'emailaddress'].lower() != emailaddress_bulk_get_user.lower()) else null

    return {
        'user': {
            'uri': dag_run.conf['user_uri']
        },
        'modifications': {
            'locationScheduleToApply': get_location_schedule_to_update(
                dag_run.conf['financesystem'], dag_run.conf['afmcostcenter'], rail.result(
                    'get_effective_group_membership')['locations'], rail.result(
                        'get_effective_group_membership')['costCenters']),
            'divisionScheduleToApply': get_division_schedule_to_update(
                dag_run.conf['companycode'], dag_run.conf['company_parent_uri'], rail.result('get_effective_group_membership')['divisions']) if dag_run.conf[
                    'companycode'] and dag_run.conf['company_parent_uri'] else null,
            'costCenterScheduleToApply': get_costcenter_schedule_to_update(
                dag_run.conf['afmcostcenter'], rail.result('get_effective_group_membership')['costCenters']) if dag_run.conf[
                    'afmcostcenter'] else null,
            'timesheetPeriodScheduleToApply': get_timesheetperiod_schedule_to_update(
                dag_run.conf['timesheet_period'], rail.result('bulk_get_user')['timesheetPeriodSchedule'], rail.result(
                    'bulk_get_user')['userDetails']['employmentDateRange']['startDate']),
            'timesheetApprovalPathToApply': {
                'name': timesheetapprovalpath_to_apply
            } if timesheetapprovalpath_to_apply else null,
            'securitySettingsToApply': {
                'loginEnabled': 'true',
                'forcePasswordChanged': 'false',
                'loginName': dag_run.conf['emailaddress'],
                'ssoName': dag_run.conf['emailaddress'],
                'enabledAuthenticationTypeUris': [
                    'urn:replicon:user-authentication-type:sso'
                ],
                'userSSONameModificationOptionUri': 'urn:replicon:sso-name-modification-option:login-name'
            } if dag_run.conf['emailaddress'] != loginname_bulk_get_user else null,
            'customFieldValuesToApply': get_custom_field_values_to_update(
                dag_run.conf['financesystem'], dag_run.conf['workertype'],
                dag_run.conf['worker_type_uri'], dag_run.conf['worker_type_value_uri'], dag_run.conf[
                    'finance_system_uri'], dag_run.conf['finance_system_value_uri'], rail.result(
                        'bulk_get_user')['userDetails']['customFieldValues']),
            'userDetailsToApply': {
                'firstName': firstname_to_apply,
                'lastName': lastname_to_apply,
                'emailAddress': {
                    'emailAddress': emailaddress_to_apply
                } if emailaddress_to_apply else null,
                'employeeId': {
                    'employeeId': employeeid_to_apply
                } if employeeid_to_apply else null,
                'displayNameParameter': {
                    'displayName': get_displayname(dag_run.conf['firstname'], dag_run.conf['lastname'], dag_run.conf['hpid'], dag_run.conf['emailaddress'])
                } if firstname_to_apply or lastname_to_apply or employeeid_to_apply or emailaddress_to_apply else null
            }
        },
        'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
    }


def should_update_employeetype(employee_type_uri, user_employeetypes):
    current_employeetype = user_employeetypes[0] if user_employeetypes else null

    current_employeetype_displayname = current_employeetype['employeeType']['employeeType']['displayText'] if current_employeetype and current_employeetype.get(
        'employeeType') else null

    current_employeetype_uri = current_employeetype['employeeType']['employeeType']['uri'] if current_employeetype and current_employeetype.get(
        'employeeType') else null

    return not current_employeetype_displayname or (
        current_employeetype_uri and current_employeetype_uri != employee_type_uri and current_employeetype_displayname != 'Leveraged Non-Hrly AC')


def update_employeetype(dag_run):

    return {
        'user': {
            'uri': dag_run.conf['user_uri']
        },
        'modifications': {
            'employeeTypeGroupScheduleToApply': {
                'userEmployeeTypeGroupScheduleModificationOptionUri': 'urn:replicon:schedule-modification-option:update-schedule-over-date-range',
                'updateEmployeeTypeGroupScheduleOverDateRange': {
                    'replacementEmployeeTypeGroupScheduleEntries': [
                        {
                            'employeeTypeGroup': {
                                'uri': dag_run.conf['employee_type_uri']
                            },
                            'effectiveDate': rail.result('get_effective_group_date')
                        }
                    ]
                }
            }
        },
        'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
    }


def should_update_timesheet_template(dag_run):

    if dag_run.conf['timesheet_uri']:

        user_employeetypes = rail.result('get_effective_group_membership')[
            'employeeTypes']

        current_employeetype = user_employeetypes[0] if user_employeetypes else null

        current_employeetype_displayname = current_employeetype[
            'employeeType']['employeeType']['displayText'] if current_employeetype and current_employeetype.get(
                'employeeType') else null

        timesheet_template = dag_run.conf['timesheet_template']

        user_timesheet_template_name = rail.result('bulk_get_user')['timesheetTemplate'].get(
            'name') if rail.result('bulk_get_user')['timesheetTemplate'] else null

        if timesheet_template and (not user_timesheet_template_name or (
                user_timesheet_template_name and timesheet_template != user_timesheet_template_name)):
            return current_employeetype_displayname != 'Leveraged Non-Hrly AC'

    return False


def should_update_workweek_start_day(dag_run):

    user_workweek_startday_uri = rail.result(
        'bulk_get_user')['userDetails']['workWeekStartDay'].get('uri')
    return bool(dag_run.conf['work_week_uri']) and dag_run.conf['work_week_uri'] != user_workweek_startday_uri


def get_supervisor_child_dag_conf(item):

    supervisor_permission = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'Supervisor User Permission', 'value')
    supervisor_end_user_permission = rail.find_first_by_attr_and_get_attr(
        rail.result('get_mapper_data'), 'type', 'Supervisor End User Permission', 'value')
    supervisor_loginname = item['properties']['supervisor_loginname'].split(
        '|')

    return {
        **dict(item['properties'].items()),
        'manageremail': supervisor_loginname[0].strip(),
        'managerid': supervisor_loginname[-1].strip(),
        'supervisor_permission': supervisor_permission,
        'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_permissionsets'), 'name', supervisor_permission, 'uri'),
        'supervisor_end_user_permission': supervisor_end_user_permission,
        'supervisor_end_user_permission_uri': rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_permissionsets'), 'name', supervisor_end_user_permission, 'uri')
    }
