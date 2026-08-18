from datetime import datetime
import hashlib
from uuid import uuid4
import rail
from technicolorg3.user_import.utils.python_callable_method import get_groupuri_from_mapper


null = None


def get_columns():
    return {
        'Global ID': 'globalid',
        'Last Name': 'lastname',
        'First Name': 'firstname',
        'Employee Status': 'employeestatus',
        'Email': 'email',
        'Manager Global ID': 'managerglobalid',
        'Reference Job Code': 'referencejobcode',
        'Job Title': 'jobtitle',
        'Reference Job Domain': 'referencejobdomain',
        'Reference Job Family': 'referencejobfamily',
        'Reference Job SubFamily': 'referencejobsubfamily',
        'Business Group Code': 'businessgroupcode',
        'Business Group Name': 'businessgroupname',
        'Business Division Code': 'businessdivisioncode',
        'Business Division Name': 'businessdivisionname',
        'Business Unit Code': 'businessunitcode',
        'Business Unit Name': 'businessunitname',
        'Country': 'country',
        'Work Location': 'worklocation',
        'Creative/Non Creative': 'creativenoncreative',
        'Standard Weekly Hours': 'standardweeklyhours',
        'Department': 'department',
        'Legal Entity ID': 'legalentityid',
        'Legal Entity Name': 'legalentityname',
        'Cost Centre Code': 'costcentercode',
        'Cost Centre Name': 'costcentername',
        'FTE': 'fte',
        'Encoded': 'encoded',
        'Service Line Code': 'servicelinecode',
        'Service Line Name': 'servicelinename',
        'Job Category': 'jobcategory',
    }


def supress_none(val):
    return '' if val is null else val


def get_department_name(business_group_name, business_division_name, service_line_name, business_unit_name):
    department_fields = ['Technicolor', business_group_name,
                         business_division_name, service_line_name]
    department_fields.extend(['MPC - Advertising', business_unit_name]
                             ) if business_unit_name == 'MikrosMPC' else department_fields.append(business_unit_name)
    return rail.smartjoin_by_delim(department_fields, '|') if department_fields else ''


def get_service_center(reference_job_domain, reference_job_family, reference_job_subfamily):
    servicecenter_fields = [reference_job_domain,
                            reference_job_family, reference_job_subfamily]
    return rail.smartjoin_by_delim(servicecenter_fields, '|') if servicecenter_fields else ''


def get_location(country, work_location):
    location_fields = [country, work_location]
    return rail.smartjoin_by_delim(location_fields, '|') if location_fields else ''


def get_md5(item):
    columns = get_columns().values()
    column_string_vals = [str(v) for k, v in item.items() if k in columns]
    input_reference = hashlib.md5(''.join(column_string_vals).encode('utf-8'))
    return input_reference.hexdigest()


def get_row_data_from_file(item):
    row_data = []
    for k, v in item.items():
        if k == 'employeestatus':
            row_data.append(v.strip() if v else 'Inactive')
        elif k == 'managerglobalid':
            row_data.append(v.strip().lower() if v else '')
        elif k == 'encoded':
            row_data.append(get_md5(item))
        else:
            row_data.append(v.strip() if v else '')
    return row_data


def get_row_data(item):

    row_data = get_row_data_from_file(item)

    row_data.append(get_department_name(supress_none(item['businessgroupname']), supress_none(
        item['businessdivisionname']), supress_none(item['servicelinename']), supress_none(item['businessunitname'])))

    row_data.extend([supress_none(item['businessgroupcode']), supress_none(
        item['businessdivisioncode']), supress_none(item['servicelinecode']), supress_none(item['businessunitcode'])])

    row_data.append(get_service_center(supress_none(item['referencejobdomain']), supress_none(
        item['referencejobfamily']), supress_none(item['referencejobsubfamily'])))

    row_data.append(get_location(supress_none(
        item['country']), supress_none(item['worklocation'])))

    return row_data


def get_replicon_groups_list(group):

    group_column_uri = {
        'costcenter': [
            'urn:replicon:cost-center-list-column:cost-center',
            'urn:replicon:cost-center-list-column:full-path'
        ],
        'servicecenter': [
            'urn:replicon:service-center-list-column:service-center',
            'urn:replicon:service-center-list-column:full-path'
        ],
        'division': [
            'urn:replicon:division-list-column:division',
            'urn:replicon:division-list-column:full-path'
        ],
        'location': [
            'urn:replicon:location-list-column:location',
            'urn:replicon:location-list-column:full-path'
        ],
        'department': [
            'urn:replicon:department-group-list-column:department-group',
            'urn:replicon:department-group-list-column:full-path'
        ]
    }

    return {
        'page': 1,
        'pagesize': 1000000,
        'columnUris': group_column_uri[group]
    }


def create_costcenter_payload(dag_run):

    return {
        'modifications': {
            'name': dag_run.conf['costcenter'],
            'codeToApply': {
                'value': dag_run.conf['costcentercode']
            },
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def get_process_other_dept_levels_conf(dag_run, index):

    required_department_fullpath = null
    parent_department_fullpath = null
    required_department_name = rail.result('get_department_params')[
        'required_name_array'][index].strip()

    dag_run_conf = dag_run.conf
    level = index + 1

    if level == 1:
        required_department_fullpath = f"Technicolor|{rail.result('get_department_params')['required_name_array'][index]}"
    elif level not in (1, 6):
        required_department_fullpath = f"Technicolor|{rail.smartjoin_by_delim(rail.result('get_department_params')['required_name_array'][:level], '|')}"
        parent_department_fullpath = f"Technicolor|{rail.smartjoin_by_delim(rail.result('get_department_params')['required_name_array'][:level-1], '|')}"
    else:
        required_department_name = dag_run_conf['departmentgroup'].split(
            '|')[-1].strip()
        required_department_fullpath = dag_run_conf['departmentgroup']
        parent_department_fullpath = dag_run_conf['departmentgroup'].split('|')[
            :-1]
    return {
        'level': level,
        'required_department_name': required_department_name,
        'required_department_fullpath': required_department_fullpath,
        'parent_department_fullpath': parent_department_fullpath,
        'gmbh_groups_log': dag_run_conf['gmbh_groups_log'],
        'company_department_uri': dag_run_conf['company_department_uri'],
        'codes_to_be_added': rail.result('get_department_params')['codes_to_be_added']
    }


def create_departmentgroup_level1_payload(dag_run):
    return {
        'departmentGroup': {
            'parent': {
                'uri': dag_run.conf['company_department_uri']
            }
        },
        'modifications': {
            'name': rail.result('get_department_params')['required_name'],
            'codeToApply': {
                'value': rail.result('get_department_params')['codes_to_be_added'][0]
            },
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_departmentgroup_level2_payload(dag_run):
    return {
        'departmentGroup': {
            'parent': {
                'uri': dag_run.conf['company_department_uri']
            }
        },
        'modifications': {
            'name': dag_run.conf['required_department_name'],
            'codeToApply': {
                'value': dag_run.conf['codes_to_be_added'][0]
            },
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_departmentgroup_specificlevel_payload(dag_run):

    parent_departmenturi = get_groupuri_from_mapper(rail.result(
        'search_gmbh_departmentgroup_parent_department_entries'))

    return {
        'departmentGroup': {
            'parent': {
                'uri': parent_departmenturi
            }
        },
        'modifications': {
            'name': dag_run.conf['required_department_name'],
            'codeToApply': {
                'value': dag_run.conf['codes_to_be_added'][dag_run.conf['level'] - 1]
            },
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_mikros_departmentgroup_lastlevel_payload():
    parent_departmenturi = get_groupuri_from_mapper(rail.result(
        'search_gmbh_departmentgroup_last_lvl_parent_department_entries'))
    return {
        'departmentGroup': {
            'parent': {
                'uri': parent_departmenturi
            }
        },
        'modifications': {
            'name': 'MPC - Advertising',
            'codeToApply': {
                'value': 'T69'
            },
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_departmentgroup_lastlevel_payload(dag_run):

    parent_departmenturi = null
    if dag_run.conf['required_department_name'] == 'MikrosMPC':
        mikros_department_entry_uri = get_groupuri_from_mapper(rail.result(
            'search_gmbh_departmentgroup_last_lvl_mikros_department_entries'))

        parent_departmenturi = mikros_department_entry_uri if mikros_department_entry_uri else rail.result(
            'create_lastlevel_mikros_departmentgroup_in_replicon')['uri']
    else:
        parent_departmenturi = get_groupuri_from_mapper(rail.result(
            'search_gmbh_departmentgroup_last_lvl_parent_department_entries'))

    return {
        'departmentGroup': {
            'parent': {
                'uri': parent_departmenturi
            }
        },
        'modifications': {
            'name': dag_run.conf['required_department_name'],
            'codeToApply': {
                'value': dag_run.conf['codes_to_be_added'][dag_run.conf['level'] - 1]
            },
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_servicecenter_level1_payload(dag_run):
    return {
        'modifications': {
            'name': dag_run.conf['servicecenter'],
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def get_create_servicecenter_lvl1_payload(dag_run):
    parent_servicecenter_lvl2 = rail.smartjoin_by_delim(rail.smartjoin_by_delim(
        dag_run.conf['servicecenter'].split('|')[:-1], '|').split('|')[:-1], '|')
    return {
        'modifications': {
            'name': parent_servicecenter_lvl2.split('|', maxsplit=1)[0],
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def get_create_servicecenter_lvl2_payload(dag_run):

    parent_servicecenter = rail.smartjoin_by_delim(
        dag_run.conf['servicecenter'].split('|')[:-1], '|')
    parent_department = {
        'uri': rail.result('create_servicecenter_or_applymodifications_lvl1')['uri'] if rail.result(
            'create_servicecenter_or_applymodifications_lvl1') else rail.result('get_servicecenter_lvl2_parent')
    }
    return {
        'serviceCenter': {
            'parent': null if rail.result('get_servicecenter_params')['required_level'] == 2 else parent_department
        },
        'modifications': {
            'name': parent_servicecenter.rsplit('|', maxsplit=1)[-1].strip(),
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def get_create_servicecenter_lvl3_payload(dag_run):

    parent_department = {
        'uri': rail.result('create_servicecenter_or_applymodifications_lvl2')['uri'] if rail.result(
            'create_servicecenter_or_applymodifications_lvl2') else rail.result('get_servicecenter')
    }

    return {
        'serviceCenter': {
            'parent': parent_department
        },
        'modifications': {
            'name': dag_run.conf['servicecenter'].split('|')[-1].strip(),
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_location_level1_payload(dag_run):
    return {
        'modifications': {
            'name': dag_run.conf['location'],
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_group_level1_payload(dag_run, group_name):

    if group_name == 'department':
        return create_departmentgroup_level1_payload(dag_run)
    if group_name == 'servicecenter':
        return create_servicecenter_level1_payload(dag_run)

    return create_location_level1_payload(dag_run)


def get_create_location_lvl1_payload(dag_run):

    return {
        'modifications': {
            'name': dag_run.conf['location'].split('|')[0],
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def get_create_location_lvl2_payload(dag_run):

    return {
        'location': {
            'parent': {
                'uri': rail.result('create_location_or_applymodifications_lvl1')['uri'] if rail.result(
                    'create_location_or_applymodifications_lvl1') else rail.result('get_location')
            }
        },
        'modifications': {
            'name': dag_run.conf['location'].split('|')[-1].strip(),
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def create_division_payload(dag_run):

    return {
        'modifications': {
            'name': dag_run.conf['division'],
            'codeToApply': {
                'value': dag_run.conf['code']
            },
            'isEnabled': 'true'
        },
        'unitOfWorkId': str(uuid4())
    }


def get_udf_query(udf):

    if udf == 'jobtitle':
        return """SELECT DISTINCT jobtitle FROM inputfile
                  WHERE NULLIF(jobtitle, '') IS NOT NULL AND
                  lower(jobtitle) NOT IN
                  (SELECT DISTINCT LOWER(displayText) FROM jobtitlevalues)"""
    if udf == 'department':
        return """SELECT DISTINCT department FROM inputfile
                  WHERE NULLIF(department, '') IS NOT NULL AND
                  lower(department) NOT IN
                  (SELECT DISTINCT LOWER(displayText) FROM departmentvalues)"""
    return """SELECT DISTINCT referencejobcode FROM inputfile
              WHERE NULLIF(referencejobcode, '') IS NOT NULL AND
              lower(referencejobcode) NOT IN
              (SELECT DISTINCT LOWER(displayText) FROM referencejobcodevalues)"""


def get_customfield_dropdown_option_uris(custom_field_name, existing_dropdowns, new_dropdown_collection):

    existing_dropdowns_list = rail.result(existing_dropdowns)

    final_dropdown_list = list(map(lambda x: {
        'target': {
            'uri': x['uri'],
            'name': x['displayText'] if custom_field_name == 'department' else null
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, existing_dropdowns_list)) if existing_dropdowns_list else []

    new_values_to_set = rail.load_all_records(new_dropdown_collection)

    final_dropdown_list.extend(map(lambda x: {
        'name': x[custom_field_name],
        'isEnabled': True
    }, new_values_to_set))

    return final_dropdown_list


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def update_employment_daterange_user(dag_run):

    start_date = datetime.strptime(
        dag_run.conf['startdate'], '%d %B %Y') if dag_run.conf['startdate'] else null

    return {
        'userUri': dag_run.conf['useruri'],
        'dateRange': {
            'startDate': {
                'year': start_date.year,
                'month': start_date.month,
                'day': start_date.day
            } if start_date else null,
            'endDate': get_today_date()
        }
    }


def get_processuser_conf(item):

    service_center = rail.smartjoin_by_delim(item['servicecenter'].split(
        '|'), '/') if item['servicecenter'].split('|') else null
    department_group = rail.smartjoin_by_delim(item['departmentgroup'].split(
        '|'), '/') if item['departmentgroup'].split('|') else null
    location = rail.smartjoin_by_delim(item['location'].split(
        '|'), '/') if item['location'].split('|') else null

    required_user_customfields = rail.result('get_requireduser_customfields')

    replicon_users_list = rail.load_all_records(
        rail.result('create_repliconusers_list'))
    user_uris = list({x['useruri']
                      for x in replicon_users_list if x['employeeid'] == item['globalid']})

    return {
        **{k: v for k, v in item.items() if k not in ('creativenoncreative', 'fte', 'standardweeklyhours', 'servicecenter', 'departmentgroup',
                                                      'location')},
        **{
            'fte': item['fte'] if item['fte'] else null,
            'standardweeklyhours': item['standardweeklyhours'] if item['standardweeklyhours'] else null,
            'useruri': rail.smartjoin_by_delim(user_uris, '|') if user_uris else null,
            'servicecenter': service_center,
            'departmentgroup': department_group,
            'location': location,
            'username': f"{item['firstname']} {item['lastname']}",
            'creativenoncreative': item['creativenoncreative'] if item['creativenoncreative'] else 'Non Creative',
            'managerid': item['managerglobalid'] if item['managerglobalid'] else null,
            'workemail': item['email'],
            'departmentgroup_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_departmentgroup_details'), 'fullpath', department_group, 'departmenturi'),
            'servicecenter_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_servicecenter_details'), 'fullpath', service_center, 'servicecenteruri'),
            'location_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_location_details'), 'fullpath', location, 'locationuri'),
            'division_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_division_details'), 'fullpath', item['legalentityname'], 'divisionuri'),
            'costcenter_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_costcenter_details'), 'fullpath', item['costcentername'], 'costcenteruri')
        },
        **{k: v for k, v in required_user_customfields.items() if k != 'time'},
        **{
            'departmentvalue_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_department_dropdown'), 'displayText', item['department'], 'uri'),
            'referencejobtitlevalue_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_referencejobtitle_dropdown'), 'displayText', item['jobtitle'], 'uri'),
            'referencejobcodevalue_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_referencejobcode_dropdown'), 'displayText', item['referencejobcode'], 'uri'),
            'supervisor_permission': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_permissionsets'), 'displayText', 'Supervisor', 'uri'),
            'jobcategory_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                'get_jobcategory_dropdown'), 'displayText', item['jobcategory'], 'uri'),
            'supervisor_log': rail.result('create_supervisorlog')
        }
    }


def get_schedule_name(get_mapper_entries_from_businessunitname, get_mapper_entries_from_country,
                      get_default_mapper_entries_from_country):

    user_schedulename = null

    if get_mapper_entries_from_businessunitname:
        user_schedulename = rail.find_first_by_attr_and_get_attr(
            get_mapper_entries_from_businessunitname, 'type', 'Schedule', 'value')
    else:
        user_schedulename = rail.find_first_by_attr_and_get_attr(
            get_mapper_entries_from_country, 'type', 'Schedule', 'value') if get_mapper_entries_from_country else rail.find_first_by_attr_and_get_attr(
                get_default_mapper_entries_from_country, 'type', 'Schedule', 'value')

    return user_schedulename


def get_userpermission_name(get_mapper_entries_from_businessunitname, get_default_mapper_entries_from_country):

    user_permissionname = null
    if get_mapper_entries_from_businessunitname:
        permission_name_from_mapper = [x['value'] for x in get_mapper_entries_from_businessunitname if x['type']
                                       == 'Permission' and x['identifier2(employeetype_businessunit_type)'] == 'User']
        user_permissionname = permission_name_from_mapper[0] if permission_name_from_mapper else null

    else:
        get_mapper_entries_from_country = rail.result(
            'get_mapper_entries_from_country')
        permission_name_from_mapper = [x['value'] for x in get_mapper_entries_from_country if x['type'] ==
                                       'Permission' and x['identifier2(employeetype_businessunit_type)'] == 'User'] if get_mapper_entries_from_country else null
        user_permissionname = (permission_name_from_mapper[0] if permission_name_from_mapper else
                               null) if permission_name_from_mapper else rail.find_first_by_attr_and_get_attr(
            get_default_mapper_entries_from_country, 'type', 'Permission', 'value')

    return user_permissionname


def get_timesheettemplate_name(job_category, get_mapper_entries_from_businessunitname, get_mapper_entries_from_country_location,
                               get_default_mapper_entries_from_country):
    timesheet_template = null
    if get_mapper_entries_from_businessunitname:
        timesheet_template = rail.find_first_by_attr_and_get_attr(
            get_mapper_entries_from_businessunitname, 'type', 'Timesheet Template', 'value')
    else:
        timesheet_template_entries = [x['value'] for x in get_mapper_entries_from_country_location if x[
            'type'] == 'Timesheet Template' and x[
                'identifier2(employeetype_businessunit_type)'] == job_category] if get_mapper_entries_from_country_location else null
        timesheet_template = timesheet_template_entries[0] if timesheet_template_entries else null

    if not timesheet_template:
        timesheet_template = rail.find_first_by_attr_and_get_attr(
            get_default_mapper_entries_from_country, 'type', 'Timesheet Template', 'value')

    return timesheet_template


def get_policy_sets(job_category, get_mapper_entries_from_businessunitname, get_mapper_entries_from_country_location,
                    get_mapper_entries_from_country, get_default_mapper_entries_from_country):

    policy_sets = []
    timesheet_template = get_timesheettemplate_name(job_category, get_mapper_entries_from_businessunitname, get_mapper_entries_from_country_location,
                                                    get_default_mapper_entries_from_country)

    if timesheet_template:
        policy_sets.append({
            'name': timesheet_template
        })

    timeoff_template = rail.find_first_by_attr_and_get_attr(
        get_mapper_entries_from_country, 'type', 'Timeoff Template', 'value')

    if timeoff_template:
        policy_sets.append({
            'name': timeoff_template
        })

    punchentry_template_entries = [x['value'] for x in get_mapper_entries_from_country_location if x['type'] ==
                                   'Punch Entry Policy' and x[
        'identifier2(employeetype_businessunit_type)'] == job_category] if get_mapper_entries_from_country_location else null

    punch_entry_template = punchentry_template_entries[0] if punchentry_template_entries else null

    if punch_entry_template:
        policy_sets.append({
            'name': punch_entry_template
        })

    return policy_sets


def get_timesheetapproval_path_entry(creativenoncreative, get_mapper_entries_from_country_location, get_mapper_entries_from_country,
                                     business_unit_name, department=null):
    timesheetapproval_path_entry = []
    businessunit = 'MPC - Advertising' if business_unit_name == 'MikrosMPC' else business_unit_name
    identifier2 = f'{creativenoncreative} | {businessunit}'
    if department:
        if creativenoncreative == 'Creative':
            timesheetapproval_path_entry = [x['value'] for x in get_mapper_entries_from_country_location if x['type'] ==
                                            'Timesheet Approval path' and x[
                                                'identifier3(department)'] == department and x[
                                                    'identifier2(employeetype_businessunit_type)'] == identifier2]
        else:
            timesheetapproval_path_entry = [x['value'] for x in get_mapper_entries_from_country if x['type'] ==
                                            'Timesheet Approval path' and x[
                'identifier2(employeetype_businessunit_type)'] == creativenoncreative]
    else:
        timesheetapproval_path_entry = [x['value'] for x in get_mapper_entries_from_country_location if x['type'] ==
                                        'Timesheet Approval path' and x[
            'identifier2(employeetype_businessunit_type)'] == identifier2]

    return timesheetapproval_path_entry


# pylint: disable=too-many-arguments
def get_timesheetapproval_path(creativenoncreative, get_mapper_entries_from_businessunitname, get_mapper_entries_from_country_location,
                               get_mapper_entries_from_country, get_default_mapper_entries_from_country, department, business_unit_name):

    timesheetapproval_path = null

    timesheetapproval_path_entries = get_timesheetapproval_path_entry(
        creativenoncreative, get_mapper_entries_from_country_location, get_mapper_entries_from_country, business_unit_name, department)

    if not timesheetapproval_path_entries:
        if get_mapper_entries_from_businessunitname:
            timesheetapproval_path = rail.find_first_by_attr_and_get_attr(
                get_mapper_entries_from_businessunitname, 'type', 'Timesheet Approval path', 'value')

        elif creativenoncreative == 'Creative':
            timesheetapproval_path_entries = get_timesheetapproval_path_entry(
                creativenoncreative, get_mapper_entries_from_country_location, get_mapper_entries_from_country, business_unit_name)
            timesheetapproval_path = timesheetapproval_path_entries[
                0] if timesheetapproval_path_entries else null
    else:
        timesheetapproval_path = timesheetapproval_path_entries[
            0] if timesheetapproval_path_entries else null

    if not timesheetapproval_path:
        timesheetapproval_path = rail.find_first_by_attr_and_get_attr(
            get_default_mapper_entries_from_country, 'type', 'Timesheet Approval path', 'value')

    return timesheetapproval_path


def get_timeoffapproval_path(creative_noncreative, get_mapper_entries_from_country_location, business_unitname):

    timeoff_approvalpath_entries = [x['value'] for x in get_mapper_entries_from_country_location if
                                    x['type'] == 'Timeoff Approval Path' and x[
        'identifier2(employeetype_businessunit_type)'] == creative_noncreative and x[
        'identifier3(department)'] == business_unitname]
    return timeoff_approvalpath_entries[0] if timeoff_approvalpath_entries else null


def get_timezone(get_mapper_entries_from_country_location, get_default_mapper_entries_from_country):

    timezone_entries = [x['defaulturi'] for x in get_mapper_entries_from_country_location if
                        x['type'] == 'Time Zone']
    return timezone_entries[0] if timezone_entries else rail.find_first_by_attr_and_get_attr(
        get_default_mapper_entries_from_country, 'type', 'Time Zone', 'defaulturi')


def get_timesheet_period(get_mapper_entries_from_businessunitname, get_mapper_entries_from_country, get_default_mapper_entries_from_country,
                         creative_noncreative):

    timesheet_period = null

    timesheet_period_entries = null
    if get_mapper_entries_from_businessunitname:
        timesheet_period = rail.find_first_by_attr_and_get_attr(
            get_mapper_entries_from_businessunitname, 'type', 'Timesheet Period', 'value')

    else:
        timesheet_period_entries = [x['value'] for x in get_mapper_entries_from_country if
                                    x['type'] == 'Timesheet Period' and x[
            'identifier2(employeetype_businessunit_type)'] == creative_noncreative]

    if not timesheet_period_entries:
        timesheet_period_entries = [x['value'] for x in get_default_mapper_entries_from_country if
                                    x['type'] == 'Timesheet Period' and x[
                                        'identifier2(employeetype_businessunit_type)'] == creative_noncreative]

    timesheet_period = timesheet_period_entries[0] if timesheet_period_entries else null

    return timesheet_period


def get_payrulescript_name(get_mapper_entries_from_businessunitname, get_mapper_entries_from_country_location, jobcategory):

    payrule_script_name = null

    if get_mapper_entries_from_businessunitname:
        payrule_script_name = rail.find_first_by_attr_and_get_attr(
            get_mapper_entries_from_businessunitname, 'type', 'Payrule', 'value')
    else:
        payrule_script_entries = [x['value'] for x in get_mapper_entries_from_country_location if
                                  x['type'] == 'Payrule' and x[
            'identifier2(employeetype_businessunit_type)'] == jobcategory]
        payrule_script_name = payrule_script_entries[0] if payrule_script_entries else null

    return payrule_script_name


def get_put_user_payload(dag_run):

    get_mapper_entries_from_businessunitname = rail.result(
        'get_mapper_entries_from_businessunitname')
    get_mapper_entries_from_country_location = rail.result(
        'get_mapper_entries_from_country_location')
    get_mapper_entries_from_country = rail.result(
        'get_mapper_entries_from_country')
    get_default_mapper_entries_from_country = rail.result(
        'get_default_mapper_entries_from_country')

    user_permissionname = get_userpermission_name(get_mapper_entries_from_businessunitname,
                                                  get_default_mapper_entries_from_country)
    user_schedulename = get_schedule_name(get_mapper_entries_from_businessunitname,
                                          get_mapper_entries_from_country, get_default_mapper_entries_from_country)

    required_authentication = rail.find_first_by_attr_and_get_attr(
        get_default_mapper_entries_from_country, 'type', 'Authentication', 'defaulturi')
    work_email = dag_run.conf['workemail']

    timesheet_approval_path = get_timesheetapproval_path(dag_run.conf['creativenoncreative'],
                                                         get_mapper_entries_from_businessunitname, get_mapper_entries_from_country_location,
                                                         get_mapper_entries_from_country, get_default_mapper_entries_from_country, dag_run.conf[
                                                             'department'], dag_run.conf['businessunitname'])

    timeoff_approval_path = get_timeoffapproval_path(dag_run.conf['creativenoncreative'],
                                                     get_mapper_entries_from_country_location, dag_run.conf['businessunitname'])

    time_zone = get_timezone(
        get_mapper_entries_from_country_location, get_default_mapper_entries_from_country)
    timesheet_period = get_timesheet_period(get_mapper_entries_from_businessunitname, get_mapper_entries_from_country,
                                            get_default_mapper_entries_from_country, dag_run.conf['creativenoncreative'])

    payrule_script_name = get_payrulescript_name(get_mapper_entries_from_businessunitname, get_mapper_entries_from_country_location,
                                                 dag_run.conf['jobcategory'])

    return {
        'user': {
            'target': {
                'loginName': work_email
            },
            'firstname': dag_run.conf['firstname'],
            'lastname': dag_run.conf['lastname'],
            'emailAddress': work_email,
            'employeeId': dag_run.conf['globalid'],
            'schedulePolicySchedule': [
                {
                    'schedulePolicy': {
                        'name': user_schedulename,
                        'officeSchedule': {
                            'name': user_schedulename
                        },
                        'scheduleTypeUri': 'urn:replicon:schedule-type:office-schedule'
                    }
                }
            ] if user_schedulename else [],
            'employmentDateRange': {
                'startDate': get_today_date()
            },
            'securityConfiguration': {
                'enabledAuthenticationTypeUris': [required_authentication],
                'isLoginEnabled': 'true',
                'loginName': work_email,
                'SSOName': work_email,
                'password': 'Replicon@12#' if required_authentication.endswith('replicon') else null
            },
            'permissionSets': [
                {
                    'name': user_permissionname
                }
            ] if user_permissionname else [],
            'policySets': get_policy_sets(dag_run.conf['jobcategory'], get_mapper_entries_from_businessunitname,
                                          get_mapper_entries_from_country_location, get_mapper_entries_from_country,
                                          get_default_mapper_entries_from_country),
            'timesheetApprovalPath': {
                'name': timesheet_approval_path
            } if timesheet_approval_path else null,
            'timeOffApprovalPath': {
                'name': timeoff_approval_path
            } if timeoff_approval_path else null,
            'timeZone': {
                'uri': time_zone
            } if time_zone else null,
            'locationSchedule': [
                {
                    'location': {
                        'uri': dag_run.conf['location_uri']
                    }
                }
            ] if dag_run.conf['location'] and dag_run.conf['location_uri'] else null,
            'divisionSchedule': [
                {
                    'division': {
                        'uri': dag_run.conf['division_uri'],
                    }
                }
            ] if dag_run.conf['legalentityname'] and dag_run.conf['division_uri'] else null,
            'costCenterSchedule': [
                {
                    'costCenter': {
                        'uri': dag_run.conf['costcenter_uri']
                    },
                }
            ] if dag_run.conf['costcentername'] and dag_run.conf['costcenter_uri'] else null,
            'serviceCenterSchedule': [
                {
                    'serviceCenter': {
                        'uri': dag_run.conf['servicecenter_uri']
                    },
                }
            ] if dag_run.conf['servicecenter'] and dag_run.conf['servicecenter_uri'] else null,
            'departmentGroupSchedule': [
                {
                    'departmentGroup': {
                        'uri': dag_run.conf['departmentgroup_uri']
                    },
                }
            ] if dag_run.conf['departmentgroup'] and dag_run.conf['departmentgroup_uri'] else null,
            'employeeTypeGroupSchedule': [
                {
                    'employeeTypeGroup': {
                        'name': dag_run.conf['creativenoncreative']
                    },
                }
            ],
            'timesheetPeriodSchedule': [
                {
                    'timesheetPeriod': {
                        'name': timesheet_period
                    }
                }
            ] if timesheet_period else null,
            'payRuleScriptSchedule': [
                {
                    'payRuleScript': {
                        'name': payrule_script_name
                    }
                }
            ] if payrule_script_name else []
        }
    }


def get_product_uris():
    product_uris = []
    get_mapper_entries_from_businessunitname = rail.result(
        'get_mapper_entries_from_businessunitname')
    get_mapper_entries_from_country = rail.result(
        'get_mapper_entries_from_country')
    get_default_mapper_entries_from_country = rail.result(
        'get_default_mapper_entries_from_country')

    if get_mapper_entries_from_businessunitname:

        product_uris = list({x['defaulturi'] for x in get_mapper_entries_from_businessunitname if x[
            'type'] == 'License'})
    else:
        product_uris = list({x['defaulturi'] for x in get_mapper_entries_from_country if x[
            'type'] == 'License'})

        if not product_uris:
            product_uris = list({x['defaulturi'] for x in get_default_mapper_entries_from_country if x[
                'type'] == 'License'})
    return product_uris


def get_put_product_assignments_payload():

    return {
        'userUri': rail.result('createuser_in_replicon')['uri'],
        'productUris': get_product_uris()
    }


def get_listdata_for_supervisor(dag_run):
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


def get_adminudf_modified_value(customfield_values, udf_displaytext):
    adminudf_value = 'no'
    if customfield_values:
        adminudf_values = [
            x['text'] for x in customfield_values if x['customField']['displayText'] == udf_displaytext]
        adminudf_value = adminudf_values[0] if adminudf_values else 'no'
    return adminudf_value.lower()
