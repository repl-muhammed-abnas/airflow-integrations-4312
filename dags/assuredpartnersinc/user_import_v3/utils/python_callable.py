import rail
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import re
from assuredpartnersinc.user_import_v3 import config
null = None


def get_split_date(date_value, split_type='str'):

    if isinstance(date_value, str):
        date_value = datetime.strptime(
            date_value, config.DATE_DEFAULT_FORMAT).date()

    if split_type == 'no_split':
        return date_value
    if split_type == 'datetime':
        return {
            "day": date_value.day,
            "month": date_value.month,
            "year": date_value.year
        }
    if split_type == 'int':
        return {
            "day": int(date_value.strftime("%d")),
            "month": int(date_value.strftime("%m")),
            "year": int(date_value.strftime("%Y"))
        }
    return {
        "day": date_value.strftime("%d"),
        "month": date_value.strftime("%m"),
        "year": date_value.strftime("%Y")
    }


def dict_date_to_datetime(dict_date):
    return datetime.strptime(str(dict_date['month']) + "/" + str(dict_date['day']) + "/" + str(dict_date['year']), config.DATE_DEFAULT_FORMAT).date()


def get_department_group_list(response):
    return list(map(lambda x: {
        'name': x['cells'][0]['textValue'],
        'uri': x['cells'][0]['uri'],
        'fullpath': '/'.join(list(map(lambda c: c['textValue'], x['cells'][1]['cellCollection'])))
    }, response['rows']))


def get_required_uris(response):
    return {
        "eetype_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Employee Type", 'uri'),
        "job_code_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Job Code(UDF)", 'uri'),
        "flsastatus_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "FLSA Status", 'uri'),
        "agencyorg2_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Agency (Org 2)", 'uri'),
        "hourlyrate_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Hourly Rate", 'uri'),
        "cpnycode_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Cpny Code", 'uri'),
        "pay_group_code_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Pay Group Code", 'uri'),
        "location_code_work_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Location Code (Work)", 'uri'),
        "dept_org4_desc_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Dept (Org 4 Desc)", 'uri'),
        "core_supervisorID_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Core Supervisor ID", 'uri'),
        "core_supervisor_name_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Core Supervisor Name", 'uri'),
        "EEstatusuri": rail.find_first_by_attr_and_get_attr(response, "displayText", "EEstatus", 'uri'),
        "loastartdateuri": rail.find_first_by_attr_and_get_attr(response, "displayText", "LOA Suspend PTO Start", 'uri'),
        "loaenddateuri": rail.find_first_by_attr_and_get_attr(response, "displayText", "LOA Suspend PTO End", 'uri'),
        "dailyhoursudfuri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Daily Hours", 'uri'),
        "replicontsdateudfuri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Replicon TS Date", 'uri'),
        "enddateudfuri": rail.find_first_by_attr_and_get_attr(response, "displayText", "End Date", 'uri'),
        "pto_seniority_date_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "PTO Seniority Date", 'uri'),
        "change_effective_date_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Change Effective Date", 'uri'),
        "assignmentnumber_udf_uri": rail.find_first_by_attr_and_get_attr(response, "displayText", "Assignment Number", 'uri'),
    }


def get_supervisor_uri_status(response, supervisor_id):
    for item in response['rows']:
        if item['cells'][0]['textValue'] == supervisor_id:
            return {
                'uri': item['cells'][1]['uri'],
                'status': item['cells'][2]['textValue']
            }
    return null


def input_validation_logs(item):
    exception_list = []
    if not item['FirstName']:
        exception_list.append("Employee First Name not present")
    if not item['LastName']:
        exception_list.append("Employee Last Name not present")
    if not item['EEStatus']:
        exception_list.append("Employee status not present")
    if not item['EmplID_Login']:
        exception_list.append("Employee ID not present")
    if not item['ServiceDate']:
        exception_list.append("Service date is not present")
    if (item['ServiceDate'] and not ("/" in item['ServiceDate'])):
        exception_list.append("Service date is not in required format")
    if (item['TerminationDate'] and not ("/" in item['TerminationDate'])):
        exception_list.append("Termination date is not in required format")

    if len(exception_list) > 0:
        return ','.join(exception_list)

    return ''


def data_handler_for_replicon_groups(response):
    if bool(response['rows']):
        return [{
            "name": item['cells'][0]['textValue'],
            "uri": item['cells'][0]['uri'],
            "fullpath": '/'.join(list(map(lambda c: c['textValue'], item['cells'][1]['cellCollection']))),
            "length": len(item['cells'][1]['cellCollection']),
            "status": item['cells'][2]['textValue']
        }for item in response['rows']]

    return []


def get_required_policy_uris(response, dag_run):
    return {
        "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['TimesheetTemplate'], 'uri'),
        "timeoff_template_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['TimeOffTemplate'], 'uri'),
        "punch_entry_policy_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['punch_entry_policy'], 'uri'),
    }


def get_custom_fields_to_be_added_list(dag_run):
    custom_field_list = []
    if dag_run.conf['JobCode'] and dag_run.conf['job_code_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['job_code_udf_uri']
            },
            "text": dag_run.conf['JobCode']
        })

    if dag_run.conf['LOASuspendPTOStart'] and dag_run.conf['loastartdateuri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['loastartdateuri']
            },
            "date": get_split_date(dag_run.conf['LOASuspendPTOStart'])
        })

    if dag_run.conf['LOASuspendPTOEnd'] and dag_run.conf['loaenddateuri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['loaenddateuri']
            },
            "date": get_split_date(dag_run.conf['LOASuspendPTOEnd'])
        })

    if dag_run.conf['PTOSeniorityDate'] and dag_run.conf['pto_seniority_date_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['pto_seniority_date_udf_uri']
            },
            "date": get_split_date(dag_run.conf['PTOSeniorityDate'])
        })

    if dag_run.conf['ChangeEffectiveDate'] and dag_run.conf['change_effective_date_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['change_effective_date_udf_uri']
            },
            "date": get_split_date(dag_run.conf['ChangeEffectiveDate'])
        })

    if dag_run.conf['Agency_Org2'] and dag_run.conf['agencyorg2_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['agencyorg2_udf_uri']
            },
            "text": dag_run.conf['Agency_Org2']
        })

    if dag_run.conf['DailyHours'] and dag_run.conf['dailyhoursudfuri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['dailyhoursudfuri']
            },
            "text": dag_run.conf['DailyHours']
        })

    if dag_run.conf['RepliconTSDate'] and dag_run.conf['replicontsdateudfuri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['replicontsdateudfuri']
            },
            "text": dag_run.conf['RepliconTSDate']
        })

    if dag_run.conf['CpnyCode'] and dag_run.conf['cpnycode_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['cpnycode_udf_uri']
            },
            "text": dag_run.conf['CpnyCode']
        })

    if dag_run.conf['PayGroupCode'] and dag_run.conf['pay_group_code_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['pay_group_code_udf_uri']
            },
            "text": dag_run.conf['PayGroupCode']
        })

    if dag_run.conf['LocationCode_Work'] and dag_run.conf['location_code_work_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['location_code_work_udf_uri']
            },
            "text": dag_run.conf['LocationCode_Work']
        })

    if dag_run.conf['Dept_Org4Desc'] and dag_run.conf['dept_org4_desc_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['dept_org4_desc_udf_uri']
            },
            "text": dag_run.conf['Dept_Org4Desc']
        })

    if dag_run.conf['EEStatus'] and dag_run.conf['EEstatusuri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['EEstatusuri']
            },
            "text": dag_run.conf['EEStatus']
        })

    if dag_run.conf['CoreSupervisorID'] and dag_run.conf['core_supervisorID_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['core_supervisorID_udf_uri']
            },
            "text": dag_run.conf['CoreSupervisorID']
        })

    if dag_run.conf['CoreSupervisorName'] and dag_run.conf['core_supervisor_name_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['core_supervisor_name_udf_uri']
            },
            "text": dag_run.conf['CoreSupervisorName']
        })

    if dag_run.conf['AssignmentNumber'] and dag_run.conf['assignmentnumber_udf_uri']:
        custom_field_list.append({
            "customField": {
                "uri": dag_run.conf['assignmentnumber_udf_uri']
            },
            "text": dag_run.conf['AssignmentNumber']
        })

    return custom_field_list


def parse_hourly_rate(hourly_rate_value, currency_mapper):
    """
    Parse hourly rate value to extract currency and amount.

    Args:
        hourly_rate_value: String like "$25", "€25", "GBP25", or "25"
        This is based on future currency support requirement.

    Returns:
        dict with 'hourly_rate_amount' (float or None) and 'hourly_rate_amount_currency_name' (str or None)
    """
    if not hourly_rate_value:
        return {"hourly_rate_amount": None, "hourly_rate_amount_currency_name": None}

    hourly_rate_str = str(hourly_rate_value).strip()

    if not hourly_rate_str:
        return {"hourly_rate_amount": None, "hourly_rate_amount_currency_name": None}

    # Sort keys by length (descending) to match longer prefixes first (e.g., "GBP" before "G")
    sorted_currency_keys = sorted(
        currency_mapper.keys(), key=len, reverse=True)

    currency_name = None
    amount_str = hourly_rate_str

    for currency_identifier in sorted_currency_keys:
        if hourly_rate_str.startswith(currency_identifier):
            currency_name = currency_mapper[currency_identifier]
            amount_str = hourly_rate_str[len(currency_identifier):]
            break

    # Default to US Dollar if no currency identifier found
    if not currency_name:
        currency_name = currency_mapper.get("$", "US Dollar")

    # Parse the amount
    try:
        amount = float(amount_str.strip()) if amount_str.strip() else None
    except ValueError:
        amount = None

    return {"hourly_rate_amount": amount, "hourly_rate_amount_currency_name": currency_name}


def parse_schedule_name(schedule_name):
    """
    Parse new schedule format: US_M8_T8_W8_T8_F8_S0_S0 or US_M7P5_T7P5_W7P5_T7P5_F0_S0_S0

    Returns:
        dict with:
            - 'number_of_working_days_in_week': count of days with hours > 0
            - 'weekly_scheduled_hours': total hours across all days
    """
    # if schedule is 9.80_1- or 9.80_2- or empty, return default values
    if not schedule_name or "-" in schedule_name:
        return {"number_of_working_days_in_week": 5, "weekly_scheduled_hours": 40.0}

    parts = schedule_name.split('_')  # ['US', 'M8', 'T8', ...]

    working_days = 0
    total_hours = 0.0

    for part in parts[1:]:  # Skip 'US' prefix
        hours_str = re.sub(r'^[A-Za-z]+', '', part)  # Remove letter prefix
        hours_str = hours_str.replace('P', '.')  # Convert P to decimal
        hours = float(hours_str) if hours_str else 0

        if hours > 0:
            working_days += 1
            total_hours += hours

    if working_days == 0:
        return {"number_of_working_days_in_week": 5, "weekly_scheduled_hours": 40.0}

    return {"number_of_working_days_in_week": working_days, "weekly_scheduled_hours": total_hours}


def get_all_exceptions_from_exception_log(log_artifact):
    exception_entries = rail.load_all_records(log_artifact)
    final_exceptions = [entry['properties']['value']
                        for entry in exception_entries]
    if final_exceptions:
        return ",".join(final_exceptions)
    return null


def get_user_tenure_in_years(timesheet_start_date_or_pto_senioritydate, reference_date, dag_run):
    return float(abs((datetime.strptime(timesheet_start_date_or_pto_senioritydate, config.DATE_DEFAULT_FORMAT) - datetime.strptime(
        reference_date, config.DATE_DEFAULT_FORMAT)).days / 365)) if reference_date else float(abs((
            datetime.strptime(timesheet_start_date_or_pto_senioritydate, config.DATE_DEFAULT_FORMAT) - datetime.strptime(dag_run.conf['integration_run_date'], config.DATE_DEFAULT_FORMAT)).days / 365))


def final_timeoffs_to_be_added_list(dag_run, all_timeoffs_list):
    final_list_timeoff_types_to_be_added = []
    if dag_run.conf['additionaltimeofftypes']:
        additional_timeoff_types_list = dag_run.conf['additionaltimeofftypes'].split(
            "|")
        for item in additional_timeoff_types_list:
            final_list_timeoff_types_to_be_added.append({
                "name": item,
                "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', item.strip(), 'uri')
            })

    if dag_run.conf['illnesspto']:
        final_list_timeoff_types_to_be_added.append({
            "name": "Sick Pay-P",
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['illnesspto'], 'uri')
        })

    if dag_run.conf['PTO_1']:
        final_list_timeoff_types_to_be_added.append({
            "name": dag_run.conf['PTO_1'],
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['PTO_1'], 'uri')
        })

    if dag_run.conf['PTO_1'] and dag_run.conf['TimesheetTemplate']:
        final_list_timeoff_types_to_be_added.append({
            "name": "Scheduled Holiday",
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', "Scheduled Holiday", 'uri')
        })

    if dag_run.conf['makeuptimepto']:
        final_list_timeoff_types_to_be_added.append({
            "name": dag_run.conf['makeuptimepto'],
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['makeuptimepto'], 'uri')
        })

    if dag_run.conf['PTO_Bereavement']:
        final_list_timeoff_types_to_be_added.append({
            "name": dag_run.conf['PTO_Bereavement'],
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['PTO_Bereavement'], 'uri')
        })

    if dag_run.conf['PTO_JuryDuty']:
        final_list_timeoff_types_to_be_added.append({
            "name": dag_run.conf['PTO_JuryDuty'],
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['PTO_JuryDuty'], 'uri')
        })

    if dag_run.conf['HolidayType']:
        final_list_timeoff_types_to_be_added.append({
            "name": dag_run.conf['HolidayType'],
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['HolidayType'], 'uri')
        })

    if dag_run.conf['Illness']:
        final_list_timeoff_types_to_be_added.append({
            "name": dag_run.conf['Illness'],
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['Illness'], 'uri')
        })

    if dag_run.conf['VTO']:
        final_list_timeoff_types_to_be_added.append({
            "name": dag_run.conf['VTO'],
            "uri": rail.find_first_by_attr_and_get_attr(all_timeoffs_list, 'displayText', dag_run.conf['VTO'], 'uri')
        })

    return final_list_timeoff_types_to_be_added


def policies_to_be_assigned(default_timeoff_policy_set_schedule, dag_run):
    policies_to_be_assigned_list_1 = []
    policies_to_be_assigned_list_2 = []

    for entry in default_timeoff_policy_set_schedule:
        if float(entry['startOffset']['offsetValue']) < float(dag_run.conf['tenure']):
            policies_to_be_assigned_list_2.append({
                'policyset': entry['policySet'],
                'offset': int(entry['startOffset']['offsetValue']),
                'offsetunituri': entry['startOffset']['offsetUnitUri']
            })

        if float(entry['startOffset']['offsetValue']) >= float(dag_run.conf['tenure']):
            policies_to_be_assigned_list_1.append({
                'policyset': entry['policySet'],
                'offset': int(entry['startOffset']['offsetValue']),
                'offsetunituri': entry['startOffset']['offsetUnitUri']
            })

        offsets_list_policies2 = [
            item['offset'] for item in policies_to_be_assigned_list_2] if policies_to_be_assigned_list_2 else []
        offsets_list_policies1 = [
            item['offset'] for item in policies_to_be_assigned_list_1] if policies_to_be_assigned_list_1 else []

    return {
        'policies_to_be_assigned_1': policies_to_be_assigned_list_1,
        'policies_to_be_assigned_2': policies_to_be_assigned_list_2,
        'max_offset_from_policies_2': max(offsets_list_policies2) if offsets_list_policies2 else 0,
        'min_offset_from_policies_1': min(offsets_list_policies1) if offsets_list_policies1 else 0
    }


def add_items_to_policysets_list(policies_to_be_assigned_and_max_min_offsets, dag_run):
    policy_sets = []

    for entry in policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_2']:
        if str(entry['offset']) == str(policies_to_be_assigned_and_max_min_offsets['max_offset_from_policies_2']):
            policy_sets.append({
                "description": "Effective on - " + dag_run.conf['startdate'],
                "effectiveDate": get_split_date(dag_run.conf['startdate'], 'int'),
                "policySet": entry['policyset']
            })

    for entry in policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_1']:
        service_date_with_offset = datetime.strptime(
            dag_run.conf['servicedate'], config.DATE_DEFAULT_FORMAT) + relativedelta(months=int(entry['offset'])*12)
        if str(entry['offset']) == str(policies_to_be_assigned_and_max_min_offsets['min_offset_from_policies_1']):
            if bool(policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_2']):
                policy_sets.append({
                    "description": "Effective on - " + datetime.strftime(service_date_with_offset, config.DATE_DEFAULT_FORMAT),
                    "effectiveDate": get_split_date(service_date_with_offset, 'int'),
                    "policySet": entry['policyset']
                })
            else:
                policy_sets.append({
                    "description": "Effective on - " + dag_run.conf['startdate'],
                    "effectiveDate": get_split_date(dag_run.conf['startdate'], 'int'),
                    "policySet": entry['policyset']
                })

        elif str(entry['offset']) != str(policies_to_be_assigned_and_max_min_offsets['min_offset_from_policies_1']):
            policy_sets.append({
                "description": "Effective on - " + datetime.strftime(service_date_with_offset, config.DATE_DEFAULT_FORMAT),
                "effectiveDate": get_split_date(service_date_with_offset, 'int'),
                "policySet": entry['policyset']
            })

    return policy_sets


def timeoff_proration_assignment_initial_tasks(dag_run, config):
    return {
        'time_off_policy_mapper_search_entries': list(filter(lambda x: x["type"] == (dag_run.conf['timeofftypename'].replace('-H', "").replace('-EX', "").replace(' H', "").replace(' EX', "")).strip(), config.TO_POLICY_MAPPER)),
        'number_of_working_days_in_week': parse_schedule_name(
            dag_run.conf['schedulename'])['number_of_working_days_in_week'],
        'effective_date_derived_split': get_split_date(dag_run.conf['startdate'], 'int'),
    }


def get_required_value_from_policy_set_schedule(policy_set_schedule, offset, scipt_desciption, key_uri):
    for item in policy_set_schedule:
        if str(item['startOffset']['offsetValue']) == str(offset):
            for x in item['policySet']['timeOffBalanceEventScripts']:
                if x['script']['description'] == scipt_desciption:
                    for y in x['additionalParameters']:
                        if y['keyUri'] == key_uri:
                            return y['value']['number']
    return null


def get_policy_set_list_1(mapper_search_entries, hours_per_day, policies_to_be_assigned_and_max_min_offsets, default_timeoff_policy_set_schedule, dag_run):
    policy_set = []
    for entry in policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_2']:
        if str(entry['offset']) == str(policies_to_be_assigned_and_max_min_offsets['max_offset_from_policies_2']):
            entitlement_derived_in_hours = float(list(filter(
                lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[0]['entitlement']) * hours_per_day
            accrual_annual_amount_from_default_policy = get_required_value_from_policy_set_schedule(
                default_timeoff_policy_set_schedule, entry['offset'], 'Accrues time once per month.', 'urn:replicon:script-key:parameter:accrual-annual-amount')
            default_accrual_annual_amount_script = json.dumps(
                {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": accrual_annual_amount_from_default_policy}})
            new_accrual_annual_amount_script = json.dumps(
                {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": entitlement_derived_in_hours}})

            new_carry_over = float(list(filter(lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[
                                   0]['carryover']) * hours_per_day
            existing_carry_over_from_default_policy = get_required_value_from_policy_set_schedule(
                default_timeoff_policy_set_schedule, entry['offset'], 'Reset balance once a year', 'urn:replicon:script-key:parameter:reset-balance-amount')
            default_gsub_value_for_carry_over = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                "number": existing_carry_over_from_default_policy}}) if existing_carry_over_from_default_policy else 'abc~'
            new_carry_over_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
                "number": new_carry_over}}) if existing_carry_over_from_default_policy else 'abc~'

            complete_policyset_based_on_offset = json.loads(json.dumps(entry['policyset'], ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(default_gsub_value_for_carry_over, new_carry_over_gsub).replace('"null"', '"effective"').replace(
                '"script"', '"scriptTarget"'))
            policy_set.append({
                "effectiveDate": get_split_date(dag_run.conf['startdate'], 'int'),
                "policySet": complete_policyset_based_on_offset,
                "description": "Effective on - " + dag_run.conf['startdate']
            })
    return policy_set


def get_timeoffbalanceeventscript_to_gsub(policy_set_schedule, offset, scipt_desciption):
    for item in policy_set_schedule:
        if str(item['startOffset']['offsetValue']) == str(offset):
            for x in item['policySet']['timeOffBalanceEventScripts']:
                if x['script']['description'] == scipt_desciption:
                    return json.dumps(x)
    return ''


def get_final_policyset_list(final_policy_set_list, mapper_search_entries, hours_per_day, policies_to_be_assigned_and_max_min_offsets, default_timeoff_policy_set_schedule, dag_run):
    for entry in policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_1']:
        entitlement_derived_in_hours = float(list(filter(
            lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[0]['entitlement']) * hours_per_day
        accrual_annual_amount_from_default_policy = get_required_value_from_policy_set_schedule(
            default_timeoff_policy_set_schedule, entry['offset'], 'Accrues time once per month.', 'urn:replicon:script-key:parameter:accrual-annual-amount')
        default_accrual_annual_amount_script = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": accrual_annual_amount_from_default_policy}})
        new_accrual_annual_amount_script = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": entitlement_derived_in_hours}})

        gsub_to_get_rid_of_starting_balance = get_timeoffbalanceeventscript_to_gsub(
            default_timeoff_policy_set_schedule, entry['offset'], 'Set initial balance for the first day of a policy')

        new_carry_over = float(list(filter(lambda x: x['offset'] == str(entry['offset']), mapper_search_entries))[
                               0]['carryover']) * hours_per_day
        existing_carry_over_from_default_policy = get_required_value_from_policy_set_schedule(
            default_timeoff_policy_set_schedule, entry['offset'], 'Reset balance once a year', 'urn:replicon:script-key:parameter:reset-balance-amount')
        default_gsub_value_for_carry_over = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
            "number": existing_carry_over_from_default_policy}}) if existing_carry_over_from_default_policy else 'abc~'
        new_carry_over_gsub = json.dumps({"keyUri": "urn:replicon:script-key:parameter:reset-balance-amount", "value": {
            "number": new_carry_over}}) if existing_carry_over_from_default_policy else 'abc~'

        complete_policyset_based_on_offset = json.loads(json.dumps(entry['policyset'], ensure_ascii=False).replace(default_accrual_annual_amount_script, new_accrual_annual_amount_script).replace(
            default_gsub_value_for_carry_over, new_carry_over_gsub).replace(gsub_to_get_rid_of_starting_balance, "").replace(", ]", "]").replace("[,", "[").replace(", ,", ",").replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        pto_policy_effective_date_with_offset = datetime.strptime(
            dag_run.conf['PTOSeniorityDate'], config.DATE_DEFAULT_FORMAT) + relativedelta(months=int(entry['offset'])*12) if dag_run.conf['PTOSeniorityDate'] else datetime.strptime(
            dag_run.conf['servicedate'], config.DATE_DEFAULT_FORMAT) + relativedelta(months=int(entry['offset'])*12)
        if str(entry['offset']) == str(policies_to_be_assigned_and_max_min_offsets['min_offset_from_policies_1']):
            if bool(policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_2']):
                final_policy_set_list.append({
                    "description": "Effective on - " + datetime.strftime(pto_policy_effective_date_with_offset, config.DATE_DEFAULT_FORMAT),
                    "effectiveDate": get_split_date(pto_policy_effective_date_with_offset, 'int'),
                    "policySet": complete_policyset_based_on_offset
                })
            elif bool(not (policies_to_be_assigned_and_max_min_offsets['policies_to_be_assigned_2'])):
                final_policy_set_list.append({
                    "effectiveDate": get_split_date(dag_run.conf['startdate'], 'int'),
                    "policySet": complete_policyset_based_on_offset,
                    "description": "Effective on - " + dag_run.conf['startdate']
                })

        elif str(entry['offset']) != str(policies_to_be_assigned_and_max_min_offsets['min_offset_from_policies_1']):
            final_policy_set_list.append({
                "description": "Effective on - " + datetime.strftime(pto_policy_effective_date_with_offset, config.DATE_DEFAULT_FORMAT),
                "effectiveDate": get_split_date(pto_policy_effective_date_with_offset, 'int'),
                "policySet": complete_policyset_based_on_offset
            })

    return final_policy_set_list


def do_format_logs():
    log_artifacts = []
    log_records = []

    userlogs = rail.result("gather_user_logs")
    otherlogs = rail.result("user_import_log")

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Skipped', final_log_records))))
    rail.set_result(key="total_record_count", val=rail.result(
        "create_collection_from_input_csv", "length"))

    return final_log_records


def update_user_log():
    formatted_logs = rail.result('format_logs')
    user_logs_from_supervisor_dag = rail.load_all_records(
        rail.result('create_supervisor_user_temp_logs'))

    for item in user_logs_from_supervisor_dag:
        for entry in formatted_logs:
            if item['properties']['childjobid'] == entry['childjobid']:
                if "Exception" in item['properties']['entry_type']:
                    entry['status'] = "Error" if "Error" in entry['status'] else "Exception"
                    entry['details'] = item['properties']['details'] if "No change to the user record in Replicon" in entry['details'] else (
                        rail.smartjoin_by_delim((str(entry['details']) + ',' + item['properties']['details']).split(','), ";"))
                    break
                if "Error" in item['properties']['entry_type']:
                    entry['status'] = "Error"
                    entry['details'] = entry['details'] + \
                        ";" + item['properties']['details']
                    break
                if "Processed" in item['properties']['entry_type']:
                    entry['status'] = "Error" if "Error" in entry['status'] else (
                        "Exception" if "Exception" in entry['status'] else item['properties']['status'])
                    entry['details'] = entry['details'] + \
                        ";" + item['properties']['details']
                    break

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', formatted_logs))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', formatted_logs))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', formatted_logs))))
    rail.set_result(key="skipped_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Skipped', formatted_logs))))
    rail.set_result(key="total_record_count", val=rail.result(
        "create_collection_from_input_csv", "length"))

    return formatted_logs


def get_effective_date_derived(dag_run):
    eff_date = get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')

    if dag_run.conf['type'] == 'loa':
        eff_date = get_split_date(
            dag_run.conf['loaend'] or dag_run.conf['ChangeEffectiveDate'], 'int')

    if dag_run.conf['type'] == 'transfer':
        eff_date = get_split_date(
            dag_run.conf['ChangeEffectiveDate'], 'int')

    if dag_run.conf['type'] == 'rehire':
        eff_date = get_split_date(
            dag_run.conf['startdate'], 'int')

    return eff_date


def get_required_customfield_values(customfield_values):
    return {
        "ee_status": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'EEstatus', 'text', ''),
        "job_code": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Job Code(UDF)', 'text', ''),
        "cpny_code": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Cpny Code', 'text', ''),
        "replicon_ts_date": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Replicon TS Date', 'text', ''),
        "daily_hours": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Daily Hours', 'text', ''),
        "agency_org_2": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Agency (Org 2)', 'text', ''),
        "hourly_rate": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Hourly Rate', 'text', ''),
        "loa_suspend_pto_end": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'LOA Suspend PTO End', 'text', ''),
        "loa_suspend_pto_start": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'LOA Suspend PTO Start', 'text', ''),
        "pay_group_code": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Pay Group Code', 'text', ''),
        "location_code_work": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Location Code (Work)', 'text', ''),
        "dept_org_4_desc": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Dept (Org 4 Desc)', 'text', ''),
        "core_supervisor_id": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Core Supervisor ID', 'text', ''),
        "core_supervisor_name": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Core Supervisor Name', 'text', ''),
        "employee_type": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Employee Type', 'text', ''),
        "flsa_status": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'FLSA Status', 'text', ''),
        "pto_seniority_date": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'PTO Seniority Date', 'date', ''),
        "change_effective_date": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Change Effective Date', 'date', ''),
        "assignment_number": rail.find_first_by_attr_and_get_attr(customfield_values, 'customField.displayText', 'Assignment Number', 'text', '')
    }


def get_relevant_historical_policies(existing_timeoff_policysetschedule, effective_date_derived):
    if bool(existing_timeoff_policysetschedule and existing_timeoff_policysetschedule[0] and existing_timeoff_policysetschedule[0]['description']):
        count = 0
        for item in existing_timeoff_policysetschedule:
            if dict_date_to_datetime(item['effectiveDate']) < dict_date_to_datetime(effective_date_derived):
                count += 1

        relevant_historical_policies = json.loads(json.dumps(existing_timeoff_policysetschedule[0:count]).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        return relevant_historical_policies

    return []


def get_effective_date(dag_run):
    effective_date = get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')
    if dag_run.conf['type'] == 'rehire':
        effective_date = get_split_date(dag_run.conf['startdate'], 'int')

    if dag_run.conf['type'] == 'loa':
        effective_date = get_split_date(
            dag_run.conf['loaend'], 'int') if dag_run.conf['loaend'] else get_split_date(dag_run.conf['ChangeEffectiveDate'], 'int')

    return effective_date


def get_payrule_list_and_payrule_schedule(user_payrule_script_schedule, user_start_date, dag_run):
    payrule_schedule = []
    payrule_list = []
    if 'urn' in json.dumps(user_payrule_script_schedule):
        for item in user_payrule_script_schedule:
            if not (item['effectiveDate']):
                payrule_schedule.append({
                    "uri": item['payRuleScript']['uri'],
                    "effectivedate": dict_date_to_datetime(user_start_date),
                    "name": item['payRuleScript']['displayText']
                })
                payrule_list.append({
                    "effectiveDate": null,
                    "payRuleScript": {
                        "uri": item['payRuleScript']['uri'],
                        "parentUri": null,
                        "name": null
                    }
                })
            if item['effectiveDate']:
                if dict_date_to_datetime(item['effectiveDate']) <= datetime.strptime(dag_run.conf['ChangeEffectiveDate'], config.DATE_DEFAULT_FORMAT).date():
                    payrule_schedule.append({
                        "uri": item['payRuleScript']['uri'],
                        "effectivedate": dict_date_to_datetime(item['effectiveDate']),
                        "name": item['payRuleScript']['displayText']
                    })

                if dict_date_to_datetime(item['effectiveDate']) < datetime.strptime(dag_run.conf['ChangeEffectiveDate'], config.DATE_DEFAULT_FORMAT).date():
                    payrule_list.append({
                        "effectiveDate": item['effectiveDate'],
                        "payRuleScript": {
                            "uri": item['payRuleScript']['uri'],
                            "parentUri": null,
                            "name": null
                        }
                    })

    current_payrule_name = (max(payrule_schedule, key=lambda y: y['effectivedate']))[
        'name'] if payrule_schedule else null

    return {
        'current_payrule_name': current_payrule_name,
        'payrule_list': payrule_list
    }


def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key,  dag_run, config):
    current_value = null
    initial_value = null
    current_min_day_diff = "*"
    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:

            if not item['effectiveDate']:
                initial_value = item
                continue

            daydiff = (datetime.strptime(dag_run.conf['ChangeEffectiveDate'], config.DATE_DEFAULT_FORMAT).date()) - dict_date_to_datetime(
                item['effectiveDate'])

            # ignore the future ones
            if daydiff.days < 0:
                continue

            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue

            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    return current_value[scrpit_name][required_key] if current_value else (initial_value[scrpit_name][required_key] if initial_value else '')


def check_not_simplepattern_or_0_hours(office_schedule_details, dag_run):
    if not (office_schedule_details):
        return {
            'details': f"Office Schedule Details Not found for User's current office schedule ('{dag_run.conf['currentschedule']}')",
            'hours_per_week': 0
        }

    if not (office_schedule_details['simplePattern']):
        return {
            'details': f"User's Current Office schedule ('{dag_run.conf['currentschedule']}') doesnot follow simple 7 day pattern",
            'hours_per_week': 0
        }

    hours_per_week = ((
        office_schedule_details['simplePattern']['day1WorkDuration']['hours'] + office_schedule_details['simplePattern']['day2WorkDuration']['hours'] +
        office_schedule_details['simplePattern']['day3WorkDuration']['hours'] + office_schedule_details['simplePattern']['day4WorkDuration']['hours'] +
        office_schedule_details['simplePattern']['day5WorkDuration']['hours'] + office_schedule_details['simplePattern']['day6WorkDuration']['hours'] +
        office_schedule_details['simplePattern']['day7WorkDuration']['hours']) + float(
            office_schedule_details['simplePattern']['day1WorkDuration']['minutes'] + office_schedule_details['simplePattern']['day2WorkDuration']['minutes'] +
            office_schedule_details['simplePattern']['day3WorkDuration']['minutes'] + office_schedule_details['simplePattern']['day4WorkDuration']['minutes'] +
            office_schedule_details['simplePattern']['day5WorkDuration']['minutes'] + office_schedule_details['simplePattern']['day6WorkDuration']['minutes'] +
            office_schedule_details['simplePattern']['day7WorkDuration']['minutes'])/60)

    if hours_per_week == 0:
        return {
            'details': f"User's Current Office schedule ('{dag_run.conf['currentschedule']}') has 0 hours per week",
            'hours_per_week': 0
        }

    return {
        'details': '',
        'hours_per_week': hours_per_week
    }


def sort_updates_exceptions_logs(exception_log, update_log):
    exception_entries = rail.load_all_records(exception_log)
    update_entries = rail.load_all_records(update_log)
    final_exceptions = [entry['properties']['details']
                        for entry in exception_entries]
    final_updates = [entry['properties']['details']
                     for entry in update_entries]
    if final_exceptions:
        return {
            'status': "Exception",
            'details': "Partially updated ;" + ";".join(final_exceptions)
        }
    if final_updates:
        return {
            'status': "Success",
            'details': "Successfully updated"
        }
    return {
        'status': "Skipped",
        'details': "No change to the user record in Replicon"
    }


def starting_balance_script_with_required_starting_balance(default_starting_balance_script_json_loads, derived_starting_balance):
    default_starting_balance_script_json_loads['additionalParameters'][0][
        'value']['number'] = derived_starting_balance
    return json.dumps(default_starting_balance_script_json_loads)


def add_historical_policies_to_policysets_list(relevant_historical_policies):
    policyset_list = []
    if "urn" in json.dumps(relevant_historical_policies):
        for item in relevant_historical_policies:
            policyset_list.append({
                'description': item['description'],
                'effectiveDate': item['effectiveDate'],
                'policySet': item['policySet']
            })
    return policyset_list
