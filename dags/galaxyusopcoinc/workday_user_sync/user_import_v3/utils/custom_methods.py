import ast
import itertools
from json import dumps
import rail
from rail.lib.artifact import new_artifact
from rail.lib.artifact import is_artifact_name
from galaxyusopcoinc.workday_user_sync.user_import_v3 import config
from galaxyusopcoinc.workday_user_sync.user_import_v3.mapper.user_import_location_master_mapper_v8 import MAPPER_TO_USE_FOR_COUNTRY_TRIAL as trial_mapper
from galaxyusopcoinc.workday_user_sync.user_import_v3.mapper.user_import_location_master_mapper_v8 import MAPPER_TO_USE_FOR_COUNTRY_UAT as uat_mapper
# Note:  this needs to be updated when deployed to PROD
from galaxyusopcoinc.workday_user_sync.user_import_v3.mapper.user_import_location_master_mapper_v8 import MAPPER_TO_USE_FOR_COUNTRY_PROD as production_mapper

key_mapping_for_feed_felids = {
    'employee_id': 'EmployeeID', 'legal_first_name': 'LegalFirstName', 'legal_last_name': 'LegalLastName',
    'hire_date': 'HireDate', 'business_title': 'BusinessTitle', 'job_profile': 'JobProfile',
    'job_profile_code': 'JobProfileCode', 'job_family': 'JobFamily', 'job_family_group': 'JobFamilyGroup',
    'compensation_grade': 'CompensationGrade', 'cost_center_id': 'CostCenterID', 'cost_center_name': 'CostCenterName',
    'cost_center_hierarchy': 'CostCenterHierarchy', 'company': 'Company', 'company_code': 'CompanyCode',
    'country': 'Country', 'location': 'Location', 'location_type': 'LocationType',
    'scheduled_weekly_hours': 'ScheduledWeeklyHours', 'default_weekly_hours': 'DefaultWeeklyHours', 'employee_type': 'EmployeeType',
    'contract_type': 'ContractType', 'contract_end_date': 'ContractEndDate', 'collective_agreement': 'CollectiveAgreement',
    'manager_id': 'ManagerID', 'workers_manager': 'WorkersManager', 'manager_email': 'ManagerEmail',
    'termination_date': 'TerminationDate', 'work_email': 'WorkEmail', 'position_id': 'PositionID',
    'exempt': 'Exempt', 'fte': 'FTE', 'management_level': 'ManagementLevel', 'worker_type': 'WorkerType',
    'job_category': 'JobCategory', 'additional_job_classification': 'AdditionalJobClassification'
}

mapper_keys_for_data_retrieve = ['location', 'worker_type', 'compensation_grade',
                                 'contract_type', 'employee_type', 'job_category',
                                 'management_level', 'additional_job_classification', 'company_code']

location_not_set_as_all = ["united_states_of_america", "canada"]

# countries_to_check_company_code_in_mapper = ['india']

VALUE_TO_USER_FOR_COMPARE_MAPPER = {
    "location": "all",
    "compensation_grade": "na",
    "additional_job_classification": ['feed_file_blank', 'na'],
    "company_code": 'na'
}

# IA/STA Holiday Calendar Mapper - matching keys and feed field mapping
ia_mapper_keys_for_data_retrieve = ['country', 'location', 'compensation_grade', 'contract_type', 'company_code']

ia_key_mapping_for_feed_fields = {
    'country': 'Country',
    'location': 'Location',
    'compensation_grade': 'CompensationGrade',
    'contract_type': 'ContractType',
    'company_code': 'CompanyCode'
}

IA_VALUE_TO_USE_FOR_COMPARE_MAPPER = {
    "country": "no_na_logic_applicable",
    "location": "N/A",
    "compensation_grade": "no_na_logic_applicable",
    "contract_type": "all",
    "company_code": "N/A"
}


def get_ia_holiday_calendar(feed_item, config_ia_holiday_mapper):
    """
    Look up holiday calendar for IA/STA employees from the IA mapper.
    Uses same compare_key_data_in_filter logic as regular mapper.
    Returns holiday_calendar string or empty string if no match.
    """
    mapper_to_use = list(config_ia_holiday_mapper)

    for key_to_use in ia_mapper_keys_for_data_retrieve:
        feed_key = ia_key_mapping_for_feed_fields[key_to_use]
        feed_value = feed_item.get(feed_key, '')

        filtered = list(filter(
            lambda row, k=key_to_use: compare_key_data_in_filter(
                row[k], feed_value, k),
            mapper_to_use
        ))

        if filtered:
            mapper_to_use = filtered
        else:
            fallback = IA_VALUE_TO_USE_FOR_COMPARE_MAPPER.get(key_to_use, 'no_na_logic_applicable')
            if fallback == 'no_na_logic_applicable':
                return ''
            mapper_to_use = list(filter(
                lambda row, k=key_to_use: compare_key_data_in_filter(
                    row[k], fallback, k),
                mapper_to_use
            ))
            if not mapper_to_use:
                return ''

    result = mapper_to_use[0].get('holiday_calendar', '').strip() if mapper_to_use else ''
    return result


def get_user_mapper(instance):
    if instance.lower() == "trial":
        return trial_mapper
    if instance.lower() == "uat":
        return uat_mapper
    return production_mapper

def get_country_key_as_per_mapper(country: str):
    return country.lower().replace(" ", "_")


def create_artifact(data):
    with new_artifact(mode="w") as attachment:
        attachment.file.write(data)
        attachment.set_attribute(name="type", value='json')
        return attachment.name

#pylint: disable=unused-argument
def get_location_mapper(instance, user_country: str, return_as_artifact: bool = False):
    mapper_data = (get_user_mapper(instance)).get(
        get_country_key_as_per_mapper(user_country), {})
    return mapper_data


def set_mappers_as_xcom(instance):
    unique_country_from_feed = list(filter(None,
                                    map(lambda country: country.get("Country"), rail.load_all_records(rail.result("query_unique_country_from_feed")))))
    count_unique_country_from_feed = rail.result(
        "query_unique_country_from_feed", "length")
    set_result_as_artifact = False
    if count_unique_country_from_feed > 15:
        set_result_as_artifact = True
    for country in unique_country_from_feed:
        rail.set_result(key=get_country_key_as_per_mapper(
            country), val=get_location_mapper(instance, country, set_result_as_artifact))
    return unique_country_from_feed


def is_mapper_value_found(mapper_data, input_data, user_country, value_found=False):
    def compare(value, compare_value:str):
        if isinstance(value, str):
            return value.lower() == compare_value.lower()
        return False

    for key in mapper_keys_for_data_retrieve:
        # Company Code check is only for the Country India
        # Any update to country to check company_code update the below list
        # list: `countries_to_check_company_code_in_mapper` with the country as per format of `get_country_key_as_per_mapper`
        # if (user_country not in countries_to_check_company_code_in_mapper) and key == "company_code":
        #     continue
        if key == "location" and compare(mapper_data[key], 'all'):
            value_found = True
            continue
        if compare(mapper_data[key], "NA"):
            value_found = True
            continue
        # there are couple of keys which are in list
        # equals will not work for them due to this below condition is there.
        # not added for string as it will return True for substring
        if isinstance(mapper_data[key], list):
            value_found = bool(
                input_data[key_mapping_for_feed_felids.get(key)] in mapper_data[key])
        else:
            value_found = bool(
                input_data[key_mapping_for_feed_felids.get(key)] == mapper_data[key])
        if not value_found:
            # if there is no value found for any of
            # the key do not proceed to check other values
            break
    return value_found


def strip_mapper_values(mapper_values: dict):
    new_mapper = {}
    for key, value in mapper_values.items():
        if isinstance(value, list):
            new_mapper[key] = [val.strip() for val in value]
        else:
            new_mapper[key] = value.strip()
        if key == "time_off_types" and value == 'NA':
            new_mapper[key] = []
    return new_mapper


def get_mapper_values_for_keys(user_country, mapper_key_values_feed):
    country_mapper = rail.result(
        "load_mapper_per_country", get_country_key_as_per_mapper(user_country))
    if not country_mapper:
        return {}
    if is_artifact_name(country_mapper):
        country_mapper = rail.load_all_records(country_mapper)
    country_mapper_where_company_code_is_na = list(filter(lambda row: row['company_code'].lower() == "na", country_mapper))
    country_mapper_where_company_code_is_non_na = list(filter(lambda row: row['company_code'].lower() != "na", country_mapper))

    for mapper_row_non_na in country_mapper_where_company_code_is_non_na:
        if is_mapper_value_found(mapper_row_non_na, mapper_key_values_feed, user_country):
            return strip_mapper_values(mapper_row_non_na)

    for mapper_row_na in country_mapper_where_company_code_is_na:
        if is_mapper_value_found(mapper_row_na, mapper_key_values_feed, user_country):
            return strip_mapper_values(mapper_row_na)
    return {}

def compare_key_data_in_filter(mapper_data_for_key, user_data_for_key, key_to_use):
    if isinstance(mapper_data_for_key, list):
        value_found = bool(
                user_data_for_key.lower() in list(map(str.lower, mapper_data_for_key)))
    else:
        value_found = bool(
            user_data_for_key.lower() == mapper_data_for_key.lower())

    if ((value_found is False) and isinstance(mapper_data_for_key, str) and (mapper_data_for_key.lower() == "na")):
        value_found = True

    if ((value_found is False) and (not user_data_for_key) and isinstance(mapper_data_for_key, str) and (mapper_data_for_key.lower() == "null")):
        value_found = True

    if ((value_found is False) and (key_to_use.lower() == "location") and isinstance(mapper_data_for_key, str) and (mapper_data_for_key.lower() == "all")):
        value_found = True

    return value_found

def get_data_from_mapper_columns_iterations(user_country, mapper_key_values_feed):
    country_mapper_to_use = rail.result(
        "load_mapper_per_country", get_country_key_as_per_mapper(user_country))
    if not country_mapper_to_use:
        return {}
    if is_artifact_name(country_mapper_to_use):
        country_mapper_to_use = rail.load_all_records(country_mapper_to_use)

    for key_to_use in mapper_keys_for_data_retrieve:

        feed_file_key_name = key_mapping_for_feed_felids.get(key_to_use)
        country_mapper_to_use = list(filter(lambda row: compare_key_data_in_filter(
                row[key_to_use], mapper_key_values_feed[feed_file_key_name], key_to_use), country_mapper_to_use))
        if country_mapper_to_use:
            continue
        else:
            value_to_use_for_compare = VALUE_TO_USER_FOR_COMPARE_MAPPER.get(key_to_use, 'no_na_logic_applicable')
            if value_to_use_for_compare == "no_na_logic_applicable":
                return {}
            if "feed_file_blank" in value_to_use_for_compare:
                if not mapper_key_values_feed[feed_file_key_name]:
                    value_to_use_for_compare = "Null"
                else:
                    value_to_use_for_compare = "na"   
            country_mapper_to_use = list(filter(lambda row: compare_key_data_in_filter(row[key_to_use], value_to_use_for_compare, key_to_use), country_mapper_to_use))
            if not country_mapper_to_use:
                return {}

    return strip_mapper_values(country_mapper_to_use[0]) if country_mapper_to_use else {}

def get_generic_values_to_assign():
    return {
        "timesheet_template": "Vialto Dummy TSD Agile",
        "time_off_types": [
            "Holiday",
            "Leave of Absence"
        ],
        "payrule": "Vialto Dummy Pay Rule",
        "default_schedule": "8|8|8|8|8|0|0"
    }

def get_payroll_type_based_on_country(country, ajc):
    
    # this will be defaulted to the "Weekly"
    return {
        "payroll_type": "Weekly",
        "payroll_type_mapping_value": "Weekly"
    }

def get_derived_values_mapper_row(mapper_row, country, ajc):
    payroll_type = get_payroll_type_based_on_country(country, ajc)
    if not mapper_row:
        return {
            **{"mapper_value_found": "No"},
            **get_generic_values_to_assign(),
            **payroll_type
        }
    return {
        **{
            "mapper_value_found": "Yes",
            "default_schedule": "8|8|8|8|8|0|0",
            "payroll_type_vals": payroll_type
        },
        **{k: v if v is not None else '' for k, v in mapper_row.items() if k not in mapper_keys_for_data_retrieve}
    }


def get_invalid_log_message(item):
    missing_fields = rail.smartjoin_by_delim(
        arr=[key for key, value in item.items() if key in (config.mandatory_columns_worker if
                                                           item['WorkerType'] == "Contingent Worker" else config.mandatory_columns_employee) and not value],
        separator=";")
    return f'User Record not processed due to following reason: {missing_fields} not present in feed file.'


def get_cost_center_name(dag_run):
    return f"{dag_run.conf['cost_center_name']} ({dag_run.conf['cost_center_code']})"


def get_service_center_name(dag_run):
    return f"{dag_run.conf['name']} ({dag_run.conf['code']})"

def is_user_update_callable(dag_run):
    if dag_run.conf['action'] == "update":
        if dag_run.conf.get('rehire', 'no').lower() == "yes":
            # User action is update, but is a rehire 
            return False
        # User action is only update
        return True
    # User action is add
    return False

def get_policy_to_assign(dag_run):
    previous_policies = []
    if is_user_update_callable(dag_run):
        previous_policies = rail.result('get_all_policies_assigned')
    data = rail.result('get_default_time_off_policy_schedule')
    if not data:
        return None
    new_policies = list(map(lambda item: {
        'description': 'effective',
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, data))

    def has_same_effective_date(policy_a, policy_b):
        return policy_a['effectiveDate'] == policy_b['effectiveDate']

    # Skip a new policy line that is already assigned to the user.
    fresh_policies = [
        new_policy for new_policy in new_policies
        if not any(has_same_effective_date(new_policy, existing)
                   for existing in previous_policies)
    ]
    res = previous_policies + fresh_policies
    return dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

def page_handler(request, result_resp):
    if len(result_resp['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_timeoff_uris(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    return [x['cells'][0]['uri'] for x in flatten_rows] if flatten_rows else []
