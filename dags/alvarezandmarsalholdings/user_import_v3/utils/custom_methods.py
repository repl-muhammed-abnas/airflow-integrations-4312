import itertools
import rail
import json
from datetime import datetime
from ast import literal_eval


DATE_FORMAT = "%m/%d/%Y"


def get_process_users_dag_ids(parallel_count):
    active_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_active_users_{x+1}') if rail.result(
            f'process_active_users_{x+1}') else []), range(parallel_count)))))

    return active_users


def get_custom_schedule(dag_run):
    if dag_run.conf["scheduletype"]:
        return None

    day_hour = round(float(dag_run.conf["weekly_working_hours"])/5, 2)
    schedule_type_string = "0.00|" + \
        "".join([str(day_hour) for i in range(5)])+"|0.00"
    return schedule_type_string


def get_today_date():
    now_date = datetime.utcnow()
    return {
        'year': now_date.year,
        'month': now_date.month,
        'day': now_date.day
    }


def check_supervisor_update(dag_run):
    update_supervisor_uri = rail.result("get_supervisor_user_details")[
        0]["userDetails"]["uri"]
    if dag_run.conf["reporting_manager"] and update_supervisor_uri and dag_run.conf['type'] == 'reporting_manager':
        if not dag_run.conf["supervisor_uri"]:
            return True
        if update_supervisor_uri != dag_run.conf["supervisor_uri"]:
            return True
    return False


def get_event_management_level_oef_uri(response):
    rail.set_result(key="response", val=response)
    return rail.find_first_by_attr_and_get_attr(response, 'name', 'Management Level', 'uri', '')


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        elif isinstance(userlogs, str) and userlogs[0] == '[':
            userlogs = literal_eval(userlogs)
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
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
        **{"ecid": log['ecid']},
        **dict(log['properties'].items()),
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))

    return final_log_records


def dict_date_to_datetime(dict_date):
    return datetime.strptime(str(dict_date['month']) + "/" + str(dict_date['day']) + "/" + str(dict_date['year']), DATE_FORMAT).date()


def get_relevant_historical_policies(existing_policysetschedule, effective_date_derived):
    relevant_historical_policies = []
    for item in existing_policysetschedule:
        if dict_date_to_datetime(item['effectiveDate']) < datetime.strptime(effective_date_derived, DATE_FORMAT).date():
            relevant_historical_policies.append(
                {
                    "dateRange": {
                        "startDate": item['effectiveDate']
                    },
                    "item": {
                        "description": "Effective on - " + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['year']),
                        "policySet": item['policySet']
                    }
                }
            )
    return relevant_historical_policies


def get_final_policyset_schedule_for_timeoff_type(dag_run, current_policy_uri, relevant_historical_policies, default_policyset_for_timeoff_type, effective_date):
    existing_fte_percentage = int(float(rail.find_first_by_attr_and_get_attr(rail.result('get_user_details')[
        "userDetails"]['extensionFieldValues'], 'definition.displayText', 'FTE Percentage', 'textValue')))
    payload_fte_percentage = int(float(dag_run.conf["fte_percentage"]))

    if (existing_fte_percentage < 100 and payload_fte_percentage == 100) or current_policy_uri not in rail.result('list_uri_for_existing_timeoff_policies'):
        relevant_historical_policies.append(
            {
                "dateRange": {
                    "startDate": {
                        'day': effective_date.split('/')[1],
                        'month': effective_date.split('/')[0],
                        'year': effective_date.split('/')[2],
                    }
                },
                "item": {
                    "description": f"Effective on - {effective_date}",
                    "policySet": default_policyset_for_timeoff_type
                }
            }
        )

    final_policyset_schedule = json.loads(json.dumps(relevant_historical_policies).replace('"null"', '"effective"').replace(
        '"script"', '"scriptTarget"'))

    return final_policyset_schedule


def data_handler_for_cost_centers(response):
    return [{"name": row["cells"][0]["textValue"], "code": row["cells"][1]["textValue"]} for row in response["rows"]]


def get_cost_centers_to_be_created():
    cost_centers_to_be_created = []
    cost_centers_to_be_updated = []
    seen = set()
    existing_cost_center_name_list = [
        cc['name'].lower() for cc in rail.result('get_all_cost_centers')]
    existing_cost_center_code_list = [
        cc['code'].lower() for cc in rail.result('get_all_cost_centers')]

    for each_record in rail.load_all_records(rail.result('query_valid_records')):
        record ={}
        if each_record['cost_center_code'].lower() not in existing_cost_center_code_list:
            if each_record['cost_center_description'].lower() in existing_cost_center_name_list:
                record = {
                    "name": each_record['cost_center_description'] + " - " + each_record['cost_center_code'],
                    "code": each_record['cost_center_code'],
                    "type": "add"
                }
            else:
                record = {
                    "name": each_record['cost_center_description'],
                    "code": each_record['cost_center_code'],
                    "type": "add"
                }
            if (record["name"].lower(), record["code"].lower()) not in seen:
                seen.add((record["name"].lower(), record["code"].lower()))
                cost_centers_to_be_created.append(record)
        else:
            if each_record['cost_center_description'].lower() not in existing_cost_center_name_list:
                record = {
                    "name": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_cost_centers'), 'code', each_record['cost_center_code'], 'name'),
                    "updatedname": each_record['cost_center_description'],
                    "code": each_record['cost_center_code'],
                    "type": "update"
                }
            else:
                if each_record['cost_center_description'].lower() != rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_cost_centers'), 'code', each_record['cost_center_code'], 'name').lower():
                    if each_record['cost_center_description'] + " - " + each_record['cost_center_code'] == rail.find_first_by_attr_and_get_attr(
                            rail.result('get_all_cost_centers'), 'code', each_record['cost_center_code'], 'name'):
                        continue
                    record = {
                        "name": rail.find_first_by_attr_and_get_attr(
                            rail.result('get_all_cost_centers'), 'code', each_record['cost_center_code'], 'name'),
                        "updatedname": each_record['cost_center_description'] + " - " + each_record['cost_center_code'],
                        "code": each_record['cost_center_code'],
                        "type": "update"
                    }
            if record:
                if (record["name"].lower(), record["code"].lower()) not in seen:
                    seen.add((record["name"].lower(), record["code"].lower()))
                    cost_centers_to_be_updated.append(record)

    return cost_centers_to_be_created + cost_centers_to_be_updated


def get_list_of_uri_for_existing_timeoff_policies():
    return [timeoff_details['timeOffType']['uri'] for timeoff_details in rail.result('log_existing_timeoff_policies_for_user')]