from datetime import datetime, timedelta
import itertools
import rail

null=None


def get_start_date():
    time_now = datetime.now()
    return {'day': time_now.strftime("%e"),
            'month': time_now.strftime("%-m"),
            'year': time_now.strftime("%Y")
            }


def get_end_date():
    time_now = datetime.now()
    next_date = time_now + timedelta(days=366)
    return {'day': next_date.strftime("%e"),
            'month': next_date.strftime("%-m"),
            'year': next_date.strftime("%Y")
            }


def get_holiday_uri(dag_run):
    return [data for data in dag_run.conf['list_item']['value'] if data['holidaycalendaruri'] == dag_run.conf[
        'holidaycalendaruri']] if dag_run.conf['list_item'] else []

def get_project_allocation_data(payload):
    projects = payload['projectsAllocatedTo']
    for project in projects:
        if 'displayText' in project['project']:
            return True
    return False

def get_process_each_user_payload_dag_ids():
    dag_run_list = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_each_user_{x+1}') if rail.result(
            f'process_each_user_{x+1}') else []), range(50)))))
    return dag_run_list

def do_format_logs():
    log_artifacts = []
    log_records = []

    userlogs = rail.result("gather_user_logs")

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
    rail.set_result(key="ignored_record_count", val=len(
                list(filter(lambda x: x['status'] == 'Ignored', final_log_records))))

    return final_log_records