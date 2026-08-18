import hashlib
import rail
from rail.lib.ecid import get_dagrun_ecid


def is_previous_import_running(config):
    files_in_processing_dir = rail.result(
        "list_import_files").get(config.processing_file_directory)
    return files_in_processing_dir and len(files_in_processing_dir) > 0


def has_input_file(file_type):
    files_in_input_dir = rail.result("get_timeoff_file_groups")
    valid_files = files_in_input_dir.get(
        file_type) if files_in_input_dir else None
    return valid_files and len(valid_files) > 0


def get_archive_file_info(item):
    return {
        "file_name": item['file_name']
    }


def get_leave_info(dag_run, item):
    return {
        "file_name": item["name"],
        "query_userdata": rail.result('query_userdata'),
        "master_ecid": get_dagrun_ecid(dag_run)
    }


def get_timeoff_file_info(item):
    return {
        "file_name": item["name"]
    }


def lap_form_code_validation(dag_run):
    if (dag_run.conf['form_code'].lower() == 'lap') and dag_run.conf['action_status'].lower() in ['approved', 'requested']:
        if not dag_run.conf['days_taken'] or not dag_run.conf['hours_taken']:
            return True
    return False


def staff_member_validation(dag_run):
    return dag_run.conf['staff_member'] and dag_run.conf['start_date'] and dag_run.conf['end_date']


def check_timeoff_type_present(dag_run):
    existingtimeoff = rail.result('get_all_time_off_types')
    # timeoff_type_uri = rail.find_first_by_attr_and_get_attr(
    #     existingtimeoff, 'displayText', dag_run.conf['leave_description'], "uri")
    timeoff_type_info = list(filter(
        lambda data: dag_run.conf['leave_description'] and
        data['displayText'].lower() == dag_run.conf['leave_description'].lower(), existingtimeoff))
    return not timeoff_type_info


def get_lvc_form_code_validation(dag_run):
    booking_info = rail.result('timeoff_booking_info')
    booking_uri = rail.find_first_by_attr_and_get_attr(
        booking_info, 'request_key_existing', dag_run.conf['request_key'], "booking_uri")
    return not booking_uri and dag_run.conf['form_code'].lower() == 'lvc' and dag_run.conf['action_status'].lower() == "approved"


def get_lap_form_code_validation(dag_run):
    booking_info = rail.result('timeoff_booking_info')
    booking_uri = rail.find_first_by_attr_and_get_attr(
        booking_info, 'request_key_existing', dag_run.conf['request_key'], "booking_uri")
    if not booking_uri and dag_run.conf['form_code'].lower() == 'lap':
        if dag_run.conf['action_status'].lower() in ['deleted', 'rejected', 'declined', 'reject']:
            return True
    return False


def get_booking_status(dag_run):
    booking_info = rail.result('timeoff_booking_info')
    booking_uri = rail.find_first_by_attr_and_get_attr(
        booking_info, 'request_key_existing', dag_run.conf['request_key'], "booking_uri")
    return not booking_uri and dag_run.conf['days_taken'] and dag_run.conf['hours_taken']


def get_shift_status(dag_run):
    return dag_run.conf['is_shift_user'].lower() == 'yes'


def get_end_booking_status(dag_run):
    days_taken = dag_run.conf['days_taken']
    days_taken_decimals = str(days_taken).split('.')
    return len(days_taken_decimals) == 2 and float(days_taken_decimals[1]) > 0 and dag_run.conf['form_code'].lower() == 'lap'


def get_referance_shift_info(dag_run):
    shift_referance = hashlib.md5((dag_run.conf['user_uri']+","
                                   + dag_run.conf['pattern']+","
                                   + dag_run.conf['effective_date']).encode()).hexdigest()
    return {
        "shift_md5": shift_referance
    }


def get_timesheet_reopen_status(dag_run):
    return dag_run.conf['timesheet_approval_status'].lower() != 'waiting' and dag_run.conf['timesheet_approval_status'].lower() != 'approved'


def lvc_form_code_validation(dag_run):
    return dag_run.conf['form_code'].lower() == 'lvc' and dag_run.conf['action_status'].lower() == 'approved' \
        and dag_run.conf['seq_no'] == dag_run.conf['sequencenol']


def lvc_timesheet_status(dag_run):
    return dag_run.conf['form_code'].lower() == 'lvc' and dag_run.conf['action_status'].lower() != 'approved'


def is_lap_form_code(dag_run):
    return dag_run.conf['form_code'].lower() == 'lap'


def get_action_status(dag_run):
    return dag_run.conf['action_status'].lower() == 'approved' and dag_run.conf['timeoff_approval_status'].lower() != 'approved'


def get_action_request(dag_run):
    return dag_run.conf['action_status'].lower() == 'requested' or dag_run.conf['action_status'].lower() == 'request'


def get_timeoff_status(dag_run):
    return dag_run.conf['timeoff_approval_status'].lower() == 'approved' and dag_run.conf['action_status'].lower() == 'approved'


def get_action_deleted(dag_run):
    return dag_run.conf['action_status'].lower() in ['deleted', 'declined', 'rejected', 'reject']


def actions_present(dag_run):
    return not dag_run.conf['action_status']


def get_timesheet_approved_status(dag_run):
    return dag_run.conf['timesheet_approval_status'].lower() in ['waiting', 'approved']


def get_lap_action_aproved(dag_run):
    return dag_run.conf['form_code'].lower() == 'lap' and dag_run.conf['action_status'].lower() == 'approved' \
        and dag_run.conf['timeoff_approval_status'].lower() != "approved"


def get_lap_timeoff_approved(dag_run):
    return dag_run.conf['form_code'].lower() == 'lap' and dag_run.conf['action_status'].lower() == 'approved' \
        and dag_run.conf['timeoff_approval_status'].lower() == "approved"


def get_lap_action_delete(dag_run):
    return dag_run.conf['form_code'].lower() == 'lap' and dag_run.conf['action_status'].lower() in ['deleted', 'declined', 'rejected', 'reject']


def lap_actions_present(dag_run):
    return dag_run.conf['form_code'].lower() == 'lap' and not dag_run.conf['action_status']


def get_user_status(dag_run):
    return dag_run.conf['user_status'] and dag_run.conf['user_status'].lower() == 'enabled'


def has_shift_to_delete():
    assignment_category = rail.result('shift_assignment_category')
    return assignment_category and assignment_category[0] and len(assignment_category[0])


def has_shift_to_assign():
    assignment_category = rail.result('shift_assignment_category')
    return assignment_category and assignment_category[2]


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_processed_shift():
    shift_processed = get_data_from_document(
        rail.result('get_processed_shift_info'))
    if len(shift_processed) > 0:
        return True
    return False


def get_download_links(task_name):
    download_result_info = rail.result(task_name)
    logs_download_links = []

    for record in download_result_info:
        logs_download_links.append({
            "download_link": record["download_link"],
            "file_name": record["file_name"]
        })
    return logs_download_links


def get_processed_logs():
    download_leave_info = rail.result(
        'gather_leave_request_logs_download_link')
    download_approve_info = rail.result(
        'gather_leave_approved_logs_download_link')
    download_cancell_info = rail.result(
        'gather_leave_cancelled_logs_download_link')
    logs_download_links = []
    if len(download_leave_info) > 0:
        logs_download_links.append({
            "download_link": download_leave_info[0],
            "log_name": "Leave request logs"
        })

    if len(download_approve_info) > 0:
        logs_download_links.append({
            "download_link": download_approve_info[0],
            "log_name": "Leave approved logs"
        })

    if len(download_cancell_info) > 0:
        logs_download_links.append({
            "download_link": download_cancell_info[0],
            "log_name": "Leave cancellation logs"
        })
    return logs_download_links


def is_request_keys_same(dag_run):
    return dag_run.conf['request_key'].lower() == dag_run.conf['request_key_existing'].lower()
