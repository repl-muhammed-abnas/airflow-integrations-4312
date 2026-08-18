from datetime import datetime
import uuid
import rail
from necau.auto_shift_assignment.utils.python_callable_method import get_combination_of_array


def get_shift_summary_payload(dag_run, user_shift_info, shift_day_diff, week):
    user_shift_info = rail.result(user_shift_info)
    shift_day_diff = rail.result(shift_day_diff)
    week_start_date = None
    week_end_date = None
    if week == "week1":
        week_start_date = datetime.strptime(user_shift_info['startdateofcurrentweek'], '%Y%m%d') if shift_day_diff and shift_day_diff[
            'daydiffforiterations'] else datetime.strptime(user_shift_info['startdatederived'], '%Y%m%d')
        week_end_date = datetime.strptime(user_shift_info['endofthecurrentweekfriday'], '%Y%m%d') if shift_day_diff and shift_day_diff[
            'daydiffforiterations'] else datetime.strptime(user_shift_info['endofweek1'], '%Y%m%d')
    else:
        week_start_date = datetime.strptime(
            user_shift_info['startodsecondweeksat'], '%Y%m%d')
        week_end_date = datetime.strptime(user_shift_info['endweek2'], '%Y%m%d')

    return {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [dag_run.conf['Useruri']]
        },
        "shiftSearch": None,
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": {
                "year": week_start_date.year,
                "month": week_start_date.month,
                "day": week_start_date.day
            },
            "endDate": {
                "year": week_end_date.year,
                "month": week_end_date.month,
                "day": week_end_date.day
            },
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }


def get_assignment_uris_to_delete():
    shifts_to_delete = []
    week1_delete_list = rail.result('create_assigned_shift_details_week1')['shifts_to_delete'] if rail.result(
        'create_assigned_shift_details_week1') and rail.result('create_assigned_shift_details_week1')['shifts_to_delete'] else []
    week2_delete_list = rail.result('create_assigned_shift_details_week2')['shifts_to_delete'] if rail.result(
        'create_assigned_shift_details_week2') and rail.result('create_assigned_shift_details_week2')['shifts_to_delete'] else []
    shifts_to_delete = get_combination_of_array(
        week1_delete_list, week2_delete_list)
    shift_uris_to_delete = [x.get("assignmenturi") for x in shifts_to_delete]
    return {
        "shiftAssignmentUris": shift_uris_to_delete
    }


def get_put_shift_payload(dag_run):
    shift_to_assign_payloads = []
    usr_shift_info = rail.result('get_shift_week_info')

    week1_assignment_list = rail.result('shift_to_assign_week1')['shift_to_assign_week'] if rail.result(
        'shift_to_assign_week1') and rail.result('shift_to_assign_week1')['shift_to_assign_week'] else []
    week2_assignment_list = rail.result('shift_to_assign_week2')['shift_to_assign_week'] if rail.result(
        'shift_to_assign_week2') and rail.result('shift_to_assign_week2')['shift_to_assign_week'] else []
    shift_to_assign_details = get_combination_of_array(
        week1_assignment_list, week2_assignment_list)

    for shift_to_assign in shift_to_assign_details:
        effective_date = datetime.strptime(
            shift_to_assign['effectivedate'], '%Y%m%d')
        shift_to_assign_info = {
            "date": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "target": {
                "uri": None
            },
            "shift": {
                "name": usr_shift_info['shiftnamederived']
            },
            "user": {
                "uri": dag_run.conf['Useruri']
            },
            "note": "Assigned By Integration",
            "publishState": "urn:replicon:shift-assignment-publish-state:published"
        }

        shift_to_assign_payloads.append(shift_to_assign_info)
    return {
        "assignments": shift_to_assign_payloads,
        "unitOfWorkId": str(uuid.uuid4())
    }
