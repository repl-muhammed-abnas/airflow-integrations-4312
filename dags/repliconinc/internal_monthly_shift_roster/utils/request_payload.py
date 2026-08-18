import json
import rail
import uuid


null = None


def get_team_shift_roster_from_conf(dag_run):
    if not dag_run or not dag_run.conf:
        return []

    webhook_data = dag_run.conf.get('webhook', {}).get('data')
    if not webhook_data:
        return []

    shift_assignments = webhook_data.get('value', [])
    if not shift_assignments:
        return []

    return json.loads(shift_assignments)


def create_shift_schedule_summary_details_payload(dag_run):
    return {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [
                dag_run.conf["user_uri"]
            ]
        },
        "ShiftSearch": null,
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": {
                "year": dag_run.conf["start_date_year"],
                "month": dag_run.conf["start_date_month"],
                "day": dag_run.conf["start_date_day"]
            },
            "endDate": {
                "year": dag_run.conf["end_date_year"],
                "month": dag_run.conf["end_date_month"],
                "day": dag_run.conf["end_date_day"]
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def create_bulk_delete_user_shift_schedule_payload():
    return {
        "shiftAssignmentUris": rail.result("get_shift_schedule_summary_details")
    }


def create_final_shift_assignment_payload(final_shift_assignment):
    return {
        "assignments": final_shift_assignment,
        "unitOfWorkId": str(uuid.uuid4())
    }