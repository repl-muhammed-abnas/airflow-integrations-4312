import rail
import requests
import json
from datetime import datetime, timedelta
from airflow.models import Variable
import uuid

null = None


def get_shift_assignments_from_conf(dag_run):
    if dag_run and dag_run.conf:
        shift_assignments = dag_run.conf['webhook']['data'].get('value',[]) if dag_run.conf['webhook']['data'] else False
        if shift_assignments:
            return json.loads(shift_assignments)

    return False

def prepare_teams_message(dag_run, config):
    # Teams webhook URL
    share_point_details = json.loads(
        Variable.get(config.get_inhouse_shift_assignment_variable)
    )
    teams_webhook_url = share_point_details.get(
        'inhouse_weekend_shift_assignment_teams_webhook_url'
    )

    # Sample input JSON
    individual_message_payload = {
        "type": "AdaptiveCard",
        "body": [
            {
                "type": "TextBlock",
                "size": "Medium",
                "weight": "Bolder",
                "text": "Weekend Shift Assignment"
            },
            {
                "type": "TextBlock",
                "text": f"Hello {dag_run.conf['name']}, you are assigned for the weekend shift.",
                "wrap": True
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "email:", "value": dag_run.conf['emailaddress']},
                    {"title": "Month:", "value": dag_run.conf['ShiftMonth']},
                    {"title": "Date:", "value": dag_run.conf['ShiftDate']},
                    {"title": "Shift Time:", "value": dag_run.conf['ShiftTime']},
                    {"title": "Shift Type:", "value": "Weekend Shift"}
                ]
            }
        ],
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.2"
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(
        teams_webhook_url, headers=headers, data=json.dumps(individual_message_payload)
    )
    if response.status_code == 202:
        return "Message posted successfully."
    else:
        return f"Failed to post message. Status code: {response.status_code}, Response: {response.text}"


def logging_details():
    today = datetime.today()
    saturday = today + timedelta((5 - today.weekday()) % 7)
    sunday = saturday + timedelta(1)
    return {
        "weekend_shift_start_date": saturday.strftime('%m/%d/%Y'),
        "weekend_shift_end_date": sunday.strftime('%m/%d/%Y')
    }


def get_all_user_details():
    return {
        "users": [
            {"employeeId": item['EmployeeID']}
            for item in rail.result('get_shift_assignments')
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def get_shift_details(config, dag_run):
    return {
        "shiftAssignments": {
            "assignmentDateRange": {
                "startDate": rail.get_replicon_date(
                    datetime.strptime(dag_run.conf['ShiftDate'], '%m/%d/%Y')
                ),
                "endDate": rail.get_replicon_date(
                    datetime.strptime(dag_run.conf['ShiftDate'], '%m/%d/%Y')
                )
            },
            "relativeDates": [],
            "assignmentDaysOfWeek": [
                "urn:replicon:day-of-week:sunday"
                if dag_run.conf['Weekday'] == 'Sunday'
                else "urn:replicon:day-of-week:saturday"
            ],
            "userUris": [dag_run.conf['uri']],
            "shift": {"name": config.shift_name},
            "note": "Published by shift automation",
            "publishState": "urn:replicon:shift-assignment-publish-state:published",
            "assignmentOptionUri": "urn:replicon:shift-assignment-option:replace-assignments-on-overlapping-day"
        }
    }


def get_shift_summary(dag_run):
    return {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [dag_run.conf['uri']]
        },
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": rail.get_replicon_date(
                datetime.strptime(dag_run.conf['ShiftDate'], '%m/%d/%Y')
            ),
            "endDate": rail.get_replicon_date(
                datetime.strptime(dag_run.conf['ShiftDate'], '%m/%d/%Y')
            )
        }
    }


def get_shift_delete(dag_run):
    return {
        "shiftAssignmentUris": rail.result('get_shift_schedule_summary')
    }


def get_shift_assignment_payload(dag_run, config):
    return {
        "assignments": [
            {
                "date": rail.get_replicon_date(
                    datetime.strptime(dag_run.conf['ShiftDate'], '%m/%d/%Y')
                ),
                "target": {
                    "uri": null
                },
                "shift": {
                    "uri": null,
                    "name": config.shift_name
                },
                "user": {
                    "uri": dag_run.conf['uri'],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
                "note": "Published by weekend shift automation",
                "publishState": "urn:replicon:shift-assignment-publish-state:published"
            }
        ],
        "unitOfWorkId": str(uuid.uuid4())
    }