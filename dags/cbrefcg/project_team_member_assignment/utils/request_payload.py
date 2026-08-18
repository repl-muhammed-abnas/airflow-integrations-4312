import datetime
import json
import pytz
import rail
from airflow.models import Variable

def get_project_team_change_payload(dag_run):
    variable_date = json.loads(
        Variable.get("cbrefcg_project_team_change_payload_date", default_var=""))
    utc_now = datetime.datetime.utcnow().replace(tzinfo= pytz.UTC)
    two_hours_ago = utc_now - datetime.timedelta(hours=2)

    if variable_date['Date']:
        two_hours_ago = datetime.datetime.strptime(variable_date['Date'], variable_date['format'])

    return {
            "projectUri": dag_run.conf['projecturi'],
            "startTimestampUtc": {
            "year": two_hours_ago.strftime("%Y"),
            "month": two_hours_ago.strftime("%m"),
            "day": two_hours_ago.strftime("%d"),
            "hour": two_hours_ago.strftime("%H"),
            "minute": two_hours_ago.strftime("%M"),
            "second": "0",
            "millisecond": "0"
            },
            "endTimestampUtc": None
        }


def get_groups_data_payload(filter_name,group_name):
    group_uri = rail.result("for_each_team_member_added")['resource'][group_name]['uri']
    return {
            "page": "1",
            "pagesize": "1000",
            "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:"+filter_name
            ],
            "sort": [],
            "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:"+filter_name
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "value": {
                    "uri": group_uri,
                }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "value": {
                    "bool": "true"
                }
                }
            }
            }
        }

def get_update_project_team_payload(dag_run):
    return {
            "projectUri": dag_run.conf['projecturi'],
            "resourceUri": rail.result("get_all_user_uris"),
            "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
        }
