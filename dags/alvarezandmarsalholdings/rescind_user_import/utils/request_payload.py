import rail
import uuid
from datetime import datetime, timedelta


MANDATORY_FIELDS = {
    "employee_id": "Employee_ID"
}

DATE_FORMAT = '%m/%d/%Y'
null = None


def get_required_date(dag_run):
    run_date = datetime.today()
    replicon_start_date = rail.result("get_user_details")[
        0]["userDetails"]["employmentDateRange"]["startDate"]
    start_date =  datetime(replicon_start_date['year'], replicon_start_date['month'], replicon_start_date['day']) if replicon_start_date else None
    rescind_date = datetime.strptime(
        dag_run.conf["rescind_date"], DATE_FORMAT) if dag_run.conf["rescind_date"] else run_date

    if start_date and rescind_date <= start_date:
        start_date = rescind_date - timedelta(days=1)

    return rail.get_replicon_date(start_date), rail.get_replicon_date(rescind_date) 


def get_all_mandatory_check(dag_run):
    for _, value in MANDATORY_FIELDS.items():
        if not dag_run.conf[value]:
            return False
    return True


def get_exception_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"{log_value} not present in the payload")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_update_user_payload(dag_run):
    start_date, end_date = get_required_date(dag_run)
    return {
        "target": {
            "uri": rail.result("get_user_details")[0]["userDetails"]['uri']
        },
        "modifications": {
            "employmentDateRange": {
                "value": {
                    "startDate": start_date,
                    "endDate": end_date
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "false"
                    }
                }
            },
            "extensionFields": [
                {
                    "value": {
                        "definition": {
                            "uri": null,
                            "name": "Event Identifier"
                        },
                        "tag": {
                            "uri": dag_run.conf['event_identifier_value_uri'],
                            "slug": null,
                            "tagName": null
                        },
                        "numericValue": null,
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                    }
                }
            ] if dag_run.conf['event_identifier_value_uri'] else []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
