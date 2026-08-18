from datetime import datetime
from uuid import uuid4
import rail
from airflow.models import Variable
from urllib.parse import urlencode
import json

null = None


def get_date_in_json(date_string):
    """Convert date string (YYYY-MM-DD or ISO format) to Replicon date JSON."""
    if 'T' in date_string:
        date_string = date_string.split('T')[0]
    
    date_obj = datetime.strptime(date_string, "%Y-%m-%d")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }


def get_bulk_users_payload(dag_run):
    """Payload to fetch user details from Replicon by employee ID."""
    return {
        "users": [
            {
                "employeeId": dag_run.conf["booking_data"]["employeeNumber"],
                "loginName": null,
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": null
    }


def get_put_and_submit_timeoff_payload(dag_run):
    """Build payload to create time-off booking in Replicon using OEF."""
    booking_data = dag_run.conf["booking_data"]
    
    from_date = booking_data["fromDate"]
    to_date = booking_data["toDate"]
    note = booking_data.get("note", "")
    keka_booking_id = booking_data["id"]
    
    # Determine relative duration based on session
    from_session = booking_data.get("fromSession", 0)
    to_session = booking_data.get("toSession", 1)
    
    if from_session == 1:
        start_relative_duration = "urn:replicon:time-off-relative-duration:half-day"
    else:
        start_relative_duration = "urn:replicon:time-off-relative-duration:full-day"
    
    if to_session == 0:
        end_relative_duration = "urn:replicon:time-off-relative-duration:half-day"
    else:
        end_relative_duration = "urn:replicon:time-off-relative-duration:full-day"
    
    # Build OEF values for Keka Booking ID
    oef_values = []
    if dag_run.conf.get("booking_id_oef_value"):
        oef_values.append({
            "definition": {
                "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:object-extension-tag-definition:{dag_run.conf['booking_id_oef_value']}",
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": keka_booking_id,
            "fileValue": null,
            "jsonValue": null
        })
    
    return {
        "timeOff": {
            "target": null,
            "owner": {
                "uri": rail.result("get_user_info")["userDetails"]["uri"],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": rail.result("get_specific_time_off_type"),
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_date_in_json(from_date),
                    "timeOfDay": null,
                    "relativeDuration": start_relative_duration,
                    "specificDuration": null
                },
                "timeOffEnd": {
                    "date": get_date_in_json(to_date),
                    "timeOfDay": null,
                    "relativeDuration": end_relative_duration,
                    "specificDuration": null
                }
            },
            "userExplicitEntries": [],
            "comments": f"Synced from Keka - {note}" if note else "Synced from Keka",
            "customFieldValues": [],
            "objectExtensionFieldValues": oef_values
        },
        "comments": "Submitted by Replicon Admin via Keka Integration",
        "unitOfWorkId": str(uuid4())
    }


def get_approve_holiday_booking_payload():
    """Payload to approve time-off booking in Replicon."""
    return {
        "timeOffUri": rail.result("put_and_submit_timeoff_booking_for_user")["uri"],
        "unitOfWorkId": str(uuid4()),
        "comments": "Approved by Replicon Admin via Keka Integration"
    }


def get_booking_id_oef_value_payload():
    """Payload to get the Keka Booking ID OEF definition from Replicon."""
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:object-extension-tag-definition-list-column:name",
            "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_time_off_details_by_keka_booking_id(dag_run):
    """
    Payload to search for existing time-off in Replicon by Keka Booking ID.
    Uses the OEF filter to find matching records.
    """
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon:time-off-list-column:time-off-type",
            f"urn:replicon-tenant:{rail.get_tenant_slug()}:time-off-object-extension-column:{dag_run.conf['booking_id_oef_value']}",
            "urn:replicon:time-off-list-column:start-date",
            "urn:replicon:time-off-list-column:end-date",
            "urn:replicon:time-off-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:time-off-object-extension-filter:{dag_run.conf['booking_id_oef_value']}"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run.conf["booking_data"]["id"],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


# ============================================================================
# KEKA API TOKEN FUNCTIONS
# ============================================================================

def get_keka_token_request_body(instance_config):
    """
    Get the request body for Keka OAuth2 token endpoint.
    Returns URL-encoded form data string.
 
    DAG Task: get_keka_access_token (used in SimpleHttpOperator data parameter)
 
    Expects Airflow Variable to be stored as JSON with keys:
    - KEKA_CLIENT_ID
    - KEKA_CLIENT_SECRET
    - KEKA_API_KEY
    """
    credentials = Variable.get(instance_config.keka_conn_variables,default_var={}, deserialize_json=True)
 
    return urlencode({
        'grant_type': instance_config.KEKA_GRANT_TYPE,
        'scope': instance_config.KEKA_SCOPE,
        'client_id': credentials.get('KEKA_CLIENT_ID'),
        'client_secret': credentials.get('KEKA_CLIENT_SECRET'),
        'api_key': credentials.get('KEKA_API_KEY')
    })
 
 
def get_keka_token_headers():
    """
    Get headers for Keka OAuth2 token request.
    Some APIs require additional headers to pass through Azure Gateway.
    """
    return {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "User-Agent": "Mozilla"
    }
 
 
def extract_keka_access_token():
    """
    Extract access token from Keka OAuth2 response.
 
    DAG Task: extract_keka_token (PythonOperator)
    Uses result from get_keka_access_token via XCom.
    Returns: access_token string
    """
    response = rail.result('get_keka_access_token')
    if isinstance(response, str):
        response = json.loads(response)
    return response.get('access_token', '')