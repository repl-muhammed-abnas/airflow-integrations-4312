from base64 import b64encode
from datetime import datetime
from hashlib import sha256, md5
from hmac import new as hmac_new
from json import dumps
from urllib.parse import quote as urllib_parse_quote
from uuid import UUID as GET_UUID
from dateutil.parser import parse as date_parser
from pendulum import now as pendulum_now
import rail
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

EXPORT_DATE_FORMAT = "%Y-%m-%d"
EXPORT_FILE_TIMESTAMP = "%Y%m%d%H%M%S"


null = None


def retrieve_export_uri(response):
    if response['error'] is not None:
        raise AirflowFailException('Export failed - ' + response)
    return response['timeDataExportUri']


def get_timeexport_fileformat(config, response):
    file_format = rail.find_first_by_attr_and_get_attr(
        response, 'displayText', config.time_export_file_format, 'uri')
    if file_format:
        return file_format
    raise Exception(
        f'Unable to locate script `{config.time_export_file_format}`')


def get_time_export_name(config) -> dict:
    offset_time = pendulum_now(
        config.time_zone).strftime(EXPORT_FILE_TIMESTAMP)
    return {
        "time_export_name": f"Time Extract_{offset_time}",
        "no_data_time_export_name": f"NO_DATA_Time Extract_{offset_time}"
    }


def convert_date_to_export_formate(date_string: str) -> str:
    return date_parser(date_string).strftime(EXPORT_DATE_FORMAT)


def get_transaction_id(item) -> str:
    transaction_id_string_value = f"{item['login_name']}{item['employee_id']}{item['timesheet_period']}"
    transaction_id = md5(transaction_id_string_value.encode()).hexdigest()
    return str(GET_UUID(transaction_id))


def create_json_payload_callable(task_id, dag_run):
    export_data = rail.load_all_records(rail.result(task_id))
    user_company = export_data[0]['user_location_code']
    submitted_date = pendulum_now("Etc/UTC").isoformat().split('T')[0]

    return dumps({
        "Company": user_company,
        "MessageType": "120TimeReportBatch",
        "TransactionId": dag_run.conf['transaction_id'],
        "Payload": list(map(lambda row: {
            "TimeReportID": row["time_entry_id"],
            "Date": row['entry_date'].replace('/', '-'),
            "SubmitDate": submitted_date,
            "ProjectID": row['project_code'],
            "Resource": row["employee_id"],
            "ExternalComment": row["comments"],
            "ActivityID": row["task_code"],
            "Hours": float(row["hours"]),
            "Department": row["user_department_code"],
            "LegalUnit": row['project_location_code'],
            "Billable": "True",
            "Rate": row['report_billing_rate_amount'],
            "Currency": row['report_billing_rate_currency'],
            "Category": row["billing_rate_name"],
            "RepActivity": row["activity_name"],
            "RepActivityCode": row["activity_code"]
        }, export_data))
    })


def get_expiry(ttl) -> str:
    expiry_since_epoch = datetime.utcnow() - datetime.utcfromtimestamp(0) + ttl
    return str(int(expiry_since_epoch.total_seconds()))


def get_client_id_secret_from_var(variable_name) -> tuple:
    client_details = Variable.get(key=variable_name, deserialize_json=True)
    return (client_details['client_id'], client_details['client_secret'])


def get_sas_token(resource_uri, key_name, client_key_var, ttl) -> str:
    client_id, _ = get_client_id_secret_from_var(client_key_var)
    expiry = get_expiry(ttl)
    string_to_sign = urllib_parse_quote(resource_uri) + "\n" + expiry
    hmac_sha256 = hmac_new(bytes(client_id, 'utf-8'),
                           msg=bytes(string_to_sign, 'utf-8'), digestmod=sha256)
    signature = b64encode(hmac_sha256.digest()).decode()
    return f"SharedAccessSignature sr={urllib_parse_quote(resource_uri)}&sig={urllib_parse_quote(signature)}&se={expiry}&skn={key_name}"


def get_query_to_get_final_data_callable(config):
    filtered_data = rail.result("filter_raw_time_data", "length")
    if filtered_data and filtered_data > 0 and config.SHOULD_USE_REPORT:
        return """SELECT
            frtd.*,
            rd.project_location as project_location_name,
            rd.project_location_code as project_location_code,
            rd.billing_rate_name as report_billing_rate_name,
            rd.billing_rate_currency as report_billing_rate_currency,
            rd.billing_rate_amount as report_billing_rate_amount
        FROM
            filter_raw_time_data frtd
        LEFT JOIN
            report_data rd ON frtd.login_name = rd.login_name AND
            frtd.project_code = rd.project_code AND frtd.billing_rate_name = rd.billing_rate_name"""

    return """SELECT
                frtd.*,
                frtd.project_location as project_location_name,
                frtd.proj_location_code as project_location_code,
                frtd.billing_rate_currency as report_billing_rate_currency,
                frtd.billing_rate_rate as report_billing_rate_amount
            FROM filter_raw_time_data frtd
            """
