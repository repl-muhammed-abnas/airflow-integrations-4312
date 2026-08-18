from datetime import datetime
import pytz
import dateutil.relativedelta
import rail


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_cwf_data():
    utc_tz = datetime.utcnow().replace(tzinfo=pytz.utc)
    seven_days_past = utc_tz + dateutil.relativedelta.relativedelta(days=-30)
    response = rail.result("get_all_time_download_scripts")

    return {
        'fileformaturi': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'CWFTime - Master', 'uri'),
        'contractoruri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'CWFTime - Master', 'uri'),
        'agencycontractoruri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'CWFTime - Master', 'uri'),
        'ackdateday': seven_days_past.strftime("%d"),
        'ackdatemonth': seven_days_past.strftime("%m"),
        'ackdateyear': seven_days_past.strftime("%Y"),
    }


def completed_exports_list():
    response = rail.result("get_data_For_all_past_time_exports_for_C1")['rows']
    if not response:
        return []

    return list(filter(lambda x: x['Status'] == 'Complete', map(lambda item: {
        'Timeexport': item["cells"][0]['textValue'],
        'Status': item["cells"][1]['textValue'],
        'Creationdate': item["cells"][2]['textValue'],
        'uri': item["cells"][0]['uri']
    }, response)))[0]


def get_c1_final_line_data(dag_run):
    return [{
            'Employeeid': '',
            'Date': '',
            'Tasktype': '',
            'Costcenter': '',
            'Activitytype': '',
            'Recwbselement': '',
            'Recorder': '',
            'Labortype': '',
            'Billableindicator': '',
            'Task': '',
            'Hours': '',
            'Attendencetype': '',
            'Comments': '',
            'Entryid': dag_run.conf['payload_identifier_replicon_uniqueid'],
            'Activitynumber': '',
            'Sendingorder': '',
            'Sendpoitem': '',
            'Oppid': ''
            }
            ]


def get_compass_final_line_data(region, internal_oef_name):
    if internal_oef_name == 'COMPASS_PN1_sent':
        short_id = get_dag_run_conf(
        )['payload_identifier_replicon_uniqueid_pn1']
    if internal_oef_name == 'COMPASS_PJ1_sent':
        short_id = get_dag_run_conf(
        )['payload_identifier_replicon_uniqueid_pj1']
    if internal_oef_name == 'COMPASS_P01_sent':
        short_id = get_dag_run_conf(
        )['payload_identifier_replicon_uniqueid_p01']
    return [{
            'Shortid': short_id,
            'Externalsystemidentifier': '',
            'Perner': '',
            'Date': '',
            'Projectname': '',
            'Attendanceabsencetype': '',
            'Hours': '',
            'Comments': '',
            'Iwoexternalsystem': '',
            'Attribute1': '',
            'Attribute2': '',
            'Externalprojecttask': '',
            'Remainingwork': '',
            'Cfield1': '',
            'Cfield2': '',
            'Cfield3': '',
            'workorder': '',
            'Ratetype': '',
            'Tmrate': '',
            'Gsapbillingkey': '',
            'Gsaptask': '',
            'Gsapbillableflag': '',
            'region': region
            }
            ]


def check_ack_received(data, oef_name):
    if not data['extensionFieldValues']:
        return False

    check = list(filter(lambda x: x['name'] == oef_name, map(lambda item: {
        'name': item['definition']['displayText'],
        'textValue': item['textValue']
    }, data['extensionFieldValues'])))

    return bool(check[0]['textValue'] == "Yes") if check else False
