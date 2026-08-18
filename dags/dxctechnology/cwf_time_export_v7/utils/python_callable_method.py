from datetime import datetime
import pytz
import dateutil.relativedelta
import rail


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_cwf_data():
    utc_tz = datetime.utcnow().replace(tzinfo=pytz.utc)
    thirty_days_back= utc_tz + dateutil.relativedelta.relativedelta(days=-60)
    response = rail.result("get_all_time_download_scripts")

    return {
        'fileformaturi': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'CWFTime - Master', 'uri'),
        'contractoruri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'CWFTime - Master', 'uri'),
        'agencycontractoruri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'CWFTime - Master', 'uri'),
        'ackdateday': thirty_days_back.strftime("%d"),
        'ackdatemonth': thirty_days_back.strftime("%m"),
        'ackdateyear': thirty_days_back.strftime("%Y"),
    }


def completed_exports_list():
    response = rail.result("get_data_For_all_past_time_exports")['rows']
    if not response:
        return []

    return list(filter(lambda x: x['Status'] == 'Complete', map(lambda item: {
        'Timeexport': item["cells"][0]['textValue'],
        'Status': item["cells"][1]['textValue'],
        'Creationdate': item["cells"][2]['textValue'],
        'uri': item["cells"][0]['uri']
    }, response)))[0]


def get_psa_final_line_data(dag_run):
    return [{
            'PERSNO': '',
            'DATE': '',
            'WBS': '',
            'AttendanceAbsenceType': '',
            'LaborType': '',
            'BILLABLEINDICATOR': '',
            'Hours': '',
            'TASKS': '',
            'Attribute1': '',
            'Attribute2': '',
            'SHORTTEXT': '',
            'BillingKey': '',
            'GSAPTask': '',
            'GSAPBillableFlag': '',
            'RepliconUniqueID': dag_run.conf['payload_identifier_replicon_uniqueid'],
            'Home_ERP': '',
            'Child_Project_ERP': '',
            'Home_Location': '',
            'TimeOff': '',
            'Project_Type_Flag': ''
            }
            ]

def get_psa_f142d_final_line_data(dag_run):
    return [{
            'Employee_ID': '',
            'DATE': '',
            'WBS': '',
            'Task': '',
            'Hours': '',
            'RepliconUniqueID': dag_run.conf['payload_identifier_replicon_uniqueid'],
            'Comments': '',
            'Billing_Key': '',
            'Project_Key': '',
            'Attendance_Type': '',
            'Child_Project_WBS': '',
            'Child_Project_ERP': '',
            'IWO_WBS_Flag': '',
            'Parent_Project_ERP': '',
            'Labour_Type': '',
            'Home_ERP': ''
            }
            ]

def get_gsap_final_line_data(dag_run):
    return [{
            'Replicon_Unique_ID': dag_run.conf['payload_identifier_replicon_uniqueid'],
            'Workday_PERNR': '',
            'Date': '',
            'GSAP_WBS': '',
            'Billing_Key': '',
            'Billing_Indicator': '',
            'Tasks': '',
            'Hours': '',
            'Attendance_Absence_Type': '',
            'Status': '',
            'Remarks': '',
            'Reference_Number': ''
            }
            ]

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
            'Cfield1': '',
            'Cfield2': '',
            'Cfield3': '',
            'workorder': '',
            'Ratetype': '',
            'Tmrole': '',
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
