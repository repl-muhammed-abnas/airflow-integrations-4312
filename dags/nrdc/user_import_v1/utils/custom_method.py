import hashlib
from ast import literal_eval
from airflow.models import Variable


def get_formated_user_row(item):
    return {
        "displayname": item["Display Name"],
        "firstname": item["First Name"],
        "lastname": item["Last Name"],
        "emailaddress": item["Email Address"].lower() if item["Email Address"] else item["Email Address"],
        "empid": item["Employee ID"],
        "empnumber": item["Employee Number"],
        "whencreated": item["When Created"],
        "whenchanged": item["When Changed"],
        "office": item["Office"],
        "logonname": item["Logon Name"],
        "accountstatus": item["Account Status"],
        "department": item["Department"],
        "memberof": item["Member of"],
        "title": item["Title"],
        "md5": hashlib.md5((
            item['Display Name']+"," +
            item['First Name']+"," +
            item['Last Name']+"," +
            item['Email Address']+"," +
            item['Employee ID']+"," +
            item['Employee Number']+"," +
            item['When Created']+"," +
            item['When Changed']+"," +
            item['Office']+"," +
            item['Logon Name']+"," +
            item['Account Status']+"," +
            item['Department']+"," +
            item['Member of']+"," +
            item['Title']).encode())
        .hexdigest()
    }.values()

def c3_c4_supervisors_loginname(variable_name):
    supervisors_loginname = literal_eval(Variable.get(variable_name))
    return {
        "c3_supervisor":supervisors_loginname['c3_supervisor_loginname'],
        "c4_supervisor":supervisors_loginname['c4_supervisor_loginname']
    }
