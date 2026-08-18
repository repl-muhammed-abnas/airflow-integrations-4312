from datetime import datetime
import rail
from omd.singapore_timeoff_import.utils import custom_methods

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_conf_timeoff_import(item):
    users = custom_methods.get_data_from_document(
        rail.result('load_report_data'))
    holidays = rail.result('result_of_final_holiday_entries')

    def get_user_details():
        data = list(
            filter(null, list(filter(lambda user: user['Employee ID'] == item['empid'], users))))
        return {
            'useruri': data[0]['UserUri'] if data and data[0]['UserUri'] else null,
            'holidaycalendarname': data[0]['Holiday Calendar'] if data and data[0]['Holiday Calendar'] else null
        } if data else {}

    def get_isholiday():
        def get_dateobject(date, dateformat):
            return datetime.strptime(date, dateformat).date()
        return list(filter(
            lambda x: x["holidaycalendarname"] == get_user_details().get('holidaycalendarname') and
            get_dateobject(x["holidaydate"], "%Y-%m-%d") == get_dateobject(item['startdate'], "%d/%m/%Y"), holidays)) if holidays else []

    return {
        "employeeid": item['empid'],
        "leavecode": item['leavecode'],
        "startdaytype": item['startdaytype'],
        "startdate": item['startdate'],
        "recordid": item['recordid'],
        "status": item['status'],
        "useruri": get_user_details().get('useruri'),
        "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_all_timeofftypes_16'),
                                                           'timeoffdescription', item['leavecode'], 'timeoffuri', null),
        "timeoffname": rail.find_first_by_attr_and_get_attr(rail.result('get_data_all_timeofftypes_16'),
                                                            'timeoffdescription', item['leavecode'], 'timeoffname', null),
        "timeoffstatus": rail.find_first_by_attr_and_get_attr(rail.result('get_data_all_timeofftypes_16'),
                                                              'timeoffdescription', item['leavecode'], 'status', null),
        "holidaycalendarname": get_user_details().get('holidaycalendarname'),
        "isholiday": get_isholiday()
    }
