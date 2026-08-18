from datetime import datetime as dt
import pendulum
import rail
from dateutil.relativedelta import relativedelta


def get_time_in_formats(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "start_time": str(current_time),
        "ymd_format": current_time.strftime("%Y%m%d"),
        "hms_format": current_time.strftime("%H%M%S")
    }


def get_all_required_employee_types(mapper):
    return [rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_type'),
                                                 "displaytext", data['employee_type_name'], "uri") for data in mapper if data["export"] == "yes"]


def get_filtered_allowed_location_uris(response):
    if not response['rows']:
        return []
    location_list = list(filter(lambda x: x['displaytext'] == "USA", list(map(lambda item: {
        "uri": item['cells'][0]['uri'],
        "displaytext": item['cells'][1]['cellCollection'][0]['textValue']
    }, response['rows']))))

    return [item['uri'] for item in location_list]


def getenabledemployee(response):
    response = response.json()['d']
    if not response:
        return []

    return list(set(map(lambda data: data['cells'][0]['uri'], response['rows'])))


def get_employeetype(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['uri'], list(map(lambda item: {
        "uri": item['uri'],
        "displaytext": item['displayText']
    }, response))))

def get_hourly_employee_types(mapper):
    return (data['employee_type_name'] for data in mapper if data["export"] == "yes")

def get_start_date_begin_of_week():
    start_date=dt.utcnow() + relativedelta(months=-3)
    return dt.strftime(start_date, "%Y-%m-%d")


def get_end_date_begin_of_week():
    return dt.utcnow().strftime("%Y-%m-%d")

def get_query_data(instance):
    
    return f'''SELECT finalpayrolldata.RECTY,finalpayrolldata.CLIID,finalpayrolldata.INTCA,finalpayrolldata.ORDNO,
            finalpayrolldata.IOPER,finalpayrolldata.INFTY,finalpayrolldata.SUBTY,finalpayrolldata.BEGDA,finalpayrolldata.ENDDA,
            finalpayrolldata.OBJPS,finalpayrolldata.SPRPS,finalpayrolldata.SEQNR,finalpayrolldata.EXTRA,finalpayrolldata.LGART,
            finalpayrolldata.STDAZ,finalpayrolldata.BEGUZ,finalpayrolldata.ENDUZ,finalpayrolldata.BETRG,finalpayrolldata.WAERS,
            finalpayrolldata.ANZHL,finalpayrolldata.ZEINH,finalpayrolldata.VTKEN,finalpayrolldata.BWGRL,finalpayrolldata.AUFKZ,
            finalpayrolldata.ENDOF,finalpayrolldata.UFLD1,finalpayrolldata.UFLD2,finalpayrolldata.UFLD3,finalpayrolldata.KEYPR,
            finalpayrolldata.TRFGR,finalpayrolldata.TRFST,finalpayrolldata.PRAKN,finalpayrolldata.PRAKZ,finalpayrolldata.OTYPE,
            finalpayrolldata.PLANS,finalpayrolldata.VERSL,finalpayrolldata.EXBEL,finalpayrolldata.WTART,finalpayrolldata.TDLANGU,
            finalpayrolldata.TDSUBLA,finalpayrolldata.TDTYPE FROM finalpayrolldata,getalluserdata 
            WHERE finalpayrolldata.CLIID = getalluserdata.Employee_ID AND DATE(getalluserdata.User_End_Date) > DATE('{get_start_date_begin_of_week()}') AND DATE(getalluserdata.User_End_Date) <= DATE('{get_end_date_begin_of_week()}')
            AND finalpayrolldata.SUBTY = "2000" AND finalpayrolldata.Employee_Type_Name IN  ('Hourly_Regular_Full-Time_Project','Hourly_Regular_Full-Time','Hourly_Regular_Part-Time','Hourly_Regular_Part-Time_Project',
              'Hourly_Temporary_Full-Time','Hourly_Temporary_Full-Time_Project','Hourly_Temporary_Part-Time','Hourly_Temporary_Part-Time_Project')
            UNION ALL
                SELECT finalpayrolldata.RECTY,finalpayrolldata.CLIID,finalpayrolldata.INTCA,finalpayrolldata.ORDNO,
            finalpayrolldata.IOPER,finalpayrolldata.INFTY,finalpayrolldata.SUBTY,finalpayrolldata.BEGDA,finalpayrolldata.ENDDA,
            finalpayrolldata.OBJPS,finalpayrolldata.SPRPS,finalpayrolldata.SEQNR,finalpayrolldata.EXTRA,finalpayrolldata.LGART,
            finalpayrolldata.STDAZ,finalpayrolldata.BEGUZ,finalpayrolldata.ENDUZ,finalpayrolldata.BETRG,finalpayrolldata.WAERS,
            finalpayrolldata.ANZHL,finalpayrolldata.ZEINH,finalpayrolldata.VTKEN,finalpayrolldata.BWGRL,finalpayrolldata.AUFKZ,
            finalpayrolldata.ENDOF,finalpayrolldata.UFLD1,finalpayrolldata.UFLD2,finalpayrolldata.UFLD3,finalpayrolldata.KEYPR,
            finalpayrolldata.TRFGR,finalpayrolldata.TRFST,finalpayrolldata.PRAKN,finalpayrolldata.PRAKZ,finalpayrolldata.OTYPE,
            finalpayrolldata.PLANS,finalpayrolldata.VERSL,finalpayrolldata.EXBEL,finalpayrolldata.WTART,finalpayrolldata.TDLANGU,
            finalpayrolldata.TDSUBLA,finalpayrolldata.TDTYPE FROM finalpayrolldata,getalluserdata 
            WHERE finalpayrolldata.CLIID = getalluserdata.Employee_ID 
            AND DATE(getalluserdata.User_End_Date) > DATE('{get_start_date_begin_of_week()}') AND DATE(getalluserdata.User_End_Date) <= DATE('{get_end_date_begin_of_week()}') 
            AND finalpayrolldata.SUBTY <> "2000" AND finalpayrolldata.SUBTY <> "2602" 
            UNION ALL 
            SELECT finalpayrolldata.RECTY,finalpayrolldata.CLIID,finalpayrolldata.INTCA,finalpayrolldata.ORDNO,
            finalpayrolldata.IOPER,finalpayrolldata.INFTY,finalpayrolldata.SUBTY,finalpayrolldata.BEGDA,finalpayrolldata.ENDDA,
            finalpayrolldata.OBJPS,finalpayrolldata.SPRPS,finalpayrolldata.SEQNR,finalpayrolldata.EXTRA,finalpayrolldata.LGART,
            finalpayrolldata.STDAZ,finalpayrolldata.BEGUZ,finalpayrolldata.ENDUZ,finalpayrolldata.BETRG,finalpayrolldata.WAERS,
            finalpayrolldata.ANZHL,finalpayrolldata.ZEINH,finalpayrolldata.VTKEN,finalpayrolldata.BWGRL,finalpayrolldata.AUFKZ,
            finalpayrolldata.ENDOF,finalpayrolldata.UFLD1,finalpayrolldata.UFLD2,finalpayrolldata.UFLD3,finalpayrolldata.KEYPR,
            finalpayrolldata.TRFGR,finalpayrolldata.TRFST,finalpayrolldata.PRAKN,finalpayrolldata.PRAKZ,finalpayrolldata.OTYPE,
            finalpayrolldata.PLANS,finalpayrolldata.VERSL,finalpayrolldata.EXBEL,finalpayrolldata.WTART,finalpayrolldata.TDLANGU,
            finalpayrolldata.TDSUBLA,finalpayrolldata.TDTYPE FROM finalpayrolldata,getalluserdata 
            WHERE finalpayrolldata.CLIID = getalluserdata.Employee_ID AND DATE(getalluserdata.User_End_Date) > DATE('{get_start_date_begin_of_week()}') AND DATE(getalluserdata.User_End_Date) <= DATE('{get_end_date_begin_of_week()}')
            AND finalpayrolldata.SUBTY = "2602" AND finalpayrolldata.Employee_Type_Name NOT IN  ('Hourly_Regular_Full-Time_Project','Hourly_Regular_Full-Time','Hourly_Regular_Part-Time','Hourly_Regular_Part-Time_Project',
              'Hourly_Temporary_Full-Time','Hourly_Temporary_Full-Time_Project','Hourly_Temporary_Part-Time','Hourly_Temporary_Part-Time_Project')'''

