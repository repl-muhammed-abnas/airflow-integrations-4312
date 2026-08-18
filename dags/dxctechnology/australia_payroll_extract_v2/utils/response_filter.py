import rail
from dxctechnology.australia_payroll_extract_v2.mapper.time_off_balance_mapper import Enabled_users_timeoff, Sell_back_timeoff

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_pay_groups_data(response):
    dag_run_conf = get_dag_run_conf()
    response = response.json()['d']

    return rail.find_first_by_attr_and_get_attr(
        response, 'displayText', dag_run_conf['pay_group_name'], 'uri', default="")

def get_office_schedules(response):
    response = response.json()['d']['rows']
    if not response:
        return []
    uri= 'urn:replicon:list-type:string'
    return list(map(lambda item:{
        'displaytext': item['cells'][0]['textValue'],
        'description': item['cells'][1]['textValue'] if item['cells'][1]['dataType'] == uri else None
    },response))

def convert_location_hierarchy(resp):
    if len(resp.json()['d']['rows']) > 0:
        rows = [row["cells"] for row in resp.json()['d']['rows']]

        def map_row(cells):
            full_path_names = [elem['textValue']
                               for elem in cells[1]['cellCollection']]
            return {
                "name": cells[0]['textValue'],
                "fullpath": " | ".join(full_path_names),
                "uri": cells[0]['uri']
            }
        return [map_row(row) for row in rows]
    return None


def convert_employee_type_hierarchy(resp):
    if len(resp.json()['d']['rows']) > 0:
        rows = [row["cells"] for row in resp.json()['d']['rows']]

        def map_row(cells):
            full_path_names = [elem['textValue']
                               for elem in cells[1]['cellCollection']]
            return {
                "name": cells[0]['textValue'],
                "fullpath": " | ".join(full_path_names),
                "uri": cells[0]['uri']
            }
        return [map_row(row) for row in rows]
    return None

def get_time_off_type_uris_for_active_user(response):
    response = response.json()['d']
    if not response:
        return []
    # pylint: disable=line-too-long
    time_off_list= Enabled_users_timeoff

    time_off_type_uris=[]

    for time_off_type in time_off_list:
        time_off_type_uris.append(rail.find_first_by_attr_and_get_attr(response, "displayText", time_off_type, "uri"))

    return time_off_type_uris

def get_timeoff_type_uris_for_sell_back(response):
    response = response.json()['d']
    if not response:
        return []

    time_off_list= Sell_back_timeoff

    time_off_type_uris=[]

    for time_off_type in time_off_list:
        time_off_type_uris.append(rail.find_first_by_attr_and_get_attr(response, "displayText", time_off_type, "uri"))

    return time_off_type_uris

def get_division_details(response, dag_run):
    response= response.json()['d']
    if not response:
        return []

    company_codes =[]

    for time_off_type in dag_run.conf['Company code']:
        company_codes.append(rail.find_first_by_attr_and_get_attr(response, "displayText", time_off_type, "uri"))

    return company_codes

def get_office_schedule(response):
    dag_run_conf = get_dag_run_conf()
    response = response.json()['d']
    if not response:
        return []

    return rail.find_first_by_attr_and_get_attr(
        response, 'displayText', dag_run_conf['pay_group_name'], 'uri', default="")

def get_start_and_end_dates(startdate, enddate):
    return{
        'start_date': rail.load_all_records(startdate),
        'end_date': rail.load_all_records(enddate),
    }

def get_all_holiday_calanders(response,config):
    response = response.json()['d']
    if not response:
        return []

    return {
    'es_calander_uri': rail.find_first_by_attr_and_get_attr(response,"displayText",config.es_holiday_calendar,'uri'),
    'gsap_calander_uri': rail.find_first_by_attr_and_get_attr(response,"displayText",config.gsap_holiday_calendar,'uri'),
    }
