from datetime import datetime as dt
import pendulum
import rail


def check_export_date_matches(timezone, export_schedule_mapper):
    current_date_str = pendulum.now(timezone)
    current_date = current_date_str.date()
    
    export_data = list(filter(
        lambda entry: dt.strptime(entry['export_date'], '%d.%m.%Y').date() == current_date,
        export_schedule_mapper
    ))

    if export_data:
        ## as the export date is unique, we can return the first one
        return export_data

    return None

def get_oef_field_values(response, dag_run):
    if "All" in dag_run.conf.get('legal_unit'):
        return []
    if response and 'rows' in response:
        return list( map( lambda d: d['cells'][0]['uri'], list(filter(lambda x : x['cells'][0]['textValue'] in dag_run.conf.get('legal_unit') , response['rows']))))
    return []

def get_required_companycode_uris(response, dag_run):
    if "All" in dag_run.conf.get('company_code'):
        return []
    if response and 'rows' in response:
        return list(map(lambda d: d['cells'][1]['uri'], list(filter(lambda x: x['cells'][0].get('textValue') in dag_run.conf.get('company_code'), response['rows']))))
    return []

def timeexport_process_conf(item, index):
    return {
        **item,
        "index": index + 1,
        "fileformat_script_uri": rail.result('get_fileformat_script'),
        "process_start_time": rail.result('process_start_time'),
        "legal_unit_oef_uri": rail.result('get_user_oef_uri'),
        "non_sap_project_type_oef_uri": rail.result('get_required_project_type_value'),
        "legal_unit_filter_uri": rail.result('get_required_time_export_filter_definition')['legal_unit_filter_uri'],
        "project_type_filter_uri": rail.result('get_required_time_export_filter_definition')['project_type_filter_uri']
    }
