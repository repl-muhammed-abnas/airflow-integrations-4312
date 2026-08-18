# pylint: disable=too-many-statements line-too-long
from ast import literal_eval
from airflow.models import Variable
from rail import find_first_by_attr_and_get_attr


def get_department_uri_values(response, dag_run, department_details_var_name):
    department = dag_run.conf.get('webhook', {}).get('department')
    department_var_values = literal_eval(Variable.get(
        department_details_var_name, default_var=""))
    if not department == 'All':
        return [find_first_by_attr_and_get_attr(response, 'displayText', department, 'uri')]
    return list(map(lambda item: find_first_by_attr_and_get_attr(response, 'displayText', item, 'uri'), department_var_values['department_list']))


def get_start_end_date(dag_run):
    return {
        'start_date': dag_run.conf.get('webhook', {}).get('start_date'),
        'end_date': dag_run.conf.get('webhook', {}).get('end_date'),
    }
