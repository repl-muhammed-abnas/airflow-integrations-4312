# pylint: disable=too-many-statements line-too-long
import ast
from airflow.models import Variable
from rail import find_first_by_attr_and_get_attr

def get_department_uri_values(response, department_details_var_name, default_department_list_var):
    department_var_value = Variable.get(department_details_var_name, default_var="")
    if not department_var_value == 'All':
        return [find_first_by_attr_and_get_attr(response, 'displayText', department_var_value, 'uri')]
    default_department_list = Variable.get(default_department_list_var, default_var=[], deserialize_json=True)
    return list(map(lambda item: find_first_by_attr_and_get_attr(
        response, 'displayText', item, 'uri'), default_department_list))
