from datetime import datetime as dt
import rail

def check_division_and_emptype(code):
    division = rail.find_first_by_attr_and_get_attr(rail.result(
                                "get_enabled_companycodes"), 'displayText', code, 'uri')

    employee_type = rail.find_first_by_attr_and_get_attr(rail.result(
                                "get_enabled_employeetype_groups"), 'displayText', 'Contractor', 'uri')

    return bool(division and employee_type)

def get_current_export_name():
    counter = int(rail.result("get_data_For_all_past_time_exports")[
                                "Payrun"].split('_')[-1])+1
    # pylint: disable=consider-using-f-string
    time = dt.utcnow().strftime("%Y%m%d")+"_{:04d}".format(counter)
    return {
        'replicon_export_name': "PRT_payrolldata_"+time,
        'variable_filename': "VARIABLES_"+time+".xml",
        'absence_filename': "AUSENCIAS_"+time+".xml",
        'log_filename': "log_AUSENCIAS_"+time+".csv"
    }
