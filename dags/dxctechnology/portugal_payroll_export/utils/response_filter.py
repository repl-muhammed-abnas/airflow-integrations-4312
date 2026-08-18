from datetime import datetime as dt
import rail

def convert_location_hierarchy(resp):
    response = resp.json()['d']['rows']
    if not response:
        return []

    return list(map(lambda item: {
            "name": item['cells'][0]['textValue'],
            "fullpath": " | ".join([elem['textValue']for elem in item['cells'][1]['cellCollection']]),
            "uri": item['cells'][0]['uri']
        }, response))


def add_variable_to_list():
    emp_id = rail.load_all_records(rail.result("query_user_data"))[0]['Actual_Employee_ID']
    return {
        'EMPLEADOS': [
            {
                'EMPLEADO': [
                    {
                        'TIPO': 'V',
                        'NUMERO': emp_id if emp_id else rail.result("for_each_regular_emp_id")['Employee_ID'],
                        'PROCESO': [
                            {
                                'PERIODO': dt.now().strftime("%Y%m"),
                                'TT': 'MN',
                                'PAC': '993203'
                            }
                        ],
                        'SECCION': [
                            {
                                'ID': 'RUBRICAS',
                                'RUBRICA': list(map(lambda item:{
                                                'ID': item['Pay_Code_Code'],
                                                'TIPO': 'C',
                                                'content': item['Pay_Code_Hours']
                                },rail.load_all_records(rail.result("query_user_data"))))
                            }
                        ]
                    }
                ]
            }
        ]
    }

def add_absence_data_to_list():
    emp_id = rail.load_all_records(rail.result("query_timeoff_user_data"))[0]['Actual_Employee_ID']
    return {
        'EMPLEADOS': [
            {
                'EMPLEADO': [
                    {
                        'TIPO': 'R',
                        'NUMERO': emp_id if emp_id else rail.result("for_each_timeoff_emp_id")['Employee_ID'],
                        'PROCESO': [
                            {
                                'PERIODO': dt.now().strftime("%Y%m"),
                                'TT': 'MN',
                                'PAC': '993203'
                            }
                        ],
                        'SECCION': [
                            {
                                'ID': 'AUSENCIAS',
                                'AUSENCIA': list(map(lambda item:{
                                                'INICIO': item['Entry_Date'],
                                                'FIN': item['Entry_Date'],
                                                'MOTIVO': item['Pay_Code_Code'],
                                                'ACCION': 'D' if float(item['Pay_Code_Code']) < 0 else "",
                                                'HORAS': abs(float(item['Pay_Code_Hours'])),
                                                'HOURS': ""
                                },rail.load_all_records(rail.result("query_timeoff_user_data"))))
                            }
                        ]
                    }
                ]
            }
        ]
    }

def get_completed_exports_list(response):
    if not response['rows']:
        return []

    return list(filter(lambda x: x['Status'] == 'Complete', map(lambda item: {
        'Payrun': item["cells"][0]['textValue'],
        'Status': item["cells"][1]['textValue'],
        'Creationdate': item["cells"][2]['textValue'],
        'uri': item["cells"][0]['uri']
    }, response['rows'])))[0]
