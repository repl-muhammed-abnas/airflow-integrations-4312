from datetime import datetime
from dxctechnology.c1_wbs_import_v3.utils import request_payload


def get_field_config():
    required = True  # for default required validaion message
    return {
        'SOPersonResponsible': lambda x: 'SOPersonResponsible is not available in record'
        if not x['SOPersonResponsible'] and is_service_order_project(x) else False,
        'itemcategoryvalueuri': lambda x: 'Required Item Category not present in Replicon' if not x['itemcategoryvalueuri'] and not is_service_order_project(
            x) else False,
        'serviceordertypevalueuri': lambda x: 'Required Service Order Type not available in Replicon'
        if not x['serviceordertypevalueuri'] and is_service_order_project(
            x) else False,
        'ProjectDefinition': required,
        'WBSElement': "WBS Element not present in payload",
        'Description': lambda x: 'WBS Element Name is not present in payload' if not x['Description'] and not is_service_order_project(x) else False,
        'InternalSAPObjectNumber': 'WBS Internal Object Number not present in payload',
        'PersonResponsibleNumber': lambda x: 'PersonResponsibleNum not available in record'
        if not x['PersonResponsibleNumber'] and not is_service_order_project(x) else False,
        'ProjectType': 'Project Type is not present in payload',
        'AccountAssignmentIndicator': 'Account Assignment Indicator is not present in payload',
        'WBSStartDate': lambda x: 'WBS Start Date is not present in payload' if not x['WBSStartDate'] and not is_service_order_project(x) else False,
        'WBSFinishDate': lambda x: 'WBS Finish Date is not present in payload'
        if not x['WBSFinishDate'] and not is_service_order_project(x) else
        'Finish Date should be >= Start Date'
        if x['WBSStartDate'] and datetime.strptime(x['WBSFinishDate'], '%Y%m%d') <
        datetime.strptime(x['WBSStartDate'], '%Y%m%d') and not is_service_order_project(x) else False,
        'WBSElementSystemStatus': lambda x: 'WBS Element Status is not present in payload'
        if not x['WBSElementSystemStatus'] and not is_service_order_project(x) else False,
        'Changedby': 'Changed By is not present in payload',
        'Changedon': 'Changed on is not present in payload',
        'ServiceOrderNumberActivityOperation': lambda x: "Service Order Number Activity operation is not present in payload"
        if not x['ServiceOrderNumberActivityOperation'] and is_service_order_project(x) else False,
        'ServiceOrderType': lambda x: 'Service Order Type is not available in payload' if not x['ServiceOrderType'] and is_service_order_project(x) else False,
        'ServiceOrderText': lambda x: 'Service Order Text is not present in payload' if not x['ServiceOrderText'] and is_service_order_project(x) else False,
        'CreatedOnDate': lambda x: 'Created On Date is not present in payload' if not x['CreatedOnDate'] and is_service_order_project(x) else False,
        'ServiceOrderCompanyCode': lambda x: 'Service order company code is not available in record'
        if not x['ServiceOrderCompanyCode'] and is_service_order_project(x) else
        f'Service order company code {x["ServiceOrderCompanyCode"]} is not available in Replicon'
        if not x['serviceordercompanycodeuri'] and is_service_order_project(
            x) else False,
        'ServiceOrderSystemStatus': lambda x: 'Service Order System Status is not present in payload'
        if not x['ServiceOrderSystemStatus'] and is_service_order_project(x) else False,
        # f'Service Order System Status {x["ServiceOrderSystemStatus"]} is invalid. it should start with REL/TECO'
        # if is_service_order_project(x) and not x['ServiceOrderSystemStatus'].startswith(("REL", "TECO", "CLSD")) else False,
        'BasicStartDate': lambda x: 'Basic Start Date is not present in payload' if not x['BasicStartDate'] and is_service_order_project(x) else False,
        'BasicFinishDate': lambda x: 'Basic Finish Date is not present in payload' if not x['BasicFinishDate'] and is_service_order_project(x) else
                                     'Finish Date should be >= Start Date'
        if x['BasicStartDate'] and datetime.strptime(x['BasicFinishDate'], '%Y%m%d') <
        datetime.strptime(x['BasicStartDate'], '%Y%m%d') and is_service_order_project(x) else False,
        'InternalServiceOrderobjectnumber': lambda x: 'Internal Service Order Object Number is not present in payload'
        if not x['InternalServiceOrderobjectnumber'] and is_service_order_project(x) else False,
        'SOPersonResponsibleName': lambda x: 'SO Person Responsible Name is not available in payload'
        if not x['SOPersonResponsibleName'] and is_service_order_project(x) else False,
        'CompanyCode': lambda x: 'Company code is not available in record' if not x['CompanyCode'] and not is_service_order_project(x) else
        f'Company Code {x["CompanyCode"]} is not available in Replicon' if not x['companycodeuri'] and not is_service_order_project(
            x) else False,
        'WBSElementCurrency': 'WBS Element Currency is not present in payload',
        'currencyuri': lambda x: f'currency {x["WBSElementCurrency"]} is invalid' if not x['currencyuri'] else False,
        'type': lambda x: f'The WBS has not been processed as the Item category is {x["ItemCategory"]}  and is not associated to SO'
        if x['type'] != 'SO' and x['ItemCategory'] == 'ZCGC' else False
    }


def is_service_order_project(item):
    return item['type'] == 'SO'


def validate_conf_field():
    field_info = get_field_config()
    data = request_payload.get_dag_run_conf()
    errors = []
    for field_name in data:
        if field_name in field_info:
            validate = field_info[field_name]
            field_value = data[field_name]
            if callable(validate):
                error = validate(data)
                if error:
                    errors.append(error)
            elif validate and not field_value:
                errors.append(
                    # default error message
                    f'{field_name} is not present in payload' if isinstance(validate, type(True)) else
                    validate  # string error message from config
                )
    return errors
