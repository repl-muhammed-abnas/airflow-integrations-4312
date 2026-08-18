import rail


null = None
def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_user_on_useruri_payload():
    dag_run_conf = get_dag_run_conf()
    return {
            "page": "1",
            "pagesize": "10000",
            "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:division",
                "urn:replicon:user-list-column:employee-id",
                "urn:replicon:user-list-column:enabled"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:user"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": dag_run_conf['C1useruri'],
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            }
            }


def get_user_on_empid_payload():
    dag_run_conf = get_dag_run_conf()
    return{
            "page": "1",
            "pagesize": "10000",
            "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:division",
                "urn:replicon:user-list-column:employee-id",
                "urn:replicon:user-list-column:enabled"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run_conf['COMPASSPersonnelNumber'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
                },
                "operatorUri": "urn:replicon:filter-operator:or",
                "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run_conf['C1GSAPPersonnelNumber'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            }
            }


def get_c1_payload():
    dag_run_conf = get_dag_run_conf()
    return{
            "objectUri": "{{ result('user_details') | attr_or_default('useruri','')}}",
            "customFieldUri":dag_run_conf['Udfuri'],
            "value": dag_run_conf['COMPASSPersonnelNumber']
    }


def get_user_details():
    dag_run_conf = get_dag_run_conf()
    return{
            'useruri' : dag_run_conf['C1useruri'] if dag_run_conf['C1useruri'] else \
                        rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"),'employeeid',get_personnel(),'uri'),
            'type' : rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_useruri"),'uri',dag_run_conf['C1useruri'],'type')
                    if dag_run_conf['C1useruri'] else \
                    rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"),'employeeid',get_personnel(),'type'),
            'companycode' : rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_useruri"),'uri',dag_run_conf['C1useruri'],'companycode')
                    if dag_run_conf['C1useruri'] else \
                    rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"),'employeeid',get_personnel(),'companycode'),
            'length' : len(list(filter(lambda x : x['uri'] == dag_run_conf['C1useruri'],rail.result("get_user_on_useruri"))))
                        if dag_run_conf['C1useruri'] else
                        len(list(filter(lambda x : x['employeeid'] == get_personnel(),rail.result("get_user_on_empid")))),
            'name' : rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_useruri"),'uri',dag_run_conf['C1useruri'],'name')
                    if dag_run_conf['C1useruri'] else \
                    rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"),'employeeid',get_personnel(),'name'),
            'status' :rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_useruri"),'uri',dag_run_conf['C1useruri'],'status')
                    if dag_run_conf['C1useruri'] else \
                    rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"),'employeeid',get_personnel(),'status')
        }


def get_personnel():
    dag_run_conf = get_dag_run_conf()
    # pylint: disable=line-too-long
    return dag_run_conf['COMPASSPersonnelNumber'] if rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"),'employeeid', dag_run_conf['COMPASSPersonnelNumber']) else \
            dag_run_conf['C1GSAPPersonnelNumber']


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_active_users(active_users):
    jsonValue = get_data_from_document(rail.result(active_users))
    return list(
        map(lambda x: {
            'username': x['User_Name'],
            'loginname': x['Login_Name'],
            'employeeid': x['Employeeid'],
            'iapernerid': x['IA_Perner_ID'],
            'cwfalternateid': x['CWF_C1_alternate_ID'],
            'useruri': x['UserUri'],
            'userstatus': x['User_Status']
        }, jsonValue))


def get_properties_exception ():
    dag_run_conf = get_dag_run_conf()
    return {
        'employeeid': dag_run_conf['C1GSAPPersonnelNumber'] if rail.find_first_by_attr_and_get_attr(rail.result('user_details'),'type', 'C1')  else \
            dag_run_conf['COMPASSPersonnelNumber'],
        'value': dag_run_conf['COMPASSPersonnelNumber']  if rail.find_first_by_attr_and_get_attr(rail.result('user_details'),'type', 'C1')  else \
                 dag_run_conf['C1GSAPPersonnelNumber'],
        'status': "Exception"
    }


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_report_details',
            report_name = config.iwo_report_name,
        )

        run_report_group_entry= rail.run_report(
            group_id='run_iwo_report',
            report_params = {
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id = config.replicon_conn_id,
        )
    get_report_details >> run_report_group_entry
