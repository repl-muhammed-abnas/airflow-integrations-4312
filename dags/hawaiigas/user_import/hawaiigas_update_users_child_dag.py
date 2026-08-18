
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'hawaiigas_user_import_update_users_{config.instance}',
        description=f'Live|HawaiiGas_Update users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='getuserdetails_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='getuserdetails_3',
            end_task='catch_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        getuserdetails_3=rail.RepliconServiceOperator(
            task_id='getuserdetails_3',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
              "users": [
                {
                  "uri": "{{ dag_run.conf.useruri }}",
                  "loginName": null,
                  "parameterCorrelationId": null
                }
              ],
              "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            }
        )

        if_request_hiredate_present_4=rail.IfOperator(
            task_id='if_request_hiredate_present_4',
            test='''{{ dag_run.conf.hiredate | is_truthy }}''',
            yes_task="get_start_date_object",
            no_task="if_request_status_contains_inactive_19",
        )

        def get_date_obj(datestring):
            dateobj = datetime.strptime(datestring,'%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        get_start_date_object=rail.PythonOperator(
            task_id='get_start_date_object',
            python_callable= lambda dag_run: get_date_obj(dag_run.conf['hiredate'])
        )

        if_request_terminationdate_blank_8=rail.IfOperator(
            task_id='if_request_terminationdate_blank_8',
            test='''{{ dag_run.conf.terminationdate | is_falsy }}''',
            yes_task="if_hiredate_not_equal_current_startdate",
            no_task="if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11",
        )

        def is_hiredate_unequal_current_startdate(dag_run):
            current_startdate = rail.result('getuserdetails_3')[0]['userDetails']['employmentDateRange']['startDate'] if rail.result(
              'getuserdetails_3')[0]['userDetails']['employmentDateRange'] else ''
            startdate_string = str(current_startdate['day']) + "/" + str(current_startdate['month']) + "/" + str(
              current_startdate['year']) if current_startdate else ''
            return datetime.strptime(dag_run.conf['hiredate'],'%m/%d/%Y') != datetime.strptime(
              (startdate_string if current_startdate else '1/1/2099'),'%d/%m/%Y')

        if_hiredate_not_equal_current_startdate=rail.IfOperator(
            task_id='if_hiredate_not_equal_current_startdate',
            test=is_hiredate_unequal_current_startdate,
            yes_task="updatestartdate_10",
            no_task="if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11",
        )

        updatestartdate_10=rail.RepliconServiceOperator(
            task_id='updatestartdate_10',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": {
                "startDate": {
                  "year": "{{ result('get_start_date_object').year }}",
                  "month": "{{ result('get_start_date_object').month }}",
                  "day": "{{ result('get_start_date_object').day }}"
                },
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11=rail.IfOperator(
            task_id='if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11',
            test='''{{ dag_run.conf.terminationdate | is_falsy  and result('getuserdetails_3')[0].userDetails.employmentDateRange.endDate | is_truthy }}''',
            yes_task="updatestartdate_12",
            no_task="if_request_terminationdate_present_13",
        )

        updatestartdate_12=rail.RepliconServiceOperator(
            task_id='updatestartdate_12',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": {
                "startDate": {
                  "year": "{{ result('get_start_date_object').year }}",
                  "month": "{{ result('get_start_date_object').month }}",
                  "day": "{{ result('get_start_date_object').day }}"
                },
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_request_terminationdate_present_13=rail.IfOperator(
            task_id='if_request_terminationdate_present_13',
            test='''{{ dag_run.conf.terminationdate | is_truthy }}''',
            yes_task="if_terminationdate_not_equal_current",
            no_task="if_request_status_contains_inactive_19",
        )

        if_terminationdate_not_equal_current=rail.IfOperator(
            task_id='if_terminationdate_not_equal_current',
            test=lambda dag_run: datetime.strptime(dag_run.conf['terminationdate'],'%m/%d/%Y') != (datetime.strptime(((str(rail.result(
                'getuserdetails_3')[0]['userDetails']['employmentDateRange']['endDate']['day']) + "/" +
                str(rail.result('getuserdetails_3')[0]['userDetails']['employmentDateRange']['endDate']['month']) + "/" +
                str(rail.result('getuserdetails_3')[0]['userDetails']['employmentDateRange']['endDate']['year'])) if rail.result(
                'getuserdetails_3')[0]['userDetails']['employmentDateRange']['endDate'] else '1/1/2099' ) ,'%d/%m/%Y')),
            yes_task="get_enddate_object",
            no_task="if_request_status_contains_inactive_19",
        )

        get_enddate_object=rail.PythonOperator(
            task_id='get_enddate_object',
            python_callable= lambda dag_run: get_date_obj(dag_run.conf['terminationdate'])
        )

        update_enddate_18=rail.RepliconServiceOperator(
            task_id='update_enddate_18',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": {
                "startDate": {
                  "year": "{{ result('get_start_date_object').year }}",
                  "month": "{{ result('get_start_date_object').month }}",
                  "day": "{{ result('get_start_date_object').day }}"
                },
                "endDate": {
                  "year": "{{ result('get_enddate_object').year }}",
                  "month": "{{ result('get_enddate_object').month }}",
                  "day": "{{ result('get_enddate_object').day }}"
                },
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_request_status_contains_inactive_19=rail.IfOperator(
            task_id='if_request_status_contains_inactive_19',
            test='''{{ dag_run.conf.status | matches('Inactive') }}''',
            yes_task="if_request_usercurrentstatus_converted_contains_dataworkato_service1777e7c5requeststatus_20",
            no_task="if_request_status_contains_active_25",
        )

        if_request_usercurrentstatus_converted_contains_dataworkato_service1777e7c5requeststatus_20=rail.IfOperator(
            task_id='if_request_usercurrentstatus_converted_contains_dataworkato_service1777e7c5requeststatus_20',
            test=lambda dag_run: dag_run.conf['status'] in dag_run.conf['usercurrentstatus_converted'],
            yes_task="hawaiigas_userimport_logs_prod_add_entry_21",
            no_task="disable_login_23",
        )

        hawaiigas_userimport_logs_prod_add_entry_21=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_add_entry_21',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties={
                "employeeid": "{{dag_run.conf.employee}}",
                "action": "Update",
                "status": "Exception",
                "details": "User already terminated|{{dag_run_ecid()}}",
                "jobid": "{{ dag_run.conf.callerjobid }}"
            }
        )

        disable_login_23=rail.RepliconServiceOperator(
            task_id='disable_login_23',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        hawaiigas_userimport_logs_prod_add_entry_24=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_add_entry_24',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{dag_run.conf.employee}}",
                "action": "Update",
                "status": "Success",
                "details": "User terminated|{{dag_run_ecid()}}",
                "jobid": "{{ dag_run.conf.callerjobid }}"
            }
        )

        if_request_status_contains_active_25=rail.IfOperator(
            task_id='if_request_status_contains_active_25',
            test='''{{ dag_run.conf.status | matches('Active') }}''',
            yes_task="if_request_usercurrentstatus_converted_contains_inactive_26",
            no_task="catch_log_error",
        )

        if_request_usercurrentstatus_converted_contains_inactive_26=rail.IfOperator(
            task_id='if_request_usercurrentstatus_converted_contains_inactive_26',
            test='''{{ dag_run.conf.usercurrentstatus_converted | matches('Inactive') }}''',
            yes_task="enable_user_27",
            no_task="get_departmentlist_28",
        )

        enable_user_27=rail.RepliconServiceOperator(
            task_id='enable_user_27',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_department_uri(response,dag_run):
            all_departments = response['rows']
            uri = ''
            for department in all_departments:
                if department['cells'][1]['textValue'] == dag_run.conf['department']:
                    uri = department['cells'][0]['uri']
                    break
            return uri

        get_departmentlist_28=rail.RepliconServiceOperator(
            task_id='get_departmentlist_28',
            endpoint="/services/DepartmentListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "1000",
              "columnUris": [
                "urn:replicon:department-list-column:department",
                "urn:replicon:department-list-column:code"
              ],
              "sort": [],
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": null,
                  "filterDefinitionUri": "urn:replicon:department-list-filter:text"
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
                    "text": "{{ dag_run.conf.department }}",
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                  },
                  "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
              }
            },
            data_handler=get_department_uri
        )

        if_log_departmenturiperinput_31_not_equals_to_datarestgetuserdetails_3responsedfirstuserdetailsdepartmenturi_32=rail.IfOperator(
            task_id='if_log_departmenturiperinput_31_not_equals_to_datarestgetuserdetails_3responsedfirstuserdetailsdepartmenturi_32',
            test=lambda: bool( rail.result('get_departmentlist_28') and (not(rail.result('getuserdetails_3')[0]['userDetails']['department'] and
                rail.result('getuserdetails_3')[0]['userDetails']['department']['uri']) or rail.result(
                'getuserdetails_3')[0]['userDetails']['department']['uri'] != rail.result('get_departmentlist_28')) ),
            yes_task="update_department_33",
            no_task="if_userdetails_firstname_not_equals_to_dataworkato_service1777e7c5requestfirstname_34",
        )

        update_department_33=rail.RepliconServiceOperator(
            task_id='update_department_33',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "departmentUri": "{{ result('get_departmentlist_28') }}"
            }
        )

        if_userdetails_firstname_not_equals_to_dataworkato_service1777e7c5requestfirstname_34=rail.IfOperator(
            task_id='if_userdetails_firstname_not_equals_to_dataworkato_service1777e7c5requestfirstname_34',
            test='''{{ result('getuserdetails_3')[0].userDetails.firstName != dag_run.conf.firstname }}''',
            yes_task="updatefirstname_35",
            no_task="if_userdetails_lastname_not_equals_to_dataworkato_service1777e7c5requestlastname_36",
        )

        updatefirstname_35=rail.RepliconServiceOperator(
            task_id='updatefirstname_35',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_userdetails_lastname_not_equals_to_dataworkato_service1777e7c5requestlastname_36=rail.IfOperator(
            task_id='if_userdetails_lastname_not_equals_to_dataworkato_service1777e7c5requestlastname_36',
            test='''{{ result('getuserdetails_3')[0].userDetails.lastName != dag_run.conf.lastname }}''',
            yes_task="update_lastname_37",
            no_task="if_emailaddress_not_equal_current",
        )

        update_lastname_37=rail.RepliconServiceOperator(
            task_id='update_lastname_37',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_emailaddress_not_equal_current=rail.IfOperator(
            task_id='if_emailaddress_not_equal_current',
            test=lambda dag_run: bool( rail.result('getuserdetails_3')[0]['userDetails']['emailAddress'] != (dag_run.conf['firstname'][0].lower() +
                dag_run.conf['lastname'].lower() + "@hawaiigas.com")),
            yes_task="log_emailaddressderived_39",
            no_task="get_all_employee_type_details_41",
        )

        log_emailaddressderived_39=rail.PythonOperator(
            task_id='log_emailaddressderived_39',
            python_callable= lambda dag_run:  dag_run.conf['firstname'][0].lower() + dag_run.conf['lastname'].lower() + "@hawaiigas.com"
        )

        updateemail_40=rail.RepliconServiceOperator(
            task_id='updateemail_40',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "email": "{{ result('log_emailaddressderived_39') }}"
            }
        )

        get_all_employee_type_details_41=rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details_41',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        log_employeetypeuri_42=rail.PythonOperator(
            task_id='log_employeetypeuri_42',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_employee_type_details_41'),'name',dag_run.conf['classid'],'uri')
        )

        if_log_employeetypeuri_42_present_43=rail.IfOperator(
            task_id='if_log_employeetypeuri_42_present_43',
            test=lambda: bool( rail.result('log_employeetypeuri_42') and (not(rail.result('getuserdetails_3')[0]['employeeType'] and rail.result(
                'getuserdetails_3')[0]['employeeType']['uri']) or (rail.result(
                'log_employeetypeuri_42') != rail.result('getuserdetails_3')[0]['employeeType']))),
            yes_task="update_employee_type_for_user_44",
            no_task="get_current_customfield_details",
        )

        update_employee_type_for_user_44=rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_44',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "employeeTypeUri": "{{ result('log_employeetypeuri_42') }}"
            }
        )

        def get_customfield_details():
            customfields = rail.result('getuserdetails_3')[0]['userDetails']['customFieldValues']
            return [{
                "name": customfield['customField']['displayText'],
                "textvalue": customfield['text']
            } for customfield in customfields]

        get_current_customfield_details=rail.PythonOperator(
            task_id='get_current_customfield_details',
            python_callable=get_customfield_details
        )

        log_pluckthedropdownoptionassignedfor_employmenttype_48=rail.PythonOperator(
            task_id='log_pluckthedropdownoptionassignedfor_employmenttype_48',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_current_customfield_details'),'name','Employment Type','textvalue','')
        )

        get_allcustomfields_49=rail.RepliconServiceOperator(
            task_id='get_allcustomfields_49',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
              "objectUri": "urn:replicon:object-type:user"
            }
        )

        if_log_pluckthedropdownoptionassignedfor_employmenttype_48_not_equals_to_dataworkato_service1777e7c5requestemploymenttype_50=rail.IfOperator(
            task_id='if_log_pluckthedropdownoptionassignedfor_employmenttype_48_not_equals_to_dataworkato_service1777e7c5requestemploymenttype_50',
            test=lambda dag_run: bool( dag_run.conf['employmenttype'] and ( dag_run.conf['employmenttype'] != rail.result(
                'log_pluckthedropdownoptionassignedfor_employmenttype_48'))),
            yes_task="log_get_urifor_employment_type_51",
            no_task="get_today_date_obj",
        )

        log_get_urifor_employment_type_51=rail.PythonOperator(
            task_id='log_get_urifor_employment_type_51',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_allcustomfields_49'),'displayText', "Employment Type",'uri')
        )

        get_alldropdownoptions_52=rail.RepliconServiceOperator(
            task_id='get_alldropdownoptions_52',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
              "customFieldUri": "{{ result('log_get_urifor_employment_type_51') }}"
            }
        )

        def get_dropdown_uri_and_status(dag_run):
            all_options = rail.result('get_alldropdownoptions_52')
            required_dropdown = list(filter(lambda dropdown: dropdown['displayText'] == dag_run.conf['employmenttype'],all_options))
            return {
              'uri': required_dropdown[0]['uri'] if required_dropdown else '',
              'status': required_dropdown[0]['isEnabled'] if required_dropdown else ''
            }

        get_dropdownoption_uri_and_status=rail.PythonOperator(
            task_id='get_dropdownoption_uri_and_status',
            python_callable= get_dropdown_uri_and_status
        )

        if_log_dropdownoptionuri_53_blank_55=rail.IfOperator(
            task_id='if_log_dropdownoptionuri_53_blank_55',
            test='''{{ result('get_dropdownoption_uri_and_status').uri | is_falsy }}''',
            yes_task="add_dropdown_to_current_enabled_ones",
            no_task="if_log_dropdownoptionstatus_54_equals_to_false_62",
        )

        def get_final_dropdownoptions(dag_run):
            all_dropdownoptions = rail.result('get_alldropdownoptions_52')
            final_dropdownoptions = [{
                "target": {
                    "uri": option['uri'],
                    "name": option['displayText']
                },
                "name": option['displayText'],
                "isEnabled": "true"
            }for option in all_dropdownoptions]
            final_dropdownoptions.append({
                "target": {
                    "uri": null,
                    'name': dag_run.conf['employmenttype']
                },
                "name": dag_run.conf['employmenttype'],
                "isEnabled": "true"
            })
            return final_dropdownoptions

        add_dropdown_to_current_enabled_ones=rail.PythonOperator(
            task_id='add_dropdown_to_current_enabled_ones',
            python_callable= get_final_dropdownoptions
        )

        putdropdownoptions_61=rail.RepliconServiceOperator(
            task_id='putdropdownoptions_61',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda:{
              "customFieldUri": rail.result('log_get_urifor_employment_type_51'),
              "customFieldDropDownOptionUris": rail.result('add_dropdown_to_current_enabled_ones')
            }
        )

        if_log_dropdownoptionstatus_54_equals_to_false_62=rail.IfOperator(
            task_id='if_log_dropdownoptionstatus_54_equals_to_false_62',
            test=lambda: rail.result('get_dropdownoption_uri_and_status')['status'] is False,
            yes_task="get_final_dropdown_options",
            no_task="get_alldropdownoptions_71",
        )

        def get_list_of_dropdown_options(dag_run):
            current_dropdownoptions = rail.result('get_alldropdownoptions_52')
            dropdownoption = [{
                'target':{
                    'uri': option['uri'],
                    'name': option['displayText']
                },
                'name': option['displayText'],
                'isEnabled': 'true' if option['displayText'] == dag_run.conf['employmenttype'] else option['isEnabled']
            } for option in current_dropdownoptions]
            return dropdownoption

        get_final_dropdown_options=rail.PythonOperator(
            task_id='get_final_dropdown_options',
            python_callable=get_list_of_dropdown_options
        )

        putdropdownoptions_70=rail.RepliconServiceOperator(
            task_id='putdropdownoptions_70',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda:{
              "customFieldUri": rail.result('log_get_urifor_employment_type_51'),
              "customFieldDropDownOptionUris": rail.result('get_final_dropdown_options')
            }
        )

        get_alldropdownoptions_71=rail.RepliconServiceOperator(
            task_id='get_alldropdownoptions_71',
            endpoint="/services/CustomFieldService1.svc/GetPageOfAllCustomFieldDropDownOptions",
            data={
              "page": "1",
              "pageSize": "100000",
              "customFieldUri": "{{ result('log_get_urifor_employment_type_51') }}"
            }
        )

        log_dropdownoptionuri_72=rail.PythonOperator(
            task_id='log_dropdownoptionuri_72',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_alldropdownoptions_71'),'displayText',dag_run.conf['employmenttype'],'uri')
        )

        assigndropdownoptiontotheuser_73=rail.RepliconServiceOperator(
            task_id='assigndropdownoptiontotheuser_73',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
              "objectUri": "{{ dag_run.conf.useruri }}",
              "customFieldUri": "{{ result('log_get_urifor_employment_type_51') }}",
              "customFieldDropDownOptionUri": "{{ result('log_dropdownoptionuri_72') }}"
            }
        )

        get_today_date_obj=rail.PythonOperator(
            task_id='get_today_date_obj',
            python_callable= lambda: {
                'day': datetime.now().day,
                'month': datetime.now().month,
                'year': datetime.now().year
            }
        )

        if_request_supervisor_present_77=rail.IfOperator(
            task_id='if_request_supervisor_present_77',
            test=lambda dag_run: bool( dag_run.conf['supervisor'] and (not(rail.result(
                'getuserdetails_3')[0]['userDetails']['supervisor'] and rail.result(
                'getuserdetails_3')[0]['userDetails']['supervisor']['user'] and rail.result(
                'getuserdetails_3')[0]['userDetails']['supervisor']['user']['loginName']) or rail.result(
                'getuserdetails_3')[0]['userDetails']['supervisor']['user']['loginName'] != dag_run.conf['supervisor'])),
            yes_task="searchsupervisor_78",
            no_task="get_all_time_offtypes_102",
        )

        def get_uri_and_status(response,dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['supervisor']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else ''
            }

        searchsupervisor_78=rail.RepliconServiceOperator(
            task_id='searchsupervisor_78',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "1000",
              "columnUris": [
                "urn:replicon:user-list-column:login-name",
                "urn:replicon:user-list-column:enabled"
              ],
              "sort": [],
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": null,
                  "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
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
                    "text": "{{ dag_run.conf.supervisor }}",
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                  },
                  "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
              }
            },
            data_handler=get_uri_and_status
        )

        if_log_supervisoruri_81_present_83=rail.IfOperator(
            task_id='if_log_supervisoruri_81_present_83',
            test='''{{ result('searchsupervisor_78').uri | is_truthy  and result('searchsupervisor_78').status | matches('True') }}''',
            yes_task="get_permissionsassigned_84",
            no_task="if_log_supervisoruri_81_present_98",
        )

        get_permissionsassigned_84=rail.RepliconServiceOperator(
            task_id='get_permissionsassigned_84',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
              "userUri": "{{ result('searchsupervisor_78').uri }}"
            }
        )

        if_log_checkifsupervisorpermissionisassigned_85_blank_86=rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionisassigned_85_blank_86',
            test=lambda: not(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_permissionsassigned_84'),'policyUri','urn:replicon:policy:supervision','user.uri','')),
            yes_task="get_all_permission_sets_91",
            no_task="update_supervisor_assignment_schedule_over_date_range_96",
        )

        get_all_permission_sets_91=rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_91',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        def get_required_permission_sets_uri():
            assigned_permissions = rail.result('get_permissionsassigned_84')
            required_permissions = [{
                'uri': permission['permissionSet']['uri']
            } for permission in assigned_permissions if permission['policyUri'] != 'urn:replicon:policy:user']
            print('1',required_permissions)
            required_permissions.append({
                'uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_91'),'displayText','Gen3 Supervisor','uri','')
            })
            print('2',required_permissions)
            required_permissions.append({
                'uri':rail.find_first_by_attr_and_get_attr(rail.result(
                  'get_all_permission_sets_91'),'displayText','Gen3 User - Substitute User Access','uri','')
            })
            print('3',required_permissions)
            return [permission['uri'] for permission in required_permissions if permission['uri'] != '']

        get_all_permission_sets_required_uri=rail.PythonOperator(
            task_id='get_all_permission_sets_required_uri',
            python_callable=get_required_permission_sets_uri
        )

        assign_permissionsetsto_supervisor_95=rail.RepliconServiceOperator(
            task_id='assign_permissionsetsto_supervisor_95',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda:{
              "userUri": rail.result('searchsupervisor_78')['uri'],
              "permissionSetUris": rail.result('get_all_permission_sets_required_uri')
            }
        )

        update_supervisor_assignment_schedule_over_date_range_96=rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_96',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "supervisorUri": "{{ result('searchsupervisor_78').uri }}",
              "dateRange": {
                "startDate": {
                  "year": "{{ result('get_today_date_obj').year }}",
                  "month": "{{ result('get_today_date_obj').month }}",
                  "day": "{{ result('get_today_date_obj').day }}"
                },
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_log_supervisoruri_81_present_98=rail.IfOperator(
            task_id='if_log_supervisoruri_81_present_98',
            test='''{{ result('searchsupervisor_78').uri | is_truthy  and not(result('searchsupervisor_78').status | matches('True')) }}''',
            yes_task="log_forlogging_99",
            no_task="if_log_supervisoruri_81_blank_100",
        )

        log_forlogging_99=rail.PythonOperator(
            task_id='log_forlogging_99',
            python_callable= lambda:  "Supervisor not assigned since Supervisor is in disabled status."
        )

        if_log_supervisoruri_81_blank_100=rail.IfOperator(
            task_id='if_log_supervisoruri_81_blank_100',
            test='''{{ result('searchsupervisor_78').uri | is_falsy }}''',
            yes_task="hawaii_gas_supervisor_lookup_prod_add_entry_101",
            no_task="get_all_time_offtypes_102",
        )

        hawaii_gas_supervisor_lookup_prod_add_entry_101=rail.WriteLogOperator(
            task_id='hawaii_gas_supervisor_lookup_prod_add_entry_101',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="Supervisor",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employee }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}",
                "enduseruri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_time_offtypes_102=rail.RepliconServiceOperator(
            task_id='get_all_time_offtypes_102',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_currentlyassigned_time_offtypes_103=rail.RepliconServiceOperator(
            task_id='get_currentlyassigned_time_offtypes_103',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: {
                'allassigned': response,
                'sick': rail.find_first_by_attr_and_get_attr(response,'displayText','Sick','uri',''),
                'vacation': rail.find_first_by_attr_and_get_attr(response,'displayText','Vacation','uri','')
            }
        )

        if_log_checkif_vacation_timeofftypeisassigned_105_blank_106=rail.IfOperator(
            task_id='if_log_checkif_vacation_timeofftypeisassigned_105_blank_106',
            #pylint: disable = line-too-long
            test='''{{ result('get_currentlyassigned_time_offtypes_103').sick | is_falsy  or result('get_currentlyassigned_time_offtypes_103').vacation | is_falsy }}''',
            yes_task="get_timeofftypes_to_assign_list",
            no_task="get_timesheet_for_date2_118",
        )

        def get_timeofftypes_to_assign():
            currentlyassigned = rail.result('get_currentlyassigned_time_offtypes_103')['allassigned']
            to_assign = [timeofftype['uri'] for timeofftype in currentlyassigned]
            to_assign.append(rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_offtypes_102'),'displayText','Sick','uri'))
            to_assign.append(rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_offtypes_102'),'displayText','Vacation','uri'))
            to_assign = list(filter(lambda x : x!= '',to_assign))
            return list(set(to_assign))

        get_timeofftypes_to_assign_list=rail.PythonOperator(
            task_id='get_timeofftypes_to_assign_list',
            python_callable=get_timeofftypes_to_assign
        )

        if_log_final_listof_time_offtypestobeassigned_115_present_116=rail.IfOperator(
            task_id='if_log_final_listof_time_offtypestobeassigned_115_present_116',
            test='''{{ result('get_timeofftypes_to_assign_list') | is_truthy }}''',
            yes_task="assign_time_offtypeassignment_117",
            no_task="get_timesheet_for_date2_118",
        )

        assign_time_offtypeassignment_117=rail.RepliconServiceOperator(
            task_id='assign_time_offtypeassignment_117',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run:{
              "userUri": dag_run.conf['useruri'],
              "timeOffTypeUris": rail.result('get_timeofftypes_to_assign_list')
            }
        )

        get_timesheet_for_date2_118=rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_118',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "date": {
                    "year": "{{ result('get_today_date_obj').year }}",
                    "month": "{{ result('get_today_date_obj').month }}",
                    "day": "{{ result('get_today_date_obj').day }}"
              },
              "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_details_119=rail.RepliconServiceOperator(
            task_id='get_timesheet_details_119',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
              "timesheetUri": "{{ result('get_timesheet_for_date2_118').timesheet.uri }}"
            },
            data_handler=lambda response: response['dateRange']['startDate']
        )

        get_timesheet_startdate_object=rail.PythonOperator(
            task_id='get_timesheet_startdate_object',
            python_callable= lambda: get_date_obj( str(rail.result('get_timesheet_details_119')['month']) + "/" +
                              str(rail.result('get_timesheet_details_119')['day']) + "/" + str(rail.result('get_timesheet_details_119')['year']))
        )

        get_all_time_offeventscripts_123=rail.RepliconServiceOperator(
            task_id='get_all_time_offeventscripts_123',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_scripts_validationscripts_124=rail.RepliconServiceOperator(
            task_id='get_all_scripts_validationscripts_124',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
        )

        get_required_script_uris=rail.PythonOperator(
            task_id='get_required_script_uris',
            python_callable= lambda: {
                'startingbalancescript': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_time_offeventscripts_123'),'displayText', "Starting Balance Set To",'uri',''),
                'preventbalanceoverdrawscript': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_scripts_validationscripts_124'),'displayText', "Prevent balance overdraw",'uri','')
            }
        )

        if_request_vacationbalance_present_127=rail.IfOperator(
            task_id='if_request_vacationbalance_present_127',
            test='''{{ dag_run.conf.vacationbalance | is_truthy  and dag_run.conf.vacationbalance != dag_run.conf.vacationbalancepreviousday }}''',
            yes_task="log_vacationtimeofftypeuri_128",
            no_task="if_request_sickbalance_present_131",
        )

        log_vacationtimeofftypeuri_128=rail.PythonOperator(
            task_id='log_vacationtimeofftypeuri_128',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_offtypes_102'),'displayText','Vacation','uri','')
        )

        if_log_vacationtimeofftypeuri_128_present_129=rail.IfOperator(
            task_id='if_log_vacationtimeofftypeuri_128_present_129',
            test='''{{ result('log_vacationtimeofftypeuri_128') | is_truthy }}''',
            yes_task="updatevacationbalance_130",
            no_task="if_request_sickbalance_present_131",
        )

        updatevacationbalance_130=rail.RepliconServiceOperator(
            task_id='updatevacationbalance_130',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
              "timeOffAccount": {
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUri": "{{ result('log_vacationtimeofftypeuri_128') }}"
              },
              "policySetScheduleEntries": [
                {
                  "effectiveDate": {
                    "year": "{{ result('get_timesheet_startdate_object').year }}",
                    "month": "{{ result('get_timesheet_startdate_object').month }}",
                    "day": "{{ result('get_timesheet_startdate_object').day }}"
                  },
                  #pylint: disable = line-too-long
                  "description": "Effective {{ result('get_timesheet_startdate_object').year }}/{{ result('get_timesheet_startdate_object').month }}/{{ result('get_timesheet_startdate_object').day }}",
                  "policySet": {
                    "timeOffBalanceEventScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').startingbalancescript }}",
                          "slug": null,
                          "name": null
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "{{ dag_run.conf.vacationbalance }}",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          },
                          {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "20",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          }
                        ]
                      }
                    ],
                    "timeOffValidationScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').preventbalanceoverdrawscript }}"
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                              "number": "0"
                            }
                          }
                        ]
                      }
                    ]
                  }
                }
              ]
            }
        )

        if_request_sickbalance_present_131=rail.IfOperator(
            task_id='if_request_sickbalance_present_131',
            test='''{{ dag_run.conf.sickbalance | is_truthy  and dag_run.conf.sickbalance != dag_run.conf.sickbalancepreviousday }}''',
            yes_task="log_sicktimeofftypeuri_132",
            no_task="hawaiigas_userimport_logs_prod_add_entry_135",
        )

        log_sicktimeofftypeuri_132=rail.PythonOperator(
            task_id='log_sicktimeofftypeuri_132',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_offtypes_102'),'displayText', 'Sick','uri','')
        )

        if_log_sicktimeofftypeuri_132_present_133=rail.IfOperator(
            task_id='if_log_sicktimeofftypeuri_132_present_133',
            test='''{{ result('log_sicktimeofftypeuri_132') | is_truthy }}''',
            yes_task="update_sickbalance_134",
            no_task="hawaiigas_userimport_logs_prod_add_entry_135",
        )

        update_sickbalance_134=rail.RepliconServiceOperator(
            task_id='update_sickbalance_134',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
              "timeOffAccount": {
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUri": "{{ result('log_sicktimeofftypeuri_132') }}"
              },
              "policySetScheduleEntries": [
                {
                  "effectiveDate": {
                    "year": "{{ result('get_timesheet_startdate_object').year }}",
                    "month": "{{ result('get_timesheet_startdate_object').month }}",
                    "day": "{{ result('get_timesheet_startdate_object').day }}"
                  },
                  #pylint: disable = line-too-long
                  "description": "Effective {{ result('get_timesheet_startdate_object').year }}/{{ result('get_timesheet_startdate_object').month }}/{{ result('get_timesheet_startdate_object').day }}",
                  "policySet": {
                    "timeOffBalanceEventScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').startingbalancescript }}",
                          "slug": null,
                          "name": null
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "{{ dag_run.conf.sickbalance }}",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          },
                          {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                              "uri": null,
                              "slug": null,
                              "bool": null,
                              "date": null,
                              "number": "20",
                              "text": null,
                              "time": null,
                              "calendarDayDurationValue": null,
                              "workdayDurationValue": null,
                              "dateRange": null,
                              "collection": []
                            }
                          }
                        ]
                      }
                    ],
                    "timeOffValidationScripts": [
                      {
                        "scriptTarget": {
                          "uri": "{{ result('get_required_script_uris').preventbalanceoverdrawscript }}"
                        },
                        "additionalParameters": [
                          {
                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                            "value": {
                              "number": "0"
                            }
                          }
                        ]
                      }
                    ]
                  }
                }
              ]
            }
        )

        hawaiigas_userimport_logs_prod_add_entry_135=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_add_entry_135',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{dag_run.conf.employee}}|{{dag_run.conf.firstname}} {{dag_run.conf.lastname }}",
                "action": "Update",
                "status": "Success",
                "details": "NA|{{dag_run_ecid()}}",
                "jobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        catch_log_error=rail.WriteLogOperator(
            task_id='catch_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{dag_run.conf.employee}}|{{dag_run.conf.firstname}} {{dag_run.conf.lastname }}",
                "action": "Update",
                "status": "Error",
                "details": "{{get_error_message()}}|{{dag_run_ecid()}}",
                "jobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_log_error
        can_run_batch_task >> rail.Label('No') >> getuserdetails_3
        getuserdetails_3 >> if_request_hiredate_present_4
        if_request_hiredate_present_4 >> rail.Label('Yes')  >> get_start_date_object >> if_request_terminationdate_blank_8
        if_request_terminationdate_blank_8 >> rail.Label('Yes')  >> if_hiredate_not_equal_current_startdate
        if_hiredate_not_equal_current_startdate >> rail.Label(
            'Yes') >> updatestartdate_10 >> if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11
        if_hiredate_not_equal_current_startdate >> rail.Label(
            'No') >> if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11
        if_request_terminationdate_blank_8 >> rail.Label('No') >> if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11
        if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11 >> rail.Label(
            'Yes') >> updatestartdate_12 >> if_request_terminationdate_present_13
        if_request_terminationdate_blank_toremoveenddatewhenitispresentin_repliconandnotinfeedfile_11 >> rail.Label(
            'No') >> if_request_terminationdate_present_13
        if_request_terminationdate_present_13 >> rail.Label('Yes')  >> if_terminationdate_not_equal_current
        if_terminationdate_not_equal_current >> rail.Label('Yes')  >> get_enddate_object >> update_enddate_18 >> if_request_status_contains_inactive_19
        if_terminationdate_not_equal_current >> rail.Label('No') >> if_request_status_contains_inactive_19
        if_request_terminationdate_present_13 >> rail.Label('No') >> if_request_status_contains_inactive_19
        if_request_hiredate_present_4 >> rail.Label('No') >> if_request_status_contains_inactive_19
        if_request_status_contains_inactive_19 >> rail.Label(
            'Yes') >> if_request_usercurrentstatus_converted_contains_dataworkato_service1777e7c5requeststatus_20
        if_request_usercurrentstatus_converted_contains_dataworkato_service1777e7c5requeststatus_20 >> rail.Label(
            'Yes') >> hawaiigas_userimport_logs_prod_add_entry_21 >> if_request_status_contains_active_25
        if_request_usercurrentstatus_converted_contains_dataworkato_service1777e7c5requeststatus_20 >> rail.Label(
            'No') >> disable_login_23 >> hawaiigas_userimport_logs_prod_add_entry_24 >> if_request_status_contains_active_25
        if_request_status_contains_inactive_19 >> rail.Label('No') >> if_request_status_contains_active_25
        if_request_status_contains_active_25 >> rail.Label('Yes')  >> if_request_usercurrentstatus_converted_contains_inactive_26
        if_request_usercurrentstatus_converted_contains_inactive_26 >> rail.Label('Yes')  >> enable_user_27 >> get_departmentlist_28
        if_request_usercurrentstatus_converted_contains_inactive_26 >> rail.Label(
            'No') >> get_departmentlist_28 >> if_log_departmenturiperinput_31_not_equals_to_datarestgetuserdetails_3responsedfirstuserdetailsdepartmenturi_32
        if_log_departmenturiperinput_31_not_equals_to_datarestgetuserdetails_3responsedfirstuserdetailsdepartmenturi_32 >> rail.Label(
            'Yes') >> update_department_33 >> if_userdetails_firstname_not_equals_to_dataworkato_service1777e7c5requestfirstname_34
        if_log_departmenturiperinput_31_not_equals_to_datarestgetuserdetails_3responsedfirstuserdetailsdepartmenturi_32 >> rail.Label(
            'No') >> if_userdetails_firstname_not_equals_to_dataworkato_service1777e7c5requestfirstname_34
        if_userdetails_firstname_not_equals_to_dataworkato_service1777e7c5requestfirstname_34 >> rail.Label(
            'Yes') >> updatefirstname_35 >> if_userdetails_lastname_not_equals_to_dataworkato_service1777e7c5requestlastname_36
        if_userdetails_firstname_not_equals_to_dataworkato_service1777e7c5requestfirstname_34 >> rail.Label(
            'No') >> if_userdetails_lastname_not_equals_to_dataworkato_service1777e7c5requestlastname_36
        if_userdetails_lastname_not_equals_to_dataworkato_service1777e7c5requestlastname_36 >> rail.Label(
            'Yes') >> update_lastname_37 >> if_emailaddress_not_equal_current
        if_userdetails_lastname_not_equals_to_dataworkato_service1777e7c5requestlastname_36 >> rail.Label('No') >> if_emailaddress_not_equal_current
        if_emailaddress_not_equal_current >> rail.Label('Yes')  >> log_emailaddressderived_39 >> updateemail_40 >> get_all_employee_type_details_41
        if_emailaddress_not_equal_current >> rail.Label(
            'No') >> get_all_employee_type_details_41 >> log_employeetypeuri_42 >> if_log_employeetypeuri_42_present_43
        if_log_employeetypeuri_42_present_43 >> rail.Label('Yes')  >> update_employee_type_for_user_44 >> get_current_customfield_details
        if_log_employeetypeuri_42_present_43 >> rail.Label(
            'No') >> get_current_customfield_details >> log_pluckthedropdownoptionassignedfor_employmenttype_48 >> get_allcustomfields_49
        get_allcustomfields_49 >> if_log_pluckthedropdownoptionassignedfor_employmenttype_48_not_equals_to_dataworkato_service1777e7c5requestemploymenttype_50
        if_log_pluckthedropdownoptionassignedfor_employmenttype_48_not_equals_to_dataworkato_service1777e7c5requestemploymenttype_50 >> rail.Label(
            'Yes') >> log_get_urifor_employment_type_51 >> get_alldropdownoptions_52 >> get_dropdownoption_uri_and_status
        get_dropdownoption_uri_and_status >> if_log_dropdownoptionuri_53_blank_55
        if_log_dropdownoptionuri_53_blank_55 >> rail.Label(
            'Yes') >> add_dropdown_to_current_enabled_ones >> putdropdownoptions_61 >> if_log_dropdownoptionstatus_54_equals_to_false_62
        if_log_dropdownoptionuri_53_blank_55 >> rail.Label('No') >> if_log_dropdownoptionstatus_54_equals_to_false_62
        if_log_dropdownoptionstatus_54_equals_to_false_62 >> rail.Label(
            'Yes') >> get_final_dropdown_options >> putdropdownoptions_70 >> get_alldropdownoptions_71
        if_log_dropdownoptionstatus_54_equals_to_false_62 >> rail.Label(
            'No') >> get_alldropdownoptions_71 >> log_dropdownoptionuri_72 >> assigndropdownoptiontotheuser_73 >> get_today_date_obj
        if_log_pluckthedropdownoptionassignedfor_employmenttype_48_not_equals_to_dataworkato_service1777e7c5requestemploymenttype_50 >> rail.Label(
            'No') >> get_today_date_obj >> if_request_supervisor_present_77
        if_request_supervisor_present_77 >> rail.Label('Yes')  >> searchsupervisor_78 >> if_log_supervisoruri_81_present_83
        if_log_supervisoruri_81_present_83 >> rail.Label('Yes') >> get_permissionsassigned_84 >> if_log_checkifsupervisorpermissionisassigned_85_blank_86
        if_log_checkifsupervisorpermissionisassigned_85_blank_86 >> rail.Label(
            'Yes') >> get_all_permission_sets_91 >> get_all_permission_sets_required_uri >> assign_permissionsetsto_supervisor_95
        assign_permissionsetsto_supervisor_95 >> update_supervisor_assignment_schedule_over_date_range_96
        if_log_checkifsupervisorpermissionisassigned_85_blank_86 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_96 >> if_log_supervisoruri_81_blank_100
        if_log_supervisoruri_81_present_83 >> rail.Label('No') >> if_log_supervisoruri_81_present_98
        if_log_supervisoruri_81_present_98 >> rail.Label('Yes')  >> log_forlogging_99 >> if_log_supervisoruri_81_blank_100
        if_log_supervisoruri_81_present_98 >> rail.Label('No') >> if_log_supervisoruri_81_blank_100
        if_log_supervisoruri_81_blank_100 >> rail.Label('Yes')  >> hawaii_gas_supervisor_lookup_prod_add_entry_101 >> get_all_time_offtypes_102
        if_log_supervisoruri_81_blank_100 >> rail.Label('No') >> get_all_time_offtypes_102
        if_request_supervisor_present_77 >> rail.Label(
            'No') >> get_all_time_offtypes_102 >> get_currentlyassigned_time_offtypes_103 >> if_log_checkif_vacation_timeofftypeisassigned_105_blank_106
        if_log_checkif_vacation_timeofftypeisassigned_105_blank_106 >> rail.Label(
            'Yes') >> get_timeofftypes_to_assign_list >> if_log_final_listof_time_offtypestobeassigned_115_present_116
        if_log_final_listof_time_offtypestobeassigned_115_present_116 >> rail.Label('Yes')  >> assign_time_offtypeassignment_117 >> get_timesheet_for_date2_118
        if_log_final_listof_time_offtypestobeassigned_115_present_116 >> rail.Label('No') >> get_timesheet_for_date2_118
        if_log_checkif_vacation_timeofftypeisassigned_105_blank_106 >> rail.Label(
            'No') >> get_timesheet_for_date2_118 >> get_timesheet_details_119 >> get_timesheet_startdate_object >> get_all_time_offeventscripts_123
        get_all_time_offeventscripts_123 >> get_all_scripts_validationscripts_124 >> get_required_script_uris >> if_request_vacationbalance_present_127
        if_request_vacationbalance_present_127 >> rail.Label('Yes')  >> log_vacationtimeofftypeuri_128 >> if_log_vacationtimeofftypeuri_128_present_129
        if_log_vacationtimeofftypeuri_128_present_129 >> rail.Label('Yes')  >> updatevacationbalance_130 >> if_request_sickbalance_present_131
        if_log_vacationtimeofftypeuri_128_present_129 >> rail.Label('No') >> if_request_sickbalance_present_131
        if_request_vacationbalance_present_127 >> rail.Label('No') >> if_request_sickbalance_present_131
        if_request_sickbalance_present_131 >> rail.Label('Yes')  >> log_sicktimeofftypeuri_132 >> if_log_sicktimeofftypeuri_132_present_133
        if_log_sicktimeofftypeuri_132_present_133 >> rail.Label('Yes')  >> update_sickbalance_134 >> hawaiigas_userimport_logs_prod_add_entry_135
        if_log_sicktimeofftypeuri_132_present_133 >> rail.Label('No') >> hawaiigas_userimport_logs_prod_add_entry_135
        if_request_sickbalance_present_131 >> rail.Label('No') >> hawaiigas_userimport_logs_prod_add_entry_135 >> catch_log_error
        if_request_status_contains_active_25 >> rail.Label('No') >> catch_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
