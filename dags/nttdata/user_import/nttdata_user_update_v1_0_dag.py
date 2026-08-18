
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdata_user_import_update_user_child_{config.instance}',
        description=f'NTTData_User Update Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
            no_task='create_exception_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_exception_list',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_exception_list=rail.SetVariableOperator(
            task_id='create_exception_list',
            append=False,
            name='Exception',
            value=[]
        )

        get_all_locations=rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint="/services/LocationService1.svc/GetAllLocations",
        )

        if_costcenterdell_present_but_not_in_replicon=rail.IfOperator(
            task_id='if_costcenterdell_present_but_not_in_replicon',
            test=lambda dag_run: bool(dag_run.conf['costcenterdell'] and not rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_locations'),'displayText',dag_run.conf['costcenterdell'],'uri','')),
            yes_task="create_location_or_apply_modifications",
            no_task="get_all_divisions",
        )

        create_location_or_apply_modifications=rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modifications',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
              "modifications": {
                "name": "{{ dag_run.conf.costcenterdell }}",
                "isEnabled": "true"
              },
              "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        get_all_divisions=rail.RepliconServiceOperator(
            task_id='get_all_divisions',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        if_costcenterdell_not_present_in_replicon=rail.IfOperator(
            task_id='if_costcenterdell_not_present_in_replicon',
            test=lambda dag_run: bool(dag_run.conf['costcenterdell'] and not rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_divisions'),'displayText',dag_run.conf['costcenterdell'],'uri','') ),
            yes_task="create_location_or_applymodification",
            no_task="bulk_get_users",
        )

        create_location_or_applymodification=rail.RepliconServiceOperator(
            task_id='create_location_or_applymodification',
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data={
              "modifications": {
                "name": "{{ dag_run.conf.costcenterdell }}",
                "isEnabled": "true"
              },
              "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        bulk_get_users=rail.RepliconServiceOperator(
            task_id='bulk_get_users',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
              "users": [
                {
                  "uri": "{{ dag_run.conf.useruri }}",
                  "loginName": null,
                  "parameterCorrelationId": null
                }
              ],
              "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring,'%Y-%m-%d')
            return{
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        get_today_date_object=rail.PythonOperator(
            task_id='get_today_date_object',
            python_callable=lambda: get_date_object(datetime.now().strftime("%Y-%m-%d"))
        )

        get_startdate_object=rail.PythonOperator(
            task_id='get_startdate_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['startdate'])
        )

        if_startdate_not_equal_current=rail.IfOperator(
            task_id='if_startdate_not_equal_current',
            test=lambda: bool ((datetime.strptime((str(rail.result('get_startdate_object')['day']) + '/' + str(rail.result('get_startdate_object')['month']) +
                    '/' + str(rail.result('get_startdate_object')['year'])),'%d/%m/%Y')) !=  (datetime.strptime(
                    (str(rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']['day']) + '/' +
                    str(rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']['month']) + '/' +
                    str(rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']['year'])),'%d/%m/%Y')) ),
            yes_task="if_enddate_not_present",
            no_task="if_enddate_present",
        )

        if_enddate_not_present=rail.IfOperator(
            task_id='if_enddate_not_present',
            test='''{{ dag_run.conf.enddate | is_falsy }}''',
            yes_task="update_startdate",
            no_task="get_enddate_object",
        )

        update_startdate=rail.RepliconServiceOperator(
            task_id='update_startdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
            "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": {
                "startDate": {
                  "year": "{{ result('get_startdate_object').year }}",
                  "month": "{{ result('get_startdate_object').month }}",
                  "day": "{{ result('get_startdate_object').day }}"
                },
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        get_enddate_object=rail.PythonOperator(
            task_id='get_enddate_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['enddate'])
        )

        update_enddate_with_startdate=rail.RepliconServiceOperator(
            task_id='update_enddate_with_startdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run:{
              "userUri": dag_run.conf['useruri'],
                "dateRange": {
                  "startDate": {
                    "year": rail.result('get_startdate_object')['year'],
                    "month": rail.result('get_startdate_object')['month'],
                    "day": rail.result('get_startdate_object')['day']
                  },
                  "endDate": {
                    "year": rail.result('get_enddate_object')['year'],
                    "month": rail.result('get_enddate_object')['month'],
                    "day": rail.result('get_enddate_object')['day']
                  },
                  "relativeDateRangeUri": null,
                  "relativeDateRangeAsOfDate": null
                }
              }
        )

        if_enddate_present=rail.IfOperator(
            task_id='if_enddate_present',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="get_enddateobject",
            no_task="if_firstname_present_and_unequal_current",
        )

        get_enddateobject=rail.PythonOperator(
            task_id='get_enddateobject',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['enddate'])
        )

        update_enddate_with_existing_startdate=rail.RepliconServiceOperator(
            task_id='update_enddate_with_existing_startdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run:{
            "userUri": dag_run.conf['useruri'],
              "dateRange": {
                "startDate": {
                  "year": rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                  "month": rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                  "day": rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']['day']
                },
                "endDate": {
                  "year": rail.result('get_enddateobject')['year'],
                  "month": rail.result('get_enddateobject')['month'],
                  "day": rail.result('get_enddateobject')['day']
                },
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_user_enabled_and_enddate_past_today=rail.IfOperator(
            task_id='if_user_enabled_and_enddate_past_today',
            test=lambda dag_run: bool( rail.result('bulk_get_users')[0]['userDetails']['isEnabled'] and (datetime.strptime(
                    dag_run.conf['enddate'],'%Y-%m-%d') < datetime.now())),
            yes_task="disable_login",
            no_task="if_firstname_present_and_unequal_current",
        )

        disable_login=rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_firstname_present_and_unequal_current=rail.IfOperator(
            task_id='if_firstname_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['firstname'] and (rail.result(
                    'bulk_get_users')[0]['userDetails']['firstName']).lower() != dag_run.conf['firstname'].lower()),
            yes_task="update_first_name",
            no_task="if_lastname_present_and_unequal_current",
        )

        update_first_name=rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_lastname_present_and_unequal_current=rail.IfOperator(
            task_id='if_lastname_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['lastname'] and (rail.result(
                    'bulk_get_users')[0]['userDetails']['lastName']).lower() != dag_run.conf['lastname'].lower()),
            yes_task="update_lastname",
            no_task="if_emailaddress_present_and_unequal_current",
        )

        update_lastname=rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_emailaddress_present_and_unequal_current=rail.IfOperator(
            task_id='if_emailaddress_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['emailaddress'] and ((rail.result('bulk_get_users')[0]['userDetails']['emailAddress']).lower() if
                    rail.result('bulk_get_users')[0]['userDetails']['emailAddress'] else null) != dag_run.conf['emailaddress'].lower()),
            yes_task="update_email",
            no_task="get_customfields_existing_values",
        )

        update_email=rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        def get_existing_customfield_values():
            user_customfields = rail.result('bulk_get_users')[0]['userDetails']['customFieldValues']
            return {
              "dellbadgeid": rail.find_first_by_attr_and_get_attr(user_customfields,'customField.displayText', 'Dell Badge ID','text',''),
              "jobcode": rail.find_first_by_attr_and_get_attr(user_customfields,'customField.displayText', 'Job Code','text',''),
              "costcenterdell": rail.find_first_by_attr_and_get_attr(user_customfields,'customField.displayText', 'Cost Center-Dell','text',''),
              "jobcodestartdate": rail.find_first_by_attr_and_get_attr(user_customfields,'customField.displayText', 'Job Code start date','text',''),
              "costcenterdellstartdate": rail.find_first_by_attr_and_get_attr(user_customfields,'customField.displayText',
                  'Dell Cost CenterStart Date-DSI Data','text',''),
              "country": rail.find_first_by_attr_and_get_attr(user_customfields,'customField.displayText', 'Country','text',''),
              "state": rail.find_first_by_attr_and_get_attr(user_customfields,'customField.displayText', 'State/Province','text',''),
            }

        get_customfields_existing_values=rail.PythonOperator(
            task_id='get_customfields_existing_values',
            python_callable= get_existing_customfield_values
        )

        if_dellbadgeid_present_and_enequal_current=rail.IfOperator(
            task_id='if_dellbadgeid_present_and_enequal_current',
            test=lambda dag_run: bool( dag_run.conf['dellbadgeid'] and dag_run.conf['dellbadgeid'].lower() != rail.result(
                    'get_customfields_existing_values')['dellbadgeid'].lower()),
            yes_task="update_textvalue_of_dellbadgeid",
            no_task="if_jobcode_present_and_unequal_current",
        )

        update_textvalue_of_dellbadgeid=rail.RepliconServiceOperator(
            task_id='update_textvalue_of_dellbadgeid',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
              "objectUri": "{{ dag_run.conf.useruri }}",
              "customFieldUri": "{{ dag_run.conf.dellbadgeudfuri }}",
              "value": "{{ dag_run.conf.dellbadgeid }}"
            }
        )

        if_jobcode_present_and_unequal_current=rail.IfOperator(
            task_id='if_jobcode_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['jobcode'] and dag_run.conf['jobcode'].lower() != rail.result(
              'get_customfields_existing_values')['jobcode'].lower()),
            yes_task="update_text_value_job_code",
            no_task="if_costcenterdell_present_and_unequal_current",
        )

        update_text_value_job_code=rail.RepliconServiceOperator(
            task_id='update_text_value_job_code',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
              "objectUri": "{{ dag_run.conf.useruri }}",
              "customFieldUri": "{{ dag_run.conf.jobcodeudfuri }}",
              "value": "{{ dag_run.conf.jobcode }}"
            }
        )

        if_costcenterdell_present_and_unequal_current=rail.IfOperator(
            task_id='if_costcenterdell_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['costcenterdell'] and dag_run.conf['costcenterdell'].lower() != rail.result(
              'get_customfields_existing_values')['costcenterdell'].lower()),
            yes_task="update_text_value_cost_center_dell",
            no_task="if_jobcodestartdate_present_and_unequal_current",
        )

        update_text_value_cost_center_dell=rail.RepliconServiceOperator(
            task_id='update_text_value_cost_center_dell',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
              "objectUri": "{{ dag_run.conf.useruri }}",
              "customFieldUri": "{{ dag_run.conf.costcenterdelludfuri }}",
              "value": "{{ dag_run.conf.costcenterdell }}"
            }
        )

        if_jobcodestartdate_present_and_unequal_current=rail.IfOperator(
            task_id='if_jobcodestartdate_present_and_unequal_current',
            test=lambda dag_run: bool(dag_run.conf['jobcodestartdate'] and ( not (rail.result('get_customfields_existing_values')['jobcodestartdate']) or
                    datetime.strptime(dag_run.conf['jobcodestartdate'],'%Y-%m-%d') != datetime.strptime(
                    rail.result('get_customfields_existing_values')['jobcodestartdate'],'%b %d, %Y')) ),
            yes_task="get_jobcodestartdate_object",
            no_task="if_costcenterdellstartdate_present_and_unequal_current",
        )

        get_jobcodestartdate_object=rail.PythonOperator(
            task_id='get_jobcodestartdate_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['jobcodestartdate'])
        )

        update_date_value_jobcodestartdate=rail.RepliconServiceOperator(
            task_id='update_date_value_jobcodestartdate',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run:{
              "objectUri": dag_run.conf['useruri'],
              "customFieldUri": dag_run.conf['jobcodestartdateudfuri'],
              "value":  {
                "year": rail.result('get_jobcodestartdate_object')['year'],
                "month": rail.result('get_jobcodestartdate_object')['month'],
                "day": rail.result('get_jobcodestartdate_object')['day']
              }
            }
        )

        if_costcenterdellstartdate_present_and_unequal_current=rail.IfOperator(
            task_id='if_costcenterdellstartdate_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['costcenterdellstartdate'] and ( not (rail.result(
                    'get_customfields_existing_values')['costcenterdellstartdate']) or datetime.strptime(
                    dag_run.conf['costcenterdellstartdate'],'%Y-%m-%d') != datetime.strptime(rail.result(
                    'get_customfields_existing_values')['costcenterdellstartdate'],'%b %d, %Y')) ),
            yes_task="get_costcenterdellstartdate_object",
            no_task="if_country_present_and_unequal_current",
        )

        get_costcenterdellstartdate_object=rail.PythonOperator(
            task_id='get_costcenterdellstartdate_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['costcenterdellstartdate'])
        )

        update_date_value_dellcostcenterstartdate=rail.RepliconServiceOperator(
            task_id='update_date_value_dellcostcenterstartdate',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run:{
              "objectUri": dag_run.conf['useruri'],
              "customFieldUri": dag_run.conf['costcenterdellstartdateudfuri'],
              "value":  {
                "year": rail.result('get_costcenterdellstartdate_object')['year'],
                "month": rail.result('get_costcenterdellstartdate_object')['month'],
                "day": rail.result('get_costcenterdellstartdate_object')['day']
              }
            }
        )

        if_country_present_and_unequal_current=rail.IfOperator(
            task_id='if_country_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['country'] and dag_run.conf['country'] != rail.result('get_customfields_existing_values')['country']),
            yes_task="if_countrydropdownuri_present",
            no_task="if_state_present_and_unequal_current",
        )

        if_countrydropdownuri_present=rail.IfOperator(
            task_id='if_countrydropdownuri_present',
            test='''{{ dag_run.conf.countrydropdownuri | is_truthy }}''',
            yes_task="update_dropdown_value_country",
            no_task="insert_exception_country_not_available_in_replicon",
        )

        update_dropdown_value_country=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_country',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
              "objectUri": "{{ dag_run.conf.useruri }}",
              "customFieldUri": "{{ dag_run.conf.countryudfuri }}",
              "customFieldDropDownOptionUri": "{{ dag_run.conf.countrydropdownuri }}"
            }
        )

        if_activities_present=rail.IfOperator(
            task_id='if_activities_present',
            test='''{{ dag_run.conf.activities | is_truthy }}''',
            yes_task="get_all_activities",
            no_task="if_activities_not_present",
        )

        get_all_activities=rail.RepliconServiceOperator(
            task_id='get_all_activities',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data={
              "objectUri": "{{ dag_run.conf.useruri }}",
              "customFieldUri": "{{ dag_run.conf.countryudfuri }}",
              "customFieldDropDownOptionUri": "{{ dag_run.conf.countrydropdownuri }}"
            }
        )

        def get_activities_to_assign(dag_run):
            activities = [{
                'activities': activity
            } for activity in dag_run.conf['activities'].split('|')] if dag_run.conf['activities'] else []
            activities_to_assign = [{
                'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_activities'),'displayText',activity['activities'],'uri','')
            } for activity in activities]
            return ([activity['uri'] for activity in activities_to_assign ])

        create_activities_list=rail.PythonOperator(
            task_id='create_activities_list',
            python_callable= get_activities_to_assign
        )

        put_activity_assignments_for_user=rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run:{
              "userUri": dag_run.conf['useruri'],
              "activityUris": rail.result('create_activities_list')
            }
        )

        if_activities_not_present=rail.IfOperator(
            task_id='if_activities_not_present',
            test='''{{ dag_run.conf.activities | is_falsy }}''',
            yes_task="put_activity_assignments_for_user_blank",
            no_task="if_state_present_and_unequal_current",
        )

        put_activity_assignments_for_user_blank=rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_blank',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "activityUris": []
            }
        )

        insert_exception_country_not_available_in_replicon=rail.SetVariableOperator(
            task_id='insert_exception_country_not_available_in_replicon',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Country value "{{ dag_run.conf.country }}" not available in Replicon'
            }
        )

        if_state_present_and_unequal_current=rail.IfOperator(
            task_id='if_state_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['state'] and dag_run.conf['state'] != rail.result('get_customfields_existing_values')['state']),
            yes_task="if_statedropdownuri_present",
            no_task="if_initialschedulename_present",
        )

        if_statedropdownuri_present=rail.IfOperator(
            task_id='if_statedropdownuri_present',
            test='''{{ dag_run.conf.statedropdownuri | is_truthy }}''',
            yes_task="update_dropdownvalue_stateprovince",
            no_task="get_enabled_dropdownoptions_for_stateprovince",
        )

        update_dropdownvalue_stateprovince=rail.RepliconServiceOperator(
            task_id='update_dropdownvalue_stateprovince',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
              "objectUri": "{{ dag_run.conf.useruri }}",
              "customFieldUri": "{{ dag_run.conf.stateudfuri }}",
              "customFieldDropDownOptionUri": "{{ dag_run.conf.statedropdownuri }}"
            }
        )

        get_enabled_dropdownoptions_for_stateprovince=rail.RepliconServiceOperator(
            task_id='get_enabled_dropdownoptions_for_stateprovince',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda:{
              "customFieldUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user-defined-field:21caa067-ccc7-401e-8af8-99fe5a3aefc4"
            }
        )

        update_dropdownvalue_for_state=rail.RepliconServiceOperator(
            task_id='update_dropdownvalue_for_state',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
              "objectUri": dag_run.conf['useruri'],
              "customFieldUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user-defined-field:21caa067-ccc7-401e-8af8-99fe5a3aefc4",
              "customFieldDropDownOptionUri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_dropdownoptions_for_stateprovince'),'displayText',
                  dag_run.conf['state'],'uri','')
            }
        )

        if_initialschedulename_present=rail.IfOperator(
            task_id='if_initialschedulename_present',
            test='''{{ dag_run.conf.initialschedulename | is_truthy }}''',
            yes_task="if_schedulepolicies_contains_urn",
            no_task="if_department_present_and_enequal_current",
        )

        if_schedulepolicies_contains_urn=rail.IfOperator(
            task_id='if_schedulepolicies_contains_urn',
            test=lambda: 'urn' in json.dumps(rail.result('bulk_get_users')[0]['schedulePolicies']),
            yes_task="get_current_office_schedule",
            no_task="if_schedulepolicyuri_not_present_or_initialschedulename_unequal_current",
        )

        def get_current_officeschedule():
            office_schedule = rail.result('bulk_get_users')[0]['schedulePolicies']
            user_startdate = rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']
            officeschedule = [{
                'effectivedate': (str(item['effectiveDate']['day']) + '/' + str(item['effectiveDate']['month']) + '/' + str(item['effectiveDate']['year'])) if
                  item['effectiveDate'] and item['effectiveDate']['day'] else (str(user_startdate['day']) + '/' + str(user_startdate['month']) + '/' +
                  str(user_startdate['year'])),
                'displayText': item['officeSchedule']['displayText'] if item['officeSchedule'] else '',
                'uri': item['uri'],
                'scheduletypeuri': item['scheduleTypeUri'],
                'daydiff': (datetime.now() - datetime.strptime((str(item['effectiveDate']['day']) + '/' + str(item['effectiveDate']['month']) + '/' +
                  str(item['effectiveDate']['year'])) if item['effectiveDate'] and item['effectiveDate']['day'] else (str(user_startdate['day']) + '/' +
                  str(user_startdate['month']) + '/' + str(user_startdate['year'])),'%d/%m/%Y')).days
            } for item in office_schedule]
            return min(officeschedule, key=lambda schedule: schedule['daydiff'])

        get_current_office_schedule=rail.PythonOperator(
            task_id='get_current_office_schedule',
            python_callable=get_current_officeschedule
        )

        if_schedulepolicyuri_not_present_or_initialschedulename_unequal_current=rail.IfOperator(
            task_id='if_schedulepolicyuri_not_present_or_initialschedulename_unequal_current',
            test=lambda dag_run: bool(not(rail.result('get_current_office_schedule')['uri']) or dag_run.conf['initialschedulename'] != rail.result(
                    'get_current_office_schedule')['displayText']),
            yes_task="if_officescheduleuri_not_present",
            no_task="if_department_present_and_enequal_current",
        )

        if_officescheduleuri_not_present=rail.IfOperator(
            task_id='if_officescheduleuri_not_present',
            test='''{{ dag_run.conf.officescheduleuri | is_falsy }}''',
            yes_task="if_initialschedulename_equal_shiftschedule",
            no_task="update_officeschedule",
        )

        if_initialschedulename_equal_shiftschedule=rail.IfOperator(
            task_id='if_initialschedulename_equal_shiftschedule',
            test='''{{ dag_run.conf.initialschedulename == 'Shift Schedule' }}''',
            yes_task="update_office_schedule",
            no_task="insert_exception_schedule_not_available",
        )

        update_office_schedule=rail.RepliconServiceOperator(
            task_id='update_office_schedule',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run:{
              "user": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "schedulePolicyToApply": {
                  "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementSchedule": [],
                  "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                      {
                        "schedulePolicy": {
                          "officeScheduleUri": null,
                          "name": null,
                          "officeSchedule": null,
                          "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                        },
                        "effectiveDate": {
                          "year": rail.result('get_today_date_object')['year'],
                          "month": rail.result('get_today_date_object')['month'],
                          "day": rail.result('get_today_date_object')['day']
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_exception_schedule_not_available=rail.SetVariableOperator(
            task_id='insert_exception_schedule_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Schedule "{{ dag_run.conf.initialschedulename }}" not available in Replicon'
            }
        )

        update_officeschedule=rail.RepliconServiceOperator(
            task_id='update_officeschedule',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run:{
              "user": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "schedulePolicyToApply": {
                  "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementSchedule": [],
                  "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                      {
                        "schedulePolicy": {
                          "officeScheduleUri": dag_run.conf['officescheduleuri'],
                          "name": null,
                          "officeSchedule": {
                            "officeScheduleUri": dag_run.conf['officescheduleuri'],
                            "name": null
                          },
                          "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": {
                          "year": rail.result('get_today_date_object')['year'],
                          "month": rail.result('get_today_date_object')['month'],
                          "day": rail.result('get_today_date_object')['day']
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_department_present_and_enequal_current=rail.IfOperator(
            task_id='if_department_present_and_enequal_current',
            test=lambda dag_run: bool( dag_run.conf['department'] and (not(rail.result(
                    'bulk_get_users')[0]['userDetails']['department']) or dag_run.conf['department'] != rail.result(
                    'bulk_get_users')[0]['userDetails']['department']['displayText'])),
            yes_task="if_departmenturi_not_present",
            no_task="if_employeetype_present_and_enequal_current",
        )

        if_departmenturi_not_present=rail.IfOperator(
            task_id='if_departmenturi_not_present',
            test='''{{ dag_run.conf.departmenturi | is_falsy }}''',
            yes_task="insert_exception_department_not_available",
            no_task="update_department_group",
        )

        insert_exception_department_not_available=rail.SetVariableOperator(
            task_id='insert_exception_department_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Department "{{ dag_run.conf.department }}" not available in Replicon'
            }
        )

        update_department_group=rail.RepliconServiceOperator(
            task_id='update_department_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "departmentToApply": {
                  "uri": "{{ dag_run.conf.departmenturi }}",
                  "name": null,
                  "parent": null,
                  "parameterCorrelationId": null
                }
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_employeetype_present_and_enequal_current=rail.IfOperator(
            task_id='if_employeetype_present_and_enequal_current',
            test=lambda dag_run: bool( dag_run.conf['employeetype'] and( not(rail.result(
                    'bulk_get_users')[0]['employeeType']) or dag_run.conf['employeetype'] != rail.result(
                    'bulk_get_users')[0]['employeeType']['displayText'])),
            yes_task="if_employeetypeuri_not_present",
            no_task="get_all_permission_sets",
        )

        if_employeetypeuri_not_present=rail.IfOperator(
            task_id='if_employeetypeuri_not_present',
            test='''{{ dag_run.conf.employeetypeuri | is_falsy }}''',
            yes_task="insert_exception_employeetype_not_available",
            no_task="update_employeetype_group",
        )

        insert_exception_employeetype_not_available=rail.SetVariableOperator(
            task_id='insert_exception_employeetype_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Employee type "{{ dag_run.conf.employeetype }}" not available in Replicon'
            }
        )

        update_employeetype_group=rail.RepliconServiceOperator(
            task_id='update_employeetype_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
            "employeeTypeToApply": {
                  "uri": "{{ dag_run.conf.employeetypeuri }}",
                  "name": null
                }
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        get_all_permission_sets=rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_permissions_to_assign=rail.PythonOperator(
            task_id='get_permissions_to_assign',
            python_callable=lambda dag_run: [{
                'permission': permission
            } for permission in dag_run.conf['permissionsets'].split('|')]
        )

        foreach_permission_to_assign=rail.ForEachOperator(
            task_id='foreach_permission_to_assign',
            items=lambda: rail.result('get_permissions_to_assign'),
            start_task = 'check_permission_already_assigned',
            end_task = 'foreach_permission_to_assign_end'
        )

        check_permission_already_assigned=rail.PythonOperator(
            task_id='check_permission_already_assigned',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users')[0]['permissionSets'],'displayText',rail.result(
                                'foreach_permission_to_assign')['permission'],'uri','') if (rail.result('bulk_get_users')[0]['permissionSets'] and rail.result(
                                'bulk_get_users')[0]['permissionSets'][0]['uri']) else null
        )

        if_permission_not_already_assigned=rail.IfOperator(
            task_id='if_permission_not_already_assigned',
            test='''{{ result('check_permission_already_assigned') | is_falsy }}''',
            yes_task="get_uri_of_permission_to_assign",
            no_task="foreach_permission_to_assign_end",
        )

        get_uri_of_permission_to_assign=rail.PythonOperator(
            task_id='get_uri_of_permission_to_assign',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_permission_sets'),'displayText',rail.result(
                                'foreach_permission_to_assign')['permission'],'uri','')
        )

        if_permission_uri_present=rail.IfOperator(
            task_id='if_permission_uri_present',
            test='''{{ result('get_uri_of_permission_to_assign') | is_truthy }}''',
            yes_task="assign_permission_set_to_user",
            no_task="foreach_permission_to_assign_end",
        )

        assign_permission_set_to_user=rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "permissionSetUri": "{{ result('get_uri_of_permission_to_assign') }}"
            }
        )

        foreach_permission_to_assign_end=rail.EmptyOperator(
            task_id='foreach_permission_to_assign_end',
        )

        get_effective_user_group_membership=rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": null
            }
        )

        if_location_present_and_unequal_current=rail.IfOperator(
            task_id='if_location_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['location'] and (not(rail.result(
                    'get_effective_user_group_membership')['locations'] and rail.result(
                    'get_effective_user_group_membership')['locations'][0]['location'] and rail.result(
                    'get_effective_user_group_membership')['locations'][0]['location']['location']) or dag_run.conf['locationname'] != rail.result(
                    'get_effective_user_group_membership')['locations'][0]['location']['location']['displayText'])),
            yes_task="get_locationeffectivedate_object",
            no_task="if_servicecenter_present_and_unequal_current",
        )

        get_locationeffectivedate_object=rail.PythonOperator(
            task_id='get_locationeffectivedate_object',\
            python_callable=lambda dag_run: get_date_object((dag_run.conf['locationchangedate'] if dag_run.conf['locationchangedate'] else
                              datetime.now().strftime('%Y-%m-%d')))
        )

        if_locationuri_not_present=rail.IfOperator(
            task_id='if_locationuri_not_present',
            test='''{{ dag_run.conf.locationuri | is_falsy }}''',
            yes_task="get_alllocations",
            no_task="update_locationgroup",
        )

        get_alllocations=rail.RepliconServiceOperator(
            task_id='get_alllocations',
            endpoint="/services/LocationService1.svc/GetAllLocations",
        )

        if_location_not_present_in_replicon=rail.IfOperator(
            task_id='if_location_not_present_in_replicon',
            test=lambda dag_run: not bool(rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_alllocations'),'displayText',dag_run.conf['location'],'uri','')),
            yes_task="createlocation_or_applymodification",
            no_task="get_uri_of_location_to_assign",
        )

        createlocation_or_applymodification=rail.RepliconServiceOperator(
            task_id='createlocation_or_applymodification',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
              "modifications": {
                "name": "{{ dag_run.conf.location }}",
                "isEnabled": "true"
              },
              "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        get_uri_of_location_to_assign=rail.PythonOperator(
            task_id='get_uri_of_location_to_assign',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_alllocations'),'displayText',
                                dag_run.conf['location'],'uri','') if rail.find_first_by_attr_and_get_attr(rail.result('get_alllocations'),'displayText',
                                dag_run.conf['location'],'uri','') else rail.result('createlocation_or_applymodification')['uri']
        )

        update_location_group=rail.RepliconServiceOperator(
            task_id='update_location_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "locationScheduleToApply": {
                  "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementLocationSchedule": [],
                  "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                      {
                        "location": {
                          "uri": "{{ result('get_uri_of_location_to_assign') }}",
                          "parentUri": null,
                          "name": null
                        },
                        "effectiveDate": {
                          "year": "{{result('get_locationeffectivedate_object').year }}",
                          "month": "{{result('get_locationeffectivedate_object').month }}",
                          "day": "{{result('get_locationeffectivedate_object').day }}"
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_locationgroup=rail.RepliconServiceOperator(
            task_id='update_locationgroup',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "locationScheduleToApply": {
                  "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementLocationSchedule": [],
                  "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                      {
                        "location": {
                          "uri": "{{ dag_run.conf.locationuri }}",
                          "parentUri": null,
                          "name": null
                        },
                        "effectiveDate": {
                          "year": "{{result('get_locationeffectivedate_object').year }}",
                          "month": "{{result('get_locationeffectivedate_object').month }}",
                          "day": "{{result('get_locationeffectivedate_object').day }}"
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_servicecenter_present_and_unequal_current=rail.IfOperator(
            task_id='if_servicecenter_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['servicecenter'] and ( not(rail.result(
                    'get_effective_user_group_membership')['serviceCenters'] and rail.result(
                    'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter'] and rail.result(
                    'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']) or
                    dag_run.conf['servicecenter'] != rail.result(
                    'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'])),
            yes_task="get_servicecenter_effective_dateobject",
            no_task="if_costcenter_present_and_unequal_current",
        )

        get_servicecenter_effective_dateobject=rail.PythonOperator(
            task_id='get_servicecenter_effective_dateobject',
            python_callable=lambda dag_run: get_date_object((dag_run.conf['servicecenterchangedate'] if dag_run.conf['servicecenterchangedate'] else
                               datetime.now().strftime('%Y-%m-%d')))
        )

        if_servicecenteruri_not_present=rail.IfOperator(
            task_id='if_servicecenteruri_not_present',
            test='''{{ dag_run.conf.servicecenteruri | is_falsy }}''',
            yes_task="insert_exception_servicecenter_not_available",
            no_task="updateservicecenter_group",
        )

        insert_exception_servicecenter_not_available=rail.SetVariableOperator(
            task_id='insert_exception_servicecenter_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Service center "{{ dag_run.conf.servicecenter }}" not available in Replicon'
            }
        )

        updateservicecenter_group=rail.RepliconServiceOperator(
            task_id='updateservicecenter_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "serviceCenterScheduleToApply": {
                  "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementServiceCenterSchedule": [],
                  "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                      {
                        "serviceCenter": {
                          "uri": "{{ dag_run.conf.servicecenteruri }}",
                          "parentUri": null,
                          "name": null
                        },
                        "effectiveDate": {
                          "year": "{{result('get_servicecenter_effective_dateobject').year }}",
                          "month": "{{result('get_servicecenter_effective_dateobject').month }}",
                          "day": "{{result('get_servicecenter_effective_dateobject').day }}"
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_costcenter_present_and_unequal_current=rail.IfOperator(
            task_id='if_costcenter_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['costcenter'] and ( not(rail.result(
                    'get_effective_user_group_membership')['costCenters'] and rail.result(
                    'get_effective_user_group_membership')['costCenters'][0]['costCenter'] and rail.result(
                    'get_effective_user_group_membership')['costCenters'][0]['costCenter']['costCenter']) or dag_run.conf['costcenter'] != rail.result(
                    'get_effective_user_group_membership')['costCenters'][0]['costCenter']['costCenter']['displayText'])),
            yes_task="get_costcenter_effective_date_object",
            no_task="if_division_present_and_unequal_current",
        )

        get_costcenter_effective_date_object=rail.PythonOperator(
            task_id='get_costcenter_effective_date_object',
            python_callable=lambda dag_run: get_date_object((dag_run.conf['costcentereffectivedate'] if dag_run.conf['costcentereffectivedate'] else
                                datetime.now().strftime('%Y-%m-%d')))
        )

        if_costcenteruri_not_present=rail.IfOperator(
            task_id='if_costcenteruri_not_present',
            test='''{{ dag_run.conf.costcenteruri | is_falsy }}''',
            yes_task="insert_exception_costcenter_not_available",
            no_task="updatecostcenter_group",
        )

        insert_exception_costcenter_not_available=rail.SetVariableOperator(
            task_id='insert_exception_costcenter_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Cost center (OT Eligible) "{{ dag_run.conf.costcenter }}" not available in Replicon'
            }
        )

        updatecostcenter_group=rail.RepliconServiceOperator(
            task_id='updatecostcenter_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "costCenterScheduleToApply": {
                  "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementCostCenterSchedule": [],
                  "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                      {
                        "costCenter": {
                          "uri": "{{ dag_run.conf.costcenteruri }}",
                          "parentUri": null,
                          "name": null
                        },
                        "effectiveDate": {
                          "year": "{{result('get_costcenter_effective_date_object').year }}",
                          "month": "{{result('get_costcenter_effective_date_object').month }}",
                          "day": "{{result('get_costcenter_effective_date_object').day }}"
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_division_present_and_unequal_current=rail.IfOperator(
            task_id='if_division_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['division'] and ( not(rail.result(
                    'get_effective_user_group_membership')['divisions'] and rail.result(
                    'get_effective_user_group_membership')['divisions'][0]['division'] and rail.result(
                    'get_effective_user_group_membership')['divisions'][0]['division']['division']) or dag_run.conf['division'] != rail.result(
                    'get_effective_user_group_membership')['divisions'][0]['division']['division']['displayText'])),
            yes_task="get_divisioneffective_dateobject",
            no_task="if_initialsupervisorloginname_present",
        )

        get_divisioneffective_dateobject=rail.PythonOperator(
            task_id='get_divisioneffective_dateobject',
            python_callable=lambda dag_run: get_date_object((dag_run.conf['divisionchangedate'] if dag_run.conf['divisionchangedate'] else
                              datetime.now().strftime('%Y-%m-%d')))
        )

        if_divisionuri_not_present=rail.IfOperator(
            task_id='if_divisionuri_not_present',
            test='''{{ dag_run.conf.divisionuri | is_falsy }}''',
            yes_task="get_alldivisions",
            no_task="update_divisiongroup",
        )

        get_alldivisions=rail.RepliconServiceOperator(
            task_id='get_alldivisions',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        if_division_present_in_replicon=rail.IfOperator(
            task_id='if_division_present_in_replicon',
            test=lambda dag_run: not bool(rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_alldivisions'),'displayText',dag_run.conf['division'],'uri','')),
            yes_task="create_or_apply_modification_to_location",
            no_task="get_uri_of_division_to_assign",
        )

        create_or_apply_modification_to_location=rail.RepliconServiceOperator(
            task_id='create_or_apply_modification_to_location',
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data={
              "modifications": {
                "name": "{{ dag_run.conf.division }}",
                "isEnabled": "true"
              },
              "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        get_uri_of_division_to_assign=rail.PythonOperator(
            task_id='get_uri_of_division_to_assign',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_alldivisions'),'displayText',
                                dag_run.conf['division'],'uri','') if rail.find_first_by_attr_and_get_attr(rail.result('get_alldivisions'),'displayText',
                                dag_run.conf['division'],'uri','') else rail.result('create_or_apply_modification_to_location')['uri']
        )

        update_division_group=rail.RepliconServiceOperator(
            task_id='update_division_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "divisionScheduleToApply": {
                  "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementDivisionSchedule": [],
                  "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                      {
                        "division": {
                          "uri": "{{ result('get_uri_of_division_to_assign') }}",
                          "parentUri": null,
                          "name": null
                        },
                        "effectiveDate": {
                          "year": "{{ result('get_divisioneffective_dateobject').year }}",
                          "month": "{{ result('get_divisioneffective_dateobject').month }}",
                          "day": "{{ result('get_divisioneffective_dateobject').day }}"
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_divisiongroup=rail.RepliconServiceOperator(
            task_id='update_divisiongroup',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
              "divisionScheduleToApply": {
                  "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementDivisionSchedule": [],
                  "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                      {
                        "division": {
                          "uri": "{{ dag_run.conf.divisionuri }}",
                          "parentUri": null,
                          "name": null
                        },
                        "effectiveDate": {
                          "year": "{{ result('get_divisioneffective_dateobject').year }}",
                          "month": "{{ result('get_divisioneffective_dateobject').month }}",
                          "day": "{{ result('get_divisioneffective_dateobject').day }}"
                        }
                      }
                    ],
                    "endDate": null
                  }
                },
                "projectRolesToApply": null
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_initialsupervisorloginname_present=rail.IfOperator(
            task_id='if_initialsupervisorloginname_present',
            test='''{{ dag_run.conf.initialsupervisorloginname | is_truthy }}''',
            yes_task="if_initialsupervisorloginname_equals_loginname",
            no_task="if_timeofftemplate_present_and_unequal_current",
        )

        if_initialsupervisorloginname_equals_loginname=rail.IfOperator(
            task_id='if_initialsupervisorloginname_equals_loginname',
            test='''{{ dag_run.conf.initialsupervisorloginname == dag_run.conf.loginname }}''',
            yes_task="insert_exception_supervisorloginname_same_as_loginname",
            no_task="get_supervisor_assignment_detailsforuser",
        )

        insert_exception_supervisorloginname_same_as_loginname=rail.SetVariableOperator(
            task_id='insert_exception_supervisorloginname_same_as_loginname',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": "Supervisor not updated  - Supervisor login name is same as User login name"
            }
        )

        get_supervisor_assignment_detailsforuser=rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_detailsforuser',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "asOfDate": {
                "year": "{{result('get_today_date_object').year }}",
                "month": "{{result('get_today_date_object').month }}",
                "day": "{{result('get_today_date_object').day }}"
              }
            }
        )

        if_initialsupervisorloginname_unequal_current=rail.IfOperator(
            task_id='if_initialsupervisorloginname_unequal_current',
            test=lambda dag_run: bool( not(rail.result('get_supervisor_assignment_detailsforuser') and rail.result(
                    'get_supervisor_assignment_detailsforuser')['supervisor'] and rail.result(
                    'get_supervisor_assignment_detailsforuser')['supervisor']['user']['loginName']) or (rail.result('get_supervisor_assignment_detailsforuser'
                    )['supervisor']['user']['loginName'].lower() != dag_run.conf['initialsupervisorloginname'].lower())),
            yes_task="get_supervisoreffective_date_object",
            no_task="if_timeofftemplate_present_and_unequal_current",
        )

        get_supervisoreffective_date_object=rail.PythonOperator(
            task_id='get_supervisoreffective_date_object',
            python_callable=lambda dag_run: get_date_object((dag_run.conf['supervisoreffectivedate'] if dag_run.conf['supervisoreffectivedate'] else
                              datetime.now().strftime('%Y-%m-%d')))
        )

        bulk_getusers=rail.RepliconServiceOperator(
            task_id='bulk_getusers',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
              "users": [
                {
                  "uri": null,
                  "loginName": "{{ dag_run.conf.initialsupervisorloginname }}",
                  "parameterCorrelationId": null
                }
              ],
              "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_uri_for_supervisor_user_not_present=rail.IfOperator(
            task_id='if_uri_for_supervisor_user_not_present',
            test=lambda: not bool(rail.result('bulk_getusers') and rail.result('bulk_getusers')[0]['userDetails']['uri']),
            yes_task="insert_to_nttdata_supervisor_check_lookup",
            no_task="if_supervisoruser_enabled",
        )

        insert_to_nttdata_supervisor_check_lookup=rail.WriteLogOperator(
            task_id='insert_to_nttdata_supervisor_check_lookup',
            log="{{ dag_run.conf.supervisorchecklookup }}",
            message="na",
            severity="na",
            properties=lambda dag_run:{
              "jobid": dag_run.conf['callerjobid'],
              "userloginname": dag_run.conf['loginname'],
              "useruri": dag_run.conf['useruri'],
              "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
              "supervisorloginname": dag_run.conf['initialsupervisorloginname'],
              "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
              "action": "Update",
              "status": '',
              "effectivedate": (dag_run.conf['supervisoreffectivedate'] if dag_run.conf['supervisoreffectivedate'] else datetime.now().strftime('%Y-%m-%d'))
            }
        )

        if_supervisoruser_enabled=rail.IfOperator(
            task_id='if_supervisoruser_enabled',
            test=lambda: rail.result('bulk_getusers') and rail.result('bulk_getusers')[0]['userDetails']['isEnabled'],
            yes_task="checkif_manager_permission_isassigned",
            no_task="insert_to_nttdata_supervisorchecklookup",
        )

        checkif_manager_permission_isassigned=rail.PythonOperator(
            task_id='checkif_manager_permission_isassigned',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_getusers')[0]['permissionSets'],'displayText','Manager','uri','')
        )

        if_managerpermission_is_not_assigned=rail.IfOperator(
            task_id='if_managerpermission_is_not_assigned',
            test='''{{ result('checkif_manager_permission_isassigned') | is_falsy }}''',
            yes_task="assign_manager_permission_set_to_user",
            no_task="update_supervisor_assignment_schedule_over_date_range",
        )

        assign_manager_permission_set_to_user=rail.RepliconServiceOperator(
            task_id='assign_manager_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run:{
              "userUri": rail.result('bulk_getusers')[0]['userDetails']['uri'],
              "permissionSetUri": dag_run.conf['supervisorpermissionuri']
            }
        )

        update_supervisor_assignment_schedule_over_date_range=rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run:{
              "userUri": dag_run.conf['useruri'],
              "supervisorUri": rail.result('bulk_getusers')[0]['userDetails']['uri'],
              "dateRange": {
                "startDate": {
                  "year": rail.result('get_supervisoreffective_date_object')['year'],
                  "month": rail.result('get_supervisoreffective_date_object')['month'],
                  "day": rail.result('get_supervisoreffective_date_object')['day']
                },
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        insert_to_nttdata_supervisorchecklookup=rail.WriteLogOperator(
            task_id='insert_to_nttdata_supervisorchecklookup',
            log="{{ dag_run.conf.supervisorchecklookup }}",
            message="na",
            severity="na",
            properties=lambda dag_run:{
              "jobid": dag_run.conf['callerjobid'],
              "userloginname": dag_run.conf['loginname'],
              "useruri": dag_run.conf['useruri'],
              "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
              "supervisorloginname": dag_run.conf['initialsupervisorloginname'],
              "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
              "action": "Update",
              "status": '',
              "effectivedate": (dag_run.conf['supervisoreffectivedate'] if dag_run.conf['supervisoreffectivedate'] else datetime.now().strftime('%Y-%m-%d'))
            }
        )

        if_timeofftemplate_present_and_unequal_current=rail.IfOperator(
            task_id='if_timeofftemplate_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['timeofftemplate'] and ( not(rail.result(
                    'bulk_get_users')[0]['timeOffTemplate']) or dag_run.conf['timeofftemplate'] != rail.result(
                    'bulk_get_users')[0]['timeOffTemplate']['displayText'])),
            yes_task="if_timeofftemplateuri_present",
            no_task="if_timesheettemplate_present_and_unequal_current",
        )

        if_timeofftemplateuri_present=rail.IfOperator(
            task_id='if_timeofftemplateuri_present',
            test='''{{ dag_run.conf.timeofftemplateuri | is_truthy }}''',
            yes_task="assign_policy_set_to_user_timeofftemplate",
            no_task="insert_exception_timeofftemplate_not_available",
        )

        assign_policy_set_to_user_timeofftemplate=rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_timeofftemplate',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "policySetUri": "{{ dag_run.conf.timeofftemplateuri }}"
            }
        )

        insert_exception_timeofftemplate_not_available=rail.SetVariableOperator(
            task_id='insert_exception_timeofftemplate_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Timeoff template "{{ dag_run.conf.timeofftemplate }}" not available in Replicon'
            }
        )

        if_timesheettemplate_present_and_unequal_current=rail.IfOperator(
            task_id='if_timesheettemplate_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['timesheettemplate'] and ( not(rail.result(
                    'bulk_get_users')[0]['timesheetTemplate']) or dag_run.conf['timesheettemplate'] != rail.result(
                    'bulk_get_users')[0]['timesheetTemplate']['name'])),
            yes_task="if_timesheettemplateuri_present",
            no_task="if_timesheetapprovalpath_present_and_unequal_current",
        )

        if_timesheettemplateuri_present=rail.IfOperator(
            task_id='if_timesheettemplateuri_present',
            test='''{{ dag_run.conf.timesheettemplateuri | is_truthy }}''',
            yes_task="assign_policy_set_to_user_timesheettemplate",
            no_task="insert_exception_timesheettemplate_not_available",
        )

        assign_policy_set_to_user_timesheettemplate=rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_timesheettemplate',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "policySetUri": "{{ dag_run.conf.timesheettemplateuri }}"
            }
        )

        insert_exception_timesheettemplate_not_available=rail.SetVariableOperator(
            task_id='insert_exception_timesheettemplate_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Timesheet template "{{ dag_run.conf.timesheettemplate }}" not available in Replicon'
            }
        )

        if_timesheetapprovalpath_present_and_unequal_current=rail.IfOperator(
            task_id='if_timesheetapprovalpath_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['timesheetapprovalpath'] and dag_run.conf['timesheetapprovalpath'] != rail.result(
                    'bulk_get_users')[0]['timesheetApprovalPath']['displayText']),
            yes_task="if_timesheetapprovalpathuri_present",
            no_task="if_timeoffapprovalpath_present_and_unequal_current",
        )

        if_timesheetapprovalpathuri_present=rail.IfOperator(
            task_id='if_timesheetapprovalpathuri_present',
            test='''{{ dag_run.conf.timesheetapprovalpathuri | is_truthy }}''',
            yes_task="update_approval_path_for_user_timesheet",
            no_task="insert_exception_timesheetapprovalpath_not_available",
        )

        update_approval_path_for_user_timesheet=rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "approvalPathUri": "{{ dag_run.conf.timesheetapprovalpathuri }}"
            }
        )

        insert_exception_timesheetapprovalpath_not_available=rail.SetVariableOperator(
            task_id='insert_exception_timesheetapprovalpath_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Timesheet approval path "{{ dag_run.conf.timesheetapprovalpath }}" not available in Replicon'
            }
        )

        if_timeoffapprovalpath_present_and_unequal_current=rail.IfOperator(
            task_id='if_timeoffapprovalpath_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['timeoffapprovalpath'] and dag_run.conf['timeoffapprovalpath'] != rail.result(
                    'bulk_get_users')[0]['timeOffApprovalPath']['displayText']),
            yes_task="if_timeoffapprovalpathuri_present",
            no_task="if_timezone_present",
        )

        if_timeoffapprovalpathuri_present=rail.IfOperator(
            task_id='if_timeoffapprovalpathuri_present',
            test='''{{ dag_run.conf.timeoffapprovalpathuri | is_truthy }}''',
            yes_task="update_approval_path_for_user_timeoff",
            no_task="insert_exception_timeoffapprovalpath_not_available",
        )

        update_approval_path_for_user_timeoff=rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user_timeoff',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "approvalPathUri": "{{ dag_run.conf.timeoffapprovalpathuri }}"
            }
        )

        insert_exception_timeoffapprovalpath_not_available=rail.SetVariableOperator(
            task_id='insert_exception_timeoffapprovalpath_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Timeoff approval path "{{ dag_run.conf.timeoffapprovalpath }}" not available in Replicon'
            }
        )

        if_timezone_present=rail.IfOperator(
            task_id='if_timezone_present',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="if_timezoneuri_present",
            no_task="if_workweek_present_and_unequal_current",
        )

        if_timezoneuri_present=rail.IfOperator(
            task_id='if_timezoneuri_present',
            test='''{{ dag_run.conf.timezoneuri | is_truthy }}''',
            yes_task="if_timezone_unequal_current",
            no_task="insert_exception_timezone_not_available",
        )

        if_timezone_unequal_current=rail.IfOperator(
            task_id='if_timezone_unequal_current',
            test=lambda dag_run: bool(dag_run.conf['timezone'] and (not(rail.result('bulk_get_users')[0]['timeZone'] and
              rail.result('bulk_get_users')[0]['timeZone']['ianaName']) or
              dag_run.conf['timezone'] != rail.result('bulk_get_users')[0]['timeZone']['ianaName'])),
            yes_task="update_time_zone_for_user",
            no_task="if_workweek_present_and_unequal_current",
        )

        update_time_zone_for_user=rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "timeZoneUri": "{{ dag_run.conf.timezoneuri }}"
            }
        )

        insert_exception_timezone_not_available=rail.SetVariableOperator(
            task_id='insert_exception_timezone_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Time zone "{{ dag_run.conf.timezone }}" not available in Replicon'
            }
        )

        if_workweek_present_and_unequal_current=rail.IfOperator(
            task_id='if_workweek_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['workweek'] and ( not(rail.result(
                    'bulk_get_users')[0]['userDetails']['workWeekStartDay']) or dag_run.conf['workweek'] != rail.result(
                    'bulk_get_users')[0]['userDetails']['workWeekStartDay']['uri'])),
            yes_task="update_work_week_start_day_for_user",
            no_task="if_holidaycalendar_present_and_unequal_current",
        )

        update_work_week_start_day_for_user=rail.RepliconServiceOperator(
            task_id='update_work_week_start_day_for_user',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dayOfWeekUri": "{{ dag_run.conf.workweek }}"
            }
        )

        if_holidaycalendar_present_and_unequal_current=rail.IfOperator(
            task_id='if_holidaycalendar_present_and_unequal_current',
            test=lambda dag_run: bool( dag_run.conf['holidaycalendar'] and ( not(rail.result(
                    'bulk_get_users')[0]['holidayCalendar']) or dag_run.conf['holidaycalendar'] != rail.result(
                    'bulk_get_users')[0]['holidayCalendar']['displayText'])),
            yes_task="if_request_holidaycalendaruri_present",
            no_task="add_final_log_for_this_user",
        )

        if_request_holidaycalendaruri_present=rail.IfOperator(
            task_id='if_request_holidaycalendaruri_present',
            test='''{{ dag_run.conf.holidaycalendaruri | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user",
            no_task="insert_exception_holdiaycalendar_not_available",
        )

        update_holiday_calendar_for_user=rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "holidayCalendarUri": "{{ dag_run.conf.holidaycalendaruri }}"
            }
        )

        insert_exception_holdiaycalendar_not_available=rail.SetVariableOperator(
            task_id='insert_exception_holdiaycalendar_not_available',
            append=True,
            name='{{ result("create_exception_list").name }}',
            value={
              "value": 'Holiday calendar "{{ dag_run.conf.holidaycalendar }}" not available in Replicon'
            }
        )

        add_final_log_for_this_user=rail.WriteLogOperator(
            task_id='add_final_log_for_this_user',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: 'Exception' if rail.get_dag_run_var('Exception') else "Success",
            properties=lambda dag_run:{
                "userid": dag_run.conf['loginname'],
                "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
                "action": "update",
                "status": 'Exception' if rail.get_dag_run_var('Exception') else "Success",
                "details": 'Partialy updated - ' + ';'.join([exception['value'] for exception in rail.get_dag_run_var('Exception')]) if
                  rail.get_dag_run_var('Exception') else 'Updated successfully',
                "childjobis": rail.render_template("{{dag_run_ecid()}}"),
                "parentjobid": dag_run.conf['callerjobid']
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.logslookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="error",
            properties={
                "userid": "{{dag_run.conf.loginname}}",
                "username": "{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}",
                "action": "update",
                "status": 'error',
                "details": "{{get_error_message()}}",
                "childjobis": "{{dag_run_ecid()}}",
                "parentjobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_exception_list
        create_exception_list >> get_all_locations >> if_costcenterdell_present_but_not_in_replicon
        if_costcenterdell_present_but_not_in_replicon >> rail.Label('Yes')  >> create_location_or_apply_modifications >> get_all_divisions
        if_costcenterdell_present_but_not_in_replicon >> rail.Label('No') >> get_all_divisions >> if_costcenterdell_not_present_in_replicon
        if_costcenterdell_not_present_in_replicon >> rail.Label('Yes')  >> create_location_or_applymodification >> bulk_get_users
        if_costcenterdell_not_present_in_replicon >> rail.Label(
            'No') >> bulk_get_users >> get_today_date_object >> get_startdate_object >> if_startdate_not_equal_current
        if_startdate_not_equal_current >> rail.Label('Yes')  >> if_enddate_not_present
        if_enddate_not_present >> rail.Label('Yes')  >> update_startdate >> if_enddate_present
        if_enddate_not_present >> rail.Label('No') >> get_enddate_object >> update_enddate_with_startdate >> if_enddate_present
        if_startdate_not_equal_current >> rail.Label('No') >> if_enddate_present
        if_enddate_present >> rail.Label('Yes')  >> get_enddateobject >> update_enddate_with_existing_startdate >> if_user_enabled_and_enddate_past_today
        if_user_enabled_and_enddate_past_today >> rail.Label('Yes')  >> disable_login >> if_firstname_present_and_unequal_current
        if_user_enabled_and_enddate_past_today >> rail.Label('No') >> if_firstname_present_and_unequal_current
        if_enddate_present >> rail.Label('No') >> if_firstname_present_and_unequal_current
        if_firstname_present_and_unequal_current >> rail.Label('Yes')  >> update_first_name >> if_lastname_present_and_unequal_current
        if_firstname_present_and_unequal_current >> rail.Label('No') >> if_lastname_present_and_unequal_current
        if_lastname_present_and_unequal_current >> rail.Label('Yes')  >> update_lastname >> if_emailaddress_present_and_unequal_current
        if_lastname_present_and_unequal_current >> rail.Label('No') >> if_emailaddress_present_and_unequal_current
        if_emailaddress_present_and_unequal_current >> rail.Label('Yes')  >> update_email >> get_customfields_existing_values
        if_emailaddress_present_and_unequal_current >> rail.Label('No') >> get_customfields_existing_values >> if_dellbadgeid_present_and_enequal_current
        if_dellbadgeid_present_and_enequal_current >> rail.Label('Yes')  >> update_textvalue_of_dellbadgeid >> if_jobcode_present_and_unequal_current
        if_dellbadgeid_present_and_enequal_current >> rail.Label('No') >> if_jobcode_present_and_unequal_current
        if_jobcode_present_and_unequal_current >> rail.Label('Yes')  >> update_text_value_job_code >> if_costcenterdell_present_and_unequal_current
        if_jobcode_present_and_unequal_current >> rail.Label('No') >> if_costcenterdell_present_and_unequal_current
        if_costcenterdell_present_and_unequal_current >> rail.Label(
            'Yes')  >> update_text_value_cost_center_dell >> if_jobcodestartdate_present_and_unequal_current
        if_costcenterdell_present_and_unequal_current >> rail.Label('No') >> if_jobcodestartdate_present_and_unequal_current
        if_jobcodestartdate_present_and_unequal_current >> rail.Label(
            'Yes')  >> get_jobcodestartdate_object >> update_date_value_jobcodestartdate >> if_costcenterdellstartdate_present_and_unequal_current
        if_jobcodestartdate_present_and_unequal_current >> rail.Label('No') >> if_costcenterdellstartdate_present_and_unequal_current
        if_costcenterdellstartdate_present_and_unequal_current >> rail.Label(
            'Yes') >> get_costcenterdellstartdate_object >> update_date_value_dellcostcenterstartdate >> if_country_present_and_unequal_current
        if_costcenterdellstartdate_present_and_unequal_current >> rail.Label('No') >> if_country_present_and_unequal_current
        if_country_present_and_unequal_current >> rail.Label('Yes')  >> if_countrydropdownuri_present
        if_countrydropdownuri_present >> rail.Label('No') >> insert_exception_country_not_available_in_replicon >> if_state_present_and_unequal_current
        if_countrydropdownuri_present >> rail.Label('Yes') >> update_dropdown_value_country >> if_activities_present
        if_activities_present >> rail.Label(
            'Yes') >> get_all_activities >> create_activities_list >> put_activity_assignments_for_user >> if_activities_not_present
        if_activities_present >> rail.Label('No') >> if_activities_not_present
        if_activities_not_present >> rail.Label('Yes')  >> put_activity_assignments_for_user_blank >> if_state_present_and_unequal_current
        if_activities_not_present >> rail.Label('No') >> if_state_present_and_unequal_current
        if_country_present_and_unequal_current >> rail.Label('No') >> if_state_present_and_unequal_current
        if_state_present_and_unequal_current >> rail.Label('Yes')  >> if_statedropdownuri_present
        if_statedropdownuri_present >> rail.Label('Yes')  >> update_dropdownvalue_stateprovince >> if_initialschedulename_present
        if_statedropdownuri_present >> rail.Label(
            'No') >> get_enabled_dropdownoptions_for_stateprovince >> update_dropdownvalue_for_state >>if_initialschedulename_present
        if_state_present_and_unequal_current >> rail.Label('No') >> if_initialschedulename_present
        if_initialschedulename_present >> rail.Label('Yes')  >> if_schedulepolicies_contains_urn
        if_schedulepolicies_contains_urn >> rail.Label(
            'Yes') >> get_current_office_schedule >> if_schedulepolicyuri_not_present_or_initialschedulename_unequal_current
        if_schedulepolicies_contains_urn >> rail.Label('No') >> if_schedulepolicyuri_not_present_or_initialschedulename_unequal_current
        if_schedulepolicyuri_not_present_or_initialschedulename_unequal_current >> rail.Label('Yes')  >> if_officescheduleuri_not_present
        if_officescheduleuri_not_present >> rail.Label('Yes')  >> if_initialschedulename_equal_shiftschedule
        if_initialschedulename_equal_shiftschedule >> rail.Label('Yes')  >> update_office_schedule >> if_department_present_and_enequal_current
        if_initialschedulename_equal_shiftschedule >> rail.Label('No') >> insert_exception_schedule_not_available >> if_department_present_and_enequal_current
        if_officescheduleuri_not_present >> rail.Label('No') >> update_officeschedule >> if_department_present_and_enequal_current
        if_schedulepolicyuri_not_present_or_initialschedulename_unequal_current >> rail.Label('No') >> if_department_present_and_enequal_current
        if_initialschedulename_present >> rail.Label('No') >> if_department_present_and_enequal_current
        if_department_present_and_enequal_current >> rail.Label('Yes')  >> if_departmenturi_not_present
        if_departmenturi_not_present >> rail.Label('Yes')  >> insert_exception_department_not_available >> if_employeetype_present_and_enequal_current
        if_departmenturi_not_present >> rail.Label('No') >> update_department_group >> if_employeetype_present_and_enequal_current
        if_department_present_and_enequal_current >> rail.Label('No') >> if_employeetype_present_and_enequal_current
        if_employeetype_present_and_enequal_current >> rail.Label('Yes')  >> if_employeetypeuri_not_present
        if_employeetypeuri_not_present >> rail.Label('Yes')  >> insert_exception_employeetype_not_available >> get_all_permission_sets
        if_employeetypeuri_not_present >> rail.Label('No') >> update_employeetype_group >> get_all_permission_sets
        if_employeetype_present_and_enequal_current >> rail.Label('No') >> get_all_permission_sets >> get_permissions_to_assign >> foreach_permission_to_assign
        foreach_permission_to_assign >> check_permission_already_assigned >> if_permission_not_already_assigned
        if_permission_not_already_assigned >> rail.Label('Yes')  >> get_uri_of_permission_to_assign >> if_permission_uri_present
        if_permission_uri_present >> rail.Label('Yes')  >> assign_permission_set_to_user >> foreach_permission_to_assign_end
        if_permission_uri_present >> rail.Label('No') >> foreach_permission_to_assign_end
        if_permission_not_already_assigned >> rail.Label('No') >> foreach_permission_to_assign_end
        foreach_permission_to_assign >> foreach_permission_to_assign_end >> get_effective_user_group_membership >> if_location_present_and_unequal_current
        if_location_present_and_unequal_current >> rail.Label('Yes')  >> get_locationeffectivedate_object >> if_locationuri_not_present
        if_locationuri_not_present >> rail.Label('Yes')  >> get_alllocations >> if_location_not_present_in_replicon
        if_location_not_present_in_replicon >> rail.Label('Yes')  >> createlocation_or_applymodification >> get_uri_of_location_to_assign
        if_location_not_present_in_replicon >> rail.Label(
            'No') >> get_uri_of_location_to_assign >> update_location_group >> if_servicecenter_present_and_unequal_current
        if_locationuri_not_present >> rail.Label('No') >> update_locationgroup >> if_servicecenter_present_and_unequal_current
        if_location_present_and_unequal_current >> rail.Label('No') >> if_servicecenter_present_and_unequal_current
        if_servicecenter_present_and_unequal_current >> rail.Label('Yes')  >> get_servicecenter_effective_dateobject >> if_servicecenteruri_not_present
        if_servicecenteruri_not_present >> rail.Label('Yes')  >> insert_exception_servicecenter_not_available >> if_costcenter_present_and_unequal_current
        if_servicecenteruri_not_present >> rail.Label('No') >> updateservicecenter_group >> if_costcenter_present_and_unequal_current
        if_servicecenter_present_and_unequal_current >> rail.Label('No') >> if_costcenter_present_and_unequal_current
        if_costcenter_present_and_unequal_current >> rail.Label('Yes')  >> get_costcenter_effective_date_object >> if_costcenteruri_not_present
        if_costcenteruri_not_present >> rail.Label('Yes')  >> insert_exception_costcenter_not_available >> if_division_present_and_unequal_current
        if_costcenteruri_not_present >> rail.Label('No') >> updatecostcenter_group >> if_division_present_and_unequal_current
        if_costcenter_present_and_unequal_current >> rail.Label('No') >> if_division_present_and_unequal_current
        if_division_present_and_unequal_current >> rail.Label('Yes')  >> get_divisioneffective_dateobject >> if_divisionuri_not_present
        if_divisionuri_not_present >> rail.Label('Yes')  >> get_alldivisions >> if_division_present_in_replicon
        if_division_present_in_replicon >> rail.Label('Yes')  >> create_or_apply_modification_to_location >> get_uri_of_division_to_assign
        if_division_present_in_replicon >> rail.Label('No') >> get_uri_of_division_to_assign >> update_division_group >> if_initialsupervisorloginname_present
        if_divisionuri_not_present >> rail.Label('No') >> update_divisiongroup >> if_initialsupervisorloginname_present
        if_division_present_and_unequal_current >> rail.Label('No') >> if_initialsupervisorloginname_present
        if_initialsupervisorloginname_present >> rail.Label('Yes')  >> if_initialsupervisorloginname_equals_loginname
        if_initialsupervisorloginname_equals_loginname >> rail.Label(
            'Yes') >> insert_exception_supervisorloginname_same_as_loginname >> if_timeofftemplate_present_and_unequal_current
        if_initialsupervisorloginname_equals_loginname >> rail.Label(
            'No') >> get_supervisor_assignment_detailsforuser >> if_initialsupervisorloginname_unequal_current
        if_initialsupervisorloginname_unequal_current >> rail.Label(
            'Yes') >> get_supervisoreffective_date_object >> bulk_getusers >> if_uri_for_supervisor_user_not_present
        if_uri_for_supervisor_user_not_present >> rail.Label(
            'Yes') >> insert_to_nttdata_supervisor_check_lookup >> if_timeofftemplate_present_and_unequal_current
        if_uri_for_supervisor_user_not_present >> rail.Label('No') >> if_supervisoruser_enabled
        if_supervisoruser_enabled >> rail.Label('Yes')  >> checkif_manager_permission_isassigned >> if_managerpermission_is_not_assigned
        if_managerpermission_is_not_assigned >> rail.Label(
            'Yes') >> assign_manager_permission_set_to_user >> update_supervisor_assignment_schedule_over_date_range
        if_managerpermission_is_not_assigned >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range >> if_timeofftemplate_present_and_unequal_current
        if_supervisoruser_enabled >> rail.Label('No') >> insert_to_nttdata_supervisorchecklookup >> if_timeofftemplate_present_and_unequal_current
        if_initialsupervisorloginname_unequal_current >> rail.Label('No') >> if_timeofftemplate_present_and_unequal_current
        if_initialsupervisorloginname_present >> rail.Label('No') >> if_timeofftemplate_present_and_unequal_current
        if_timeofftemplate_present_and_unequal_current >> rail.Label('Yes') >> if_timeofftemplateuri_present
        if_timeofftemplate_present_and_unequal_current >> rail.Label('No') >> if_timesheettemplate_present_and_unequal_current
        if_timeofftemplateuri_present >> rail.Label('Yes')  >> assign_policy_set_to_user_timeofftemplate >> if_timesheettemplate_present_and_unequal_current
        if_timeofftemplateuri_present >> rail.Label('No') >> insert_exception_timeofftemplate_not_available >> if_timesheettemplate_present_and_unequal_current
        if_timesheettemplate_present_and_unequal_current >> rail.Label('Yes')  >> if_timesheettemplateuri_present
        if_timesheettemplate_present_and_unequal_current >> rail.Label(
            'No') >> if_timesheetapprovalpath_present_and_unequal_current
        if_timesheettemplateuri_present >> rail.Label(
            'Yes') >> assign_policy_set_to_user_timesheettemplate >> if_timesheetapprovalpath_present_and_unequal_current
        if_timesheettemplateuri_present >> rail.Label(
            'No') >> insert_exception_timesheettemplate_not_available >> if_timesheetapprovalpath_present_and_unequal_current
        if_timesheetapprovalpath_present_and_unequal_current >> rail.Label('Yes')  >> if_timesheetapprovalpathuri_present
        if_timesheetapprovalpath_present_and_unequal_current >> rail.Label('No')  >> if_timeoffapprovalpath_present_and_unequal_current
        if_timesheetapprovalpathuri_present >> rail.Label(
            'Yes') >> update_approval_path_for_user_timesheet >> if_timeoffapprovalpath_present_and_unequal_current
        if_timesheetapprovalpathuri_present >> rail.Label(
            'No') >> insert_exception_timesheetapprovalpath_not_available >> if_timeoffapprovalpath_present_and_unequal_current
        if_timeoffapprovalpath_present_and_unequal_current >> rail.Label('Yes') >> if_timeoffapprovalpathuri_present
        if_timeoffapprovalpath_present_and_unequal_current >> rail.Label('No') >> if_timezone_present
        if_timeoffapprovalpathuri_present >> rail.Label('Yes')  >> update_approval_path_for_user_timeoff >> if_timezone_present
        if_timeoffapprovalpathuri_present >> rail.Label('No') >> insert_exception_timeoffapprovalpath_not_available >> if_timezone_present
        if_timezone_present >> rail.Label('Yes')  >> if_timezoneuri_present
        if_timezoneuri_present >> rail.Label('Yes')  >> if_timezone_unequal_current
        if_timezoneuri_present >> rail.Label('No')  >> insert_exception_timezone_not_available >> if_workweek_present_and_unequal_current
        if_timezone_unequal_current >> rail.Label('Yes')  >> update_time_zone_for_user >> if_workweek_present_and_unequal_current
        if_timezone_unequal_current >> rail.Label('No') >> if_workweek_present_and_unequal_current
        if_timezone_present >> rail.Label('No') >> if_workweek_present_and_unequal_current
        if_workweek_present_and_unequal_current >> rail.Label('Yes')  >> update_work_week_start_day_for_user >> if_holidaycalendar_present_and_unequal_current
        if_workweek_present_and_unequal_current >> rail.Label('No') >> if_holidaycalendar_present_and_unequal_current
        if_holidaycalendar_present_and_unequal_current >> rail.Label('Yes')  >> if_request_holidaycalendaruri_present
        if_request_holidaycalendaruri_present >> rail.Label('Yes')  >> update_holiday_calendar_for_user >> add_final_log_for_this_user
        if_request_holidaycalendaruri_present >> rail.Label('No') >> insert_exception_holdiaycalendar_not_available >> add_final_log_for_this_user
        if_holidaycalendar_present_and_unequal_current >> rail.Label('No') >> add_final_log_for_this_user >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
