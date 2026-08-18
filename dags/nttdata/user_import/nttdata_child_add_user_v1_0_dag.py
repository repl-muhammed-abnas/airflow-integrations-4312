
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdata_user_import_add_user_child_{config.instance}',
        description=f'NTTData_Child_Add User {config.instance}',
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
            no_task='get_exception_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_exception_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_exception_log=rail.PythonOperator(
            task_id='get_exception_log',
            python_callable= lambda dag_run: ','.join(((( '' if dag_run.conf['departmenturi'] else 'Department not is not available in Replicon,' ) if
                                dag_run.conf['department'] else 'Department not assigned as it is blank in feedfile,') +
                                (( '' if dag_run.conf['servicecenteruri'] else 'Grade provided is not available in Replicon,') if
                                dag_run.conf['grade'] else 'Grade not provided in feedfile,' ) +
                                (( '' if dag_run.conf['timesheetperioduri'] else 'Timesheet period type not is not available in Replicon,') if
                                dag_run.conf['timesheetperiodtype'] else 'Timesheet period type is blank in feedfile,') +
                                ('' if dag_run.conf['employeetypeuri'] else 'Employee type is not not available in Replicon,') +
                                (('' if dag_run.conf['countrydropdownuri'] else 'Country provided is not available in Replicon,') if
                                dag_run.conf['country'] else 'Country not provided in feedfile,')).split(','))
        )

        create_exceptions_log_list=rail.SetVariableOperator(
            task_id='create_exceptions_log_list',
            append=False,
            name='exceptions',
            value=[]
        )

        if_exception_present=rail.IfOperator(
            task_id='if_exception_present',
            test='''{{ result('get_exception_log') | is_truthy }}''',
            yes_task="add_exceptions_log",
            no_task="get_start_date_object",
        )

        add_exceptions_log=rail.WriteLogOperator(
            task_id='add_exceptions_log',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties={
                "userid": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Add",
                "status": "Exception",
                "details": "User is not created" + "{{result('get_exception_log')}}",
                "childjobis": "{{dag_run_ecid()}}",
                "parentjobid": "{{dag_run.conf.callerjobid}}"
            }
        )

        def get_date_object(datestring):
            date = datetime.strptime(datestring,'%Y-%m-%d')
            return {
                'day': date.day,
                'month': date.month,
                'year': date.year
            }

        get_start_date_object=rail.PythonOperator(
            task_id='get_start_date_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['startdate'])
        )

        declare_schedulepolicyschedule_variable=rail.SetVariableOperator(
            task_id='declare_schedulepolicyschedule_variable',
            append=False,
            name='schedulePolicySchedule',
            value=None
        )

        if_officescheduleuri_present=rail.IfOperator(
            task_id='if_officescheduleuri_present',
            test='''{{ dag_run.conf.officescheduleuri | is_truthy }}''',
            yes_task="update_schedulepolicy_variable",
            no_task="if_initial_schedulename_equals_shiftschedule",
        )

        update_schedulepolicy_variable=rail.SetVariableOperator(
            task_id='update_schedulepolicy_variable',
            append=False,
            name='{{ result("declare_schedulepolicyschedule_variable").name }}',
            value=[
                {
                    "schedulePolicy": {
                    "officeScheduleUri": "{{ dag_run.conf.officescheduleuri }}",
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": "{{ dag_run.conf.officescheduleuri }}",
                        "name": null
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ]
        )

        if_initial_schedulename_equals_shiftschedule=rail.IfOperator(
            task_id='if_initial_schedulename_equals_shiftschedule',
            test='''{{ dag_run.conf.initialschedulename == 'Shift Schedule' }}''',
            yes_task="update_variable_schedulepolicyschedule",
            no_task="log_officeschedule_not_assigned",
        )

        update_variable_schedulepolicyschedule=rail.SetVariableOperator(
            task_id='update_variable_schedulepolicyschedule',
            append=False,
            name='{{ result("declare_schedulepolicyschedule_variable").name }}',
            value=[
                {
                    "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": null
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                    },
                    "effectiveDate": null
                }
            ]
        )

        log_officeschedule_not_assigned=rail.SetVariableOperator(
            task_id='log_officeschedule_not_assigned',
            append=True,
            name='{{ result("create_exceptions_log_list").name }}',
            value={
                "log": "Office schedule not assigned since {{ dag_run.conf.initialschedulename }} not available in Replicon"
            }
        )

        if_holidaycalendaruri_present=rail.IfOperator(
            task_id='if_holidaycalendaruri_present',
            test='''{{ dag_run.conf.holidaycalendaruri | is_truthy }}''',
            yes_task="get_holidaycalendar",
            no_task="log_holidaycalendar_not_available",
        )

        get_holidaycalendar=rail.PythonOperator(
            task_id='get_holidaycalendar',
            python_callable=lambda dag_run: {
                "uri": dag_run.conf['holidaycalendaruri'],
                "name": null
            }
        )

        log_holidaycalendar_not_available=rail.SetVariableOperator(
            task_id='log_holidaycalendar_not_available',
            append=True,
            name='{{ result("create_exceptions_log_list").name }}',
            value={
                "log": 'Holiday calendar "{{ dag_run.conf.holidaycalendar }}" not avaiilble in Replicon'
            }
        )

        get_all_permission_sets=rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_permissionstoassign=rail.PythonOperator(
            task_id='get_permissionstoassign',
            python_callable= lambda dag_run: [{
                'permission': item
            } for item in (dag_run.conf['permissionsets']).split('|')]
        )

        create_permission_sets=rail.PythonOperator(
            task_id='create_permission_sets',
            python_callable=lambda: [{
                'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),'displayText',permission['permission'],'uri',''),
                'name': permission['permission']
            } for permission in rail.result('get_permissionstoassign')]
        )

        get_all_activities=rail.RepliconServiceOperator(
            task_id='get_all_activities',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
        )

        def get_activities_list(dag_run):
            if not dag_run.conf['activities']:
                return []
            activities = (dag_run.conf['activities']).split('|')
            all_activities_from_replicon = rail.result('get_all_activities')
            return [{
                'uri': rail.find_first_by_attr_and_get_attr(all_activities_from_replicon,'displayText',activity,'uri',''),
                'name': activity
            } for activity in activities]

        create_activities_list=rail.PythonOperator(
            task_id='create_activities_list',
            python_callable= get_activities_list
        )

        create_policysets_list=rail.SetVariableOperator(
            task_id='create_policysets_list',
            append=False,
            name='policySets',
            value=[]
        )

        if_timesheettemplateuri_present=rail.IfOperator(
            task_id='if_timesheettemplateuri_present',
            test='''{{ dag_run.conf.timesheettemplateuri | is_truthy }}''',
            yes_task="add_to_policysets_list",
            no_task="log_timesheettemplate_not_available",
        )

        add_to_policysets_list=rail.SetVariableOperator(
            task_id='add_to_policysets_list',
            append=True,
            name='{{ result("create_policysets_list").name }}',
            value={
                "uri": "{{ dag_run.conf.timesheettemplateuri }}",
                "name": "{{ dag_run.conf.timesheettemplate }}"
            }
        )

        log_timesheettemplate_not_available=rail.SetVariableOperator(
            task_id='log_timesheettemplate_not_available',
            append=True,
            name='{{ result("create_exceptions_log_list").name }}',
            value={
                "log": "TImesheet tempalte not assigned since  {{ dag_run.conf.timesheettemplate }} not available in Replicon"
            }
        )

        if_timeofftemplateuri_present=rail.IfOperator(
            task_id='if_timeofftemplateuri_present',
            test='''{{ dag_run.conf.timeofftemplateuri | is_truthy }}''',
            yes_task="insert_to_policysets",
            no_task="log_policysets_to_assign",
        )

        insert_to_policysets=rail.SetVariableOperator(
            task_id='insert_to_policysets',
            append=True,
            name='{{ result("create_policysets_list").name }}',
            value={
                "uri": "{{ dag_run.conf.timeofftemplateuri }}",
                "name": "{{ dag_run.conf.timeofftemplate }}"
            }
        )

        log_policysets_to_assign=rail.PythonOperator(
            task_id='log_policysets_to_assign',
            python_callable= lambda: rail.get_dag_run_var('policySets')
        )

        if_timesheetapprovalpathuri_present=rail.IfOperator(
            task_id='if_timesheetapprovalpathuri_present',
            test='''{{ dag_run.conf.timesheetapprovalpathuri | is_truthy }}''',
            yes_task="get_timesheetapproval_path",
            no_task="if_timeoffapprovalpathuri_present",
        )

        get_timesheetapproval_path=rail.PythonOperator(
            task_id='get_timesheetapproval_path',
            python_callable=lambda dag_run: {
                "uri": dag_run.conf['timesheetapprovalpathuri'],
                "name": null
            }
        )

        if_timeoffapprovalpathuri_present=rail.IfOperator(
            task_id='if_timeoffapprovalpathuri_present',
            test='''{{ dag_run.conf.timeoffapprovalpathuri | is_truthy }}''',
            yes_task="get_timeoffapproval_path",
            no_task="if_departmenturi_present",
        )

        get_timeoffapproval_path=rail.PythonOperator(
            task_id='get_timeoffapproval_path',
            python_callable=lambda dag_run:{
                "uri": dag_run.conf['timeoffapprovalpathuri'],
                "name": null
            }
        )

        if_departmenturi_present=rail.IfOperator(
            task_id='if_departmenturi_present',
            test='''{{ dag_run.conf.departmenturi | is_truthy }}''',
            yes_task="get_departmentgroup_schedule",
            no_task="if_employeetypeuri_present",
        )

        get_departmentgroup_schedule=rail.PythonOperator(
            task_id='get_departmentgroup_schedule',
            python_callable=lambda dag_run: {
                "uri": dag_run.conf['departmenturi'],
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            }
        )

        if_employeetypeuri_present=rail.IfOperator(
            task_id='if_employeetypeuri_present',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="get_employeetypegroup_schedule",
            no_task="get_all_locations",
        )

        get_employeetypegroup_schedule=rail.PythonOperator(
            task_id='get_employeetypegroup_schedule',
            python_callable= lambda dag_run:{
                "uri": dag_run.conf['employeetypeuri'],
                "name": null
            }
        )

        get_all_locations=rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint="/services/LocationService1.svc/GetAllLocations",
        )

        if_location_not_present_in_replicon=rail.IfOperator(
            task_id='if_location_not_present_in_replicon',
            test=lambda dag_run: not bool(rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_locations'),'displayText',dag_run.conf['location'],'uri','')),
            yes_task="create_location_or_apply_modifications",
            no_task="if_location_present_or_created",
        )

        create_location_or_apply_modifications=rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modifications',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "modifications": {
                    "name": "{{ dag_run.conf.location }}",
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        if_location_present_or_created=rail.IfOperator(
            task_id='if_location_present_or_created',
            test=lambda dag_run: bool(dag_run.conf['locationuri'] if dag_run.conf['locationuri'] else ( rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_locations'),'displayText',dag_run.conf['location'],'uri','') if rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_locations'),'displayText',dag_run.conf['location'],'uri','') else
                    rail.result('create_location_or_apply_modifications')['uri'])),
            yes_task="get_location_uri",
            no_task="if_servicecenteruri_present",
        )

        get_location_uri=rail.PythonOperator(
            task_id='get_location_uri',
            python_callable= lambda dag_run: (dag_run.conf['locationuri'] if dag_run.conf['locationuri'] else ( rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_locations'),'displayText',dag_run.conf['location'],'uri','') if rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_locations'),'displayText',dag_run.conf['location'],'uri','') else
                                rail.result('create_location_or_apply_modifications')['uri']))
        )

        get_location_schedule=rail.PythonOperator(
            task_id='get_location_schedule',
            python_callable= lambda:[
                {
                    "location": {
                    "uri": rail.result('get_location_uri'),
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        if_servicecenteruri_present=rail.IfOperator(
            task_id='if_servicecenteruri_present',
            test='''{{ dag_run.conf.servicecenteruri | is_truthy }}''',
            yes_task="get_servicecenter_schedule",
            no_task="if_costcenteruri_present",
        )

        get_servicecenter_schedule=rail.PythonOperator(
            task_id='get_servicecenter_schedule',
            python_callable=lambda dag_run:[
                {
                    "serviceCenter": {
                    "uri": dag_run.conf['servicecenteruri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        if_costcenteruri_present=rail.IfOperator(
            task_id='if_costcenteruri_present',
            test='''{{ dag_run.conf.costcenteruri | is_truthy }}''',
            yes_task="get_costcenter_schedule",
            no_task="get_all_divisions",
        )

        get_costcenter_schedule=rail.PythonOperator(
            task_id='get_costcenter_schedule',
            python_callable=lambda dag_run:[
                {
                    "costCenter": {
                    "uri": dag_run.conf['costcenteruri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        get_all_divisions=rail.RepliconServiceOperator(
            task_id='get_all_divisions',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        if_costcenterntt_not_available_in_replicon=rail.IfOperator(
            task_id='if_costcenterntt_not_available_in_replicon',
            test=lambda dag_run: dag_run.conf['costcenterntt'] and not bool(rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_divisions'),'displayText',dag_run.conf['costcenterntt'],'uri','')),
            yes_task="create_division_or_apply_modifications",
            no_task="if_divisionuri_present_or_created",
        )

        create_division_or_apply_modifications=rail.RepliconServiceOperator(
            task_id='create_division_or_apply_modifications',
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data={
                "modifications": {
                    "name": "{{ dag_run.conf.costcenterntt }}",
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        if_divisionuri_present_or_created=rail.IfOperator(
            task_id='if_divisionuri_present_or_created',
            test=lambda dag_run: bool( dag_run.conf['divisionuri'] if dag_run.conf['divisionuri'] else (rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_divisions'),'displayText',dag_run.conf['costcenterntt'],'uri','') if rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_divisions'),'displayText',dag_run.conf['costcenterntt'],'uri','') else
                    rail.result('create_division_or_apply_modifications'))),
            yes_task="get_division_uri",
            no_task="if_payruleuri_present",
        )

        get_division_uri=rail.PythonOperator(
            task_id='get_division_uri',
            python_callable= lambda dag_run: dag_run.conf['divisionuri'] if dag_run.conf['divisionuri'] else (rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_divisions'),'displayText',dag_run.conf['costcenterntt'],'uri','') if rail.find_first_by_attr_and_get_attr(
                                rail.result('get_all_divisions'),'displayText',dag_run.conf['costcenterntt'],'uri','') else
                                rail.result('create_division_or_apply_modifications')['uri'])
        )

        get_division_schedule=rail.PythonOperator(
            task_id='get_division_schedule',
            python_callable=lambda:[
                {
                    "division": {
                    "uri": rail.result('get_division_uri'),
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        if_payruleuri_present=rail.IfOperator(
            task_id='if_payruleuri_present',
            test='''{{ dag_run.conf.payruleuri | is_truthy }}''',
            yes_task="get_payrulescript_schedule",
            no_task="if_timezone_present",
        )

        get_payrulescript_schedule=rail.PythonOperator(
            task_id='get_payrulescript_schedule',
            python_callable=lambda dag_run:[
                {
                    "payRuleScript": {
                    "uri": dag_run.conf['payruleuri'],
                    "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        if_timezone_present=rail.IfOperator(
            task_id='if_timezone_present',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="get_timezone",
            no_task="create_customfieldvalues_list",
        )

        get_timezone=rail.PythonOperator(
            task_id='get_timezone',
            python_callable= lambda dag_run:{
                "uri": null,
                "IANAName": dag_run.conf['timezone']
            }
        )

        create_customfieldvalues_list=rail.SetVariableOperator(
            task_id='create_customfieldvalues_list',
            append=False,
            name='customFieldValues',
            value=[]
        )

        if_dellbadgeid_present=rail.IfOperator(
            task_id='if_dellbadgeid_present',
            test='''{{ dag_run.conf.dellbadgeid | is_truthy }}''',
            yes_task="add_to_customfieldvalues_list",
            no_task="if_jobcode_present",
        )

        add_to_customfieldvalues_list=rail.SetVariableOperator(
            task_id='add_to_customfieldvalues_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.dellbadgeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.dellbadgeid }}",
            }
        )

        if_jobcode_present=rail.IfOperator(
            task_id='if_jobcode_present',
            test='''{{ dag_run.conf.jobcode | is_truthy }}''',
            yes_task="insert_to_customfieldvalues_list",
            no_task="if_jobcodestartdate_present",
        )

        insert_to_customfieldvalues_list=rail.SetVariableOperator(
            task_id='insert_to_customfieldvalues_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.jobcodeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.jobcode }}",
            }
        )

        if_jobcodestartdate_present=rail.IfOperator(
            task_id='if_jobcodestartdate_present',
            test='''{{ dag_run.conf.jobcodestartdate | is_truthy }}''',
            yes_task="get_jobcodestartdate_object",
            no_task="if_costcenterdell_present",
        )

        get_jobcodestartdate_object=rail.PythonOperator(
            task_id='get_jobcodestartdate_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['jobcodestartdate'])
        )

        insert_to_custom_field_values_list=rail.SetVariableOperator(
            task_id='insert_to_custom_field_values_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.jobcodestartdateudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": {
                    "year": "{{result('get_jobcodestartdate_object').year}}",
                    "month": "{{result('get_jobcodestartdate_object').month}}",
                    "day": "{{result('get_jobcodestartdate_object').day}}"
                },
            }
        )

        if_costcenterdell_present=rail.IfOperator(
            task_id='if_costcenterdell_present',
            test='''{{ dag_run.conf.costcenterdell | is_truthy }}''',
            yes_task="insert_to_customfield_values_list",
            no_task="if_country_present",
        )

        insert_to_customfield_values_list=rail.SetVariableOperator(
            task_id='insert_to_customfield_values_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.costcenterdelludfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.costcenterdell }}",
            }
        )

        if_country_present=rail.IfOperator(
            task_id='if_country_present',
            test='''{{ dag_run.conf.country | is_truthy }}''',
            yes_task="insert_to_custom_fieldvalues_list",
            no_task="if_costcenterdellstartdate_present",
        )

        insert_to_custom_fieldvalues_list=rail.SetVariableOperator(
            task_id='insert_to_custom_fieldvalues_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.countryudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.countrydropdownuri }}",
                    "name": null
                },
            }
        )

        if_costcenterdellstartdate_present=rail.IfOperator(
            task_id='if_costcenterdellstartdate_present',
            test='''{{ dag_run.conf.costcenterdellstartdate | is_truthy }}''',
            yes_task="get_costcenterdellstartdate_object",
            no_task="if_state_present",
        )

        get_costcenterdellstartdate_object=rail.PythonOperator(
            task_id='get_costcenterdellstartdate_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['costcenterdellstartdate'])
        )

        add_to_custom_field_values_list=rail.SetVariableOperator(
            task_id='add_to_custom_field_values_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.costcenterdellstartdateudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": {
                    "year": "{{result('get_costcenterdellstartdate_object').year}}",
                    "month": "{{result('get_costcenterdellstartdate_object').month}}",
                    "day": "{{result('get_costcenterdellstartdate_object').day}}"
                },
            }
        )

        if_state_present=rail.IfOperator(
            task_id='if_state_present',
            test='''{{ dag_run.conf.state | is_truthy }}''',
            yes_task="add_to_customfield_values_list",
            no_task="if_existinguserudfuri_present",
        )

        add_to_customfield_values_list=rail.SetVariableOperator(
            task_id='add_to_customfield_values_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.stateudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.statedropdownuri }}",
                    "name": null
                },
            }
        )

        if_existinguserudfuri_present=rail.IfOperator(
            task_id='if_existinguserudfuri_present',
            test='''{{ dag_run.conf.existinguserudfuri | is_truthy }}''',
            yes_task="add_to_custom_fieldvalues_list",
            no_task="if_clarityuserudfuri_present",
        )

        add_to_custom_fieldvalues_list=rail.SetVariableOperator(
            task_id='add_to_custom_fieldvalues_list',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.existinguserudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.existinguserdropdownuri }}",
                    "name": null
                },
            }
        )

        if_clarityuserudfuri_present=rail.IfOperator(
            task_id='if_clarityuserudfuri_present',
            test='''{{ dag_run.conf.clarityuserudfuri | is_truthy }}''',
            yes_task="add_to_list_customfieldvalues",
            no_task="log_customfieldvalues",
        )

        add_to_list_customfieldvalues=rail.SetVariableOperator(
            task_id='add_to_list_customfieldvalues',
            append=True,
            name='{{ result("create_customfieldvalues_list").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.clarityuserudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.clarityuserdropdownuri }}",
                    "name": null
                },
            }
        )

        log_customfieldvalues=rail.PythonOperator(
            task_id='log_customfieldvalues',
            python_callable= lambda:  rail.get_dag_run_var('customFieldValues')
        )

        create_user=rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                    "uri": null,
                    "loginName": dag_run.conf['loginname'],
                    "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['employeeid'],
                    "department": rail.result('get_departmentgroup_schedule'),
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": rail.get_dag_run_var('schedulePolicySchedule'),
                    "workWeekStartDayUri": dag_run.conf['workweek'],
                    "employmentDateRange": {
                    "startDate": {
                        "year": rail.result('get_start_date_object')['year'],
                        "month": rail.result('get_start_date_object')['month'],
                        "day": rail.result('get_start_date_object')['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                    "enabledAuthenticationTypeUris": [
                        dag_run.conf['authenticationtype']
                    ],
                    "isLoginEnabled": "false",
                    "loginName": dag_run.conf['loginname'],
                    "SSOName": dag_run.conf['loginname'],
                    "password": null
                    },
                    "holidayCalendar": rail.result('get_holidaycalendar'),
                    "timeOffPolicy": null,
                    "permissionSets": rail.result('create_permission_sets'),
                    "policySets": rail.result('log_policysets_to_assign'),
                    "employeeType": rail.result('get_employeetypegroup_schedule'),
                    "timesheetPeriodTypeUri": dag_run.conf['timesheetperioduri'],
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": rail.result('get_timesheetapproval_path'),
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": rail.result('get_timeoffapproval_path'),
                    "customFieldValues": rail.result('log_customfieldvalues'),
                    "assignedActivities": rail.result('create_activities_list'),
                    "timeZone": rail.result('get_timezone'),
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": rail.result('get_location_schedule'),
                    "divisionSchedule": rail.result('get_division_schedule'),
                    "costCenterSchedule": rail.result('get_costcenter_schedule'),
                    "serviceCenterSchedule": rail.result('get_servicecenter_schedule'),
                    "departmentGroupSchedule": rail.result('get_departmentgroup_schedule'),
                    "employeeTypeGroupSchedule": rail.result('get_employeetypegroup_schedule'),
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": rail.result('get_payrulescript_schedule'),
                    "displayNameParameter": null
                }
            }
        )

        getallapprovalpaths=rail.RepliconServiceOperator(
            task_id='getallapprovalpaths',
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/GetUriFromApprovalPathSlug",
            data={
                "approvalPathSlug": "time-entry-project-manager"
            }
        )

        update_approval_path_for_user=rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user',
            endpoint="/services/TimeEntryRevisionGroupApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "approvalPathUri": "{{ result('getallapprovalpaths') }}"
            }
        )

        assign_timeoff_assignments_for_new_users=rail.RepliconServiceOperator(
            task_id='assign_timeoff_assignments_for_new_users',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeOffTypeUris": []
            }
        )

        put_product_assignments_for_user_assign_license=rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_assign_license',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "productUris": [
                    "{{ dag_run.conf.license }}"
                ]
            }
        )

        get_enabled_dropdownoptions_for_state_province=rail.RepliconServiceOperator(
            task_id='get_enabled_dropdownoptions_for_state_province',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user-defined-field:21caa067-ccc7-401e-8af8-99fe5a3aefc4"
            }
        )

        get_stateprovince_uri=rail.PythonOperator(
            task_id='get_stateprovince_uri',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_enabled_dropdownoptions_for_state_province'),'displayText',dag_run.conf['stateorprovince'],'uri','')
        )

        update_dropdownvalue_for_state=rail.RepliconServiceOperator(
            task_id='update_dropdownvalue_for_state',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda:{
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user-defined-field:21caa067-ccc7-401e-8af8-99fe5a3aefc4",
                "customFieldDropDownOptionUri": rail.result('get_stateprovince_uri')
            }
        )

        if_initial_supervisorloginname_present=rail.IfOperator(
            task_id='if_initial_supervisorloginname_present',
            test='''{{ dag_run.conf.initialsupervisorloginname | is_truthy }}''',
            yes_task="get_userdata_forsupervisor",
            no_task="insert_exception_for_supervisor",
        )

        get_userdata_forsupervisor=rail.RepliconServiceOperator(
            task_id='get_userdata_forsupervisor',
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

        if_supervisoruser_is_enabled=rail.IfOperator(
            task_id='if_supervisoruser_is_enabled',
            test='''{{ result('get_userdata_forsupervisor') | is_truthy and result('get_userdata_forsupervisor')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="check_supervisor_permission",
            no_task="insert_to_supervisorcheck_lookup",
        )

        check_supervisor_permission=rail.PythonOperator(
            task_id='check_supervisor_permission',
            python_callable= lambda:  rail.find_first_by_attr_and_get_attr(
                                rail.result('get_userdata_forsupervisor')[0]['permissionSets'],'displayText','Manager','uri','')
        )

        check_enduserpermission_forsupervisor_is_assigned=rail.PythonOperator(
            task_id='check_enduserpermission_forsupervisor_is_assigned',
            python_callable= lambda:  rail.find_first_by_attr_and_get_attr(
                                rail.result('get_userdata_forsupervisor')[0]['permissionSets'],'displayText','End user with reports view','uri','')
        )

        if_supervisorpermission_not_assigned=rail.IfOperator(
            task_id='if_supervisorpermission_not_assigned',
            test='''{{ result('check_supervisor_permission') | is_falsy }}''',
            yes_task="assign_supervisor_permission",
            no_task="assign_initial_supervisor",
        )

        assign_supervisor_permission=rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_userdata_forsupervisor')[0].userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_initial_supervisor=rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "supervisorUri": "{{ result('get_userdata_forsupervisor')[0].userDetails.uri }}",
                "dateRange": null
            }
        )

        insert_to_supervisorcheck_lookup=rail.WriteLogOperator(
            task_id='insert_to_supervisorcheck_lookup',
            log="{{ dag_run.conf.supervisorchecklookup }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{result('create_user').uri}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ dag_run.conf.initialsupervisorloginname }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "action": "Add",
                "status": '',
                "effectivedate": ''
            }
        )

        insert_exception_for_supervisor=rail.SetVariableOperator(
            task_id='insert_exception_for_supervisor',
            append=True,
            name='{{ result("create_exceptions_log_list").name }}',
            value={
                "log": "Exception"
            }
        )

        log_exceptions=rail.PythonOperator(
            task_id='log_exceptions',
            python_callable= lambda: '|'.join([log['log'] for log in rail.get_dag_run_var('exceptions')] ) if
                                rail.get_dag_run_var('exceptions') and rail.get_dag_run_var('exceptions')[0]['log'] else null
        )

        add_final_log_for_user_to_lookuptable=rail.WriteLogOperator(
            task_id='add_final_log_for_user_to_lookuptable',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: 'Exception' if rail.result('log_exceptions') else "Success",
            properties=lambda dag_run:{
                "userid": dag_run.conf['loginname'],
                "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
                "action": "Add",
                "status": 'Exception' if rail.result('log_exceptions') else "Success",
                "details": 'User created partially - ' + rail.result('log_exceptions') if rail.result('log_exceptions') else 'User created successfully',
                "childjobis": rail.render_template("{{dag_run_ecid()}}"),
                "parentjobid": dag_run.conf['callerjobid']
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.logslookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run:{
                "userid": dag_run.conf['loginname'],
                "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
                "action": "Add",
                "status": 'Error',
                "details": '{{get_error_message()}}',
                "childjobis": rail.render_template("{{dag_run_ecid()}}"),
                "parentjobid": dag_run.conf['callerjobid']
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_exception_log
        get_exception_log >> create_exceptions_log_list >> if_exception_present
        if_exception_present >> rail.Label('Yes')  >> add_exceptions_log >> catch_and_log_error
        if_exception_present >> rail.Label('No') >> get_start_date_object >> declare_schedulepolicyschedule_variable >> if_officescheduleuri_present
        if_officescheduleuri_present >> rail.Label('Yes')  >> update_schedulepolicy_variable >> if_holidaycalendaruri_present
        if_officescheduleuri_present >> rail.Label('No') >> if_initial_schedulename_equals_shiftschedule
        if_initial_schedulename_equals_shiftschedule >> rail.Label('Yes')  >> update_variable_schedulepolicyschedule >> if_holidaycalendaruri_present
        if_initial_schedulename_equals_shiftschedule >> rail.Label(
            'No') >> log_officeschedule_not_assigned >> if_holidaycalendaruri_present
        if_holidaycalendaruri_present >> rail.Label('Yes')  >> get_holidaycalendar >> get_all_permission_sets
        if_holidaycalendaruri_present >> rail.Label('No') >> log_holidaycalendar_not_available >> get_all_permission_sets >> get_permissionstoassign
        get_permissionstoassign >> create_permission_sets
        create_permission_sets >> get_all_activities >> create_activities_list >> create_policysets_list >> if_timesheettemplateuri_present
        if_timesheettemplateuri_present >> rail.Label('Yes')  >> add_to_policysets_list >> if_timeofftemplateuri_present
        if_timesheettemplateuri_present >> rail.Label('No') >> log_timesheettemplate_not_available >> if_timeofftemplateuri_present
        if_timeofftemplateuri_present >> rail.Label('Yes')  >> insert_to_policysets >> log_policysets_to_assign
        if_timeofftemplateuri_present >> rail.Label(
            'No') >> log_policysets_to_assign >> if_timesheetapprovalpathuri_present
        if_timesheetapprovalpathuri_present >> rail.Label('Yes')  >> get_timesheetapproval_path >> if_timeoffapprovalpathuri_present
        if_timesheetapprovalpathuri_present >> rail.Label('No') >> if_timeoffapprovalpathuri_present
        if_timeoffapprovalpathuri_present >> rail.Label('Yes')  >> get_timeoffapproval_path >> if_departmenturi_present
        if_timeoffapprovalpathuri_present >> rail.Label('No') >> if_departmenturi_present
        if_departmenturi_present >> rail.Label('Yes')  >> get_departmentgroup_schedule >> if_employeetypeuri_present
        if_departmenturi_present >> rail.Label('No') >> if_employeetypeuri_present
        if_employeetypeuri_present >> rail.Label('Yes')  >> get_employeetypegroup_schedule >> get_all_locations
        if_employeetypeuri_present >> rail.Label('No') >> get_all_locations >> if_location_not_present_in_replicon
        if_location_not_present_in_replicon >> rail.Label('Yes')  >> create_location_or_apply_modifications >> if_location_present_or_created
        if_location_not_present_in_replicon >> rail.Label('No') >> if_location_present_or_created
        if_location_present_or_created >> rail.Label('Yes')  >> get_location_uri >> get_location_schedule >> if_servicecenteruri_present
        if_location_present_or_created >> rail.Label('No') >> if_servicecenteruri_present
        if_servicecenteruri_present >> rail.Label('Yes')  >> get_servicecenter_schedule >> if_costcenteruri_present
        if_servicecenteruri_present >> rail.Label('No') >> if_costcenteruri_present
        if_costcenteruri_present >> rail.Label('Yes')  >> get_costcenter_schedule >> get_all_divisions
        if_costcenteruri_present >> rail.Label('No') >> get_all_divisions >> if_costcenterntt_not_available_in_replicon
        if_costcenterntt_not_available_in_replicon >> rail.Label('Yes')  >> create_division_or_apply_modifications >> if_divisionuri_present_or_created
        if_costcenterntt_not_available_in_replicon >> rail.Label('No') >> if_divisionuri_present_or_created
        if_divisionuri_present_or_created >> rail.Label(
            'Yes')  >> get_division_uri >> get_division_schedule >> if_payruleuri_present
        if_divisionuri_present_or_created >> rail.Label('No') >> if_payruleuri_present
        if_payruleuri_present >> rail.Label('Yes')  >> get_payrulescript_schedule >> if_timezone_present
        if_payruleuri_present >> rail.Label('No') >> if_timezone_present
        if_timezone_present >> rail.Label('Yes')  >> get_timezone >> create_customfieldvalues_list
        if_timezone_present >> rail.Label('No') >> create_customfieldvalues_list >> if_dellbadgeid_present
        if_dellbadgeid_present >> rail.Label('Yes')  >> add_to_customfieldvalues_list >> if_jobcode_present
        if_dellbadgeid_present >> rail.Label('No') >> if_jobcode_present
        if_jobcode_present >> rail.Label('Yes')  >> insert_to_customfieldvalues_list >> if_jobcodestartdate_present
        if_jobcode_present >> rail.Label('No') >> if_jobcodestartdate_present
        if_jobcodestartdate_present >> rail.Label('Yes')  >> get_jobcodestartdate_object >> insert_to_custom_field_values_list >> if_costcenterdell_present
        if_jobcodestartdate_present >> rail.Label('No') >> if_costcenterdell_present
        if_costcenterdell_present >> rail.Label('Yes')  >> insert_to_customfield_values_list >> if_country_present
        if_costcenterdell_present >> rail.Label('No') >> if_country_present
        if_country_present >> rail.Label('Yes')  >> insert_to_custom_fieldvalues_list >> if_costcenterdellstartdate_present
        if_country_present >> rail.Label('No') >> if_costcenterdellstartdate_present
        if_costcenterdellstartdate_present >> rail.Label('Yes')  >> get_costcenterdellstartdate_object >> add_to_custom_field_values_list >> if_state_present
        if_costcenterdellstartdate_present >> rail.Label('No') >> if_state_present
        if_state_present >> rail.Label('Yes')  >> add_to_customfield_values_list >> if_existinguserudfuri_present
        if_state_present >> rail.Label('No') >> if_existinguserudfuri_present
        if_existinguserudfuri_present >> rail.Label('Yes')  >> add_to_custom_fieldvalues_list >> if_clarityuserudfuri_present
        if_existinguserudfuri_present >> rail.Label('No') >> if_clarityuserudfuri_present
        if_clarityuserudfuri_present >> rail.Label('Yes')  >> add_to_list_customfieldvalues >> log_customfieldvalues
        if_clarityuserudfuri_present >> rail.Label('No') >> log_customfieldvalues >> create_user >> getallapprovalpaths >> update_approval_path_for_user
        update_approval_path_for_user >> assign_timeoff_assignments_for_new_users >> put_product_assignments_for_user_assign_license
        put_product_assignments_for_user_assign_license >> get_enabled_dropdownoptions_for_state_province >> get_stateprovince_uri
        get_stateprovince_uri >> update_dropdownvalue_for_state >> if_initial_supervisorloginname_present
        if_initial_supervisorloginname_present >> rail.Label('Yes')  >> get_userdata_forsupervisor >> if_supervisoruser_is_enabled
        if_supervisoruser_is_enabled >> rail.Label(
            'Yes')  >> check_supervisor_permission >> check_enduserpermission_forsupervisor_is_assigned >> if_supervisorpermission_not_assigned
        if_supervisorpermission_not_assigned >> rail.Label('Yes')  >> assign_supervisor_permission >> assign_initial_supervisor
        if_supervisorpermission_not_assigned >> rail.Label('No') >> assign_initial_supervisor >> log_exceptions
        if_supervisoruser_is_enabled >> rail.Label('No') >> insert_to_supervisorcheck_lookup >> log_exceptions
        if_initial_supervisorloginname_present >> rail.Label(
            'No') >> insert_exception_for_supervisor >> log_exceptions >> add_final_log_for_user_to_lookuptable >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
