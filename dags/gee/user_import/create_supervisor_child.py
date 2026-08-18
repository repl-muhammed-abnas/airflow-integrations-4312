import rail
from gee.user_import.utils import python_callable, request_payload, response_filter

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.create_supervisor_child,
        description=f'GEE create supervisor child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        def get_user_uri_by_loginname(response, dag_run):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][1]['textValue'] == dag_run.conf['LoginName'], users_found))
            return matching_user[0]['cells'][0]['uri'] if matching_user else ''

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": dag_run.conf['EmployeeId']
                        },
                        "filterDefinitionUri": None
                    },
                    "value": None,
                    "filterDefinitionUri": None
                }
            },
            data_handler=get_user_uri_by_loginname
        )

        if_user_with_same_loginname_exist = rail.IfOperator(
            task_id='if_user_with_same_loginname_exist',
            test="{{result('get_user_details') | is_truthy}}",
            yes_task='log_to_sumo',
            no_task='split_start_date'
        )

        split_start_date = rail.PythonOperator(
            task_id = "split_start_date",
            python_callable=python_callable.split_startdate
        )

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_create_user_payload
        )

        put_product_assignments = rail.RepliconServiceOperator(
            task_id='put_product_assignments',
            endpoint='/services/AccountManagementService1.svc/PutProductAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user_in_replicon').uri }}",
                'productUris': ["urn:replicon-saas:product:time-off-plus"]
            }
        )

        if_workweek_present = rail.IfOperator(
            task_id='if_workweek_present',
            test=lambda dag_run: bool(dag_run.conf['Workweek']),
            yes_task='get_required_work_week',
            no_task='if_permission_set_present'
        )

        get_required_work_week = rail.PythonOperator(
            task_id = "get_required_work_week",
            python_callable=python_callable.get_required_work_week
        )

        update_work_week_for_user = rail.RepliconServiceOperator(
            task_id='update_work_week_for_user',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('create_user_in_replicon').uri }}",
                "dayOfWeekUri": "{{ result('get_required_work_week') }}"
            }
        )

        if_permission_set_present = rail.IfOperator(
            task_id='if_permission_set_present',
            test=lambda dag_run: bool(dag_run.conf['PermissionSets']),
            yes_task='get_all_permissionsets',
            no_task='if_department_present'
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        get_all_permissionsets_from_payload = rail.PythonOperator(
            task_id = "get_all_permissionsets_from_payload",
            python_callable=python_callable.get_all_permissionsets_from_payload
        )

        get_permission_uri = rail.PythonOperator(
            task_id = "get_permission_uri",
            python_callable=python_callable.get_permission_uri
        )

        put_permissions_user = rail.RepliconServiceOperator(
            task_id='put_permissions_user',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda: {
                'userUri': rail.result('create_user_in_replicon')['uri'],
                "permissionSetUris": rail.result('get_permission_uri')['permissiontoassign']
            }
        )

        if_department_present = rail.IfOperator(
            task_id='if_department_present',
            test=lambda dag_run: bool(dag_run.conf['Department']),
            yes_task='put_department_group_schedule_for_user',
            no_task='if_employeetype_present'
        )

        put_department_group_schedule_for_user=rail.RepliconServiceOperator(
            task_id='put_department_group_schedule_for_user',
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "scheduleEntries": [
                    {
                    "departmentGroup": {
                        "uri": None,
                        "parent": {
                        "uri": dag_run.conf['companydeparmenturi']
                        },
                        "name": dag_run.conf['Department']
                    },
                    "effectiveDate": None
                    }
                ]
            }
        )

        if_employeetype_present = rail.IfOperator(
            task_id='if_employeetype_present',
            test=lambda dag_run: bool(dag_run.conf['EmployeeType']),
            yes_task='apply_user_modifications_emplyeetype',
            no_task='if_officesheduleuri_present'
        )

        apply_user_modifications_emplyeetype = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_emplyeetype',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications_emplyeetype,
        )

        if_officesheduleuri_present = rail.IfOperator(
            task_id='if_officesheduleuri_present',
            test=lambda dag_run: bool(dag_run.conf['officescheduleuri']),
            yes_task='put_schedule_policy_user',
            no_task='if_holiday_calendar_present'
        )

        put_schedule_policy_user = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                'userUri': rail.result('create_user_in_replicon')['uri'],
                "scheduleEntries": [
                    {
                        "schedulePolicy":  {
                            "officeScheduleUri": dag_run.conf['officescheduleuri'],
                            "name": None,
                            "officeSchedule": None,
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": None
                    }
                ]
            }
        )

        if_holiday_calendar_present = rail.IfOperator(
            task_id='if_holiday_calendar_present',
            test=lambda dag_run: bool(dag_run.conf['HolidayCalendar']),
            yes_task='apply_user_modifications_holiday_calendar',
            no_task='if_division_present'
        )

        apply_user_modifications_holiday_calendar = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_holiday_calendar',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications_holiday_calendar,
        )

        if_division_present = rail.IfOperator(
            task_id='if_division_present',
            test=lambda dag_run: bool(dag_run.conf['division']),
            yes_task='apply_user_modifications_division',
            no_task='if_timezone_and_location_present'
        )

        apply_user_modifications_division = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_division',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications_division,
        )

        if_timezone_and_location_present = rail.IfOperator(
            task_id='if_timezone_and_location_present',
            test=lambda dag_run: bool(dag_run.conf['Timezone'] and dag_run.conf['Location']),
            yes_task='apply_user_modifications_timezone_location',
            no_task='if_location_and_locationuri_present'
        )

        apply_user_modifications_timezone_location = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_timezone_location',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications_timezone_location,
        )

        if_location_and_locationuri_present = rail.IfOperator(
            task_id='if_location_and_locationuri_present',
            test=lambda dag_run: bool(dag_run.conf['Timezone'] and dag_run.conf['Location']),
            yes_task='get_enabled_timeoff_types',
            no_task='foreach_timeoff_list_end'
        )

        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_timeofftypes_to_assign = rail.PythonOperator(
            task_id = "get_timeofftypes_to_assign",
            python_callable=python_callable.get_timeofftypes_to_assign
        )

        if_timeoff_string_present = rail.IfOperator(
            task_id='if_timeoff_string_present',
            test=lambda dag_run: bool(dag_run.conf['Timezone'] and dag_run.conf['Location']),
            yes_task='assign_required_timeofftypes',
            no_task='foreach_timeoff_list_end'
        )

        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "timeOffTypeUris": rail.result('get_timeofftypes_to_assign')['timeoff_string']
            }
        )

        foreach_timeoff_list = rail.ForEachOperator(
            task_id='foreach_timeoff_list',
            items="{{ result('get_timeofftypes_to_assign').timeofflist | to_json }}",
            start_task='get_default_timeoff_types_policy_schedule_for_user',
            end_task='foreach_timeoff_list_end'
        )

        get_default_timeoff_types_policy_schedule_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_default_timeoff_types_policy_schedule_for_user',
            items=lambda: rail.result('get_timeofftype_uris_to_assign'),
            endpoint='/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser',
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('create_user_in_replicon')['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeoff_list')["uri"]
                }
            },
            data_handler=response_filter.get_policyschedule_entries
        )

        if_policyschedule_present = rail.IfOperator(
            task_id='if_policyschedule_present',
            test=lambda: bool(rail.result('get_default_timeoff_types_policy_schedule_for_user')),
            yes_task='put_user_time_off_account_policy_set_schedule',
            no_task='foreach_timeoff_list_end'
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('create_user_in_replicon')['uri'],
                    "timeOffTypeUri": rail.result('foreach_timeoff_list')["uri"]
                },
                "policySetScheduleEntries": rail.result('get_default_timeoff_types_policy_schedule_for_user')
                }
        )

        foreach_timeoff_list_end = rail.EmptyOperator(
            task_id='foreach_timeoff_list_end',
        )

        if_anualsalary_and_anualsalaryuri_present = rail.IfOperator(
            task_id='if_anualsalary_and_anualsalaryuri_present',
            test=lambda dag_run: bool(dag_run.conf['AnnualSalary'] and dag_run.conf['annualuri']),
            yes_task='update_anualsalary_udf',
            no_task='if_elt_and_elturi_present'
        )

        update_anualsalary_udf = rail.RepliconServiceOperator(
            task_id='update_anualsalary_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": dag_run.conf["annualuri"],
                "value": dag_run.conf["AnnualSalary"]
            }
        )

        if_elt_and_elturi_present = rail.IfOperator(
            task_id='if_elt_and_elturi_present',
            test=lambda dag_run: bool(dag_run.conf['ELT'] and dag_run.conf['elturi']),
            yes_task='update_elt_udf',
            no_task='if_businesscardtitle_and_businesscardtitleuri_present'
        )

        update_elt_udf = rail.RepliconServiceOperator(
            task_id='update_elt_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": dag_run.conf["elturi"],
                "value": dag_run.conf["ELT"]
            }
        )

        if_businesscardtitle_and_businesscardtitleuri_present = rail.IfOperator(
            task_id='if_businesscardtitle_and_businesscardtitleuri_present',
            test=lambda dag_run: bool(dag_run.conf['businesscardtitle'] and dag_run.conf['businesscardtitleuri']),
            yes_task='update_businesscardtitle_udf',
            no_task='if_firstlineri_present'
        )

        update_businesscardtitle_udf = rail.RepliconServiceOperator(
            task_id='update_businesscardtitle_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": dag_run.conf["businesscardtitleuri"],
                "value": dag_run.conf["businesscardtitle"]
            }
        )

        if_firstlineri_present = rail.IfOperator(
            task_id='if_firstlineri_present',
            test=lambda dag_run: bool(dag_run.conf['firstlineuri']),
            yes_task='if_firstlinemanager_present',
            no_task='if_secondlinemanager_and_secondlineuri_present'
        )

        if_firstlinemanager_present = rail.IfOperator(
            task_id='if_firstlinemanager_present',
            test=lambda dag_run: bool(dag_run.conf['firstlinemanager']),
            yes_task='update_firstlinemanager_udf',
            no_task='if_supervisorid_present'
        )

        update_firstlinemanager_udf = rail.RepliconServiceOperator(
            task_id='update_firstlinemanager_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": dag_run.conf["firstlineuri"],
                "value": dag_run.conf["firstlinemanager"]
            }
        )

        if_supervisorid_present = rail.IfOperator(
            task_id='if_supervisorid_present',
            test=lambda dag_run: bool(dag_run.conf['SupervisorID']),
            yes_task='get_user_details_with_supervisorid',
            no_task='if_secondlinemanager_and_secondlineuri_present'
        )

        get_user_details_with_supervisorid = rail.RepliconServiceOperator(
            task_id="get_user_details_with_supervisorid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_param,
            data_handler=response_filter.get_filtered_user_data
        )

        if_supervisor_present = rail.IfOperator(
            task_id='if_supervisor_present',
            test=lambda: bool(rail.result('get_user_details_with_supervisorid').get('supervisor', False)),
            yes_task='update_formattedname_udf',
            no_task='if_secondlinemanager_and_secondlineuri_present'
        )

        update_formattedname_udf = rail.RepliconServiceOperator(
            task_id='update_formattedname_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": dag_run.conf["firstlineuri"],
                "value": rail.result('get_user_details_with_supervisorid')['formattedname']
            }
        )

        if_secondlinemanager_and_secondlineuri_present = rail.IfOperator(
            task_id='if_secondlinemanager_and_secondlineuri_present',
            test=lambda dag_run: bool(dag_run.conf['secondlinemanager'] and dag_run.conf['secondlineuri']),
            yes_task='update_secondlinemanager_udf',
            no_task='if_workweekhours_and_workweekdropdownuri_present'
        )

        update_secondlinemanager_udf = rail.RepliconServiceOperator(
            task_id='update_secondlinemanager_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": dag_run.conf["secondlineuri"],
                "value": dag_run.conf["secondlinemanager"]
            }
        )

        if_workweekhours_and_workweekdropdownuri_present = rail.IfOperator(
            task_id='if_workweekhours_and_workweekdropdownuri_present',
            test=lambda dag_run: bool(dag_run.conf['workweekhours'] and dag_run.conf['workweek_dropdown_valueuri']),
            yes_task='update_workweek_dropdown_udf',
            no_task='if_supervisorid_present_62'
        )

        update_workweek_dropdown_udf = rail.RepliconServiceOperator(
            task_id='update_workweek_dropdown_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_in_replicon')['uri'],
                "customFieldUri": dag_run.conf["workweekuri"],
                "customFieldDropDownOptionUri": dag_run.conf["workweek_dropdown_valueuri"]
            }
        )

        if_supervisorid_present_62 = rail.IfOperator(
            task_id='if_supervisorid_present_62',
            test=lambda dag_run: bool(dag_run.conf['SupervisorID']),
            yes_task='get_user_details_with_supervisorid_63',
            no_task='add_to_lookup_table'
        )

        get_user_details_with_supervisorid_63 = rail.RepliconServiceOperator(
            task_id="get_user_details_with_supervisorid_63",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_param,
            data_handler=response_filter.get_filtered_user_data_63
        )

        if_get_user_details_with_supervisorid_63_not_present = rail.IfOperator(
            task_id='if_get_user_details_with_supervisorid_63_not_present',
            test=lambda: not bool(rail.result('get_user_details_with_supervisorid_63')),
            yes_task='add_to_lookup_table',
            no_task='if_get_user_details_with_supervisorid_63_present'
        )

        if_get_user_details_with_supervisorid_63_present = rail.IfOperator(
            task_id='if_get_user_details_with_supervisorid_63_present',
            test=lambda: bool(rail.result('get_user_details_with_supervisorid_63')),
            yes_task='get_assigned_permissionsets_for_user',
            no_task='add_to_lookup_table'
        )

        get_assigned_permissionsets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets_for_user',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_user_details_with_supervisorid_63') }}"
            },
            data_handler=response_filter.get_permission_sets
        )

        if_supervisor_permissionsets_present = rail.IfOperator(
            task_id='if_supervisor_permissionsets_present',
            test=lambda: bool(rail.result('get_assigned_permissionsets_for_user')),
            yes_task='put_supervisors_assigment_schedule',
            no_task='if_supervisor_permissionsets_not_present'
        )

        put_supervisors_assigment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisors_assigment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda: {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "initialSupervisorUri": rail.result('get_user_details_with_supervisorid_63'),
                "scheduleEntries": []
            }
        )

        if_supervisor_permissionsets_not_present = rail.IfOperator(
            task_id='if_supervisor_permissionsets_not_present',
            test=lambda: bool(rail.result('get_assigned_permissionsets_for_user')),
            yes_task='get_all_permissionsets_71',
            no_task='add_to_lookup_table'
        )

        get_all_permissionsets_71 = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets_71',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name', "Supervisor", 'uri')
        )

        add_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('create_user_in_replicon').uri }}",
                'permissionSetUri': "{{ result('get_all_permissionsets_71') }}"
            }
        )

        put_supervisors_assigment_schedule_74 = rail.RepliconServiceOperator(
            task_id='put_supervisors_assigment_schedule_74',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda: {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "initialSupervisorUri": rail.result('get_user_details_with_supervisorid_63'),
                "scheduleEntries": []
            }
        )

        add_to_lookup_table = rail.WriteLogOperator(
            task_id='add_to_lookup_table',
            log = "{{ dag_run.conf.gee_supervisor_lookup_table }}",
            message="na",
            severity="Success",
            properties={
                "jobid" : "{{ dag_run_ecid() }}",
                "userloginname" : "{{ dag_run.conf.LoginName }}",
                "useruri" : "{{ result('create_user_in_replicon').uri }}",
                "username" : "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "supervisorloginname" : "{{ result('get_user_details_with_supervisorid').loginname }}",
                "action": "Add",
                "empid": "{{dag_run.conf.EmployeeId}}",
                "childjobid" : "",
                "status" : "Sucessfully added"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        get_user_details >> if_user_with_same_loginname_exist >> rail.Label(
            "Yes") >> log_to_sumo
        if_user_with_same_loginname_exist >> rail.Label(
            "No") >> split_start_date >> create_user_in_replicon >> \
        put_product_assignments >> if_workweek_present >> rail.Label(
            "Yes") >> get_required_work_week >> update_work_week_for_user >> if_permission_set_present
        if_workweek_present >> rail.Label(
            "No") >> if_permission_set_present >> rail.Label(
            "Yes") >> get_all_permissionsets >> get_all_permissionsets_from_payload >> get_permission_uri >> \
        put_permissions_user >> if_department_present
        if_permission_set_present >> rail.Label(
            "No") >> if_department_present >> rail.Label(
            "Yes") >> put_department_group_schedule_for_user >> if_employeetype_present
        if_department_present >> rail.Label(
            "No") >> if_employeetype_present >> rail.Label(
            "Yes") >> apply_user_modifications_emplyeetype >> if_officesheduleuri_present
        if_employeetype_present >> rail.Label(
            "No") >> if_officesheduleuri_present >> rail.Label(
            "Yes") >> put_schedule_policy_user >> if_holiday_calendar_present
        if_officesheduleuri_present >> rail.Label(
            "No") >> if_holiday_calendar_present >> rail.Label(
            "Yes") >> apply_user_modifications_holiday_calendar >> if_division_present
        if_holiday_calendar_present >> rail.Label(
            "No") >> if_division_present >> rail.Label(
            "Yes") >> apply_user_modifications_division >> if_timezone_and_location_present
        if_division_present >> rail.Label(
            "No") >> if_timezone_and_location_present >> rail.Label(
            "Yes") >> apply_user_modifications_timezone_location >> if_location_and_locationuri_present
        if_timezone_and_location_present >> rail.Label(
            "No") >> if_location_and_locationuri_present >> rail.Label(
            "Yes") >> get_enabled_timeoff_types >> get_timeofftypes_to_assign >> \
        if_timeoff_string_present >> rail.Label(
            "Yes") >> assign_required_timeofftypes >> foreach_timeoff_list >> get_default_timeoff_types_policy_schedule_for_user >> \
        if_policyschedule_present >> rail.Label(
            "Yes") >> put_user_time_off_account_policy_set_schedule >> foreach_timeoff_list_end
        if_policyschedule_present >> rail.Label(
            "No") >> foreach_timeoff_list_end
        foreach_timeoff_list >> foreach_timeoff_list_end
        if_timeoff_string_present >> rail.Label(
            "No") >> foreach_timeoff_list_end
        if_location_and_locationuri_present >> rail.Label(
            "No") >>  foreach_timeoff_list_end
        foreach_timeoff_list_end >> if_anualsalary_and_anualsalaryuri_present >> rail.Label(
            "Yes") >> update_anualsalary_udf >> if_elt_and_elturi_present
        if_anualsalary_and_anualsalaryuri_present >> rail.Label(
            "No") >> if_elt_and_elturi_present >> rail.Label(
            "Yes") >> update_elt_udf >> if_businesscardtitle_and_businesscardtitleuri_present
        if_elt_and_elturi_present >> rail.Label(
            "No") >> if_businesscardtitle_and_businesscardtitleuri_present >> rail.Label(
            "Yes") >> update_businesscardtitle_udf >> if_firstlineri_present
        if_businesscardtitle_and_businesscardtitleuri_present >> rail.Label(
            "No") >> if_firstlineri_present >> rail.Label(
            "Yes") >> if_firstlinemanager_present >> rail.Label(
            "Yes") >> update_firstlinemanager_udf >> if_secondlinemanager_and_secondlineuri_present
        if_firstlinemanager_present >> rail.Label(
            "No") >> if_supervisorid_present >> rail.Label(
            "Yes") >> get_user_details_with_supervisorid >> if_supervisor_present >> rail.Label(
            "Yes") >> update_formattedname_udf >> if_secondlinemanager_and_secondlineuri_present
        if_supervisor_present >> rail.Label(
            "No") >> if_secondlinemanager_and_secondlineuri_present
        if_supervisorid_present >> rail.Label(
            "No") >> if_secondlinemanager_and_secondlineuri_present
        if_firstlineri_present >> rail.Label(
            "No") >> if_secondlinemanager_and_secondlineuri_present >> rail.Label(
            "Yes") >> update_secondlinemanager_udf >> if_workweekhours_and_workweekdropdownuri_present
        if_secondlinemanager_and_secondlineuri_present >> rail.Label(
            "No") >> if_workweekhours_and_workweekdropdownuri_present >> rail.Label(
            "Yes") >> update_workweek_dropdown_udf >> if_supervisorid_present_62
        if_workweekhours_and_workweekdropdownuri_present >> rail.Label(
            "No") >> if_supervisorid_present_62 >> rail.Label(
            "Yes") >> get_user_details_with_supervisorid_63 >> if_get_user_details_with_supervisorid_63_not_present >> rail.Label(
            "Yes") >> add_to_lookup_table
        if_get_user_details_with_supervisorid_63_not_present >> rail.Label(
            "No") >> if_get_user_details_with_supervisorid_63_present >> rail.Label(
            "Yes") >> get_assigned_permissionsets_for_user >> if_supervisor_permissionsets_present >> rail.Label(
            "Yes") >> put_supervisors_assigment_schedule >> if_supervisor_permissionsets_not_present
        if_supervisor_permissionsets_present >> rail.Label(
            "No") >> if_supervisor_permissionsets_not_present >> rail.Label(
            "Yes") >> get_all_permissionsets_71 >> add_supervisor_permission >> put_supervisors_assigment_schedule_74 >> add_to_lookup_table
        if_supervisor_permissionsets_not_present >> rail.Label(
            "No") >> add_to_lookup_table
        if_get_user_details_with_supervisorid_63_present >> rail.Label(
            "No") >> add_to_lookup_table
        if_supervisorid_present_62 >> rail.Label(
            "No") >> add_to_lookup_table >> log_to_sumo

        return dag


rail.for_each_instance(create_child_dag)
