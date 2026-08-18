
from datetime import timedelta
import json
from airflow.models import Variable
import rail
from velaw.user_import_v1.user_import_mapper import velaw_user_import_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_user_child_dag_id,
        description=f'VelawG3_Child_Add User_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='velaw_user_import_mapper_search_entries_time_off_approval_path_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='velaw_user_import_mapper_search_entries_time_off_approval_path_3',
            end_task='velaw_user_import_logs_add_entry_92',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_time_off_approval_path(dag_run):
            timeoff_approval_path_list = list(filter(
                lambda x: x['mapper'] == 'Yes' and x["type"] == "TimeOff Approval Path", velaw_user_import_mapper))

            def get_location():
                departments = ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists",
                               "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")
                if dag_run.conf['countryisocode'] == "US":
                    if dag_run.conf['department'] in departments or dag_run.conf['jobfamilies'] == "Paralegal" or\
                    (dag_run.conf['flsastatus'] == "Exempt" and dag_run.conf['persontype'] == "Administrative"):
                        return "All"
                return dag_run.conf['location']

            return next(iter(filter(lambda x: x["employee_type"] == (dag_run.conf['department'] if (
                dag_run.conf['countryisocode'] == "US" and dag_run.conf['department'] in 
                ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists", "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators"))
                  else ("All" if dag_run.conf['countryisocode'] == "US" else "All"))
                                    and x["country_code"] == dag_run.conf['countryisocode']
                                    and x["location"] == get_location()
                                    and x["person_type"] == dag_run.conf['persontype']
                                    and x["flsa"] == ("All" if (dag_run.conf['countryisocode'] == "US" and dag_run.conf['department'] in ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists", "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")) else ("All" if dag_run.conf['jobfamilies'] == "Paralegal" else dag_run.conf['flsastatus']) if dag_run.conf['countryisocode'] == "US" else dag_run.conf['flsastatus'])
                                    and x["job_code"] == ("Paralegal" if (dag_run.conf['countryisocode'] == "US" and dag_run.conf['jobfamilies'] == "Paralegal") else "All excluding Paralegal" if dag_run.conf['countryisocode'] == "US" else "All"),
                                    timeoff_approval_path_list)), {})

        velaw_user_import_mapper_search_entries_time_off_approval_path_3 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_time_off_approval_path_3',
            python_callable=get_time_off_approval_path
        )

        def get_timesheet_approval_path(dag_run):
            timesheet_approval_path_list = list(filter(
                lambda x: x['mapper'] == 'Yes' and x["type"] == "Timesheet Approval Path", 
                velaw_user_import_mapper))

            def get_employee_type():
                if dag_run.conf['countryisocode'] != "US":
                    return "All"
                
                dept = dag_run.conf['department']
                special_depts = ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists",
                                "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")
                
                return ("All" if dag_run.conf['jobfamilies'] == "Paralegal" else dept) if dept in special_depts else "All"

            def get_location():
                if dag_run.conf['countryisocode'] != "US":
                    return dag_run.conf['location']
                
                dept = dag_run.conf['department']
                location = dag_run.conf['location']
                is_paralegal = dag_run.conf['jobfamilies'] == "Paralegal"
                special_depts = ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists",
                                "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")
                
                if is_paralegal:
                    return "All"
                
                if dept in special_depts:
                    return location if location == "Houston" else "All excluding Houston"
                else:
                    return location if location == "London" else "All excluding London"

            return next(iter(filter(
                lambda x: 
                    x["employee_type"] == get_employee_type() and
                    x["country_code"] == dag_run.conf['countryisocode'] and
                    x["location"] == get_location() and
                    x["flsa"] == dag_run.conf['flsastatus'] and
                    x["job_code"] == ("Paralegal" if dag_run.conf['jobfamilies'] == "Paralegal" else "All excluding Paralegal"),
                timesheet_approval_path_list
            )), {})
        velaw_user_import_mapper_search_entries_timesheet_approval_path_4 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timesheet_approval_path_4',
            python_callable=get_timesheet_approval_path
        )

        def get_exceptions(dag_run):
            def check_assignment(exception_list, conf_key, uri_key, error_message, feedfile_error_message=null):
                if conf_key:
                    if not uri_key:
                        exception_list.append(error_message)
                else:
                    exception_list.append(feedfile_error_message)

            exception_list = []
            check_assignment(exception_list, dag_run.conf['department'], dag_run.conf['departmenturi'], "Department not assigned as it is not available in Replicon",
                             "Department not assigned as it is blank in feedfile")

            check_assignment(exception_list, dag_run.conf['location'], dag_run.conf['locationuri'], "Location not assigned as it is not available in Replicon",
                             "Location not assigned as it is blank in feedfile")

            if not dag_run.conf['enduserpermissionseturi']:
                exception_list.append(
                    "Permission not assigned as it is not available in Replicon")

            if dag_run.conf.get('timesheettemplate') and dag_run.conf['timesheettemplate'] != "Do Not Assign":
                if not dag_run.conf.get('timesheettemplateuri'):
                    exception_list.append(
                        "Timesheet template not assigned as it is not available in Replicon")
            else:
                exception_list.append(
                    "Timesheet template not assigned as it is not defined in mapper")

            if rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_4') and not rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_4').get('value_|_default_uri'):
                exception_list.append(
                    "Global timesheet approval path assigned as it is not defined in mapper")

            check_assignment(exception_list, dag_run.conf['timezone'], dag_run.conf['timezoneuri'], "Global timezone assigned as it is not available in Replicon",
                             "Global timezone assigned as it is not defined in mapper")

            check_assignment(exception_list, dag_run.conf['holicaycalendar'], dag_run.conf['holicaycalendaruri'],
                             "Global holiday calendar assigned as it is not available in Replicon",
                             "Global holiday calendar assigned as it is not defined in mapper")

            check_assignment(exception_list, dag_run.conf['employeetype'], dag_run.conf['employeetypeuri'],
                             "Employee type not assigned as it is not available in Replicon",
                             "Employee type not assigned as it is not defined in mapper")

            check_assignment(exception_list, dag_run.conf['timeofftemplate'], dag_run.conf['timeofftemplateuri'],
                             "Timeoff template not assigned as it is not available in Replicon",
                             "Timeoff template not assigned as it is not defined in mapper")

            if rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_3') and not rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_3').get('value_|_default_uri'):
                exception_list.append(
                    "Global timeoff approval path assigned as it is not defined in mapper")

            check_assignment(exception_list, dag_run.conf['payrule'], dag_run.conf['payruleuri'],
                             "Payrule not assigned as it is not available in Replicon",
                             "Payrule not assigned as it is not defined in mapper")

            return ", ".join(exception_list)

        log_exception_log_5 = rail.PythonOperator(
            task_id='log_exception_log_5',
            python_callable=get_exceptions
        )

        velaw_add_user_import_logs = rail.CreateLogOperator(
            task_id='velaw_add_user_import_logs',
        )

        velaw_supervisor_check_user_add_logs = rail.CreateLogOperator(
            task_id='velaw_supervisor_check_user_add_logs',
        )

        date_split_start_date_6 = rail.EmptyOperator(
            task_id='date_split_start_date_6',
        )

        declare_variable_7 = rail.SetVariableOperator(
            task_id='declare_variable_7',
            append=False,
            name='schedulePolicySchedule',
            value=None
        )

        if_request_officescheduleuri_present_8 = rail.IfOperator(
            task_id='if_request_officescheduleuri_present_8',
            test='''{{ dag_run.conf.officescheduleuri | is_truthy }}''',
            yes_task="update_variable_9",
            no_task="declare_variable_10",
        )

        update_variable_9 = rail.SetVariableOperator(
            task_id='update_variable_9',
            append=False,
            name='{{ result("declare_variable_7").name }}',
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

        declare_variable_10 = rail.SetVariableOperator(
            task_id='declare_variable_10',
            append=False,
            name='holidayCalendar',
            value=None
        )

        if_request_holicaycalendaruri_present_11 = rail.IfOperator(
            task_id='if_request_holicaycalendaruri_present_11',
            test='''{{ dag_run.conf.holicaycalendaruri | is_truthy }}''',
            yes_task="update_variable_12",
            no_task="declare_variable_13",
        )

        update_variable_12 = rail.SetVariableOperator(
            task_id='update_variable_12',
            append=False,
            name='{{ result("declare_variable_10").name }}',
            value={
                "uri": "{{ dag_run.conf.holicaycalendaruri }}",
                "name": null
            }
        )

        declare_variable_13 = rail.SetVariableOperator(
            task_id='declare_variable_13',
            append=False,
            name='permissionSets',
            value=None
        )

        if_request_enduserpermissionseturi_present_14 = rail.IfOperator(
            task_id='if_request_enduserpermissionseturi_present_14',
            test='''{{ dag_run.conf.enduserpermissionseturi | is_truthy }}''',
            yes_task="update_variable_15",
            no_task="declare_list_16",
        )

        update_variable_15 = rail.SetVariableOperator(
            task_id='update_variable_15',
            append=False,
            name='{{ result("declare_variable_13").name }}',
            value=[
                {
                    "uri": "{{ dag_run.conf.enduserpermissionseturi }}",
                    "name": null
                }
            ]
        )

        declare_list_16 = rail.SetVariableOperator(
            task_id='declare_list_16',
            append=False,
            name='policySets',
            value=[]
        )

        if_request_timesheettemplateuri_present_17 = rail.IfOperator(
            task_id='if_request_timesheettemplateuri_present_17',
            test='''{{ dag_run.conf.timesheettemplateuri | is_truthy  and dag_run.conf.timesheettemplate != 'Do Not Assign' }}''',
            yes_task="insert_to_list_18",
            no_task="if_request_timeofftemplateuri_present_19",
        )

        insert_to_list_18 = rail.SetVariableOperator(
            task_id='insert_to_list_18',
            append=True,
            name='{{ result("declare_list_16").name }}',
            value={
                "uri": "{{ dag_run.conf.timesheettemplateuri }}",
                "name": "{{ dag_run.conf.timesheettemplate }}"
            }
        )

        if_request_timeofftemplateuri_present_19 = rail.IfOperator(
            task_id='if_request_timeofftemplateuri_present_19',
            test='''{{ dag_run.conf.timeofftemplateuri | is_truthy }}''',
            yes_task="insert_to_list_20",
            no_task="log_policy_settoassign_21",
        )

        insert_to_list_20 = rail.SetVariableOperator(
            task_id='insert_to_list_20',
            append=True,
            name='{{ result("declare_list_16").name }}',
            value={
                "uri": "{{ dag_run.conf.timeofftemplateuri }}",
                "name": "{{ dag_run.conf.timeofftemplate }}"
            }
        )

        log_policy_settoassign_21 = rail.GetVariableOperator(
            task_id='log_policy_settoassign_21',
            name='policySets'
        )

        declare_variable_22 = rail.SetVariableOperator(
            task_id='declare_variable_22',
            append=False,
            name='timesheetApprovalPath',
            value=None
        )

        if_entry_col10_present_23 = rail.IfOperator(
            task_id='if_entry_col10_present_23',
            test=lambda: rail.result(
                'velaw_user_import_mapper_search_entries_timesheet_approval_path_4').get('value_|_default_uri'),
            yes_task="update_variable_24",
            no_task="declare_variable_25",
        )

        update_variable_24 = rail.SetVariableOperator(
            task_id='update_variable_24',
            append=False,
            name='{{ result("declare_variable_22").name }}',
            value=lambda: {
                "uri": null,
                "name": rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_4').get('value_|_default_uri')
            }
        )

        declare_variable_25 = rail.SetVariableOperator(
            task_id='declare_variable_25',
            append=False,
            name='timeOffApprovalPath',
            value=None
        )

        if_entry_col10_present_26 = rail.IfOperator(
            task_id='if_entry_col10_present_26',
            test=lambda: rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_3') and rail.result(
                'velaw_user_import_mapper_search_entries_time_off_approval_path_3').get('value_|_default_uri'),
            yes_task="update_variable_27",
            no_task="declare_variable_28",
        )

        update_variable_27 = rail.SetVariableOperator(
            task_id='update_variable_27',
            append=False,
            name='{{ result("declare_variable_25").name }}',
            value=lambda: {
                "uri": null,
                "name": rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_3') and rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_3').get('value_|_default_uri')
            }
        )

        declare_variable_28 = rail.SetVariableOperator(
            task_id='declare_variable_28',
            append=False,
            name='departmentGroupSchedule',
            value=None
        )

        if_request_departmenturi_present_29 = rail.IfOperator(
            task_id='if_request_departmenturi_present_29',
            test='''{{ dag_run.conf.departmenturi | is_truthy }}''',
            yes_task="update_variable_30",
            no_task="declare_variable_31",
        )

        update_variable_30 = rail.SetVariableOperator(
            task_id='update_variable_30',
            append=False,
            name='{{ result("declare_variable_28").name }}',
            value=[
                {
                    "departmentGroup": {
                        "uri": "{{ dag_run.conf.departmenturi }}",
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_31 = rail.SetVariableOperator(
            task_id='declare_variable_31',
            append=False,
            name='employeeTypeGroupSchedule',
            value=None
        )

        if_request_employeetypeuri_present_32 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_32',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="update_variable_33",
            no_task="declare_variable_34"
        )

        update_variable_33 = rail.SetVariableOperator(
            task_id='update_variable_33',
            append=False,
            name='{{ result("declare_variable_31").name }}',
            value=[
                {
                    "employeeTypeGroup": {
                        "uri": "{{ dag_run.conf.employeetypeuri }}",
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_34 = rail.SetVariableOperator(
            task_id='declare_variable_34',
            append=False,
            name='costCenterSchedule',
            value=None
        )

        if_request_jobfamiliesuri_present_35 = rail.IfOperator(
            task_id='if_request_jobfamiliesuri_present_35',
            test='''{{ dag_run.conf.jobfamiliesuri | is_truthy }}''',
            yes_task="update_variable_36",
            no_task="declare_variable_37",
        )

        update_variable_36 = rail.SetVariableOperator(
            task_id='update_variable_36',
            append=False,
            name='{{ result("declare_variable_34").name }}',
            value=[
                {
                    "costCenter": {
                        "uri": "{{ dag_run.conf.jobfamiliesuri }}",
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_37 = rail.SetVariableOperator(
            task_id='declare_variable_37',
            append=False,
            name='divisionSchedule',
            value=None
        )

        if_request_paytypeuri_present_38 = rail.IfOperator(
            task_id='if_request_paytypeuri_present_38',
            test='''{{ dag_run.conf.paytypeuri | is_truthy }}''',
            yes_task="update_variable_39",
            no_task="declare_variable_40",
        )

        update_variable_39 = rail.SetVariableOperator(
            task_id='update_variable_39',
            append=False,
            name='{{ result("declare_variable_37").name }}',
            value=[
                {
                    "division": {
                        "uri": "{{ dag_run.conf.paytypeuri }}",
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_40 = rail.SetVariableOperator(
            task_id='declare_variable_40',
            append=False,
            name='locationSchedule',
            value=None
        )

        if_request_locationuri_present_41 = rail.IfOperator(
            task_id='if_request_locationuri_present_41',
            test='''{{ dag_run.conf.locationuri | is_truthy }}''',
            yes_task="update_variable_42",
            no_task="declare_variable_43",
        )

        update_variable_42 = rail.SetVariableOperator(
            task_id='update_variable_42',
            append=False,
            name='{{ result("declare_variable_40").name }}',
            value=[
                {
                    "location": {
                        "uri": "{{ dag_run.conf.locationuri }}",
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_43 = rail.SetVariableOperator(
            task_id='declare_variable_43',
            append=False,
            name='timesheetPeriodSchedule',
            value=None
        )

        if_request_timesheetperioduri_present_44 = rail.IfOperator(
            task_id='if_request_timesheetperioduri_present_44',
            test='''{{ dag_run.conf.timesheetperioduri | is_truthy }}''',
            yes_task="update_variable_45",
            no_task="declare_variable_46",
        )

        update_variable_45 = rail.SetVariableOperator(
            task_id='update_variable_45',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value=[
                {
                    "timesheetPeriod": {
                        "uri": "{{ dag_run.conf.timesheetperioduri }}",
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_46 = rail.SetVariableOperator(
            task_id='declare_variable_46',
            append=False,
            name='payRuleScriptSchedule',
            value=None
        )

        if_request_payruleuri_present_47 = rail.IfOperator(
            task_id='if_request_payruleuri_present_47',
            test='''{{ dag_run.conf.payruleuri | is_truthy }}''',
            yes_task="update_variable_48",
            no_task="declare_variable_49",
        )

        update_variable_48 = rail.SetVariableOperator(
            task_id='update_variable_48',
            append=False,
            name='{{ result("declare_variable_46").name }}',
            value=[
                {
                    "payRuleScript": {
                        "uri": "{{ dag_run.conf.payruleuri }}",
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_49 = rail.SetVariableOperator(
            task_id='declare_variable_49',
            append=False,
            name='timeZone',
            value=None
        )

        if_request_timezoneuri_present_50 = rail.IfOperator(
            task_id='if_request_timezoneuri_present_50',
            test='''{{ dag_run.conf.timezoneuri | is_truthy }}''',
            yes_task="update_variable_51",
            no_task="declare_list_52",
        )

        update_variable_51 = rail.SetVariableOperator(
            task_id='update_variable_51',
            append=False,
            name='{{ result("declare_variable_49").name }}',
            value={
                "uri": "{{ dag_run.conf.timezoneuri }}",
                "IANAName": null
            }
        )

        declare_list_52 = rail.SetVariableOperator(
            task_id='declare_list_52',
            append=False,
            name='customFieldValues',
            value=[]
        )

        if_request_jobcode_present_53 = rail.IfOperator(
            task_id='if_request_jobcode_present_53',
            test='''{{ dag_run.conf.jobcode | is_truthy }}''',
            yes_task="insert_to_list_54",
            no_task="if_request_jobtitle_present_55",
        )

        insert_to_list_54 = rail.SetVariableOperator(
            task_id='insert_to_list_54',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.jobcodeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.jobcodeudfvalueuri }}",
                    "name": null
                }
            }
        )

        if_request_jobtitle_present_55 = rail.IfOperator(
            task_id='if_request_jobtitle_present_55',
            test='''{{ dag_run.conf.jobtitle | is_truthy }}''',
            yes_task="insert_to_list_56",
            no_task="if_request_flsastatus_present_57",
        )

        insert_to_list_56 = rail.SetVariableOperator(
            task_id='insert_to_list_56',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.jobtitleudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.jobtitleudfvalueuri }}",
                    "name": null
                }
            }
        )

        if_request_flsastatus_present_57 = rail.IfOperator(
            task_id='if_request_flsastatus_present_57',
            test='''{{ dag_run.conf.flsastatus | is_truthy }}''',
            yes_task="insert_to_list_58",
            no_task="if_request_assignmentcategory_present_59",
        )

        insert_to_list_58 = rail.SetVariableOperator(
            task_id='insert_to_list_58',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.flsastatusudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.flsastatusudfvalueuri }}",
                    "name": null
                }
            }
        )

        if_request_assignmentcategory_present_59 = rail.IfOperator(
            task_id='if_request_assignmentcategory_present_59',
            test='''{{ dag_run.conf.assignmentcategory | is_truthy }}''',
            yes_task="insert_to_list_60",
            no_task="if_request_countryisocode_present_61",
        )

        insert_to_list_60 = rail.SetVariableOperator(
            task_id='insert_to_list_60',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.assignmentcategoryudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.assignmentcategoryudfvalueuri }}",
                    "name": null
                }
            }
        )

        if_request_countryisocode_present_61 = rail.IfOperator(
            task_id='if_request_countryisocode_present_61',
            test='''{{ dag_run.conf.countryisocode | is_truthy }}''',
            yes_task="insert_to_list_62",
            no_task="if_request_persontype_present_63"
        )

        insert_to_list_62 = rail.SetVariableOperator(
            task_id='insert_to_list_62',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.countryisocodeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.countryisocodeudfvalueuri }}",
                    "name": null
                }
            }
        )

        if_request_persontype_present_63 = rail.IfOperator(
            task_id='if_request_persontype_present_63',
            test='''{{ dag_run.conf.persontype | is_truthy }}''',
            yes_task="insert_to_list_64",
            no_task="if_request_legalemployer_present_65",
        )

        insert_to_list_64 = rail.SetVariableOperator(
            task_id='insert_to_list_64',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.persontypeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.persontypeudfvalueuri }}",
                    "name": null
                }
            }
        )

        if_request_legalemployer_present_65 = rail.IfOperator(
            task_id='if_request_legalemployer_present_65',
            test='''{{ dag_run.conf.legalemployer | is_truthy }}''',
            yes_task="insert_to_list_66",
            no_task="get_customfield_values",
        )

        insert_to_list_66 = rail.SetVariableOperator(
            task_id='insert_to_list_66',
            append=True,
            name='{{ result("declare_list_52").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.legalemployerudfvalue }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.legalemployerudfvalueuri }}",
                    "name": null
                }
            }
        )

        get_customfield_values = rail.GetVariableOperator(
            task_id='get_customfield_values',
            name='customFieldValues'
        )

        log_customfield_values_67 = rail.PythonOperator(
            task_id='log_customfield_values_67',
            python_callable=lambda: json.loads(json.dumps(rail.result('get_customfield_values')['value'], ensure_ascii=False).replace('"date":{}', '"date":null')
                                               .replace('{"year":null,"month":null,"day":null}', '{}')) if rail.result('get_customfield_values')['value'] else []
        )

        create_user_68 = rail.RepliconServiceOperator(
            task_id='create_user_68',
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
                    "emailAddress": dag_run.conf['email'],
                    "employeeId": dag_run.conf['employeeid'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": rail.result('update_variable_9')['value'] if rail.result('update_variable_9') else [],
                    "workWeekStartDayUri": dag_run.conf['workweekuri'],
                    "employmentDateRange": {
                        "startDate": {
                            "year": dag_run.conf['startdate'].split('/')[2],
                            "month": dag_run.conf['startdate'].split('/')[0],
                            "day": dag_run.conf['startdate'].split('/')[1]
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['loginname'],
                        "SSOName": dag_run.conf['loginname'],
                        "password": null
                    },
                    "holidayCalendar": rail.result('update_variable_12')['value'] if rail.result('update_variable_12') else null,
                    "timeOffPolicy": null,
                    "permissionSets": rail.result('update_variable_15')['value'] if rail.result('update_variable_15') else [],
                    "policySets": rail.result('log_policy_settoassign_21')['value'],
                    "employeeType": null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": {
                        "initialHourlyRate": {
                            "amount": dag_run.conf['hourlycostamount'],
                            "currency": {
                                "uri": dag_run.conf['hourlycostcurrencyuri'],
                                "name": null,
                                "symbol": null
                            }
                        },
                        "scheduleEntries": []
                    },
                    "payrollRateSchedule": {
                        "initialHourlyRate": {
                            "amount": dag_run.conf['payratesamount'],
                            "currency": {
                                "uri": dag_run.conf['payratescurrencyuri'],
                                "name": null,
                                "symbol": null
                            }
                        },
                        "scheduleEntries": []
                    },
                    "defaultBillingRate": {
                        "amount": dag_run.conf['defaultbillingrateamount'],
                        "currency": {
                            "uri": dag_run.conf['defaultbillingratecurrencyuri'],
                            "name": null,
                            "symbol": null
                        }
                    },
                    "timesheetApprovalPath": rail.result('update_variable_24')['value'] if rail.result('update_variable_24') else null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": rail.result('update_variable_27')['value'] if rail.result('update_variable_27') else null,
                    "customFieldValues": rail.result('log_customfield_values_67'),
                    "assignedActivities": [],
                    "timeZone": rail.result('update_variable_51')['value'] if rail.result('update_variable_51') else null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": rail.result('update_variable_42')['value'] if rail.result('update_variable_42') else [],
                    "divisionSchedule": rail.result('update_variable_39')['value'] if rail.result('update_variable_39') else [],
                    "costCenterSchedule": rail.result('update_variable_36')['value'] if rail.result('update_variable_36') else [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": rail.result('update_variable_30')['value'] if rail.result('update_variable_30') else [],
                    "employeeTypeGroupSchedule": rail.result('update_variable_33')['value'] if rail.result('update_variable_33') else [],
                    "timesheetPeriodSchedule": rail.result('update_variable_45')['value'] if rail.result('update_variable_45') else [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": rail.result('update_variable_48')['value'] if rail.result('update_variable_48') else [],
                    "displayNameParameter": null
                }
            }
        )

        remove_timeoffassignmentsfornewusers_69 = rail.RepliconServiceOperator(
            task_id='remove_timeoffassignmentsfornewusers_69',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_68').uri }}",
                "timeOffTypeUris": []
            }
        )

        put_policy_data_access_scopes_for_userfor_time_off_calendar_visibility_70 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_userfor_time_off_calendar_visibility_70',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data={
                "userUri": "{{ result('create_user_68').uri }}",
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:time-off",
                        "locations": [
                            {
                                "location": null,
                                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                            }
                        ],
                        "divisions": [],
                        "costCenters": [],
                        "serviceCenters": [],
                        "departmentGroups": [
                            {
                                "departmentGroup": null,
                                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:do-not-include-descendants"
                            }
                        ],
                        "employeeTypeGroups": []
                    }
                ]
            }
        )

        put_activity_assignments_for_user_71 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_71',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_68')['uri'],
                "activityUris": dag_run.conf['activitylist']
            }
        )

        if_request_supervisorloginname_present_72 = rail.IfOperator(
            task_id='if_request_supervisorloginname_present_72',
            test='''{{ dag_run.conf.supervisorloginname | is_truthy }}''',
            yes_task="if_request_supervisorloginname_equals_to_dataworkato_service3cd9c331requestloginname_73",
            no_task="trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089"
        )

        if_request_supervisorloginname_equals_to_dataworkato_service3cd9c331requestloginname_73 = rail.IfOperator(
            task_id='if_request_supervisorloginname_equals_to_dataworkato_service3cd9c331requestloginname_73',
            test='''{{ dag_run.conf.supervisorloginname == dag_run.conf.loginname }}''',
            yes_task="log_supervisor_skipped_74",
            no_task="if_request_supervisoruri_present_76",
        )

        log_supervisor_skipped_74 = rail.PythonOperator(
            task_id='log_supervisor_skipped_74',
            python_callable=lambda: "Supervisor not updated - Supervisor login name is same as User login name"
        )

        if_request_supervisoruri_present_76 = rail.IfOperator(
            task_id='if_request_supervisoruri_present_76',
            test='''{{ dag_run.conf.supervisoruri | is_truthy }}''',
            yes_task="if_request_supervisorstatus_equals_to_enabled_77",
            no_task="velaw_supervisor_check_add_entry_88",
        )

        if_request_supervisorstatus_equals_to_enabled_77 = rail.IfOperator(
            task_id='if_request_supervisorstatus_equals_to_enabled_77',
            test='''{{ dag_run.conf.supervisorstatus == 'Enabled' }}''',
            yes_task="get_assigned_permission_sets_for_userfor_supervisor_78",
            no_task="velaw_supervisor_check_add_entry_86",
        )

        get_assigned_permission_sets_for_userfor_supervisor_78 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_userfor_supervisor_78',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}"
            }
        )

        invoke_custom_ruby_code_79 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_79',
            python_callable=lambda: {
                "supervisor": rail.find_first_by_attr_and_get_attr(rail.result('get_assigned_permission_sets_for_userfor_supervisor_78'), 'displayText', "*Gen3 - Supervisor", 'permissionSet'),
                "enduser": rail.find_first_by_attr_and_get_attr(rail.result('get_assigned_permission_sets_for_userfor_supervisor_78'), 'displayText', "*Gen3 - Project Resource with reports & Substitute User", 'permissionSet')
            }
        )

        if_output_supervisor_blank_80 = rail.IfOperator(
            task_id='if_output_supervisor_blank_80',
            test='''{{ result('invoke_custom_ruby_code_79').supervisor | is_falsy }}''',
            yes_task="assign_supervsior_permission_set_to_user_gen3_supervisor_81",
            no_task="if_output_enduser_blank_82",
        )

        assign_supervsior_permission_set_to_user_gen3_supervisor_81 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_gen3_supervisor_81',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        if_output_enduser_blank_82 = rail.IfOperator(
            task_id='if_output_enduser_blank_82',
            test='''{{ result('invoke_custom_ruby_code_79').enduser | is_falsy }}''',
            yes_task="assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_83",
            no_task="assigninitialsupervisor_84",
        )

        assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_83 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_83',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorendusepermissionuri }}"
            }
        )

        assigninitialsupervisor_84 = rail.RepliconServiceOperator(
            task_id='assigninitialsupervisor_84',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_68').uri }}",
                "supervisorUri": "{{ dag_run.conf.supervisoruri }}",
                "dateRange": null
            }
        )

        velaw_supervisor_check_add_entry_86 = rail.WriteLogOperator(
            task_id='velaw_supervisor_check_add_entry_86',
            log="{{ result('velaw_supervisor_check_user_add_logs') }}",
            message="na",
            severity="pending",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "pending",
                "user_uri": "{{ result('create_user_68').uri }}",
                "importaction": "Add"
            }
        )

        velaw_supervisor_check_add_entry_88 = rail.WriteLogOperator(
            task_id='velaw_supervisor_check_add_entry_88',
            log="{{ result('velaw_supervisor_check_user_add_logs') }}",
            message="na",
            severity="pending",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "status": "pending",
                "user_uri": "{{ result('create_user_68').uri }}",
                "importaction": "Add",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089',
            retries=0,
            items=[0],
            trigger_dag_id=config.timeoff_assignment_for_new_users_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ result('create_user_68').uri }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "countryisocode": "{{ dag_run.conf.countryisocode }}",
                "location": "{{ dag_run.conf.location }}",
                "persontype": "{{ dag_run.conf.persontype }}",
                "assignmentcategory": "{{ dag_run.conf.assignmentcategory }}",
                "flsastatus": "{{ dag_run.conf.flsastatus }}",
                "jobcode": "{{ dag_run.conf.jobcode }}"
            }
        )

        wait_for_completion_trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089") }}'
        )

        velaw_user_import_logs_add_entry_90 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_90',
            log="{{ result('velaw_add_user_import_logs') }}",
            message="na",
            severity=lambda: "Exception" if rail.result('log_exception_log_5') else "Exception" if rail.result(
                'log_supervisor_skipped_74') else "Success",
            properties=lambda dag_run: {
                "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
                "loginname": dag_run.conf['loginname'],
                "employeeid": dag_run.conf['employeeid'],
                "importaction": "add",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "Exception" if rail.result('log_exception_log_5') else "Exception" if rail.result('log_supervisor_skipped_74') else "Success",
                "details": ("User created partially - " + rail.result('log_exception_log_5')) if rail.result('log_exception_log_5') else ("User created partially - " + rail.result('log_supervisor_skipped_74')) if rail.result('log_supervisor_skipped_74') else "User created successfully"
            }
        )

        velaw_user_import_logs_add_entry_92 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_92',
            log="{{ result('velaw_add_user_import_logs') }}",
            message="na",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "importaction": "add",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> velaw_user_import_logs_add_entry_92
        can_run_batch_task >> rail.Label(
            'No') >> velaw_user_import_mapper_search_entries_time_off_approval_path_3
        velaw_user_import_mapper_search_entries_time_off_approval_path_3 >> velaw_user_import_mapper_search_entries_timesheet_approval_path_4 \
            >> log_exception_log_5 >> velaw_add_user_import_logs >> velaw_supervisor_check_user_add_logs \
            >> date_split_start_date_6 >> declare_variable_7 >> if_request_officescheduleuri_present_8
        if_request_officescheduleuri_present_8 >> rail.Label(
            'Yes') >> update_variable_9 >> declare_variable_10
        if_request_officescheduleuri_present_8 >> rail.Label(
            'No') >> declare_variable_10 >> if_request_holicaycalendaruri_present_11
        if_request_holicaycalendaruri_present_11 >> rail.Label(
            'Yes') >> update_variable_12 >> declare_variable_13
        if_request_holicaycalendaruri_present_11 >> rail.Label(
            'No') >> declare_variable_13 >> if_request_enduserpermissionseturi_present_14
        if_request_enduserpermissionseturi_present_14 >> rail.Label(
            'Yes') >> update_variable_15 >> declare_list_16
        if_request_enduserpermissionseturi_present_14 >> rail.Label(
            'No') >> declare_list_16 >> if_request_timesheettemplateuri_present_17
        if_request_timesheettemplateuri_present_17 >> rail.Label(
            'Yes') >> insert_to_list_18 >> if_request_timeofftemplateuri_present_19
        if_request_timesheettemplateuri_present_17 >> rail.Label(
            'No') >> if_request_timeofftemplateuri_present_19
        if_request_timeofftemplateuri_present_19 >> rail.Label(
            'Yes') >> insert_to_list_20 >> log_policy_settoassign_21
        if_request_timeofftemplateuri_present_19 >> rail.Label(
            'No') >> log_policy_settoassign_21 >> declare_variable_22 >> if_entry_col10_present_23
        if_entry_col10_present_23 >> rail.Label(
            'Yes') >> update_variable_24 >> declare_variable_25
        if_entry_col10_present_23 >> rail.Label(
            'No') >> declare_variable_25 >> if_entry_col10_present_26
        if_entry_col10_present_26 >> rail.Label(
            'Yes') >> update_variable_27 >> declare_variable_28
        if_entry_col10_present_26 >> rail.Label(
            'No') >> declare_variable_28 >> if_request_departmenturi_present_29
        if_request_departmenturi_present_29 >> rail.Label(
            'Yes') >> update_variable_30 >> declare_variable_31
        if_request_departmenturi_present_29 >> rail.Label(
            'No') >> declare_variable_31 >> if_request_employeetypeuri_present_32
        if_request_employeetypeuri_present_32 >> rail.Label(
            'Yes') >> update_variable_33 >> declare_variable_34
        if_request_employeetypeuri_present_32 >> rail.Label(
            'No') >> declare_variable_34 >> if_request_jobfamiliesuri_present_35
        if_request_jobfamiliesuri_present_35 >> rail.Label(
            'Yes') >> update_variable_36 >> declare_variable_37
        if_request_jobfamiliesuri_present_35 >> rail.Label(
            'No') >> declare_variable_37 >> if_request_paytypeuri_present_38
        if_request_paytypeuri_present_38 >> rail.Label(
            'Yes') >> update_variable_39 >> declare_variable_40
        if_request_paytypeuri_present_38 >> rail.Label(
            'No') >> declare_variable_40 >> if_request_locationuri_present_41
        if_request_locationuri_present_41 >> rail.Label(
            'Yes') >> update_variable_42 >> declare_variable_43
        if_request_locationuri_present_41 >> rail.Label(
            'No') >> declare_variable_43 >> if_request_timesheetperioduri_present_44
        if_request_timesheetperioduri_present_44 >> rail.Label(
            'Yes') >> update_variable_45 >> declare_variable_46
        if_request_timesheetperioduri_present_44 >> rail.Label(
            'No') >> declare_variable_46 >> if_request_payruleuri_present_47
        if_request_payruleuri_present_47 >> rail.Label(
            'Yes') >> update_variable_48 >> declare_variable_49
        if_request_payruleuri_present_47 >> rail.Label(
            'No') >> declare_variable_49 >> if_request_timezoneuri_present_50
        if_request_timezoneuri_present_50 >> rail.Label(
            'Yes') >> update_variable_51 >> declare_list_52
        if_request_timezoneuri_present_50 >> rail.Label(
            'No') >> declare_list_52 >> if_request_jobcode_present_53
        if_request_jobcode_present_53 >> rail.Label(
            'Yes') >> insert_to_list_54 >> if_request_jobtitle_present_55
        if_request_jobcode_present_53 >> rail.Label(
            'No') >> if_request_jobtitle_present_55
        if_request_jobtitle_present_55 >> rail.Label(
            'Yes') >> insert_to_list_56 >> if_request_flsastatus_present_57
        if_request_jobtitle_present_55 >> rail.Label(
            'No') >> if_request_flsastatus_present_57
        if_request_flsastatus_present_57 >> rail.Label(
            'Yes') >> insert_to_list_58 >> if_request_assignmentcategory_present_59
        if_request_flsastatus_present_57 >> rail.Label(
            'No') >> if_request_assignmentcategory_present_59
        if_request_assignmentcategory_present_59 >> rail.Label(
            'Yes') >> insert_to_list_60 >> if_request_countryisocode_present_61
        if_request_assignmentcategory_present_59 >> rail.Label(
            'No') >> if_request_countryisocode_present_61
        if_request_countryisocode_present_61 >> rail.Label(
            'Yes') >> insert_to_list_62 >> if_request_persontype_present_63
        if_request_countryisocode_present_61 >> rail.Label(
            'No') >> if_request_persontype_present_63
        if_request_persontype_present_63 >> rail.Label(
            'Yes') >> insert_to_list_64 >> if_request_legalemployer_present_65
        if_request_persontype_present_63 >> rail.Label(
            'No') >> if_request_legalemployer_present_65
        if_request_legalemployer_present_65 >> rail.Label(
            'Yes') >> insert_to_list_66 >> get_customfield_values
        if_request_legalemployer_present_65 >> rail.Label(
            'No') >> get_customfield_values >> log_customfield_values_67 >> create_user_68 >> remove_timeoffassignmentsfornewusers_69 \
            >> put_policy_data_access_scopes_for_userfor_time_off_calendar_visibility_70 >> put_activity_assignments_for_user_71 >> if_request_supervisorloginname_present_72
        if_request_supervisorloginname_present_72 >> rail.Label(
            'Yes') >> if_request_supervisorloginname_equals_to_dataworkato_service3cd9c331requestloginname_73
        if_request_supervisorloginname_equals_to_dataworkato_service3cd9c331requestloginname_73 >> rail.Label(
            'Yes') >> log_supervisor_skipped_74 >> trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089
        if_request_supervisorloginname_equals_to_dataworkato_service3cd9c331requestloginname_73 >> rail.Label(
            'No') >> if_request_supervisoruri_present_76
        if_request_supervisoruri_present_76 >> rail.Label(
            'Yes') >> if_request_supervisorstatus_equals_to_enabled_77
        if_request_supervisoruri_present_76 >> rail.Label(
            'No') >> velaw_supervisor_check_add_entry_88 >> trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089
        if_request_supervisorstatus_equals_to_enabled_77 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_userfor_supervisor_78 >> invoke_custom_ruby_code_79 >> if_output_supervisor_blank_80
        if_output_supervisor_blank_80 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_gen3_supervisor_81 >> if_output_enduser_blank_82
        if_output_supervisor_blank_80 >> rail.Label(
            'No') >> if_output_enduser_blank_82
        if_output_enduser_blank_82 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_83 >> assigninitialsupervisor_84
        if_output_enduser_blank_82 >> rail.Label(
            'No') >> assigninitialsupervisor_84 >> velaw_supervisor_check_add_entry_88
        if_request_supervisorstatus_equals_to_enabled_77 >> rail.Label(
            'No') >> velaw_supervisor_check_add_entry_86 >> trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089
        if_request_supervisorloginname_present_72 >> rail.Label(
            'No') >> trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089 \
            >> wait_for_completion_trigger_dag_run_velawg3_child_timeoff_assignment_for_new_users_v2_089 >> velaw_user_import_logs_add_entry_90 \
            >> velaw_user_import_logs_add_entry_92 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
