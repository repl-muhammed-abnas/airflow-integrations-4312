
from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from horizonmedia.user_import_v2.horizonmedia_user_import_master_mapper import horizonmedia_user_import_master_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.horizonmedia_user_import_add_user_child,
        description=f'Horizonmedia_Child_Add User_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user"
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
                            "text": "{{ dag_run.conf.User_Name }}",
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
            data_handler=lambda data: next(map(lambda item: item['cells'][1]['uri'],
                                               filter(lambda item:
                                                      item['cells'][0]['textValue'] ==
                                                      rail.get_dag_run_conf()[
                                                          'User_Name'],
                                                      data['rows'])), None)
        )

        if_pluckuri_smart_joinnil_present_4 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_present_4',
            test='''{{ result('search_users_3')| is_truthy }}''',
            yes_task="horizonmedia_user_import_logs_add_entry_5",
            no_task="declare_list_8",
        )

        horizonmedia_user_import_logs_add_entry_5 = rail.WriteLogOperator(
            task_id='horizonmedia_user_import_logs_add_entry_5',
            log="{{ result('create_log') }}",
            message="na",
            severity="Exception",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "add",
                "status": "Exception",
                "details": "User not added as the user with the login name '{{ dag_run.conf.User_Name }}' is available in Replicon with different Employee ID.",
            }
        )

        stop_6 = rail.EmptyOperator(
            task_id='stop_6',

        )

        declare_list_8 = rail.SetVariableOperator(
            task_id='declare_list_8',
            append=False,
            name='exceptions',
            value=[]
        )

        date_split_start_date_9 = rail.PythonOperator(
            task_id='date_split_start_date_9',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Start_Date'])
        )

        declare_variable_10 = rail.SetVariableOperator(
            task_id='declare_variable_10',
            append=False,
            name='schedulePolicySchedule',
            value=None
        )

        get_all_permission_sets_11 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_11',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",

        )

        declare_list_12 = rail.SetVariableOperator(
            task_id='declare_list_12',
            append=False,
            name='permissionSets',
            value=[]
        )

        declare_list_13 = rail.SetVariableOperator(
            task_id='declare_list_13',
            append=False,
            name='policySets',
            value=[]
        )

        if_request_timesheettemplate_present_14 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_14',
            test='''{{ dag_run.conf.timesheettemplate | is_truthy }}''',
            yes_task="insert_to_list_15",
            no_task="insert_to_list_17",
        )

        insert_to_list_15 = rail.SetVariableOperator(
            task_id='insert_to_list_15',
            append=True,
            name='{{ result("declare_list_13").name }}',
            value={
                "uri": "{{ dag_run.conf.timesheettemplate }}",
                "name": null
            }
        )

        insert_to_list_17 = rail.SetVariableOperator(
            task_id='insert_to_list_17',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Timesheet template not assigned since {{ dag_run.conf.timesheettemplate }} not available in Replicon"
            }
        )

        insert_to_list_timeofftemplate_18 = rail.SetVariableOperator(
            task_id='insert_to_list_timeofftemplate_18',
            append=True,
            name='{{ result("declare_list_13").name }}',
            value={
                "uri": null,
                "name": "Time Off Template"
            }
        )

        log_policy_settoassign_19 = rail.PythonOperator(
            task_id='log_policy_settoassign_19',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_13')['name'])
        )

        declare_variable_20 = rail.SetVariableOperator(
            task_id='declare_variable_20',
            append=False,
            name='departmentGroupSchedule',
            value=None
        )

        if_request_sup_org_code_present_21 = rail.IfOperator(
            task_id='if_request_sup_org_code_present_21',
            test='''{{ dag_run.conf.Sup_Org_Code | is_truthy }}''',
            yes_task="update_variable_22",
            no_task="declare_variable_23",
        )

        update_variable_22 = rail.SetVariableOperator(
            task_id='update_variable_22',
            append=False,
            name='{{ result("declare_variable_20").name }}',
            value=[
                {
                    "departmentGroup": {
                        "uri": "{{ dag_run.conf.Sup_Org_Code }}",
                        "parent": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_23 = rail.SetVariableOperator(
            task_id='declare_variable_23',
            append=False,
            name='employeeTypeGroupSchedule',
            value=None
        )

        if_request_employeetypeuri_present_24 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_24',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="update_variable_25",
            no_task="declare_variable_26",
        )

        update_variable_25 = rail.SetVariableOperator(
            task_id='update_variable_25',
            append=False,
            name='{{ result("declare_variable_23").name }}',
            value=[
                {
                    "employeeTypeGroup": {
                        "uri": "{{ dag_run.conf.employeetypeuri }}",
                        "parent": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_26 = rail.SetVariableOperator(
            task_id='declare_variable_26',
            append=False,
            name='servicecenterschedule',
            value=None
        )

        if_request_job_profile_code_present_27 = rail.IfOperator(
            task_id='if_request_job_profile_code_present_27',
            test='''{{ dag_run.conf.Job_Profile_Code | is_truthy }}''',
            yes_task="update_variable_28",
            no_task="declare_variable_29",
        )

        update_variable_28 = rail.SetVariableOperator(
            task_id='update_variable_28',
            append=False,
            name='{{ result("declare_variable_26").name }}',
            value=[
                {
                    "serviceCenter": {
                        "uri": "{{ dag_run.conf.Job_Profile_Code }}",
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_29 = rail.SetVariableOperator(
            task_id='declare_variable_29',
            append=False,
            name='costcenterschedule',
            value=None
        )

        if_request_flsa_present_30 = rail.IfOperator(
            task_id='if_request_flsa_present_30',
            test='''{{ dag_run.conf.FLSA | is_truthy }}''',
            yes_task="update_variable_31",
            no_task="declare_variable_32",
        )

        update_variable_31 = rail.SetVariableOperator(
            task_id='update_variable_31',
            append=False,
            name='{{ result("declare_variable_29").name }}',
            value=[
                {
                    "costCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": "{{ dag_run.conf.FLSA }}"
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_32 = rail.SetVariableOperator(
            task_id='declare_variable_32',
            append=False,
            name='locationSchedule',
            value=None
        )

        if_request_location_code_present_33 = rail.IfOperator(
            task_id='if_request_location_code_present_33',
            test='''{{ dag_run.conf.Location_Code | is_truthy }}''',
            yes_task="update_variable_34",
            no_task="declare_variable_35",
        )

        update_variable_34 = rail.SetVariableOperator(
            task_id='update_variable_34',
            append=False,
            name='{{ result("declare_variable_32").name }}',
            value=[
                {
                    "location": {
                        "uri": "{{ dag_run.conf.Location_Code }}",
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_35 = rail.SetVariableOperator(
            task_id='declare_variable_35',
            append=False,
            name='divisionSchedule',
            value=None
        )

        if_request_jobpositiontagcode_present_36 = rail.IfOperator(
            task_id='if_request_jobpositiontagcode_present_36',
            test='''{{ dag_run.conf.JobPositionTagCode | is_truthy }}''',
            yes_task="update_variable_37",
            no_task="declare_variable_38",
        )

        update_variable_37 = rail.SetVariableOperator(
            task_id='update_variable_37',
            append=False,
            name='{{ result("declare_variable_35").name }}',
            value=[
                {
                    "division": {
                        "uri": "{{ dag_run.conf.JobPositionTagCode }}",
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_38 = rail.SetVariableOperator(
            task_id='declare_variable_38',
            append=False,
            name='payRuleScriptSchedule',
            value=None
        )

        if_request_payrule_present_39 = rail.IfOperator(
            task_id='if_request_payrule_present_39',
            test='''{{ dag_run.conf.payrule | is_truthy }}''',
            yes_task="update_variable_40",
            no_task="declare_variable_41",
        )

        update_variable_40 = rail.SetVariableOperator(
            task_id='update_variable_40',
            append=False,
            name='{{ result("declare_variable_38").name }}',
            value=[
                {
                    "payRuleScript": {
                        "uri": null,
                        "name": "{{ dag_run.conf.payrule }}"
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_41 = rail.SetVariableOperator(
            task_id='declare_variable_41',
            append=False,
            name='timezone',
            value=None
        )

        if_request_timezone_present_42 = rail.IfOperator(
            task_id='if_request_timezone_present_42',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="update_variable_43",
            no_task="declare_list_44",
        )

        update_variable_43 = rail.SetVariableOperator(
            task_id='update_variable_43',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value={
                "uri": "{{ dag_run.conf.timezoneuri }}",
                "IANAName": null
            }
        )

        declare_list_44 = rail.SetVariableOperator(
            task_id='declare_list_44',
            append=False,
            name='customFieldValues',
            value=[]
        )

        if_request_position_id_present_45 = rail.IfOperator(
            task_id='if_request_position_id_present_45',
            test='''{{ dag_run.conf.Position_ID | is_truthy }}''',
            yes_task="insert_to_list_46",
            no_task="if_request_businesstitle_present_47",
        )

        insert_to_list_46 = rail.SetVariableOperator(
            task_id='insert_to_list_46',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.positionid_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Position_ID }}",
                "number": null
            }
        )

        if_request_businesstitle_present_47 = rail.IfOperator(
            task_id='if_request_businesstitle_present_47',
            test='''{{ dag_run.conf.BusinessTitle | is_truthy }}''',
            yes_task="insert_to_list_48",
            no_task="if_request_cost_center_code_present_49",
        )

        insert_to_list_48 = rail.SetVariableOperator(
            task_id='insert_to_list_48',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.businesstitle_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.BusinessTitle }}",
            }
        )

        if_request_cost_center_code_present_49 = rail.IfOperator(
            task_id='if_request_cost_center_code_present_49',
            test='''{{ dag_run.conf.Cost_Center_Code | is_truthy }}''',
            yes_task="insert_to_list_50",
            no_task="if_request_profit_center_code_present_51",
        )

        insert_to_list_50 = rail.SetVariableOperator(
            task_id='insert_to_list_50',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.costcentercode_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Cost_Center_Code }}",

            }
        )

        if_request_profit_center_code_present_51 = rail.IfOperator(
            task_id='if_request_profit_center_code_present_51',
            test='''{{ dag_run.conf.Profit_Center_Code | is_truthy }}''',
            yes_task="insert_to_list_52",
            no_task="if_request_department_code_present_53",
        )

        insert_to_list_52 = rail.SetVariableOperator(
            task_id='insert_to_list_52',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.profitcentercode_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Profit_Center_Code }}",

            }
        )

        if_request_department_code_present_53 = rail.IfOperator(
            task_id='if_request_department_code_present_53',
            test='''{{ dag_run.conf.Department_Code | is_truthy }}''',
            yes_task="insert_to_list_54",
            no_task="if_request_legal_name_present_55",
        )

        insert_to_list_54 = rail.SetVariableOperator(
            task_id='insert_to_list_54',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.departmentcode_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Department_Code }}",

            }
        )

        if_request_legal_name_present_55 = rail.IfOperator(
            task_id='if_request_legal_name_present_55',
            test='''{{ dag_run.conf.Legal_Name | is_truthy }}''',
            yes_task="insert_to_list_56",
            no_task="if_request_company_code_present_57",
        )

        insert_to_list_56 = rail.SetVariableOperator(
            task_id='insert_to_list_56',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.fulllegalname_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Legal_Name }}",

            }
        )

        if_request_company_code_present_57 = rail.IfOperator(
            task_id='if_request_company_code_present_57',
            test='''{{ dag_run.conf.Company_Code | is_truthy }}''',
            yes_task="insert_to_list_58",
            no_task="if_request_pref_name_present_59",
        )

        insert_to_list_58 = rail.SetVariableOperator(
            task_id='insert_to_list_58',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.companycode_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Company_Code }}",

            }
        )

        if_request_pref_name_present_59 = rail.IfOperator(
            task_id='if_request_pref_name_present_59',
            test='''{{ dag_run.conf.Pref_Name | is_truthy }}''',
            yes_task="insert_to_list_60",
            no_task="if_request_mgmt_code_present_61",
        )

        insert_to_list_60 = rail.SetVariableOperator(
            task_id='insert_to_list_60',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.prefferedfullname_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Pref_Name }}",

            }
        )

        if_request_mgmt_code_present_61 = rail.IfOperator(
            task_id='if_request_mgmt_code_present_61',
            test='''{{ dag_run.conf.Mgmt_Code | is_truthy }}''',
            yes_task="insert_to_list_62",
            no_task="if_request_work_space_present_63",
        )

        insert_to_list_62 = rail.SetVariableOperator(
            task_id='insert_to_list_62',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.managementlevelcode_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.Mgmt_Code }}",

            }
        )

        if_request_work_space_present_63 = rail.IfOperator(
            task_id='if_request_work_space_present_63',
            test='''{{ dag_run.conf.Work_Space | is_truthy }}''',
            yes_task="insert_to_list_64",
            no_task="if_request_workspace_optionuri_present_65",
        )

        insert_to_list_64 = rail.SetVariableOperator(
            task_id='insert_to_list_64',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.workspace_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.workspace_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_workspace_optionuri_present_65 = rail.IfOperator(
            task_id='if_request_workspace_optionuri_present_65',
            test='''{{ dag_run.conf.workspace_optionuri | is_truthy }}''',
            yes_task="insert_to_list_66",
            no_task="if_request_costcenter_optionuri_present_67",
        )

        insert_to_list_66 = rail.SetVariableOperator(
            task_id='insert_to_list_66',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.workspace_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.workspace_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_costcenter_optionuri_present_67 = rail.IfOperator(
            task_id='if_request_costcenter_optionuri_present_67',
            test='''{{ dag_run.conf.costcenter_optionuri | is_truthy }}''',
            yes_task="insert_to_list_68",
            no_task="if_request_department_optionuri_present_69",
        )

        insert_to_list_68 = rail.SetVariableOperator(
            task_id='insert_to_list_68',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.costcenter_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.costcenter_optionuri }}",
                    "name": null
                },
            }
        )

        if_request_department_optionuri_present_69 = rail.IfOperator(
            task_id='if_request_department_optionuri_present_69',
            test='''{{ dag_run.conf.department_optionuri | is_truthy }}''',
            yes_task="insert_to_list_70",
            no_task="if_request_profitcenter_optionuri_present_71",
        )

        insert_to_list_70 = rail.SetVariableOperator(
            task_id='insert_to_list_70',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.department_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.department_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_profitcenter_optionuri_present_71 = rail.IfOperator(
            task_id='if_request_profitcenter_optionuri_present_71',
            test='''{{ dag_run.conf.profitcenter_optionuri | is_truthy }}''',
            yes_task="insert_to_list_72",
            no_task="if_request_company_optionuri_present_73",
        )

        insert_to_list_72 = rail.SetVariableOperator(
            task_id='insert_to_list_72',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.profitcenter_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.profitcenter_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_company_optionuri_present_73 = rail.IfOperator(
            task_id='if_request_company_optionuri_present_73',
            test='''{{ dag_run.conf.company_optionuri | is_truthy }}''',
            yes_task="insert_to_list_74",
            no_task="if_request_managementlevel_optionuri_present_75",
        )

        insert_to_list_74 = rail.SetVariableOperator(
            task_id='insert_to_list_74',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.company_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.company_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_managementlevel_optionuri_present_75 = rail.IfOperator(
            task_id='if_request_managementlevel_optionuri_present_75',
            test='''{{ dag_run.conf.managementlevel_optionuri | is_truthy }}''',
            yes_task="insert_to_list_76",
            no_task="if_request_company_optionuri_present_77",
        )

        insert_to_list_76 = rail.SetVariableOperator(
            task_id='insert_to_list_76',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.managementlevel_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.managementlevel_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_company_optionuri_present_77 = rail.IfOperator(
            task_id='if_request_company_optionuri_present_77',
            test='''{{ dag_run.conf.company_optionuri | is_truthy }}''',
            yes_task="insert_to_list_78",
            no_task="if_request_employeeresidence_optionuri_present_79",
        )

        insert_to_list_78 = rail.SetVariableOperator(
            task_id='insert_to_list_78',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.company_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.company_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_employeeresidence_optionuri_present_79 = rail.IfOperator(
            task_id='if_request_employeeresidence_optionuri_present_79',
            test='''{{ dag_run.conf.employeeresidence_optionuri | is_truthy }}''',
            yes_task="insert_to_list_80",
            no_task="if_request_ceo_optionuri_present_81",
        )

        insert_to_list_80 = rail.SetVariableOperator(
            task_id='insert_to_list_80',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.employeeresidence_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.employeeresidence_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_ceo_optionuri_present_81 = rail.IfOperator(
            task_id='if_request_ceo_optionuri_present_81',
            test='''{{ dag_run.conf.ceo_optionuri | is_truthy }}''',
            yes_task="insert_to_list_82",
            no_task="if_request_ceo1_optionuri_present_83",
        )

        insert_to_list_82 = rail.SetVariableOperator(
            task_id='insert_to_list_82',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.ceo_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.ceo_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_ceo1_optionuri_present_83 = rail.IfOperator(
            task_id='if_request_ceo1_optionuri_present_83',
            test='''{{ dag_run.conf.ceo1_optionuri | is_truthy }}''',
            yes_task="insert_to_list_84",
            no_task="if_request_ceo2_optionuri_present_85",
        )

        insert_to_list_84 = rail.SetVariableOperator(
            task_id='insert_to_list_84',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.ceo1_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.ceo1_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_ceo2_optionuri_present_85 = rail.IfOperator(
            task_id='if_request_ceo2_optionuri_present_85',
            test='''{{ dag_run.conf.ceo2_optionuri | is_truthy }}''',
            yes_task="insert_to_list_86",
            no_task="if_request_ceo3_optionuri_present_87",
        )

        insert_to_list_86 = rail.SetVariableOperator(
            task_id='insert_to_list_86',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.ceo2_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.ceo2_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_ceo3_optionuri_present_87 = rail.IfOperator(
            task_id='if_request_ceo3_optionuri_present_87',
            test='''{{ dag_run.conf.ceo3_optionuri | is_truthy }}''',
            yes_task="insert_to_list_88",
            no_task="if_request_ceo4_optionuri_present_89",
        )

        insert_to_list_88 = rail.SetVariableOperator(
            task_id='insert_to_list_88',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.ceo3_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.ceo3_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_ceo4_optionuri_present_89 = rail.IfOperator(
            task_id='if_request_ceo4_optionuri_present_89',
            test='''{{ dag_run.conf.ceo4_optionuri | is_truthy }}''',
            yes_task="insert_to_list_90",
            no_task="if_request_ceo5_optionuri_present_91",
        )

        insert_to_list_90 = rail.SetVariableOperator(
            task_id='insert_to_list_90',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.ceo4_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.ceo4_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_ceo5_optionuri_present_91 = rail.IfOperator(
            task_id='if_request_ceo5_optionuri_present_91',
            test='''{{ dag_run.conf.ceo5_optionuri | is_truthy }}''',
            yes_task="insert_to_list_92",
            no_task="if_request_ceo6_optionuri_present_93",
        )

        insert_to_list_92 = rail.SetVariableOperator(
            task_id='insert_to_list_92',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.ceo5_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.ceo5_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_ceo6_optionuri_present_93 = rail.IfOperator(
            task_id='if_request_ceo6_optionuri_present_93',
            test='''{{ dag_run.conf.ceo6_optionuri | is_truthy }}''',
            yes_task="insert_to_list_94",
            no_task="if_request_groupleader_optionuri_present_95",
        )

        insert_to_list_94 = rail.SetVariableOperator(
            task_id='insert_to_list_94',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.ceo6_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.ceo6_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_groupleader_optionuri_present_95 = rail.IfOperator(
            task_id='if_request_groupleader_optionuri_present_95',
            test='''{{ dag_run.conf.groupleader_optionuri | is_truthy }}''',
            yes_task="insert_to_list_96",
            no_task="if_request_businesleader_optionuri_present_97",
        )

        insert_to_list_96 = rail.SetVariableOperator(
            task_id='insert_to_list_96',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.groupleader_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.groupleader_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_businesleader_optionuri_present_97 = rail.IfOperator(
            task_id='if_request_businesleader_optionuri_present_97',
            test='''{{ dag_run.conf.businesleader_optionuri | is_truthy }}''',
            yes_task="insert_to_list_98",
            no_task="if_request_contingentworkertype_optionuri_present_99",
        )

        insert_to_list_98 = rail.SetVariableOperator(
            task_id='insert_to_list_98',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.businesleader_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.businesleader_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_contingentworkertype_optionuri_present_99 = rail.IfOperator(
            task_id='if_request_contingentworkertype_optionuri_present_99',
            test='''{{ dag_run.conf.contingentworkertype_optionuri | is_truthy }}''',
            yes_task="insert_to_list_100",
            no_task="if_request_workerstatus_optionuri_present_101",
        )

        insert_to_list_100 = rail.SetVariableOperator(
            task_id='insert_to_list_100',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.contingentworkertype_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.contingentworkertype_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_workerstatus_optionuri_present_101 = rail.IfOperator(
            task_id='if_request_workerstatus_optionuri_present_101',
            test='''{{ dag_run.conf.workerstatus_optionuri | is_truthy }}''',
            yes_task="insert_to_list_102",
            no_task="if_request_country_optionuri_present_103",
        )

        insert_to_list_102 = rail.SetVariableOperator(
            task_id='insert_to_list_102',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.workerstatus_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.workerstatus_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_country_optionuri_present_103 = rail.IfOperator(
            task_id='if_request_country_optionuri_present_103',
            test='''{{ dag_run.conf.country_optionuri | is_truthy }}''',
            yes_task="insert_to_list_104",
            no_task="if_request_firstdayofleave_present_105",
        )

        insert_to_list_104 = rail.SetVariableOperator(
            task_id='insert_to_list_104',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.country_udfuri }}",
                    "name": null,
                    "groupUri": null
                }, "dropDownOption": {
                    "uri": "{{ dag_run.conf.country_optionuri }}",
                    "name": null
                },

            }
        )

        if_request_firstdayofleave_present_105 = rail.IfOperator(
            task_id='if_request_firstdayofleave_present_105',
            test='''{{ dag_run.conf.FirstDayofLeave | is_truthy }}''',
            yes_task="date_split_106",
            no_task="if_request_actuallastdayofleave_present_108",
        )

        def get_replicon_date(date_str):
            if not date_str:
                return None
            # date format in "07/15/2019"
            date = datetime.strptime(date_str.split(" ")[0], '%m/%d/%Y')
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }
        date_split_106 = rail.PythonOperator(
            task_id='date_split_106',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['FirstDayofLeave'])
        )

        insert_to_list_107 = rail.SetVariableOperator(
            task_id='insert_to_list_107',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.firstdayofleave_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": {
                    "year": "{{ result('date_split_106').year }}",
                    "month": "{{ result('date_split_106').month }}",
                    "day": "{{ result('date_split_106').day }}"
                },

            }
        )

        if_request_actuallastdayofleave_present_108 = rail.IfOperator(
            task_id='if_request_actuallastdayofleave_present_108',
            test='''{{ dag_run.conf.ActualLastDayofLeave | is_truthy }}''',
            yes_task="date_split_109",
            no_task="if_request_manager_optionuri_present_111",
        )

        date_split_109 = rail.PythonOperator(
            task_id='date_split_109',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['ActualLastDayofLeave'])
        )

        insert_to_list_110 = rail.SetVariableOperator(
            task_id='insert_to_list_110',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.lastdayofleave_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": {
                    "year": "{{ result('date_split_109').year }}",
                    "month": "{{ result('date_split_109').month }}",
                    "day": "{{ result('date_split_109').day }}"
                },


            }
        )

        if_request_manager_optionuri_present_111 = rail.IfOperator(
            task_id='if_request_manager_optionuri_present_111',
            test='''{{ dag_run.conf.manager_optionuri | is_truthy }}''',
            yes_task="insert_to_list_112",
            no_task="log_customfield_values_113",
        )

        insert_to_list_112 = rail.SetVariableOperator(
            task_id='insert_to_list_112',
            append=True,
            name='{{ result("declare_list_44").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.manager_udfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.manager_optionuri }}",
                    "name": null
                },
            }
        )

        log_customfield_values_113 = rail.PythonOperator(
            task_id='log_customfield_values_113',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_44')['name'])
        )

        log_displayname_114 = rail.PythonOperator(
            task_id='log_displayname_114',
            python_callable=lambda:  f"{rail.get_dag_run_conf()['firstname']} {rail.get_dag_run_conf()['lastname']}"
        )

        create_user_115 = rail.RepliconServiceOperator(
            task_id='create_user_115',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.get_dag_run_conf()['User_Name'],
                        "parameterCorrelationId": null
                    },
                    "firstname": rail.get_dag_run_conf()['firstname'],
                    "lastname": rail.get_dag_run_conf()['lastname'],
                    "emailAddress": rail.get_dag_run_conf()['Work_Email'],
                    "employeeId": rail.get_dag_run_conf()['employeeid'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": "8 hours/day, Su, Sa off",
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": "8 hours/day, Su, Sa off"
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
                    "employmentDateRange": {
                        "startDate": {
                            "year":  rail.result('date_split_start_date_9')['year'],
                            "month":  rail.result('date_split_start_date_9')['month'],
                            "day":  rail.result('date_split_start_date_9')['day'],
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
                        "loginName": rail.get_dag_run_conf()['User_Name'],
                        "SSOName": rail.get_dag_run_conf()['User_Name'],
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": "USER"
                        }
                    ],
                    "policySets": rail.result('log_policy_settoassign_19'),
                    "employeeType": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "timesheetPeriodTypeUri": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": rail.get_dag_run_conf()['TS_Approval_Path']
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": "System Approval"
                    },
                    "customFieldValues": rail.result('log_customfield_values_113'),
                    "assignedActivities": null,
                    "timeZone": rail.get_dag_run_var(rail.result('declare_variable_41')['name']),
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": rail.get_dag_run_var(rail.result('declare_variable_32')['name']),
                    "divisionSchedule": rail.get_dag_run_var(rail.result('declare_variable_35')['name']),
                    "costCenterSchedule": rail.get_dag_run_var(rail.result('declare_variable_29')['name']),
                    "serviceCenterSchedule": rail.get_dag_run_var(rail.result('declare_variable_26')['name']),
                    "departmentGroupSchedule": rail.get_dag_run_var(rail.result('declare_variable_20')['name']),
                    "employeeTypeGroupSchedule": rail.get_dag_run_var(rail.result('declare_variable_23')['name']),
                    "timesheetPeriodSchedule": [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": "Weekly starting on Monday"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": rail.get_dag_run_var(rail.result('declare_variable_38')['name']),
                    "displayNameParameter": {
                        "displayName": rail.result('log_displayname_114')
                    }
                }
            }
        )

        is_holidaycalendar_uri_present = rail.IfOperator(
            task_id='is_holidaycalendar_uri_present',
            test="{{ dag_run.conf.holiday_calendar_uri | is_truthy }}",
            yes_task="update_holiday_calendar",
            no_task="if_request_scheduledweeklyhours_present_116"
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                'userUri': "{{ result('create_user_115').uri }}",
                "holidayCalendarUri": "{{ dag_run.conf.holiday_calendar_uri }}"
            }
        )

        if_request_scheduledweeklyhours_present_116 = rail.IfOperator(
            task_id='if_request_scheduledweeklyhours_present_116',
            test='''{{ dag_run.conf.scheduledweeklyhours | is_truthy }}''',
            yes_task="update_text_value_schedulehours_118",
            no_task="if_request_payrollid_present_119",
        )

        update_text_value_schedulehours_118 = rail.RepliconServiceOperator(
            task_id='update_text_value_schedulehours_118',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user_115').uri }}",
                "customFieldUri": "{{ dag_run.conf.scheduledweeklyhours_udfuri }}",
                "value": "{{ dag_run.conf.scheduledweeklyhours }}"
            }
        )

        if_request_payrollid_present_119 = rail.IfOperator(
            task_id='if_request_payrollid_present_119',
            test='''{{ dag_run.conf.payrollid | is_truthy }}''',
            yes_task="update_text_value_payrollid_121",
            no_task="if_request_activities_present_122",
        )

        update_text_value_payrollid_121 = rail.RepliconServiceOperator(
            task_id='update_text_value_payrollid_121',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user_115').uri }}",
                "customFieldUri": "{{ dag_run.conf.payrollid_udfuri }}",
                "value": "{{ dag_run.conf.payrollid }}"
            }
        )

        if_request_activities_present_122 = rail.IfOperator(
            task_id='if_request_activities_present_122',
            test='''{{ dag_run.conf.activities | is_truthy }}''',
            yes_task="update_activity_assignments_for_user_123",
            no_task="horizonmedia_user_import_master_mapper_search_entries_124",
        )

        update_activity_assignments_for_user_123 = rail.RepliconServiceOperator(
            task_id='update_activity_assignments_for_user_123',
            endpoint="/services/ActivityService1.svc/UpdateActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_115')['uri'],
                "activityUris": rail.get_dag_run_conf()['activities']
            }
        )

        horizonmedia_user_import_master_mapper_search_entries_124 = rail.PythonOperator(
            task_id='horizonmedia_user_import_master_mapper_search_entries_124',
            python_callable=lambda:  list(filter(
                lambda x: x["field"] == "Time Off Types" and x["check"] == "yes", horizonmedia_user_import_master_mapper))
        )

        get_enabled_time_off_types_125 = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_125',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",

        )

        if_first_uri_present_126 = rail.IfOperator(
            task_id='if_first_uri_present_126',
            test='''{{ result('get_enabled_time_off_types_125') | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_127",
            no_task="foreach_output_130",
        )

        invoke_custom_ruby_code_127 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_127',
            python_callable=lambda: list(map(lambda x: {
                "name": x['value'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types_125'), 'displayText', x['value'], 'uri')
            }, rail.result('horizonmedia_user_import_master_mapper_search_entries_124')))

        )

        log_timeofftype_uris_128 = rail.PythonOperator(
            task_id='log_timeofftype_uris_128',
            python_callable=lambda:  list(
                map(lambda x: x['uri'], rail.result('invoke_custom_ruby_code_127')))
        )

        put_time_off_type_assignments_for_user_129 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_129',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_115')['uri'],
                "timeOffTypeUris": rail.result('log_timeofftype_uris_128')
            }
        )

        foreach_output_130 = rail.ForEachOperator(
            task_id='foreach_output_130',
            items="{{ result('invoke_custom_ruby_code_127') | to_json }}",
            start_task='if_foreach_output_130_uri_present_131',
            end_task='foreach_output_130_end'
        )

        if_foreach_output_130_uri_present_131 = rail.IfOperator(
            task_id='if_foreach_output_130_uri_present_131',
            test='''{{ result('foreach_output_130').uri | is_truthy }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_133",
            no_task="foreach_output_130_end",
        )

        get_default_time_off_type_policy_schedule_for_user_133 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_133',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('create_user_115').uri }}",
                    "timeOffTypeUri": "{{ result('foreach_output_130').uri }}"
                }
            }
        )

        put_time_off_type_assignments_for_user_137 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_137',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('create_user_115')['uri'],
                    "timeOffTypeUri": rail.result('foreach_output_130')['uri']
                },
                "policySetScheduleEntries": json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_133'))
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
            }
        )

        foreach_output_130_end = rail.EmptyOperator(
            task_id='foreach_output_130_end',
        )

        if_request_worker_status_equals_to_active_139 = rail.IfOperator(
            task_id='if_request_worker_status_equals_to_active_139',
            test='''{{ dag_run.conf.Worker_Status == 'Active' }}''',
            yes_task="put_user_notification_preferences_assigntimesheetandusernotifications_140",
            no_task="put_user_notification_preferences_removenotifications_142",
        )

        put_user_notification_preferences_assigntimesheetandusernotifications_140 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_assigntimesheetandusernotifications_140',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ result('create_user_115').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-off",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:holiday",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:project",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
            }
        )

        put_user_notification_preferences_removenotifications_142 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_removenotifications_142',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ result('create_user_115').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:project",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-off",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:holiday",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
            }
        )

        if_request_supervisor_present_143 = rail.IfOperator(
            task_id='if_request_supervisor_present_143',
            test='''{{ dag_run.conf.Supervisor | is_truthy  and dag_run.conf.Supervisor != 'N/A' }}''',
            yes_task="if_request_supervisor_equals_to_dataworkato_service3cd9c331requestemployeeid_144",
            no_task="insert_to_list_170",
        )

        if_request_supervisor_equals_to_dataworkato_service3cd9c331requestemployeeid_144 = rail.IfOperator(
            task_id='if_request_supervisor_equals_to_dataworkato_service3cd9c331requestemployeeid_144',
            test='''{{ dag_run.conf.Supervisor == dag_run.conf.employeeid }}''',
            yes_task="insert_to_list_145",
            no_task="get_data_supervisor_148",
        )

        insert_to_list_145 = rail.SetVariableOperator(
            task_id='insert_to_list_145',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Supervisor not assigned as User and Supervisor's employee id are the same."
            }
        )

        get_data_supervisor_148 = rail.RepliconServiceOperator(
            task_id='get_data_supervisor_148',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
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
                            "text": "{{ dag_run.conf.Supervisor }}",
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
        )

        invoke_custom_ruby_code_149 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_149',
            python_callable=lambda: list(filter(lambda x: x['employeeid'] == rail.get_dag_run_conf()['Supervisor'], map(lambda x: {
                "name": x['cells'][0]['textValue'],
                "loginname": x['cells'][1]['textValue'],
                "uri": x['cells'][0]['uri'],
                "employeeid": x['cells'][2]['textValue']
            }, rail.result('get_data_supervisor_148')['rows'])))
        )

        if_first_uri_present_150 = rail.IfOperator(
            task_id='if_first_uri_present_150',
            test='''{{ result('invoke_custom_ruby_code_149') | is_truthy }}''',
            yes_task="if_split_lengthnil_greater_than_1_151",
            no_task="queue_supervisor_assignment",
        )

        if_split_lengthnil_greater_than_1_151 = rail.IfOperator(
            task_id='if_split_lengthnil_greater_than_1_151',
            test='''{{ result('invoke_custom_ruby_code_149') | length >1 }}''',
            yes_task="insert_to_list_152",
            no_task="log_supervisorcheck_154",
        )

        insert_to_list_152 = rail.SetVariableOperator(
            task_id='insert_to_list_152',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Supervisor not assigned as there are multiple users with the ID '{{ dag_run.conf.Supervisor }}' in Replicon."
            }
        )

        log_supervisorcheck_154 = rail.PythonOperator(
            task_id='log_supervisorcheck_154',
            python_callable=lambda:  rail.result(
                'invoke_custom_ruby_code_149')[0]['uri']
        )

        if_log_supervisorcheck_154_present_155 = rail.IfOperator(
            task_id='if_log_supervisorcheck_154_present_155',
            test='''{{ result('log_supervisorcheck_154') | is_truthy }}''',
            yes_task="get_userdataforsupervisor_156",
            no_task="queue_supervisor_assignment",
        )

        get_userdataforsupervisor_156 = rail.RepliconServiceOperator(
            task_id='get_userdataforsupervisor_156',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('log_supervisorcheck_154') }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_userdetails_isenabled_is_true_157 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_157',
            test='''{{ result('get_userdataforsupervisor_156') | is_truthy and result('get_userdataforsupervisor_156')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="log_checkifsupervisorpermissionisassigned_158",
            no_task="insert_to_list_164",
        )

        log_checkifsupervisorpermissionisassigned_158 = rail.PythonOperator(
            task_id='log_checkifsupervisorpermissionisassigned_158',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_userdataforsupervisor_156')[0]['permissionSets'], 'displayText', "SUPERVISOR")
        )

        if_log_checkifsupervisorpermissionisassigned_158_blank_159 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionisassigned_158_blank_159',
            test='''{{ result('log_checkifsupervisorpermissionisassigned_158') | is_falsy and result('get_userdataforsupervisor_156')[0].userDetails.customFieldValues | find_first_by_attr_and_get_attr("customField.displayText",'Manager',"text")  | matches('Yes')  }}''',
            yes_task="assign_supervsior_permission_set_to_user_manager_160",
            no_task="assigninitialsupervisor_162",
        )

        assign_supervsior_permission_set_to_user_manager_160 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_160',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_userdataforsupervisor_156')[0].userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_supervsior_permission_set_to_user_manager_161 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_161',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_userdataforsupervisor_156')[0].userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.teammanagerpermissionuri }}"
            }
        )

        assigninitialsupervisor_162 = rail.RepliconServiceOperator(
            task_id='assigninitialsupervisor_162',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_115').uri }}",
                "supervisorUri": "{{ result('get_userdataforsupervisor_156')[0].userDetails.uri }}",
                "dateRange": null
            }
        )

        insert_to_list_164 = rail.SetVariableOperator(
            task_id='insert_to_list_164',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Supervisor not assigned as the the Initial Supervisor '{{ dag_run.conf.Supervisor }}' is in disabled status."
            }
        )

        queue_supervisor_assignment = rail.PythonOperator(
            task_id='queue_supervisor_assignment',
            python_callable=lambda: {
                "userloginname": rail.get_dag_run_conf()['User_Name'],
                "username": f"{rail.get_dag_run_conf()['firstname']} {rail.get_dag_run_conf()['lastname']}",
                "supervisorempid": rail.get_dag_run_conf()['Supervisor'],
                "employeeid": rail.get_dag_run_conf()['employeeid'],
                "useruri": rail.result('create_user_115')['uri'],
                "action": "Add",
                "effectivedate": {
                    "day": datetime.utcnow().day, "month": datetime.utcnow().month, "year": datetime.utcnow().year
                }
            }
        )

        insert_to_list_170 = rail.SetVariableOperator(
            task_id='insert_to_list_170',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "User created - Supervisor not assigned as the the Initial Supervisor was not present in the input file."
            }
        )

        if_request_manager_contains_yes_171 = rail.IfOperator(
            task_id='if_request_manager_contains_yes_171',
            test='''{{ dag_run.conf.manager | matches('Yes') }}''',
            yes_task="assign_supervsior_permission_set_to_user_manager_172",
            no_task="if_request_substitute_user_present_174",
        )

        assign_supervsior_permission_set_to_user_manager_172 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_172',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('create_user_115').uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_supervsior_permission_set_to_user_manager_173 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_173',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('create_user_115').uri }}",
                "permissionSetUri": "{{ dag_run.conf.teammanagerpermissionuri }}"
            }
        )

        if_request_substitute_user_present_174 = rail.IfOperator(
            task_id='if_request_substitute_user_present_174',
            test='''{{ dag_run.conf.Substitute_User | is_truthy  and dag_run.conf.Subs_User_StartDate | is_truthy  and dag_run.conf.Sub_User_EndDate | is_truthy  and dag_run.conf.Substitute_User != 'N/A' }}''',
            yes_task="get_datasubstituteuser_176",
            no_task="log_exceptions_198",
        )

        get_datasubstituteuser_176 = rail.RepliconServiceOperator(
            task_id='get_datasubstituteuser_176',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
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
                            "text": "{{ dag_run.conf.Substitute_User }}",
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
        )

        invoke_custom_ruby_code_177 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_177',
            python_callable=lambda: list(filter(lambda x: x['employeeid'] == rail.get_dag_run_conf()['Substitute_User'], map(lambda x: {
                "name": x['cells'][0]['textValue'],
                "loginname": x['cells'][1]['textValue'],
                "uri": x['cells'][0]['uri'],
                "employeeid": x['cells'][2]['textValue'],
            }, rail.result('get_datasubstituteuser_176')['rows'])))
        )

        if_first_uri_present_178 = rail.IfOperator(
            task_id='if_first_uri_present_178',
            test='''{{ result('invoke_custom_ruby_code_177') | is_truthy }}''',
            yes_task="if_split_lengthnil_greater_than_1_179",
            no_task="log_exceptions_198",
        )

        if_split_lengthnil_greater_than_1_179 = rail.IfOperator(
            task_id='if_split_lengthnil_greater_than_1_179',
            test='''{{ result('invoke_custom_ruby_code_177') | length > 1 }}''',
            yes_task="insert_to_list_180",
            no_task="log_substituteusercheck_182",
        )

        insert_to_list_180 = rail.SetVariableOperator(
            task_id='insert_to_list_180',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Subsititute user not assigned as there are multiple users with the ID '{{ dag_run.conf.Substitute_User }}' in Replicon."
            }
        )

        log_substituteusercheck_182 = rail.PythonOperator(
            task_id='log_substituteusercheck_182',
            python_callable=lambda:  rail.result(
                'invoke_custom_ruby_code_177')[0]['uri']
        )

        if_log_supervisorcheck_154_present_183 = rail.IfOperator(
            task_id='if_log_supervisorcheck_154_present_183',
            test='''{{ result('log_supervisorcheck_154') | is_truthy }}''',
            yes_task="impersonate_and_create_interactive_session_184",
            no_task="insert_to_list_196",
        )

        def map_impersonate_and_create_interactive_session(res):
            data = res.json()['d']
            auth_token = list(
                filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
            tenant = list(
                filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
            return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

        impersonate_and_create_interactive_session_184 = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session_184',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ result('create_user_115').uri }}"
            },
            response_filter=map_impersonate_and_create_interactive_session
        )

        log_authtoken_185 = rail.PythonOperator(
            task_id='log_authtoken_185',
            python_callable=lambda:  rail.result(
                'impersonate_and_create_interactive_session_184')
        )

        create_new_draft_187 = rail.RepliconServiceOperator(
            task_id='create_new_draft_187',
            endpoint='/services/SubstituteUserAssignmentService1.svc/CreateNewDraft',
            data={
                    "userUri": "{{ result('create_user_115').uri }}"
            },
            headers=lambda: rail.result('log_authtoken_185'),
        )

        update_substitute_user_188 = rail.RepliconServiceOperator(
            task_id='update_substitute_user_188',
            endpoint='/services/SubstituteUserAssignmentService1.svc/UpdateSubstituteUser',
            data={
                    "substituteUserAssignmentUri": "{{ result('create_user_115').uri }}",
                    "substituteUser": {
                        "uri": "{{ result('log_substituteusercheck_182') }}",
                        "loginName": null,
                        "parameterCorrelationId": "{{ dag_run_ecid() + result('create_user_115').uri}}"
                    }
            },
            headers=lambda: rail.result('log_authtoken_185'),
        )

        date_split_subsititutestartdate_189 = rail.PythonOperator(
            task_id='date_split_subsititutestartdate_189',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Subs_User_StartDate'])
        )

        date_split_subsitituteenddate_190 = rail.PythonOperator(
            task_id='date_split_subsitituteenddate_190',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Sub_User_EndDate'])
        )

        update_date_range_191 = rail.RepliconServiceOperator(
            task_id='update_date_range_191',
            endpoint='/services/SubstituteUserAssignmentService1.svc/UpdateDateRange',
            data={
                    "substituteUserAssignmentUri": "{{ result('create_new_draft_187') }}",
                    "dateRange": {
                        "startDate": {
                            "year": "{{result('date_split_subsititutestartdate_189').year}}",
                            "month": "{{result('date_split_subsititutestartdate_189').month}}",
                            "day": "{{result('date_split_subsititutestartdate_189').day}}"
                        },
                        "endDate": {
                            "year": "{{result('date_split_subsitituteenddate_190').year}}",
                            "month": "{{result('date_split_subsitituteenddate_190').month}}",
                            "day": "{{result('date_split_subsitituteenddate_190').day}}"
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    }
            },
            headers=lambda: rail.result('log_authtoken_185'),
        )

        put_access_levels_192 = rail.RepliconServiceOperator(
            task_id='put_access_levels_192',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PutAccessLevels",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_187') }}",
                "accessLevelUris": [
                    "urn:replicon:substitute-user-access-level:full-access"
                ]
            },
            headers=lambda: rail.result('log_authtoken_185'),
        )

        update_is_notification_forwarding_enabled_193 = rail.RepliconServiceOperator(
            task_id='update_is_notification_forwarding_enabled_193',
            endpoint="/services/SubstituteUserAssignmentService1.svc/UpdateIsNotificationForwardingEnabled",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_187') }}",
                "isEnabled": "1"
            },
            headers=lambda: rail.result('log_authtoken_185'),
        )

        publish_draft_194 = rail.RepliconServiceOperator(
            task_id='publish_draft_194',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft_187') }}"
            },
            headers=lambda: rail.result('log_authtoken_185'),
        )

        insert_to_list_196 = rail.SetVariableOperator(
            task_id='insert_to_list_196',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Subsititute user not assigned as the required user with the ID '{{ dag_run.conf.Substitute_User }}' is not available in Replicon."
            }
        )

        log_exceptions_198 = rail.PythonOperator(
            task_id='log_exceptions_198',
            python_callable=lambda:  "|".join(list(map(lambda x: x['log'], rail.get_dag_run_var(rail.result('declare_list_8')[
                                              'name'])))) if rail.get_dag_run_var(rail.result('declare_list_8')['name']) else null
        )

        horizonmedia_user_import_logs_add_entry_199 = rail.WriteLogOperator(
            task_id='horizonmedia_user_import_logs_add_entry_199',
            log="{{ result('create_log') }}",
            message="na",
            severity='''{{ "Exception" if result('log_exceptions_198') | is_truthy  else  "Success" }}''',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Add",
                "status": '''{{ "Exception" if result('log_exceptions_198') | is_truthy  else  "Success" }}''',
                "details": '''{{ "User created partially - " + result('log_exceptions_198') if result('log_exceptions_198') | is_truthy else "User created successfully"}}''',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Add",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_log
        create_log >> search_users_3 >> if_pluckuri_smart_joinnil_present_4
        if_pluckuri_smart_joinnil_present_4 >> rail.Label(
            'Yes') >> horizonmedia_user_import_logs_add_entry_5 >> stop_6 >> finish
        if_pluckuri_smart_joinnil_present_4 >> rail.Label(
            'No') >> declare_list_8 >> date_split_start_date_9 >> declare_variable_10 >> get_all_permission_sets_11 >> declare_list_12 >> declare_list_13 >> if_request_timesheettemplate_present_14
        if_request_timesheettemplate_present_14 >> rail.Label(
            'Yes') >> insert_to_list_15 >> insert_to_list_timeofftemplate_18
        if_request_timesheettemplate_present_14 >> rail.Label(
            'No') >> insert_to_list_17 >> insert_to_list_timeofftemplate_18
        insert_to_list_timeofftemplate_18 >> log_policy_settoassign_19 >> declare_variable_20 >> if_request_sup_org_code_present_21
        if_request_sup_org_code_present_21 >> rail.Label(
            'Yes') >> update_variable_22 >> declare_variable_23
        if_request_sup_org_code_present_21 >> rail.Label(
            'No') >> declare_variable_23 >> if_request_employeetypeuri_present_24
        if_request_employeetypeuri_present_24 >> rail.Label(
            'Yes') >> update_variable_25 >> declare_variable_26
        if_request_employeetypeuri_present_24 >> rail.Label(
            'No') >> declare_variable_26 >> if_request_job_profile_code_present_27
        if_request_job_profile_code_present_27 >> rail.Label(
            'Yes') >> update_variable_28 >> declare_variable_29
        if_request_job_profile_code_present_27 >> rail.Label(
            'No') >> declare_variable_29 >> if_request_flsa_present_30
        if_request_flsa_present_30 >> rail.Label(
            'Yes') >> update_variable_31 >> declare_variable_32
        if_request_flsa_present_30 >> rail.Label(
            'No') >> declare_variable_32 >> if_request_location_code_present_33
        if_request_location_code_present_33 >> rail.Label(
            'Yes') >> update_variable_34 >> declare_variable_35
        if_request_location_code_present_33 >> rail.Label(
            'No') >> declare_variable_35 >> if_request_jobpositiontagcode_present_36
        if_request_jobpositiontagcode_present_36 >> rail.Label(
            'Yes') >> update_variable_37 >> declare_variable_38
        if_request_jobpositiontagcode_present_36 >> rail.Label(
            'No') >> declare_variable_38 >> if_request_payrule_present_39
        if_request_payrule_present_39 >> rail.Label(
            'Yes') >> update_variable_40 >> declare_variable_41
        if_request_payrule_present_39 >> rail.Label(
            'No') >> declare_variable_41 >> if_request_timezone_present_42
        if_request_timezone_present_42 >> rail.Label(
            'Yes') >> update_variable_43 >> declare_list_44
        if_request_timezone_present_42 >> rail.Label(
            'No') >> declare_list_44 >> if_request_position_id_present_45
        if_request_position_id_present_45 >> rail.Label(
            'Yes') >> insert_to_list_46 >> if_request_businesstitle_present_47
        if_request_position_id_present_45 >> rail.Label(
            'No') >> if_request_businesstitle_present_47
        if_request_businesstitle_present_47 >> rail.Label(
            'Yes') >> insert_to_list_48 >> if_request_cost_center_code_present_49
        if_request_businesstitle_present_47 >> rail.Label(
            'No') >> if_request_cost_center_code_present_49
        if_request_cost_center_code_present_49 >> rail.Label(
            'Yes') >> insert_to_list_50 >> if_request_profit_center_code_present_51
        if_request_cost_center_code_present_49 >> rail.Label(
            'No') >> if_request_profit_center_code_present_51
        if_request_profit_center_code_present_51 >> rail.Label(
            'Yes') >> insert_to_list_52 >> if_request_department_code_present_53
        if_request_profit_center_code_present_51 >> rail.Label(
            'No') >> if_request_department_code_present_53
        if_request_department_code_present_53 >> rail.Label(
            'Yes') >> insert_to_list_54 >> if_request_legal_name_present_55
        if_request_department_code_present_53 >> rail.Label(
            'No') >> if_request_legal_name_present_55
        if_request_legal_name_present_55 >> rail.Label(
            'Yes') >> insert_to_list_56 >> if_request_company_code_present_57
        if_request_legal_name_present_55 >> rail.Label(
            'No') >> if_request_company_code_present_57
        if_request_company_code_present_57 >> rail.Label(
            'Yes') >> insert_to_list_58 >> if_request_pref_name_present_59
        if_request_company_code_present_57 >> rail.Label(
            'No') >> if_request_pref_name_present_59
        if_request_pref_name_present_59 >> rail.Label(
            'Yes') >> insert_to_list_60 >> if_request_mgmt_code_present_61
        if_request_pref_name_present_59 >> rail.Label(
            'No') >> if_request_mgmt_code_present_61
        if_request_mgmt_code_present_61 >> rail.Label(
            'Yes') >> insert_to_list_62 >> if_request_work_space_present_63
        if_request_mgmt_code_present_61 >> rail.Label(
            'No') >> if_request_work_space_present_63
        if_request_work_space_present_63 >> rail.Label(
            'Yes') >> insert_to_list_64 >> if_request_workspace_optionuri_present_65
        if_request_work_space_present_63 >> rail.Label(
            'No') >> if_request_workspace_optionuri_present_65
        if_request_workspace_optionuri_present_65 >> rail.Label(
            'Yes') >> insert_to_list_66 >> if_request_costcenter_optionuri_present_67
        if_request_workspace_optionuri_present_65 >> rail.Label(
            'No') >> if_request_costcenter_optionuri_present_67
        if_request_costcenter_optionuri_present_67 >> rail.Label(
            'Yes') >> insert_to_list_68 >> if_request_department_optionuri_present_69
        if_request_costcenter_optionuri_present_67 >> rail.Label(
            'No') >> if_request_department_optionuri_present_69
        if_request_department_optionuri_present_69 >> rail.Label(
            'Yes') >> insert_to_list_70 >> if_request_profitcenter_optionuri_present_71
        if_request_department_optionuri_present_69 >> rail.Label(
            'No') >> if_request_profitcenter_optionuri_present_71
        if_request_profitcenter_optionuri_present_71 >> rail.Label(
            'Yes') >> insert_to_list_72 >> if_request_company_optionuri_present_73
        if_request_profitcenter_optionuri_present_71 >> rail.Label(
            'No') >> if_request_company_optionuri_present_73
        if_request_company_optionuri_present_73 >> rail.Label(
            'Yes') >> insert_to_list_74 >> if_request_managementlevel_optionuri_present_75
        if_request_company_optionuri_present_73 >> rail.Label(
            'No') >> if_request_managementlevel_optionuri_present_75
        if_request_managementlevel_optionuri_present_75 >> rail.Label(
            'Yes') >> insert_to_list_76 >> if_request_company_optionuri_present_77
        if_request_managementlevel_optionuri_present_75 >> rail.Label(
            'No') >> if_request_company_optionuri_present_77
        if_request_company_optionuri_present_77 >> rail.Label(
            'Yes') >> insert_to_list_78 >> if_request_employeeresidence_optionuri_present_79
        if_request_company_optionuri_present_77 >> rail.Label(
            'No') >> if_request_employeeresidence_optionuri_present_79
        if_request_employeeresidence_optionuri_present_79 >> rail.Label(
            'Yes') >> insert_to_list_80 >> if_request_ceo_optionuri_present_81
        if_request_employeeresidence_optionuri_present_79 >> rail.Label(
            'No') >> if_request_ceo_optionuri_present_81
        if_request_ceo_optionuri_present_81 >> rail.Label(
            'Yes') >> insert_to_list_82 >> if_request_ceo1_optionuri_present_83
        if_request_ceo_optionuri_present_81 >> rail.Label(
            'No') >> if_request_ceo1_optionuri_present_83
        if_request_ceo1_optionuri_present_83 >> rail.Label(
            'Yes') >> insert_to_list_84 >> if_request_ceo2_optionuri_present_85
        if_request_ceo1_optionuri_present_83 >> rail.Label(
            'No') >> if_request_ceo2_optionuri_present_85
        if_request_ceo2_optionuri_present_85 >> rail.Label(
            'Yes') >> insert_to_list_86 >> if_request_ceo3_optionuri_present_87
        if_request_ceo2_optionuri_present_85 >> rail.Label(
            'No') >> if_request_ceo3_optionuri_present_87
        if_request_ceo3_optionuri_present_87 >> rail.Label(
            'Yes') >> insert_to_list_88 >> if_request_ceo4_optionuri_present_89
        if_request_ceo3_optionuri_present_87 >> rail.Label(
            'No') >> if_request_ceo4_optionuri_present_89
        if_request_ceo4_optionuri_present_89 >> rail.Label(
            'Yes') >> insert_to_list_90 >> if_request_ceo5_optionuri_present_91
        if_request_ceo4_optionuri_present_89 >> rail.Label(
            'No') >> if_request_ceo5_optionuri_present_91
        if_request_ceo5_optionuri_present_91 >> rail.Label(
            'Yes') >> insert_to_list_92 >> if_request_ceo6_optionuri_present_93
        if_request_ceo5_optionuri_present_91 >> rail.Label(
            'No') >> if_request_ceo6_optionuri_present_93
        if_request_ceo6_optionuri_present_93 >> rail.Label(
            'Yes') >> insert_to_list_94 >> if_request_groupleader_optionuri_present_95
        if_request_ceo6_optionuri_present_93 >> rail.Label(
            'No') >> if_request_groupleader_optionuri_present_95
        if_request_groupleader_optionuri_present_95 >> rail.Label(
            'Yes') >> insert_to_list_96 >> if_request_businesleader_optionuri_present_97
        if_request_groupleader_optionuri_present_95 >> rail.Label(
            'No') >> if_request_businesleader_optionuri_present_97
        if_request_businesleader_optionuri_present_97 >> rail.Label(
            'Yes') >> insert_to_list_98 >> if_request_contingentworkertype_optionuri_present_99
        if_request_businesleader_optionuri_present_97 >> rail.Label(
            'No') >> if_request_contingentworkertype_optionuri_present_99
        if_request_contingentworkertype_optionuri_present_99 >> rail.Label(
            'Yes') >> insert_to_list_100 >> if_request_workerstatus_optionuri_present_101
        if_request_contingentworkertype_optionuri_present_99 >> rail.Label(
            'No') >> if_request_workerstatus_optionuri_present_101
        if_request_workerstatus_optionuri_present_101 >> rail.Label(
            'Yes') >> insert_to_list_102 >> if_request_country_optionuri_present_103
        if_request_workerstatus_optionuri_present_101 >> rail.Label(
            'No') >> if_request_country_optionuri_present_103
        if_request_country_optionuri_present_103 >> rail.Label(
            'Yes') >> insert_to_list_104 >> if_request_firstdayofleave_present_105
        if_request_country_optionuri_present_103 >> rail.Label(
            'No') >> if_request_firstdayofleave_present_105
        if_request_firstdayofleave_present_105 >> rail.Label(
            'Yes') >> date_split_106 >> insert_to_list_107 >> if_request_actuallastdayofleave_present_108
        if_request_firstdayofleave_present_105 >> rail.Label(
            'No') >> if_request_actuallastdayofleave_present_108
        if_request_actuallastdayofleave_present_108 >> rail.Label(
            'Yes') >> date_split_109 >> insert_to_list_110 >> if_request_manager_optionuri_present_111
        if_request_actuallastdayofleave_present_108 >> rail.Label(
            'No') >> if_request_manager_optionuri_present_111
        if_request_manager_optionuri_present_111 >> rail.Label(
            'Yes') >> insert_to_list_112 >> log_customfield_values_113
        if_request_manager_optionuri_present_111 >> rail.Label(
            'No') >> log_customfield_values_113 >> log_displayname_114 >> create_user_115 >> is_holidaycalendar_uri_present >> rail.Label(
            "Yes") >> update_holiday_calendar >> if_request_scheduledweeklyhours_present_116
        is_holidaycalendar_uri_present >> rail.Label(
            "No") >> if_request_scheduledweeklyhours_present_116
        if_request_scheduledweeklyhours_present_116 >> rail.Label(
            'Yes') >> update_text_value_schedulehours_118 >> if_request_payrollid_present_119
        if_request_scheduledweeklyhours_present_116 >> rail.Label(
            'No') >> if_request_payrollid_present_119
        if_request_payrollid_present_119 >> rail.Label(
            'Yes') >> update_text_value_payrollid_121 >> if_request_activities_present_122
        if_request_payrollid_present_119 >> rail.Label(
            'No') >> if_request_activities_present_122
        if_request_activities_present_122 >> rail.Label(
            'Yes') >> update_activity_assignments_for_user_123 >> horizonmedia_user_import_master_mapper_search_entries_124
        if_request_activities_present_122 >> rail.Label(
            'No') >> horizonmedia_user_import_master_mapper_search_entries_124 >> get_enabled_time_off_types_125 >> if_first_uri_present_126
        if_first_uri_present_126 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_127 >> log_timeofftype_uris_128 >> put_time_off_type_assignments_for_user_129 >> foreach_output_130
        if_first_uri_present_126 >> rail.Label(
            'No') >> foreach_output_130 >> if_foreach_output_130_uri_present_131
        if_foreach_output_130_uri_present_131 >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user_133 >> put_time_off_type_assignments_for_user_137 >> foreach_output_130_end
        if_foreach_output_130_uri_present_131 >> rail.Label(
            'No') >> foreach_output_130_end
        foreach_output_130 >> foreach_output_130_end >> if_request_worker_status_equals_to_active_139
        if_request_worker_status_equals_to_active_139 >> rail.Label(
            'Yes') >> put_user_notification_preferences_assigntimesheetandusernotifications_140 >> if_request_supervisor_present_143
        if_request_worker_status_equals_to_active_139 >> rail.Label(
            'No') >> put_user_notification_preferences_removenotifications_142 >> if_request_supervisor_present_143
        if_request_supervisor_present_143 >> rail.Label(
            'Yes') >> if_request_supervisor_equals_to_dataworkato_service3cd9c331requestemployeeid_144
        if_request_supervisor_equals_to_dataworkato_service3cd9c331requestemployeeid_144 >> rail.Label(
            'Yes') >> insert_to_list_145 >> if_request_manager_contains_yes_171
        if_first_uri_present_150 >> rail.Label(
            'Yes') >> if_split_lengthnil_greater_than_1_151
        if_split_lengthnil_greater_than_1_151 >> rail.Label(
            'Yes') >> insert_to_list_152 >> if_request_manager_contains_yes_171
        if_log_supervisorcheck_154_present_155 >> rail.Label(
            'Yes') >> get_userdataforsupervisor_156 >> if_userdetails_isenabled_is_true_157
        if_userdetails_isenabled_is_true_157 >> rail.Label(
            'Yes') >> log_checkifsupervisorpermissionisassigned_158 >> if_log_checkifsupervisorpermissionisassigned_158_blank_159
        if_log_checkifsupervisorpermissionisassigned_158_blank_159 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_manager_160 >> assign_supervsior_permission_set_to_user_manager_161 >> assigninitialsupervisor_162
        if_log_checkifsupervisorpermissionisassigned_158_blank_159 >> rail.Label(
            'No') >> assigninitialsupervisor_162 >> if_request_manager_contains_yes_171

        if_userdetails_isenabled_is_true_157 >> rail.Label(
            'No') >> insert_to_list_164 >> if_request_manager_contains_yes_171
        if_log_supervisorcheck_154_present_155 >> rail.Label(
            'No') >> queue_supervisor_assignment >> if_request_manager_contains_yes_171
        if_split_lengthnil_greater_than_1_151 >> rail.Label(
            'No') >> log_supervisorcheck_154 >> if_log_supervisorcheck_154_present_155
        if_first_uri_present_150 >> rail.Label(
            'No') >> queue_supervisor_assignment >> if_request_manager_contains_yes_171
        if_request_supervisor_equals_to_dataworkato_service3cd9c331requestemployeeid_144 >> rail.Label(
            'No') >> get_data_supervisor_148 >> invoke_custom_ruby_code_149 >> if_first_uri_present_150
        if_request_supervisor_present_143 >> rail.Label(
            'No') >> insert_to_list_170 >> if_request_manager_contains_yes_171
        if_request_manager_contains_yes_171 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_manager_172 >> assign_supervsior_permission_set_to_user_manager_173 >> if_request_substitute_user_present_174
        if_request_manager_contains_yes_171 >> rail.Label(
            'No') >> if_request_substitute_user_present_174
        if_request_substitute_user_present_174 >> rail.Label(
            'Yes') >> get_datasubstituteuser_176 >> invoke_custom_ruby_code_177 >> if_first_uri_present_178
        if_first_uri_present_178 >> rail.Label(
            'Yes') >> if_split_lengthnil_greater_than_1_179
        if_split_lengthnil_greater_than_1_179 >> rail.Label(
            'Yes') >> insert_to_list_180 >> log_exceptions_198
        if_log_supervisorcheck_154_present_183 >> rail.Label(
            'Yes') >> impersonate_and_create_interactive_session_184 >> log_authtoken_185 >> create_new_draft_187 >> update_substitute_user_188 >> date_split_subsititutestartdate_189 >> date_split_subsitituteenddate_190 >> update_date_range_191 >> put_access_levels_192 >> update_is_notification_forwarding_enabled_193 >> publish_draft_194 >> log_exceptions_198
        if_log_supervisorcheck_154_present_183 >> rail.Label(
            'No') >> insert_to_list_196 >> log_exceptions_198
        if_split_lengthnil_greater_than_1_179 >> rail.Label(
            'No') >> log_substituteusercheck_182 >> if_log_supervisorcheck_154_present_183
        if_first_uri_present_178 >> rail.Label('No') >> log_exceptions_198
        if_request_substitute_user_present_174 >> rail.Label(
            'No') >> log_exceptions_198
        log_exceptions_198 >> horizonmedia_user_import_logs_add_entry_199 >> finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
