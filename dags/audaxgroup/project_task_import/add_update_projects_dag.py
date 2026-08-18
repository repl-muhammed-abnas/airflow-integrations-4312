
from datetime import timedelta, datetime
import uuid
from airflow.models import Variable
import rail


null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_update_projects_dag_id,
        description=f'Audaxgroup add update projects {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_add_update_projects_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_add_update_projects_child_logs',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_add_update_projects_child_logs = rail.CreateLogOperator(
            task_id='create_add_update_projects_child_logs'
        )

        log_timenow_2=rail.PythonOperator(
            task_id='log_timenow_2',
            python_callable= lambda:  datetime.now().strftime('%m-%d-%Y %H:%M')
        )

        declare_list_4=rail.SetVariableOperator(
            task_id='declare_list_4',
            append=False,
            name='resourceassignment_logs',
            value=[]
        )

        if_request_timeentrystartdate_present_5=rail.IfOperator(
            task_id='if_request_timeentrystartdate_present_5',
            test='''{{ dag_run.conf.timeentrystartdate | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_6",
            no_task="if_request_timeentryenddate_present_9",
        )

        def get_date_object(date_string):
            if not date_string or not date_string.strip():
                return None
            try:
                dateobj = datetime.strptime(date_string.strip(),'%m/%d/%Y')
                return {
                  "day": dateobj.day,
                  "month": dateobj.month,
                  "year": dateobj.year
                }
            except ValueError as e:
                raise ValueError(f"Invalid date format '{date_string}'. Expected MM/DD/YYYY (e.g., 10/13/2025)")

        invoke_custom_ruby_code_6=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_6',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['timeentrystartdate']) if dag_run.conf['timeentrystartdate'] else None
        )

        if_request_timeentryenddate_present_9=rail.IfOperator(
            task_id='if_request_timeentryenddate_present_9',
            test='''{{ dag_run.conf.timeentryenddate | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_10",
            no_task="if_request_type_contains_add_13",
        )


        invoke_custom_ruby_code_10=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_10',
            python_callable= lambda dag_run: get_date_object(dag_run.conf['timeentryenddate']) if dag_run.conf['timeentryenddate'] else None
        )

        if_request_type_contains_add_13=rail.IfOperator(
            task_id='if_request_type_contains_add_13',
            test='''{{ dag_run.conf.type | matches('add') }}''',
            yes_task="if_request_projectleaderloginname_present_14",
            no_task="if_request_type_contains_update_64",
        )

        if_request_projectleaderloginname_present_14=rail.IfOperator(
            task_id='if_request_projectleaderloginname_present_14',
            test='''{{ dag_run.conf.projectleaderloginname | is_truthy }}''',
            yes_task="list_project_leaders_15",
            no_task="create_project_19",
        )

        list_project_leaders_15=rail.RepliconServiceOperator(
            task_id='list_project_leaders_15',
            endpoint="/services/ProjectService1.svc/GetEligibleProjectLeaders",
        )

        log_project_manager_uri_16=rail.PythonOperator(
            task_id='log_project_manager_uri_16',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('list_project_leaders_15'), 'user.loginName', dag_run.conf['projectleaderloginname'], 'uri', None) if rail.result('list_project_leaders_15') else None
        )

        if_log_project_manager_uri_16_blank_17=rail.IfOperator(
            task_id='if_log_project_manager_uri_16_blank_17',
            test='''{{ result('log_project_manager_uri_16') | is_falsy }}''',
            yes_task="log_exception_18",
            no_task="create_project_19",
        )

        log_exception_18=rail.PythonOperator(
            task_id='log_exception_18',
            python_callable= lambda:  rail.render_template("Project Manager not found or does not have Project Manager permissions '{{ dag_run.conf.projectleaderloginname }}'")
        )

        create_project_19 = rail.RepliconServiceOperator(
            task_id='create_project_19',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": dag_run.conf['projectname'],
                    },
                    "descriptionToApply": {
                        "value":  dag_run.conf['projectdescription'][:256] if dag_run.conf['projectdescription'] else None
                    },
                    "percentCompletedToApply": null,
                    "startDateToApply": {
                        "date": {
                            "year": datetime.strptime(dag_run.conf['timeentrystartdate'], "%m/%d/%Y").year,
                            "month": datetime.strptime(dag_run.conf['timeentrystartdate'], "%m/%d/%Y").month,
                            "day": datetime.strptime(dag_run.conf['timeentrystartdate'], "%m/%d/%Y").day
                        } if dag_run.conf['timeentrystartdate'] else null
                    },
                    "endDateToApply": {
                        "date": {
                            "year": datetime.strptime(dag_run.conf['timeentryenddate'], "%m/%d/%Y").year,
                            "month": datetime.strptime(dag_run.conf['timeentryenddate'], "%m/%d/%Y").month,
                            "day": datetime.strptime(dag_run.conf['timeentryenddate'], "%m/%d/%Y").day
                        } if dag_run.conf['timeentryenddate'] else null
                    },
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:time-and-material"
                    },
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": null,
                    "statusToApply":  {
                            "name": 'In Progress'
                        }if dag_run.conf['projectstatus'] and "rogress"  in dag_run.conf['projectstatus'] else null,
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": {
                            "user": {
                                "uri": rail.result('log_project_manager_uri_16'),
                                "loginName":null,
                                "employeeId": null,
                                "parameterCorrelationId": null
                        },
                    } if rail.result('log_project_manager_uri_16') else null,
                    "isProjectLeaderApprovalRequired": null,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "budgetedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "budgetedCostToApply": null,
                    "expenseBudgetedCostToApply": null,
                    "totalEstimatedContractValueToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                    },
                    "billingContractToApply": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "resourceProjectAssignmentModifications": null,
                    "billingContractModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [],
                    "portfolioToApply": null,
                    "locationToApply": null,
                    "divisionToApply": null,
                    "serviceCenterToApply": null,
                    "costCenterToApply": null,
                    "departmentGroupToApply": null,
                    "employeeTypeGroupToApply": null
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            })

        if_request_timeentryallowed_present_20=rail.IfOperator(
            task_id='if_request_timeentryallowed_present_20',
            test='''{{ dag_run.conf.timeentryallowed | is_truthy }}''',
            yes_task="if_timeentryallowed_downcase_contains_false_21",
            no_task="if_request_projectteamusers_present_25",
        )

        if_timeentryallowed_downcase_contains_false_21=rail.IfOperator(
            task_id='if_timeentryallowed_downcase_contains_false_21',
            test='''{{ dag_run.conf.timeentryallowed.lower() | matches('false') }}''',
            yes_task="update_allow_time_entry_against_tasks_only_22",
            no_task="if_timeentryallowed_downcase_contains_true_23",
        )

        update_allow_time_entry_against_tasks_only_22=rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_22',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
                "projectUri": "{{ result('create_project_19').uri }}",
                "allowTimeEntryAgainstTasksOnly": "true"
            }
        )

        if_timeentryallowed_downcase_contains_true_23=rail.IfOperator(
            task_id='if_timeentryallowed_downcase_contains_true_23',
            test='''{{ dag_run.conf.timeentryallowed.lower() | matches('true') }}''',
            yes_task="update_allow_time_entry_against_tasks_only_24",
            no_task="if_request_projectteamusers_present_25",
        )

        update_allow_time_entry_against_tasks_only_24=rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_24',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
                "projectUri": "{{ result('create_project_19').uri }}",
                "allowTimeEntryAgainstTasksOnly": "false"
            }
        )

        if_request_projectteamusers_present_25=rail.IfOperator(
            task_id='if_request_projectteamusers_present_25',
            test='''{{ dag_run.conf.projectteamusers | is_truthy }}''',
            yes_task="log_users_26",
            no_task="if_request_projectteamdepartments_present_36",
        )

        log_users_26=rail.PythonOperator(
            task_id='log_users_26',
            python_callable=lambda dag_run: "|".join(dag_run.conf['projectteamusers'].split("|")).split("|")
        )

        create_list_27=rail.PythonOperator(
            task_id='create_list_27',
            python_callable=lambda dag_run: dag_run.conf['projectteamusers'].split('|')
        )


        foreach_create_list_27_28=rail.ForEachOperator(
            task_id='foreach_create_list_27_28',
            items=lambda: rail.result('create_list_27'),
            start_task = 'log_loginname_29',
            end_task = 'foreach_create_list_27_28_end'
        )

        log_loginname_29=rail.PythonOperator(
            task_id='log_loginname_29',
            python_callable=lambda: rail.result('foreach_create_list_27_28')
        )

        search_users_30 = rail.RepliconServiceOperator(
            task_id='search_users_30',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: {
                    "users": [
                        {
                            "uri": null,
                            "loginName": rail.result('foreach_create_list_27_28'),
                            "employeeId": null,
                            "parameterCorrelationId": null
                        }
                    ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )

        log_resource_uri_31=rail.PythonOperator(
            task_id='log_resource_uri_31',
            python_callable= lambda: rail.result('search_users_30')[0]['userDetails']['uri'] if rail.result('search_users_30') else None
        )

        if_log_resource_uri_31_present_32=rail.IfOperator(
            task_id='if_log_resource_uri_31_present_32',
            test='''{{ result('log_resource_uri_31') | is_truthy }}''',
            yes_task="bulk_update_project_team_members_assignment_33",
            no_task="insert_to_list_35",
        )

        bulk_update_project_team_members_assignment_33=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment_33',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
            "projectUri": "{{ result('create_project_19').uri }}",
            "resourceUri": ["{{ result('log_resource_uri_31') }}"],
            "projectTeamMemberAssignmentOptionUri":"urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        insert_to_list_35=rail.SetVariableOperator(
            task_id='insert_to_list_35',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "resourceassignment_logs": "User not assigned to Project - User with login name '{{ result('log_loginname_29') }}' not found;"
            }
        )

        foreach_create_list_27_28_end=rail.EmptyOperator(
            task_id='foreach_create_list_27_28_end',
        )

        if_request_projectteamdepartments_present_36=rail.IfOperator(
            task_id='if_request_projectteamdepartments_present_36',
            test='''{{ dag_run.conf.projectteamdepartments | is_truthy }}''',
            yes_task="log_departments_37",
            no_task="if_request_programname_present_46",
        )


        log_departments_37=rail.PythonOperator(
            task_id='log_departments_37',
            python_callable= lambda dag_run:  "|".join(dag_run.conf['projectteamdepartments'].split("|")).split("|")
        )

        create_list_38=rail.PythonOperator(
            task_id='create_list_38',
            python_callable=lambda dag_run: dag_run.conf['projectteamdepartments'].split('|')
        )


        foreach_create_list_38_39=rail.ForEachOperator(
            task_id='foreach_create_list_38_39',
            items=lambda: rail.result('create_list_38'),
            start_task = 'log_departmentname_40',
            end_task = 'foreach_create_list_38_39_end'
        )


        log_departmentname_40=rail.PythonOperator(
            task_id='log_departmentname_40',
            python_callable= lambda: rail.result('foreach_create_list_38_39')
        )


        log_lookup_department_uri_41=rail.PythonOperator(
            task_id='log_lookup_department_uri_41',
            python_callable= lambda dag_run: next((obj["properties"]["uri"] for obj in rail.load_all_records(dag_run.conf['audax_users_and_departments_lookup_table']) if obj["properties"]["name"] == rail.result('foreach_create_list_38_39')), None)
        )


        if_log_lookup_department_uri_41_present_42=rail.IfOperator(
            task_id='if_log_lookup_department_uri_41_present_42',
            test='''{{ result('log_lookup_department_uri_41') | is_truthy }}''',
            yes_task="bulk_update_project_team_members_assignment_43",
            no_task="insert_to_list_45",
        )

        bulk_update_project_team_members_assignment_43=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment_43',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
            "projectUri":"{{ result('create_project_19').uri }}",
            "resourceUri": ["{{ result('log_lookup_department_uri_41') }}"],
            "projectTeamMemberAssignmentOptionUri":"urn:replicon:project-team-member-assignment-option:assign"
            }
        )


        insert_to_list_45=rail.SetVariableOperator(
            task_id='insert_to_list_45',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "resourceassignment_logs": "Department not assigned to Project - Department name '{{ result('log_departmentname_40') }}' not found;"
            }
        )

        foreach_create_list_38_39_end=rail.EmptyOperator(
            task_id='foreach_create_list_38_39_end',
        )


        if_request_programname_present_46=rail.IfOperator(
            task_id='if_request_programname_present_46',
            test='''{{ dag_run.conf.programname | is_truthy }}''',
            yes_task="if_request_program_optionuri_present_47",
            no_task="if_request_projectinfo1_present_51",
        )


        if_request_program_optionuri_present_47=rail.IfOperator(
            task_id='if_request_program_optionuri_present_47',
            test='''{{ dag_run.conf.Program_OptionURI | is_truthy }}''',
            yes_task="update_dropdown_value_48",
            no_task="log_exceptions_50",
        )


        update_dropdown_value_48=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_48',
            endpoint="/services/customfieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ result('create_project_19').uri }}",
            "customFieldUri": "{{ dag_run.conf.UDFProgramURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.Program_OptionURI }}"
            }
        )


        log_exceptions_50=rail.PythonOperator(
            task_id='log_exceptions_50',
            python_callable= lambda:  rail.render_template("Program not found - '{{ dag_run.conf.programname }}';")
        )


        if_request_projectinfo1_present_51=rail.IfOperator(
            task_id='if_request_projectinfo1_present_51',
            test='''{{ dag_run.conf.projectinfo1 | is_truthy }}''',
            yes_task="update_text_value_52",
            no_task="if_request_projectinfo3_present_53",
        )


        update_text_value_52=rail.RepliconServiceOperator(
            task_id='update_text_value_52',
            endpoint="/services/customfieldService1.svc/UpdateTextValue",
            data={
            "objectUri": "{{ result('create_project_19').uri }}",
            "customFieldUri": "{{ dag_run.conf.UDFDealOpportunityIDProjectURI }}",
            "value": "{{ dag_run.conf.projectinfo1 }}"
            }
        )

        if_request_projectinfo3_present_53=rail.IfOperator(
            task_id='if_request_projectinfo3_present_53',
            test='''{{ dag_run.conf.projectinfo3 | is_truthy }}''',
            yes_task="if_request_projectdepartment_optionuri_present_54",
            no_task="if_request_projectinfo2_present_58",
        )


        if_request_projectdepartment_optionuri_present_54=rail.IfOperator(
            task_id='if_request_projectdepartment_optionuri_present_54',
            test='''{{ dag_run.conf.ProjectDepartment_OptionURI | is_truthy }}''',
            yes_task="update_dropdown_value_55",
            no_task="log_exceptions_57",
        )


        update_dropdown_value_55=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_55',
            endpoint="/services/customfieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ result('create_project_19').uri }}",
            "customFieldUri": "{{ dag_run.conf.UDFProjectDepartmentURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.ProjectDepartment_OptionURI }}"
            }
        )


        log_exceptions_57=rail.PythonOperator(
            task_id='log_exceptions_57',
            python_callable= lambda:  rail.render_template("Project Department not found - '{{ dag_run.conf.projectinfo3 }}';")
        )


        if_request_projectinfo2_present_58=rail.IfOperator(
            task_id='if_request_projectinfo2_present_58',
            test='''{{ dag_run.conf.projectinfo2 | is_truthy }}''',
            yes_task="if_request_projectbusinessunit_optionuri_present_59",
            no_task="log_job_history_63",
        )


        if_request_projectbusinessunit_optionuri_present_59=rail.IfOperator(
            task_id='if_request_projectbusinessunit_optionuri_present_59',
            test='''{{ dag_run.conf.ProjectBusinessUnit_OptionURI | is_truthy }}''',
            yes_task="update_dropdown_value_60",
            no_task="log_exceptions_62",
        )


        update_dropdown_value_60=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_60',
            endpoint="/services/customfieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ result('create_project_19').uri }}",
            "customFieldUri": "{{ dag_run.conf.UDFProjectBusinessUnitURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.ProjectBusinessUnit_OptionURI }}"
            }
        )


        log_exceptions_62=rail.PythonOperator(
            task_id='log_exceptions_62',
            python_callable= lambda:  rail.render_template("Project Business Unit not found - '{{ dag_run.conf.projectinfo2 }}';")
        )


        log_job_history_63=rail.PythonOperator(
            task_id='log_job_history_63',
            python_callable= lambda:  "Added"
        )

        if_request_type_contains_update_64=rail.IfOperator(
            task_id='if_request_type_contains_update_64',
            test='''{{ dag_run.conf.type | matches('update') }}''',
            yes_task="bulk_get_project_details3_65",
            no_task="log_resource_assignmentexceptions_147",
        )


        bulk_get_project_details3_65=rail.RepliconServiceOperator(
            task_id='bulk_get_project_details3_65',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
            "projects": [
                {
                "uri": "{{ dag_run.conf.projecturi }}",
                "name": null,
                "parameterCorrelationId": null
                }
            ]
            }
        )


        log_udf_existing_program_dropdown_value_66=rail.PythonOperator(
            task_id='log_udf_existing_program_dropdown_value_66',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['customFields'], 'customField.displayText', 'Program', 'text', '')
        )

        if_request_programname_present_67=rail.IfOperator(
            task_id='if_request_programname_present_67',
            test='''{{ dag_run.conf.programname | is_truthy  and dag_run.conf.programname != result('log_udf_existing_program_dropdown_value_66') }}''',
            yes_task="if_request_program_optionuri_present_68",
            no_task="log_udf_existing_deal_opportunity_i_d_project_text_value_73",
        )


        if_request_program_optionuri_present_68=rail.IfOperator(
            task_id='if_request_program_optionuri_present_68',
            test='''{{ dag_run.conf.Program_OptionURI | is_truthy }}''',
            yes_task="update_dropdown_value_69",
            no_task="log_exceptions_72",
        )


        update_dropdown_value_69=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_69',
            endpoint="/services/customfieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ dag_run.conf.projecturi }}",
            "customFieldUri": "{{ dag_run.conf.UDFProgramURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.Program_OptionURI }}"
            }
        )


        log_logs_70=rail.PythonOperator(
            task_id='log_logs_70',
            python_callable= lambda:  rail.render_template("Program updated - '{{ dag_run.conf.programname }}';")
        )


        log_exceptions_72=rail.PythonOperator(
            task_id='log_exceptions_72',
            python_callable= lambda:  rail.render_template("Program not found - '{{ dag_run.conf.programname }}';")
        )


        log_udf_existing_deal_opportunity_i_d_project_text_value_73=rail.PythonOperator(
            task_id='log_udf_existing_deal_opportunity_i_d_project_text_value_73',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['customFields'], 'customField.displayText', 'Deal Opportunity ID (Project)', 'text', '')
        )


        if_request_projectinfo1_present_74=rail.IfOperator(
            task_id='if_request_projectinfo1_present_74',
            test='''{{ dag_run.conf.projectinfo1 | is_truthy  and dag_run.conf.projectinfo1 != result('log_udf_existing_deal_opportunity_i_d_project_text_value_73') }}''',
            yes_task="update_text_value_75",
            no_task="log_udf_existing_project_department_dropdown_value_77",
        )


        update_text_value_75=rail.RepliconServiceOperator(
            task_id='update_text_value_75',
            endpoint="/services/customfieldService1.svc/UpdateTextValue",
            data={
            "objectUri": "{{ dag_run.conf.projecturi }}",
            "customFieldUri": "{{ dag_run.conf.UDFDealOpportunityIDProjectURI }}",
            "value": "{{ dag_run.conf.projectinfo1 }}"
            }
        )


        log_logs_76=rail.PythonOperator(
            task_id='log_logs_76',
            python_callable= lambda:  rail.render_template("Deal Opportunity ID (Project) updated - '{{ dag_run.conf.projectinfo1 }}';")
        )


        log_udf_existing_project_department_dropdown_value_77=rail.PythonOperator(
            task_id='log_udf_existing_project_department_dropdown_value_77',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['customFields'], 'customField.displayText', 'Project Department', 'text', '')
        )


        if_request_projectinfo3_present_78=rail.IfOperator(
            task_id='if_request_projectinfo3_present_78',
            test='''{{ dag_run.conf.projectinfo3 | is_truthy  and dag_run.conf.projectinfo3 != result('log_udf_existing_project_department_dropdown_value_77') }}''',
            yes_task="if_request_projectdepartment_optionuri_present_79",
            no_task="log_udf_existing_project_business_unit_value_84",
        )


        if_request_projectdepartment_optionuri_present_79=rail.IfOperator(
            task_id='if_request_projectdepartment_optionuri_present_79',
            test='''{{ dag_run.conf.ProjectDepartment_OptionURI | is_truthy }}''',
            yes_task="update_dropdown_value_80",
            no_task="log_exceptions_83",
        )


        update_dropdown_value_80=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_80',
            endpoint="/services/customfieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ dag_run.conf.projecturi }}",
            "customFieldUri": "{{ dag_run.conf.UDFProjectDepartmentURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.ProjectDepartment_OptionURI }}"
            }
        )

        log_logs_81=rail.PythonOperator(
            task_id='log_logs_81',
            python_callable= lambda:  rail.render_template("Project Department updated - '{{ dag_run.conf.projectinfo3 }}';")
        )


        log_exceptions_83=rail.PythonOperator(
            task_id='log_exceptions_83',
            python_callable= lambda:  rail.render_template("Project Department not found - '{{ dag_run.conf.projectinfo3 }}';")
        )


        log_udf_existing_project_business_unit_value_84=rail.PythonOperator(
            task_id='log_udf_existing_project_business_unit_value_84',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['customFields'], 'customField.displayText', 'Project Business Unit', 'text', '')
        )


        if_request_projectinfo2_present_85=rail.IfOperator(
            task_id='if_request_projectinfo2_present_85',
            test='''{{ dag_run.conf.projectinfo2 | is_truthy  and dag_run.conf.projectinfo2 != result('log_udf_existing_project_business_unit_value_84') }}''',
            yes_task="if_request_projectbusinessunit_optionuri_present_86",
            no_task="if_request_projectleaderloginname_present_91",
        )


        if_request_projectbusinessunit_optionuri_present_86=rail.IfOperator(
            task_id='if_request_projectbusinessunit_optionuri_present_86',
            test='''{{ dag_run.conf.ProjectBusinessUnit_OptionURI | is_truthy }}''',
            yes_task="update_dropdown_value_87",
            no_task="log_exceptions_90",
        )


        update_dropdown_value_87=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_87',
            endpoint="/services/customfieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ dag_run.conf.projecturi }}",
            "customFieldUri": "{{ dag_run.conf.UDFProjectBusinessUnitURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.ProjectBusinessUnit_OptionURI }}"
            }
        )


        log_logs_88=rail.PythonOperator(
            task_id='log_logs_88',
            python_callable= lambda:  rail.render_template("Project Business Unit updated - '{{ dag_run.conf.projectinfo2 }}';")
        )


        log_exceptions_90=rail.PythonOperator(
            task_id='log_exceptions_90',
            python_callable= lambda:  rail.render_template("Project Business Unit not found - '{{ dag_run.conf.projectinfo2 }}';")
        )


        if_request_projectleaderloginname_present_91=rail.IfOperator(
            task_id='if_request_projectleaderloginname_present_91',
            test='''{{ dag_run.conf.projectleaderloginname | is_truthy }}''',
            yes_task="list_project_leaders_92",
            no_task="log_is_time_entry_allowed_existingvalue_100",
        )

        list_project_leaders_92= rail.RepliconServiceOperator(
            task_id='list_project_leaders_92',
            endpoint="/services/ProjectService1.svc/GetEligibleProjectLeaders",
        )


        log_project_manager_uri_93=rail.PythonOperator(
            task_id='log_project_manager_uri_93',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('list_project_leaders_92'), 'user.loginName', dag_run.conf['projectleaderloginname'], 'uri', null) if rail.result('list_project_leaders_92') else null
        )


        if_log_project_manager_uri_93_present_94=rail.IfOperator(
            task_id='if_log_project_manager_uri_93_present_94',
            test='''{{ result('log_project_manager_uri_93') | is_truthy }}''',
            yes_task="if_log_project_manager_uri_not_equals_projectleaderuri_95",
            no_task="log_exception_99",
        )


        if_log_project_manager_uri_not_equals_projectleaderuri_95=rail.IfOperator(
            task_id='if_log_project_manager_uri_not_equals_projectleaderuri_95',
            test=lambda: rail.result('log_project_manager_uri_93') != rail.result('bulk_get_project_details3_65')[0]['projectDetails']['projectLeader']['uri'],
            yes_task="update_project_leader_96",
            no_task="log_is_time_entry_allowed_existingvalue_100",
        )


        update_project_leader_96=rail.RepliconServiceOperator(
            task_id='update_project_leader_96',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data={
            "projectUri": "{{ dag_run.conf.projecturi }}",
            "userUri": "{{ result('log_project_manager_uri_93') }}"
            }
        )


        log_logs_97=rail.PythonOperator(
            task_id='log_logs_97',
            python_callable= lambda:  rail.render_template("Project Manager updated - '{{ dag_run.conf.projectleaderloginname }}';")
        )


        log_exception_99=rail.PythonOperator(
            task_id='log_exception_99',
            python_callable= lambda:  rail.render_template("Project Manager not found or does not have Project Manager permissions '{{ dag_run.conf.projectleaderloginname }}'")
        )


        log_is_time_entry_allowed_existingvalue_100=rail.PythonOperator(
            task_id='log_is_time_entry_allowed_existingvalue_100',
            python_callable= lambda:  rail.render_template("{{ result('bulk_get_project_details3_65')[0].projectDetails.isTimeEntryAllowed }}")
        )

        if_request_timeentryallowed_present_101=rail.IfOperator(
            task_id='if_request_timeentryallowed_present_101',
            test='''{{ dag_run.conf.timeentryallowed | is_truthy  and dag_run.conf.timeentryallowed != result('log_is_time_entry_allowed_existingvalue_100') }}''',
            yes_task="if_timeentryallowed_downcase_contains_false_102",
            no_task="get_project_team_members_108",
        )


        if_timeentryallowed_downcase_contains_false_102=rail.IfOperator(
            task_id='if_timeentryallowed_downcase_contains_false_102',
            test='''{{ dag_run.conf.timeentryallowed.lower() | matches('false') }}''',
            yes_task="update_allow_time_entry_against_tasks_only_103",
            no_task="if_timeentryallowed_downcase_contains_true_105",
        )


        update_allow_time_entry_against_tasks_only_103=rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_103',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
            "projectUri": "{{ dag_run.conf.projecturi }}",
            "allowTimeEntryAgainstTasksOnly": "true"
            }
        )


        log_logs_104=rail.PythonOperator(
            task_id='log_logs_104',
            python_callable= lambda:  "Allow Time entry against Tasks only updated to TRUE;"
        )


        if_timeentryallowed_downcase_contains_true_105=rail.IfOperator(
            task_id='if_timeentryallowed_downcase_contains_true_105',
            test='''{{ dag_run.conf.timeentryallowed.lower() | matches('true') }}''',
            yes_task="update_allow_time_entry_against_tasks_only_106",
            no_task="get_project_team_members_108",
        )


        update_allow_time_entry_against_tasks_only_106=rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_106',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
            "projectUri": "{{ dag_run.conf.projecturi }}",
            "allowTimeEntryAgainstTasksOnly": "false"
            }
        )


        log_logs_107=rail.PythonOperator(
            task_id='log_logs_107',
            python_callable= lambda:  "Allow Time entry against Tasks only updated to FALSE;"
        )

        get_project_team_members_108= rail.RepliconServiceOperator(
            task_id='get_project_team_members_108',
            endpoint="/services/ProjectService1.svc/BulkGetAllProjectTeamMembers2",
            data=lambda dag_run: {
                "projectUris": [dag_run.conf['projecturi']]
            }
        )


        if_request_projectteamusers_present_109=rail.IfOperator(
            task_id='if_request_projectteamusers_present_109',
            test='''{{ dag_run.conf.projectteamusers | is_truthy }}''',
            yes_task="log_users_110",
            no_task="if_request_projectteamdepartments_present_122",
        )


        log_users_110=rail.PythonOperator(
            task_id='log_users_110',
            python_callable= lambda dag_run: "|".join(dag_run.conf['projectteamusers'].split("|")).split("|")
        )

        create_list_111=rail.PythonOperator(
            task_id='create_list_111',
            python_callable=lambda dag_run: dag_run.conf['projectteamusers'].split('|')
        )


        foreach_create_list_111_112=rail.ForEachOperator(
            task_id='foreach_create_list_111_112',
            items=lambda: rail.result('create_list_111'),
            start_task = 'log_loginname_113',
            end_task = 'foreach_create_list_111_112_end'
        )


        log_loginname_113=rail.PythonOperator(
            task_id='log_loginname_113',
            python_callable= lambda:  rail.result('foreach_create_list_111_112')
        )

        search_users_114=rail.RepliconServiceOperator(
            task_id='search_users_114',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda:{
                    "users": [
                        {
                            "uri": null,
                            "loginName": rail.result('foreach_create_list_111_112'),
                            "employeeId": null,
                            "parameterCorrelationId": null
                        }
                    ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )


        log_resource_uri_115=rail.PythonOperator(
            task_id='log_resource_uri_115',
            python_callable= lambda: rail.result('search_users_114')[0]['userDetails']['uri'] if rail.result('search_users_114') else None
        )


        if_log_resource_uri_115_present_116=rail.IfOperator(
            task_id='if_log_resource_uri_115_present_116',
            test='''{{ result('log_resource_uri_115') | is_truthy }}''',
            yes_task="log_checkifuserisalreadyassigned_117",
            no_task="insert_to_list_121",
        )


        log_checkifuserisalreadyassigned_117=rail.PythonOperator(
            task_id='log_checkifuserisalreadyassigned_117',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_project_team_members_108'), 'resource.uri', rail.result('log_resource_uri_115'), 'uri', '') if rail.result('get_project_team_members_108') else null
        )


        if_log_checkifuserisalreadyassigned_117_blank_118=rail.IfOperator(
            task_id='if_log_checkifuserisalreadyassigned_117_blank_dataloggerlog_resource_uri_115message_118',
            test='''{{ result('log_checkifuserisalreadyassigned_117') | is_falsy }}''',
            yes_task="bulk_update_project_team_members_assignment_119",
            no_task="insert_to_list_121",
        )


        bulk_update_project_team_members_assignment_119=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment_119',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
            "projectUri":"{{ dag_run.conf.projecturi }}",
            "resourceUri": ["{{ result('log_resource_uri_115') }}"],
            "projectTeamMemberAssignmentOptionUri":"urn:replicon:project-team-member-assignment-option:assign"
            }
        )


        insert_to_list_121=rail.SetVariableOperator(
            task_id='insert_to_list_121',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "resourceassignment_logs": "User not assigned to Project - User with login name '{{ result('log_loginname_113') }}' not found;"
            }
        )


        foreach_create_list_111_112_end=rail.EmptyOperator(
            task_id='foreach_create_list_111_112_end',
        )


        if_request_projectteamdepartments_present_122=rail.IfOperator(
            task_id='if_request_projectteamdepartments_present_122',
            test='''{{ dag_run.conf.projectteamdepartments | is_truthy }}''',
            yes_task="log_departments_123",
            no_task="if_request_projectstatus_present_134",
        )


        log_departments_123=rail.PythonOperator(
            task_id='log_departments_123',
            python_callable= lambda dag_run:  "|".join(dag_run.conf['projectteamdepartments'].split("|")).split("|")
        )

        create_list_124=rail.PythonOperator(
            task_id='create_list_124',
            python_callable=lambda dag_run: dag_run.conf['projectteamdepartments'].split('|')
        )


        foreach_create_list_124_125=rail.ForEachOperator(
            task_id='foreach_create_list_124_125',
            items=lambda: rail.result('create_list_124'),
            start_task = 'log_departmentname_126',
            end_task = 'foreach_create_list_124_125_end'
        )


        log_departmentname_126=rail.PythonOperator(
            task_id='log_departmentname_126',
            python_callable= lambda: rail.result('foreach_create_list_124_125')
        )


        log_lookup_department_uri_127=rail.PythonOperator(
            task_id='log_lookup_department_uri_127',
            python_callable= lambda dag_run: next((obj["properties"]["uri"] for obj in rail.load_all_records(dag_run.conf['audax_users_and_departments_lookup_table']) if obj["properties"]["name"] == rail.result('foreach_create_list_124_125')), None)
        )


        if_log_lookup_department_uri_127_present_128=rail.IfOperator(
            task_id='if_log_lookup_department_uri_127_present_128',
            test='''{{ result('log_lookup_department_uri_127') | is_truthy }}''',
            yes_task="log_checkifdepartmentisalreadyassigned_129",
            no_task="insert_to_list_133",
        )


        log_checkifdepartmentisalreadyassigned_129=rail.PythonOperator(
            task_id='log_checkifdepartmentisalreadyassigned_129',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_project_team_members_108'), 'resource.uri', rail.result('log_lookup_department_uri_127'), 'uri', ' ') if rail.result('get_project_team_members_108') else null
        )


        if_log_checkifdepartmentisalreadyassigned_129_blank_130=rail.IfOperator(
            task_id='if_log_checkifdepartmentisalreadyassigned_129_blank_130',
            test='''{{ result('log_checkifdepartmentisalreadyassigned_129') | is_falsy }}''',
            yes_task="bulk_update_project_team_members_assignment_131",
            no_task="insert_to_list_133",
        )


        bulk_update_project_team_members_assignment_131=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment_131',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
            "projectUri":"{{ dag_run.conf.projecturi }}",
            "resourceUri": ["{{ result('log_lookup_department_uri_127') }}"],
            "projectTeamMemberAssignmentOptionUri":"urn:replicon:project-team-member-assignment-option:assign"
            }
        )


        insert_to_list_133=rail.SetVariableOperator(
            task_id='insert_to_list_133',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "resourceassignment_logs": "Department not assigned to Project - Department name '{{ result('log_departmentname_126') }}' not found;"
            }
        )


        foreach_create_list_124_125_end=rail.EmptyOperator(
            task_id='foreach_create_list_124_125_end',
        )


        if_request_projectstatus_present_134=rail.IfOperator(
            task_id='if_request_projectstatus_present_134',
            test='''{{ dag_run.conf.projectstatus | is_truthy  and dag_run.conf.projectstatus != result('bulk_get_project_details3_65')[0].projectDetails.status.name }}''',
            yes_task="log_formatsuppliedstatus_135",
            no_task="if_request_projectdescription_present_138",
        )


        log_formatsuppliedstatus_135=rail.PythonOperator(
            task_id='log_formatsuppliedstatus_135',
            python_callable= lambda dag_run: "in-progress" if "rogress" in dag_run.conf['projectstatus'] else dag_run.conf['projectstatus'].lower()
        )


        update_status_136=rail.RepliconServiceOperator(
            task_id='update_status_136',
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data={
            "projectUri": "{{ dag_run.conf.projecturi }}",
            "projectStatusUri": "urn:replicon:project-status-type:{{ result('log_formatsuppliedstatus_135') }}"
            }
        )


        log_logs_137=rail.PythonOperator(
            task_id='log_logs_137',
            python_callable= lambda:  rail.render_template("Status updated - '{{ dag_run.conf.projectstatus }}';")
        )


        if_request_projectdescription_present_138=rail.IfOperator(
            task_id='if_request_projectdescription_present_138',
            test='''{{ dag_run.conf.projectdescription | is_truthy  and dag_run.conf.projectdescription != result('bulk_get_project_details3_65')[0].projectDetails.description }}''',
            yes_task="log_formatsupplieddescription_139",
            no_task="if_request_timeentrystartdate_present_142",
        )


        log_formatsupplieddescription_139=rail.PythonOperator(
            task_id='log_formatsupplieddescription_139',
            python_callable= lambda dag_run:  dag_run.conf['projectdescription'][:256]
        )

        update_description_140=rail.RepliconServiceOperator(
            task_id='update_description_140',
            endpoint="/services/ProjectService1.svc/UpdateDescription",
            data={
            "projectUri": "{{ dag_run.conf.projecturi }}",
            "description": "{{ result('log_formatsupplieddescription_139') }}"
            }
        )


        log_logs_141=rail.PythonOperator(
            task_id='log_logs_141',
            python_callable= lambda:  "Description updated;"
        )


        if_request_timeentrystartdate_present_142=rail.IfOperator(
            task_id='if_request_timeentrystartdate_present_142',
            test='''{{ dag_run.conf.timeentrystartdate | is_truthy  or dag_run.conf.timeentryenddate | is_truthy }}''',
            yes_task="log_existing_start_date_143",
            no_task="log_resource_assignmentexceptions_147",
        )


        log_existing_start_date_143=rail.PythonOperator(
            task_id='log_existing_start_date_143',
            python_callable= lambda:  (str(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['startDate']['month'])+'/' +
                                        str(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['startDate']['day'])+'/'+
                                            str(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['startDate']['year'])) if rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['startDate'] else None
        )


        log_existing_end_date_144=rail.PythonOperator(
            task_id='log_existing_end_date_144',
            python_callable=lambda:  (str(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['endDate']['month'])+'/' +
                                        str(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['endDate']['day'])+'/'+
                                            str(rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['endDate']['year'])) if rail.result('bulk_get_project_details3_65')[0]['projectDetails']['timeEntryDateRange']['endDate'] else None
        )


        update_time_entry_date_range_145=rail.RepliconServiceOperator(
            task_id='update_time_entry_date_range_145',
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda dag_run:{
            "projectUri": dag_run.conf['projecturi'],
            "dateRange": {
                "startDate": rail.result('invoke_custom_ruby_code_6') if dag_run.conf['timeentrystartdate'] else None,
                "endDate": rail.result('invoke_custom_ruby_code_10') if dag_run.conf['timeentryenddate'] else None,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            }
            }
        )


        log_job_history_146=rail.PythonOperator(
            task_id='log_job_history_146',
            python_callable= lambda:  "Updated"
        )

        log_resource_assignmentexceptions_147=rail.PythonOperator(
            task_id='log_resource_assignmentexceptions_147',
            python_callable= lambda: "".join([item['resourceassignment_logs'] for item in rail.get_dag_run_var('declare_list_4')['resourceassignment_logs']]) if rail.get_dag_run_var('declare_list_4') else ""
        )

        log_compile_logs_148=rail.PythonOperator(
            task_id='log_compile_logs_148',
            python_callable= lambda:  rail.render_template("{{ result('log_logs_70') | default('', true) }}{{ result('log_logs_76') | default('', true) }}{{ result('log_logs_81') | default('', true) }}{{ result('log_logs_88') | default('', true) }}{{ result('log_logs_97') | default('', true) }}{{ result('log_logs_104') | default('', true) }}{{ result('log_logs_107') | default('', true) }}{{ result('log_logs_137') | default('', true) }}{{ result('log_logs_141') | default('', true) }}")
        )


        log_compile_exceptions_149=rail.PythonOperator(
            task_id='log_compile_exceptions_149',
            python_callable= lambda: rail.render_template("{{ result('log_resource_assignmentexceptions_147') | default('', true) }}{{ result('log_exception_18') | default('', true) }}{{ result('log_exceptions_50') | default('', true) }}{{ result('log_exceptions_57') | default('', true) }}{{ result('log_exceptions_62') | default('', true) }}{{ result('log_exceptions_72') | default('', true) }}{{ result('log_exceptions_83') | default('', true) }}{{ result('log_exceptions_90') | default('', true) }}{{ result('log_exception_99') | default('', true) }}")
        )


        audaxgroup_project_task_import_logs_add_entry_150=rail.WriteLogOperator(
            task_id='audaxgroup_project_task_import_logs_add_entry_150',
            log="{{ dag_run.conf.audax_project_task_import_logs }}",
            message="na",
            severity='''{{"Exception" if result('log_compile_exceptions_149') | is_truthy else "Success"}}''',
            properties={
                "jobid": "{{ dag_run.conf.parent_ecid }}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "status": '''{{"Exception" if result('log_compile_exceptions_149') | is_truthy else "Success"}}''',
                "details": "{{ dag_run.conf.type}} - {{ result('log_compile_exceptions_149') | default('', true) }}{{ result('log_compile_logs_148') | default('', true) }}{{ result('log_resource_assignmentexceptions_147') | default('', true) }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.audax_project_task_import_logs }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
            "jobid": "{{dag_run.conf.parent_ecid}}",
            "projectname": "{{ dag_run.conf.projectname }}",
            "status": "Error",
            "details": "{{ dag_run.conf.type}} - {{ get_error_message() | default('', true) }}{{ result('log_resource_assignmentexceptions_147') | default('', true) }}{{ result('log_exception_18') | default('', true) }}{{ result('log_exceptions_50') | default('', true) }}{{ result('log_exceptions_57') | default('', true) }}{{ result('log_exceptions_62') | default('', true) }}{{ result('log_exceptions_72') | default('', true) }}{{ result('log_exceptions_83') | default('', true) }}{{ result('log_exceptions_90') | default('', true) }}{{ result('log_exception_99') | default('', true) }}{{ result('log_logs_70') | default('', true) }}{{ result('log_logs_76') | default('', true) }}{{ result('log_logs_81') | default('', true) }}{{ result('log_logs_88') | default('', true) }}{{ result('log_logs_97') | default('', true) }}{{ result('log_logs_104') | default('', true) }}{{ result('log_logs_107') | default('', true) }}{{ result('log_logs_137') | default('', true) }}{{ result('log_logs_141') | default('', true) }}",
            "childjobid": "{{ dag_run_ecid() }}"
        }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_add_update_projects_child_logs

        create_add_update_projects_child_logs >> log_timenow_2 >> declare_list_4 >> if_request_timeentrystartdate_present_5
        if_request_timeentrystartdate_present_5 >> rail.Label('Yes')  >> invoke_custom_ruby_code_6 >> if_request_timeentryenddate_present_9
        if_request_timeentryenddate_present_9 >> rail.Label('Yes') >> invoke_custom_ruby_code_10 >> if_request_type_contains_add_13

        if_request_type_contains_add_13 >> rail.Label('Yes') >> if_request_projectleaderloginname_present_14
        if_request_projectleaderloginname_present_14  >> rail.Label('Yes') >> list_project_leaders_15 >> log_project_manager_uri_16 >> if_log_project_manager_uri_16_blank_17
        if_log_project_manager_uri_16_blank_17 >> rail.Label('Yes') >> log_exception_18 >> create_project_19
        if_log_project_manager_uri_16_blank_17 >> rail.Label('No') >> create_project_19 >> if_request_timeentryallowed_present_20
        if_request_timeentryallowed_present_20 >>  rail.Label('Yes') >> if_timeentryallowed_downcase_contains_false_21
        if_timeentryallowed_downcase_contains_false_21 >> rail.Label('Yes') >> update_allow_time_entry_against_tasks_only_22 >> if_timeentryallowed_downcase_contains_true_23
        if_timeentryallowed_downcase_contains_false_21 >> rail.Label('No') >> if_timeentryallowed_downcase_contains_true_23
        if_timeentryallowed_downcase_contains_true_23 >> rail.Label('Yes') >> update_allow_time_entry_against_tasks_only_24 >> if_request_projectteamusers_present_25
        if_timeentryallowed_downcase_contains_true_23 >> rail.Label('No') >> if_request_projectteamusers_present_25
        if_request_projectteamusers_present_25 >> rail.Label('Yes') >> log_users_26 >> create_list_27 >> foreach_create_list_27_28 >> log_loginname_29 >> search_users_30 \
        >> log_resource_uri_31 >> if_log_resource_uri_31_present_32
        if_log_resource_uri_31_present_32 >> rail.Label('Yes') >> bulk_update_project_team_members_assignment_33 >> foreach_create_list_27_28_end
        if_log_resource_uri_31_present_32 >> rail.Label('No') >> insert_to_list_35 >> foreach_create_list_27_28_end
        foreach_create_list_27_28 >> foreach_create_list_27_28_end >> if_request_projectteamdepartments_present_36
        if_request_projectteamdepartments_present_36 >>  rail.Label('Yes') >> log_departments_37 >> create_list_38 >> foreach_create_list_38_39 >> log_departmentname_40 >> log_lookup_department_uri_41 \
        >> if_log_lookup_department_uri_41_present_42
        if_log_lookup_department_uri_41_present_42  >> rail.Label('Yes') >> bulk_update_project_team_members_assignment_43 >> foreach_create_list_38_39_end
        if_log_lookup_department_uri_41_present_42  >> rail.Label('No') >> insert_to_list_45 >> foreach_create_list_38_39_end
        foreach_create_list_38_39 >> foreach_create_list_38_39_end >> if_request_programname_present_46
        if_request_programname_present_46  >> rail.Label('Yes') >> if_request_program_optionuri_present_47
        if_request_program_optionuri_present_47 >> rail.Label('Yes') >> update_dropdown_value_48 >> if_request_projectinfo1_present_51
        if_request_program_optionuri_present_47 >> rail.Label('No') >> log_exceptions_50 >> if_request_projectinfo1_present_51
        if_request_projectinfo1_present_51 >> rail.Label('Yes') >> update_text_value_52 >> if_request_projectinfo3_present_53
        if_request_projectinfo3_present_53 >> rail.Label('Yes') >> if_request_projectdepartment_optionuri_present_54
        if_request_projectdepartment_optionuri_present_54 >> rail.Label('Yes') >> update_dropdown_value_55 >> if_request_projectinfo2_present_58
        if_request_projectdepartment_optionuri_present_54 >> rail.Label('No') >> log_exceptions_57 >> if_request_projectinfo2_present_58
        if_request_projectinfo2_present_58 >> rail.Label('Yes') >> if_request_projectbusinessunit_optionuri_present_59
        if_request_projectbusinessunit_optionuri_present_59 >> rail.Label('Yes') >> update_dropdown_value_60 >> log_job_history_63
        if_request_projectbusinessunit_optionuri_present_59 >> rail.Label('No') >> log_exceptions_62 >> log_job_history_63
        if_request_projectinfo2_present_58 >> rail.Label('No') >> log_job_history_63
        if_request_projectinfo3_present_53 >> rail.Label('No') >> if_request_projectinfo2_present_58
        if_request_projectinfo1_present_51 >> rail.Label('No') >> if_request_projectinfo3_present_53
        if_request_programname_present_46  >> rail.Label('No') >> if_request_projectinfo1_present_51
        if_request_projectteamdepartments_present_36 >> rail.Label('No') >> if_request_programname_present_46
        if_request_projectteamusers_present_25 >> rail.Label('No') >> if_request_projectteamdepartments_present_36
        if_request_timeentryallowed_present_20 >> rail.Label('No') >> if_request_projectteamusers_present_25
        log_job_history_63 >> if_request_type_contains_update_64
        if_request_type_contains_update_64 >> rail.Label('Yes') >> bulk_get_project_details3_65 >> log_udf_existing_program_dropdown_value_66 >> if_request_programname_present_67
        if_request_programname_present_67 >> rail.Label('Yes') >> if_request_program_optionuri_present_68
        if_request_program_optionuri_present_68 >> rail.Label('Yes') >> update_dropdown_value_69 >> log_logs_70 >> log_udf_existing_deal_opportunity_i_d_project_text_value_73
        if_request_program_optionuri_present_68 >> rail.Label('No') >> log_exceptions_72 >> log_udf_existing_deal_opportunity_i_d_project_text_value_73
        if_request_programname_present_67 >> rail.Label('No') >> log_udf_existing_deal_opportunity_i_d_project_text_value_73
        log_udf_existing_deal_opportunity_i_d_project_text_value_73 >> if_request_projectinfo1_present_74
        if_request_projectinfo1_present_74 >> rail.Label('Yes') >> update_text_value_75 >> log_logs_76 >> log_udf_existing_project_department_dropdown_value_77
        if_request_projectinfo1_present_74 >> rail.Label('No') >> log_udf_existing_project_department_dropdown_value_77 >> if_request_projectinfo3_present_78
        if_request_projectinfo3_present_78 >> rail.Label('Yes') >> if_request_projectdepartment_optionuri_present_79
        if_request_projectdepartment_optionuri_present_79 >> rail.Label('Yes') >> update_dropdown_value_80 >> log_logs_81 >> log_udf_existing_project_business_unit_value_84
        if_request_projectdepartment_optionuri_present_79 >> rail.Label('No') >> log_exceptions_83 >> log_udf_existing_project_business_unit_value_84
        if_request_projectinfo3_present_78 >> rail.Label('No') >> log_udf_existing_project_business_unit_value_84
        log_udf_existing_project_business_unit_value_84 >> if_request_projectinfo2_present_85
        if_request_projectinfo2_present_85 >> rail.Label('Yes') >> if_request_projectbusinessunit_optionuri_present_86
        if_request_projectbusinessunit_optionuri_present_86 >> rail.Label('Yes') >> update_dropdown_value_87 >> log_logs_88 >> if_request_projectleaderloginname_present_91
        if_request_projectbusinessunit_optionuri_present_86 >> rail.Label('No') >> log_exceptions_90 >> if_request_projectleaderloginname_present_91
        if_request_projectinfo2_present_85 >> rail.Label('No') >> if_request_projectleaderloginname_present_91
        if_request_projectleaderloginname_present_91 >> rail.Label('Yes') >> list_project_leaders_92 >> log_project_manager_uri_93 >> if_log_project_manager_uri_93_present_94
        if_log_project_manager_uri_93_present_94 >> rail.Label('Yes') >> if_log_project_manager_uri_not_equals_projectleaderuri_95
        if_log_project_manager_uri_not_equals_projectleaderuri_95 >> rail.Label('Yes') >> update_project_leader_96 >> log_logs_97 >> log_is_time_entry_allowed_existingvalue_100
        if_log_project_manager_uri_not_equals_projectleaderuri_95 >> rail.Label('No') >> log_is_time_entry_allowed_existingvalue_100
        if_log_project_manager_uri_93_present_94 >> rail.Label('No') >> log_exception_99 >> log_is_time_entry_allowed_existingvalue_100
        if_request_projectleaderloginname_present_91 >> rail.Label('No') >> log_is_time_entry_allowed_existingvalue_100 >> if_request_timeentryallowed_present_101
        if_request_timeentryallowed_present_101 >> rail.Label('Yes') >> if_timeentryallowed_downcase_contains_false_102
        if_timeentryallowed_downcase_contains_false_102 >> rail.Label('Yes') >> update_allow_time_entry_against_tasks_only_103 >> log_logs_104 >> get_project_team_members_108
        if_timeentryallowed_downcase_contains_false_102 >> rail.Label('No') >> if_timeentryallowed_downcase_contains_true_105
        if_timeentryallowed_downcase_contains_true_105 >> rail.Label('Yes') >> update_allow_time_entry_against_tasks_only_106 >> log_logs_107 >> get_project_team_members_108
        if_timeentryallowed_downcase_contains_true_105 >> rail.Label('No') >> get_project_team_members_108
        if_request_timeentryallowed_present_101 >> rail.Label('No') >> get_project_team_members_108 >> if_request_projectteamusers_present_109
        if_request_projectteamusers_present_109 >> rail.Label('Yes') >> log_users_110 >> create_list_111 >> foreach_create_list_111_112 >> log_loginname_113 >> search_users_114 \
        >> log_resource_uri_115 >> if_log_resource_uri_115_present_116
        if_log_resource_uri_115_present_116 >> rail.Label('Yes') >> log_checkifuserisalreadyassigned_117 >> if_log_checkifuserisalreadyassigned_117_blank_118
        if_log_checkifuserisalreadyassigned_117_blank_118 >> rail.Label('Yes') >> bulk_update_project_team_members_assignment_119 >> foreach_create_list_111_112_end
        if_log_checkifuserisalreadyassigned_117_blank_118 >> rail.Label('No') >> insert_to_list_121
        if_log_resource_uri_115_present_116 >> rail.Label('No') >> insert_to_list_121 >> foreach_create_list_111_112_end
        foreach_create_list_111_112 >> foreach_create_list_111_112_end >> if_request_projectteamdepartments_present_122
        if_request_projectteamusers_present_109 >> rail.Label('No') >> if_request_projectteamdepartments_present_122
        if_request_projectteamdepartments_present_122 >> rail.Label('Yes') >> log_departments_123 >> create_list_124 >> foreach_create_list_124_125 >> log_departmentname_126 \
        >> log_lookup_department_uri_127 >> if_log_lookup_department_uri_127_present_128
        if_log_lookup_department_uri_127_present_128 >> rail.Label('Yes') >> log_checkifdepartmentisalreadyassigned_129 >> if_log_checkifdepartmentisalreadyassigned_129_blank_130
        if_log_checkifdepartmentisalreadyassigned_129_blank_130 >> rail.Label('Yes') >> bulk_update_project_team_members_assignment_131 >> foreach_create_list_124_125_end
        if_log_checkifdepartmentisalreadyassigned_129_blank_130 >> rail.Label('No') >> insert_to_list_133
        if_log_lookup_department_uri_127_present_128 >> rail.Label('No') >> insert_to_list_133 >> foreach_create_list_124_125_end
        foreach_create_list_124_125 >> foreach_create_list_124_125_end >> if_request_projectstatus_present_134
        if_request_projectstatus_present_134 >> rail.Label('Yes') >> log_formatsuppliedstatus_135 >> update_status_136 >> log_logs_137 >> if_request_projectdescription_present_138
        if_request_projectstatus_present_134 >> rail.Label('No') >> if_request_projectdescription_present_138
        if_request_projectdescription_present_138 >> rail.Label('Yes') >> log_formatsupplieddescription_139 >> update_description_140 >> log_logs_141 >> if_request_timeentrystartdate_present_142
        if_request_projectdescription_present_138 >> rail.Label('No') >> if_request_timeentrystartdate_present_142
        if_request_timeentrystartdate_present_142 >> rail.Label('Yes') >> log_existing_start_date_143 >> log_existing_end_date_144 >> update_time_entry_date_range_145 >> log_job_history_146 >> log_resource_assignmentexceptions_147
        if_request_timeentrystartdate_present_142 >> rail.Label('No') >> log_resource_assignmentexceptions_147 >> log_compile_logs_148 >> log_compile_exceptions_149 >> audaxgroup_project_task_import_logs_add_entry_150 >> finish
        if_request_projectteamdepartments_present_122 >> rail.Label('No') >> if_request_projectstatus_present_134
        if_request_type_contains_update_64 >> rail.Label('No') >> log_resource_assignmentexceptions_147
        if_request_projectleaderloginname_present_14  >> rail.Label('No') >> create_project_19
        if_request_type_contains_add_13 >> rail.Label('No') >> if_request_type_contains_update_64

        if_request_timeentryenddate_present_9 >> rail.Label('No') >> if_request_type_contains_add_13
        if_request_timeentrystartdate_present_5 >> rail.Label('No') >> if_request_timeentryenddate_present_9

        finish >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
