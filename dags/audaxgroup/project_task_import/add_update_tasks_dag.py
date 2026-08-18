from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_update_tasks_dag_id,
        description=f'Audaxgroup add update tasks {config.instance}',
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
            no_task='log_taskcode_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_taskcode_2',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_taskcode_2=rail.PythonOperator(
            task_id='log_taskcode_2',
            python_callable= lambda dag_run: dag_run.conf['taskcode'] if 'taskcode' in dag_run.conf else None
        )

        log_taskdescription_3=rail.PythonOperator(
            task_id='log_taskdescription_3',
            python_callable= lambda dag_run: dag_run.conf['taskdescription'] if 'taskdescription' in dag_run.conf else None
        )


        log_timeentryallowed_4=rail.PythonOperator(
            task_id='log_timeentryallowed_4',
            python_callable=lambda dag_run: dag_run.conf['istimeentryallowed'].lower() if 'istimeentryallowed' in dag_run.conf else "true"
        )

        declare_list_5=rail.SetVariableOperator(
            task_id='declare_list_5',
            append=False,
            name='resources_exceptions',
            value=[]
        )


        declare_list_6=rail.SetVariableOperator(
            task_id='declare_list_6',
            append=False,
            name='resources',
            value=[]
        )


        if_request_assignedusers_present_8=rail.IfOperator(
            task_id='if_request_assignedusers_present_8',
            test='''{{ dag_run.conf.assignedusers | is_truthy }}''',
            yes_task="log_userslist_9",
            no_task="if_request_assigneddepartments_present_19",
        )


        log_userslist_9=rail.PythonOperator(
            task_id='log_userslist_9',
            python_callable=lambda dag_run: dag_run.conf['assignedusers'].split("|")
        )

        foreach_create_list_10_11=rail.ForEachOperator(
            task_id='foreach_create_list_10_11',
            items=lambda dag_run: dag_run.conf['assignedusers'].split('|'),
            start_task = 'log_userlogin_12',
            end_task = 'foreach_create_list_10_11_end'
        )

        log_userlogin_12=rail.PythonOperator(
            task_id='log_userlogin_12',
            python_callable= lambda:  rail.result('foreach_create_list_10_11')
        )


        log_lookupuseruri_13=rail.PythonOperator(
            task_id='log_lookupuseruri_13',
            python_callable= lambda dag_run: next((obj["properties"]["uri"] for obj in rail.load_all_records(dag_run.conf['audax_users_and_departments_lookup_table']) if obj["properties"]["name"] == rail.result('foreach_create_list_10_11')), None)
        )


        if_log_lookupuseruri_13_present_14=rail.IfOperator(
            task_id='if_log_lookupuseruri_13_present_14',
            test='''{{ result('log_lookupuseruri_13') | is_truthy }}''',
            yes_task="insert_to_list_15",
            no_task="audaxgroup_project_task_import_logs_add_entry_17",
        )


        insert_to_list_15=rail.SetVariableOperator(
            task_id='insert_to_list_15',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "name": "{{ result('log_userlogin_12') }}",
                "uri": "{{ result('log_lookupuseruri_13') }}"
            }
        )

        audaxgroup_project_task_import_logs_add_entry_17=rail.WriteLogOperator(
            task_id='audaxgroup_project_task_import_logs_add_entry_17',
            log="{{ dag_run.conf.audax_project_task_import_logs }}",
            message="na",
            severity="Exception",
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "{{ dag_run.conf.tasklevel1 | default('', true) }}|{{ dag_run.conf.tasklevel2 | default('', true) }}|{{ dag_run.conf.tasklevel3 | default('', true) }}",
                "taskcode": "{{ dag_run.conf.taskcode }}",
                "status": "Exception",
                "details": "User not assigned to task - '{{ result('log_userlogin_12') }}' not found in Replicon",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        insert_to_list_18=rail.SetVariableOperator(
            task_id='insert_to_list_18',
            append=True,
            name='{{ result("declare_list_5").name }}',
            value={
                "exceptions": "User not assigned to Task - User with login name '{{ result('log_userlogin_12') }}' not found;"
            }
        )

        foreach_create_list_10_11_end=rail.EmptyOperator(
            task_id='foreach_create_list_10_11_end',
        )

        if_request_assigneddepartments_present_19=rail.IfOperator(
            task_id='if_request_assigneddepartments_present_19',
            test='''{{ dag_run.conf.assigneddepartments | is_truthy }}''',
            yes_task="log_departmentlist_20",
            no_task="log_fordebugging_29",
        )


        log_departmentlist_20=rail.PythonOperator(
            task_id='log_departmentlist_20',
            python_callable= lambda dag_run:  dag_run.conf['assigneddepartments'].split("|")
        )

        create_list_21=rail.PythonOperator(
            task_id='create_list_21',
            python_callable=lambda dag_run: dag_run.conf['assigneddepartments'].split('|')
        )


        foreach_create_list_21_22=rail.ForEachOperator(
            task_id='foreach_create_list_21_22',
            items=lambda: rail.result('create_list_21'),
            start_task = 'log_departmentname_23',
            end_task = 'foreach_create_list_21_22_end'
        )


        log_departmentname_23=rail.PythonOperator(
            task_id='log_departmentname_23',
            python_callable= lambda: rail.result('foreach_create_list_21_22')
        )

        log_lookup_departmenturi_24=rail.PythonOperator(
            task_id='log_lookup_departmenturi_24',
            python_callable= lambda dag_run: next((obj["properties"]["uri"] for obj in rail.load_all_records(dag_run.conf['audax_users_and_departments_lookup_table']) if obj["properties"]["name"] == rail.result('log_departmentname_23')), None)
        )


        if_log_lookup_departmenturi_24_present_25=rail.IfOperator(
            task_id='if_log_lookup_departmenturi_24_present_25',
            test='''{{ result('log_lookup_departmenturi_24') | is_truthy }}''',
            yes_task="insert_to_list_26",
            no_task="insert_to_list_28",
        )


        insert_to_list_26=rail.SetVariableOperator(
            task_id='insert_to_list_26',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "name": "{{ result('log_departmentname_23') }}",
                "uri": "{{ result('log_lookup_departmenturi_24') }}"
            }
        )

        insert_to_list_28=rail.SetVariableOperator(
            task_id='insert_to_list_28',
            append=True,
            name='{{ result("declare_list_5").name }}',
            value={
                "exceptions": "Department not assigned to Task - Department name '{{ result('log_departmentname_23') }}' not found"
            }
        )

        foreach_create_list_21_22_end=rail.EmptyOperator(
            task_id='foreach_create_list_21_22_end',
        )

        log_fordebugging_29=rail.PythonOperator(
            task_id='log_fordebugging_29',
            python_callable=lambda: rail.get_dag_run_var(rail.result("declare_list_6")['name'])
        )

        log_resourcestobeassigned_30=rail.PythonOperator(
            task_id='log_resourcestobeassigned_30',
            python_callable=lambda: [item['uri'] for item in  rail.get_dag_run_var(rail.result("declare_list_6")['name'])]
        )

        if_request_timeentrystartdate_present_31=rail.IfOperator(
            task_id='if_request_timeentrystartdate_present_31',
            test='''{{ dag_run.conf.timeentrystartdate | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_32",
            no_task="if_request_timeentryenddate_present_35",
        )

        def get_date_object(date_string):
            if not date_string or not date_string.strip():
                return None
            try:
                dateobj = datetime.strptime(date_string.strip(),'%m/%d/%Y')
                return {
                  'day': dateobj.day,
                  'month': dateobj.month,
                  'year': dateobj.year
                }
            except ValueError as e:
                raise ValueError(f"Invalid date format '{date_string}'. Expected MM/DD/YYYY (e.g., 10/13/2025)")

        invoke_custom_ruby_code_32=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_32',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['timeentrystartdate'])
        )


        log_formatstartdate_33=rail.PythonOperator(
            task_id='log_formatstartdate_33',
            python_callable= lambda:  rail.result('invoke_custom_ruby_code_32')
        )

        log_conditional_startdate_34=rail.PythonOperator(
            task_id='log_conditional_startdate_34',
            python_callable= lambda: rail.result('log_formatstartdate_33') if rail.result('invoke_custom_ruby_code_32').get('year') else None
        )


        if_request_timeentryenddate_present_35=rail.IfOperator(
            task_id='if_request_timeentryenddate_present_35',
            test='''{{ dag_run.conf.timeentryenddate | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_36",
            no_task="log_resourceassignmentlogs_43",
        )


        invoke_custom_ruby_code_36=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_36',
            python_callable= lambda dag_run: get_date_object(dag_run.conf['timeentryenddate'])
        )


        log_formatenddate_37=rail.PythonOperator(
            task_id='log_formatenddate_37',
            python_callable= lambda: rail.result('invoke_custom_ruby_code_36')
        )


        log_conditional_enddate_38=rail.PythonOperator(
            task_id='log_conditional_enddate_38',
            python_callable= lambda: rail.result('log_formatenddate_37') if rail.result('invoke_custom_ruby_code_36').get('year') else  None
        )


        log_resourceassignmentlogs_43=rail.PythonOperator(
            task_id='log_resourceassignmentlogs_43',
            python_callable=lambda:  "".join(list(map(lambda x: x['exceptions'], rail.get_dag_run_var(rail.result('declare_list_5')[
                                              'name'])))) if rail.get_dag_run_var(rail.result('declare_list_5')['name']) else ""
        )

        if_request_tasklevel3_present_44=rail.IfOperator(
            task_id='if_request_tasklevel3_present_44',
            test='''{{ dag_run.conf.tasklevel3 | is_truthy }}''',
            yes_task="get_children_task_details_45",
            no_task="if_request_tasklevel2_present_60",
        )


        get_children_task_details_45=rail.RepliconServiceOperator(
            task_id='get_children_task_details_45',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{ dag_run.conf.projecturi }}"
            }
        )


        log_tasklevel1_uri_46=rail.PythonOperator(
            task_id='log_tasklevel1_uri_46',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_children_task_details_45'), 'name', dag_run.conf['tasklevel1'], 'uri', '')
        )

        if_log_tasklevel1_uri_46_present_47=rail.IfOperator(
            task_id='if_log_tasklevel1_uri_46_present_47',
            test='''{{ result('log_tasklevel1_uri_46') | is_truthy }}''',
            yes_task="get_children_task_details_48",
            no_task="log_exception_59",
        )


        get_children_task_details_48=rail.RepliconServiceOperator(
            task_id='get_children_task_details_48',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{ result('log_tasklevel1_uri_46') }}"
            }
        )


        log_tasklevel2_uri_49=rail.PythonOperator(
            task_id='log_tasklevel2_uri_49',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_children_task_details_48'), 'name', dag_run.conf['tasklevel2'], 'uri', '')
        )


        if_log_tasklevel2_uri_49_present_50=rail.IfOperator(
            task_id='if_log_tasklevel2_uri_49_present_50',
            test='''{{ result('log_tasklevel2_uri_49') | is_truthy }}''',
            yes_task="get_children_task_details_51",
            no_task="log_exception_57",
        )


        get_children_task_details_51=rail.RepliconServiceOperator(
            task_id='get_children_task_details_51',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{ result('log_tasklevel2_uri_49') }}"
            }
        )


        log_tasklevel3_uri_52=rail.PythonOperator(
            task_id='log_tasklevel3_uri_52',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_children_task_details_51'), 'name', dag_run.conf['tasklevel3'], 'uri', '')
        )


        if_log_tasklevel3_uri_52_blank_53=rail.IfOperator(
            task_id='if_log_tasklevel3_uri_52_blank_53',
            test='''{{ result('log_tasklevel3_uri_52') | is_falsy }}''',
            yes_task="put_task_54",
            no_task="log_exception_59",
        )

        put_task_54=rail.RepliconServiceOperator(
            task_id='put_task_54',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
            "project": {
                "uri": "{{ dag_run.conf.projecturi }}",
                "name": null,
                "parameterCorrelationId": null
            },
            "task": {
                "target": {
                "uri": null,
                "name": "{{ dag_run.conf.tasklevel3 }}",
                "parent": {
                    "uri": "{{ result('log_tasklevel2_uri_49') }}",
                    "name": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "parameterCorrelationId": null
                },
                "name": "{{ dag_run.conf.tasklevel3 }}",
                "code": "{{ result('log_taskcode_2') }}",
                "description": "{{ result('log_taskdescription_3') }}",
                "timeEntryDateRange":null,
                "percentCompleted": "0",
                "isTimeEntryAllowed": "{{ result('log_timeentryallowed_4') }}",
                "estimatedHours": null,
                "isClosed": "false",
                "customFieldValues": [],
                "estimatedCost": null,
                "costTypeUri": null,
                "timeAndExpenseEntryTypeUri": null,
                "assignedResources": []
            }
            }
        )

        log_exception_57=rail.PythonOperator(
            task_id='log_exception_57',
            python_callable= lambda:  rail.render_template("Level 2 task '{{ dag_run.conf.tasklevel2 }}' not present in Project;")
        )


        log_exception_59=rail.PythonOperator(
            task_id='log_exception_59',
            python_callable= lambda:  rail.render_template("Level 1 task '{{ dag_run.conf.tasklevel1 }}' not present in Project;")
        )


        if_request_tasklevel2_present_60=rail.IfOperator(
            task_id='if_request_tasklevel2_present_60',
            test='''{{ dag_run.conf.tasklevel2 | is_truthy  and dag_run.conf.tasklevel3 | is_falsy }}''',
            yes_task="get_children_task_details_61",
            no_task="if_request_tasklevel1_present_71",
        )


        get_children_task_details_61=rail.RepliconServiceOperator(
            task_id='get_children_task_details_61',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{ dag_run.conf.projecturi }}"
            }
        )

        log_tasklevel1_uri_62=rail.PythonOperator(
            task_id='log_tasklevel1_uri_62',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_children_task_details_61'), 'name', dag_run.conf['tasklevel1'], 'uri', '')
        )

        if_log_tasklevel1_uri_62_present_63=rail.IfOperator(
            task_id='if_log_tasklevel1_uri_62_present_63',
            test='''{{ result('log_tasklevel1_uri_62') | is_truthy }}''',
            yes_task="get_children_task_details_64",
            no_task="log_exception_70",
        )


        get_children_task_details_64=rail.RepliconServiceOperator(
            task_id='get_children_task_details_64',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
             "parentUri": "{{ result('log_tasklevel1_uri_62') }}"
            }
        )


        log_tasklevel2_uri_65=rail.PythonOperator(
            task_id='log_tasklevel2_uri_65',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_children_task_details_64'), 'name', dag_run.conf['tasklevel2'], 'uri', '')
        )


        if_log_tasklevel2_uri_65_blank_66=rail.IfOperator(
            task_id='if_log_tasklevel2_uri_65_blank_66',
            test='''{{ result('log_tasklevel2_uri_65') | is_falsy }}''',
            yes_task="put_task_67",
            no_task="if_request_tasklevel1_present_71",
        )


        put_task_67=rail.RepliconServiceOperator(
            task_id='put_task_67',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ dag_run.conf.projecturi }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                    "uri": null,
                    "name": "{{ dag_run.conf.tasklevel2 }}",
                    "parent": {
                        "uri": "{{ result('log_tasklevel1_uri_62') }}",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.tasklevel2 }}",
                    "code": "{{ result('log_taskcode_2') }}",
                    "description": "{{ result('log_taskdescription_3') }}",
                    "timeEntryDateRange":null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "{{ result('log_timeentryallowed_4') }}",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": null,
                    "assignedResources": []
                }
                }
        )


        log_exception_70=rail.PythonOperator(
            task_id='log_exception_70',
            python_callable= lambda dag_run:  "Level 1 task " + dag_run.conf['tasklevel1'] +" not present in Project;"
        )


        if_request_tasklevel1_present_71=rail.IfOperator(
            task_id='if_request_tasklevel1_present_71',
            test='''{{ dag_run.conf.tasklevel1 | is_truthy  and dag_run.conf.tasklevel2 | is_falsy }}''',
            yes_task="get_children_task_details_72",
            no_task="log_existing_task_uri_77",
        )


        get_children_task_details_72=rail.RepliconServiceOperator(
            task_id='get_children_task_details_72',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
            "parentUri": "{{ dag_run.conf.projecturi }}"
            }
        )


        log_tasklevel1_uri_73=rail.PythonOperator(
            task_id='log_tasklevel1_uri_73',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('get_children_task_details_72'), 'name', dag_run.conf['tasklevel1'], 'uri', '')
        )

        if_log_tasklevel1_uri_73_blank_74=rail.IfOperator(
            task_id='if_log_tasklevel1_uri_73_blank_74',
            test='''{{ result('log_tasklevel1_uri_73') | is_falsy }}''',
            yes_task="put_task_75",
            no_task="log_existing_task_uri_77",
        )


        put_task_75=rail.RepliconServiceOperator(
            task_id='put_task_75',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ dag_run.conf.projecturi }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                    "uri": null,
                    "name": "{{ dag_run.conf.tasklevel1 }}",
                    "parent": null,
                    "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.tasklevel1 }}",
                    "code": "{{ result('log_taskcode_2') }}",
                    "description": "{{ result('log_taskdescription_3') }}",
                    "timeEntryDateRange":null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "{{ result('log_timeentryallowed_4') }}",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": null,
                    "assignedResources": []
                }
                }
        )


        log_existing_task_uri_77=rail.PythonOperator(
            task_id='log_existing_task_uri_77',
            python_callable= lambda: rail.result('log_tasklevel3_uri_52') or rail.result('log_tasklevel2_uri_65') or rail.result('log_tasklevel1_uri_73')
        )


        if_log_existing_task_uri_77_present_78=rail.IfOperator(
            task_id='if_log_existing_task_uri_77_present_78',
            test='''{{ result('log_existing_task_uri_77') | is_truthy }}''',
            yes_task="log_task_level_79",
            no_task="log_newlycreated_task_uri_116",
        )


        log_task_level_79=rail.PythonOperator(
            task_id='log_task_level_79',
            python_callable= lambda:  "Level 3 Task Update - " if rail.result('log_tasklevel3_uri_52') else "Level 2 Task Update -" if rail.result('log_tasklevel2_uri_65') else "Level 1 Task Update -" if rail.result('log_tasklevel1_uri_73') else ""
        )


        bulk_get_task_details_80=rail.RepliconServiceOperator(
            task_id='bulk_get_task_details_80',
            endpoint="/services/TaskService1.svc/BulkGetTaskDetails",
            data={
                "taskUris": [
                    "{{ result('log_existing_task_uri_77') }}"
                ]
                }
        )


        if_log_resourcestobeassigned_30_present_81=rail.IfOperator(
            task_id='if_log_resourcestobeassigned_30_present_81',
            test='''{{ result('log_resourcestobeassigned_30') | is_truthy }}''',
            yes_task="bulk_update_resource_assignments_82",
            no_task="log_udf_existing_task_business_unit_dropdownvalue_83",
        )

        bulk_update_resource_assignments_82=rail.RepliconServiceOperator(
            task_id='bulk_update_resource_assignments_82',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: {
            "taskUri": rail.result('log_existing_task_uri_77'),
            "resourceUris": rail.result('log_resourcestobeassigned_30'),
            "isAssigned": "true"
            }
        )


        log_udf_existing_task_business_unit_dropdownvalue_83=rail.PythonOperator(
            task_id='log_udf_existing_task_business_unit_dropdownvalue_83',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_task_details_80')[0]['customFields'], 'customField.displayText', 'Task Business Unit', 'text', '')
        )

        if_request_taskinfo1_present_84=rail.IfOperator(
            task_id='if_request_taskinfo1_present_84',
            test='''{{ dag_run.conf.taskinfo1 | is_truthy  and dag_run.conf.taskinfo1 != result('log_udf_existing_task_business_unit_dropdownvalue_83') }}''',
            yes_task="if_request_taskinfo1uri_present_85",
            no_task="log_udf_existing_deal_opportunity_i_d_tasktextvalue_90",
        )


        if_request_taskinfo1uri_present_85=rail.IfOperator(
            task_id='if_request_taskinfo1uri_present_85',
            test='''{{ dag_run.conf.taskinfo1URI | is_truthy }}''',
            yes_task="update_dropdown_value_86",
            no_task="log_exception_89",
        )


        update_dropdown_value_86=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_86',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('log_existing_task_uri_77') }}",
                "customFieldUri": "{{ dag_run.conf.TaskBusinessUnitURI }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.taskinfo1URI }}"
            }
        )


        log_log_87=rail.PythonOperator(
            task_id='log_log_87',
            python_callable= lambda:  rail.render_template("Task Business Unit updated - '{{ dag_run.conf.taskinfo1 }}';")
        )


        log_exception_89=rail.PythonOperator(
            task_id='log_exception_89',
            python_callable= lambda:  rail.render_template("Task Business Unit not found - '{{ dag_run.conf.taskinfo1 }}';")
        )


        log_udf_existing_deal_opportunity_i_d_tasktextvalue_90=rail.PythonOperator(
            task_id='log_udf_existing_deal_opportunity_i_d_tasktextvalue_90',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_task_details_80')[0]['customFields'], 'customField.displayText', 'Deal Opportunity ID (Task)', 'text', '')
        )


        if_request_taskinfo2_present_91=rail.IfOperator(
            task_id='if_request_taskinfo2_present_91',
            test='''{{ dag_run.conf.taskinfo2 | is_truthy  and dag_run.conf.taskinfo2 != result('log_udf_existing_deal_opportunity_i_d_tasktextvalue_90') }}''',
            yes_task="update_text_value_92",
            no_task="log_udf_existing_task_departmentdropdownvalue_94",
        )


        update_text_value_92=rail.RepliconServiceOperator(
            task_id='update_text_value_92',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('log_existing_task_uri_77') }}",
                "customFieldUri": "{{ dag_run.conf.DealOpportunityIDTaskURI }}",
                "value": "{{ dag_run.conf.taskinfo2 }}"
            }
        )


        log_log_93=rail.PythonOperator(
            task_id='log_log_93',
            python_callable= lambda:  rail.render_template("Deal Opportunity ID (Task) updated - '{{ dag_run.conf.taskinfo2 }}';")
        )


        log_udf_existing_task_departmentdropdownvalue_94=rail.PythonOperator(
            task_id='log_udf_existing_task_departmentdropdownvalue_94',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_task_details_80')[0]['customFields'], 'customField.displayText', 'Task Department', 'text', '')
        )


        if_request_taskinfo3_present_95=rail.IfOperator(
            task_id='if_request_taskinfo3_present_95',
            test='''{{ dag_run.conf.taskinfo3 | is_truthy  and dag_run.conf.taskinfo3 != result('log_udf_existing_task_departmentdropdownvalue_94') }}''',
            yes_task="if_request_taskinfo3uri_present_96",
            no_task="if_request_taskcode_present_101",
        )


        if_request_taskinfo3uri_present_96=rail.IfOperator(
            task_id='if_request_taskinfo3uri_present_96',
            test='''{{ dag_run.conf.taskinfo3URI | is_truthy }}''',
            yes_task="update_dropdown_value_97",
            no_task="log_exception_100",
        )


        update_dropdown_value_97=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_97',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ result('log_existing_task_uri_77') }}",
            "customFieldUri": "{{ dag_run.conf.TaskDepartmentURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.taskinfo3URI }}"
            }
        )


        log_log_98=rail.PythonOperator(
            task_id='log_log_98',
            python_callable= lambda:  rail.render_template("Task Department updated - '{{ dag_run.conf.taskinfo3 }}';")
        )


        log_exception_100=rail.PythonOperator(
            task_id='log_exception_100',
            python_callable= lambda:  rail.render_template("Task Department not found - '{{ dag_run.conf.taskinfo3 }}';")
        )


        if_request_taskcode_present_101=rail.IfOperator(
            task_id='if_request_taskcode_present_101',
            test='''{{ dag_run.conf.taskcode | is_truthy  and dag_run.conf.taskcode != result('bulk_get_task_details_80')[0].code }}''',
            yes_task="update_code_102",
            no_task="log_is_time_entry_allowed_existing_value_104",
        )


        update_code_102=rail.RepliconServiceOperator(
            task_id='update_code_102',
            endpoint="/services/TaskService1.svc/UpdateCode",
            data={
            "taskUri": "{{ result('log_existing_task_uri_77') }}",
            "code": "{{ dag_run.conf.taskcode }}"
            }
        )


        log_log_103=rail.PythonOperator(
            task_id='log_log_103',
            python_callable= lambda:  rail.render_template("Task code updated - '{{ dag_run.conf.taskcode }}';")
        )

        log_is_time_entry_allowed_existing_value_104=rail.PythonOperator(
            task_id='log_is_time_entry_allowed_existing_value_104',
            python_callable= lambda:  rail.render_template("{{ result('bulk_get_task_details_80')[0].isTimeEntryAllowed }}")
        )


        if_request_istimeentryallowed_present_105=rail.IfOperator(
            task_id='if_request_istimeentryallowed_present_105',
            test='''{{ dag_run.conf.istimeentryallowed | is_truthy }}''',
            yes_task="if_istimeentryallowed_check_106",
            no_task="if_request_taskdescription_present_109",
        )


        if_istimeentryallowed_check_106=rail.IfOperator(
            task_id='if_istimeentryallowed_check_106',
            test='''{{ dag_run.conf.istimeentryallowed.lower() != result('log_is_time_entry_allowed_existing_value_104') }}''',
            yes_task="update_allow_time_entry_107",
            no_task="if_request_taskdescription_present_109",
        )


        update_allow_time_entry_107=rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_107',
            endpoint="/services/TaskService1.svc/UpdateAllowTimeEntry",
            data={
            "taskUri": "{{ result('log_existing_task_uri_77') }}",
            "allowTimeEntry": "{{ result('log_timeentryallowed_4') }}"
            }
        )


        log_log_108=rail.PythonOperator(
            task_id='log_log_108',
            python_callable= lambda:  rail.render_template("'Is time entry allowed' updated to - '{{ dag_run.conf.istimeentryallowed }}';")
        )


        if_request_taskdescription_present_109=rail.IfOperator(
            task_id='if_request_taskdescription_present_109',
            test='''{{ dag_run.conf.taskdescription | is_truthy  and dag_run.conf.taskdescription != result('bulk_get_task_details_80')[0].description }}''',
            yes_task="update_description_110",
            no_task="if_request_timeentrystartdate_present_112",
        )


        update_description_110=rail.RepliconServiceOperator(
            task_id='update_description_110',
            endpoint="/services/TaskService1.svc/UpdateDescription",
            data={
            "taskUri": "{{ result('log_existing_task_uri_77') }}",
            "description": "{{ result('log_taskdescription_3') }}"
            }
        )


        log_log_111=rail.PythonOperator(
            task_id='log_log_111',
            python_callable= lambda:  "Task description updated;"
        )


        if_request_timeentrystartdate_present_112=rail.IfOperator(
            task_id='if_request_timeentrystartdate_present_112',
            test='''{{ dag_run.conf.timeentrystartdate | is_truthy  or dag_run.conf.timeentryenddate | is_truthy }}''',
            yes_task="log_existing_start_date_113",
            no_task="log_newlycreated_task_uri_116",
        )


        log_existing_start_date_113=rail.PythonOperator(
            task_id='log_existing_start_date_113',
            python_callable= lambda: rail.render_template("{{ result('bulk_get_task_details_80')[0].timeEntryDateRange.startDate.month }}/{{ result('bulk_get_task_details_80')[0].timeEntryDateRange.startDate.day }}/{{ result('bulk_get_task_details_80')[0].timeEntryDateRange.startDate.year }}")
                                                        if rail.result('bulk_get_task_details_80')[0]['timeEntryDateRange']['startDate'] else None
        )


        log_existing_end_date_114=rail.PythonOperator(
            task_id='log_existing_end_date_114',
            python_callable= lambda: rail.render_template("{{result('bulk_get_task_details_80')[0].timeEntryDateRange.endDate.month }}/{{ result('bulk_get_task_details_80')[0].timeEntryDateRange.endDate.day }}/{{ result('bulk_get_task_details_80')[0].timeEntryDateRange.endDate.year }}")
                                                            if rail.result('bulk_get_task_details_80')[0]['timeEntryDateRange']['endDate'] else None
        )

        update_time_entry_date_range_115=rail.RepliconServiceOperator(
            task_id='update_time_entry_date_range_115',
            endpoint="/services/TaskService1.svc/UpdateTimeEntryDateRange",
            data=lambda:{
                "taskUri": rail.result('log_existing_task_uri_77'),
                "dateRange": {
                    "startDate": rail.result('log_conditional_startdate_34'),
                    "endDate": rail.result('log_conditional_enddate_38'),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
                }
        )


        log_newlycreated_task_uri_116=rail.PythonOperator(
            task_id='log_newlycreated_task_uri_116',
            python_callable= lambda:  rail.result('put_task_54')['uri'] if rail.result('put_task_54') else rail.result('put_task_67')['uri'] if rail.result('put_task_67') else  rail.result('put_task_75')['uri'] if rail.result('put_task_75') else None,
        )


        if_log_newlycreated_task_uri_116_present_117=rail.IfOperator(
            task_id='if_log_newlycreated_task_uri_116_present_117',
            test="{{ result('log_newlycreated_task_uri_116') | is_truthy }}",
            yes_task="log_task_level_118",
            no_task="log_compilelogs_141",
        )


        log_task_level_118=rail.PythonOperator(
            task_id='log_task_level_118',
            python_callable= lambda:  "Level 3 Task Add - " if rail.result('put_task_54') else "Level 2 Task Add - " if rail.result('put_task_67') else "Level 1 Task Add - " if rail.result('put_task_75') else ""
        )


        if_log_resourcestobeassigned_30_present_119=rail.IfOperator(
            task_id='if_log_resourcestobeassigned_30_present_119',
            test='''{{ result('log_resourcestobeassigned_30') | is_truthy }}''',
            yes_task="bulk_update_resource_assignments_120",
            no_task="if_request_taskinfo1_present_121",
        )


        bulk_update_resource_assignments_120=rail.RepliconServiceOperator(
            task_id='bulk_update_resource_assignments_120',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: {
                "taskUri": rail.result('log_newlycreated_task_uri_116'),
                "resourceUris": rail.result('log_resourcestobeassigned_30'),
                "isAssigned": "true"
            }
        )


        if_request_taskinfo1_present_121=rail.IfOperator(
            task_id='if_request_taskinfo1_present_121',
            test='''{{ dag_run.conf.taskinfo1 | is_truthy }}''',
            yes_task="if_request_taskinfo1uri_present_122",
            no_task="if_request_taskinfo2_present_126",
        )


        if_request_taskinfo1uri_present_122=rail.IfOperator(
            task_id='if_request_taskinfo1uri_present_122',
            test='''{{ dag_run.conf.taskinfo1URI | is_truthy }}''',
            yes_task="update_dropdown_value_123",
            no_task="log_exception_125",
        )


        update_dropdown_value_123=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_123',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('log_newlycreated_task_uri_116') }}",
                "customFieldUri": "{{ dag_run.conf.TaskBusinessUnitURI }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.taskinfo1URI }}"
            }
        )


        log_exception_125=rail.PythonOperator(
            task_id='log_exception_125',
            python_callable= lambda:  rail.render_template("Task Business Unit not found - '{{ dag_run.conf.taskinfo1 }}';")
        )


        if_request_taskinfo2_present_126=rail.IfOperator(
            task_id='if_request_taskinfo2_present_126',
            test='''{{ dag_run.conf.taskinfo2 | is_truthy }}''',
            yes_task="update_text_value_127",
            no_task="if_request_taskinfo3_present_128",
        )


        update_text_value_127=rail.RepliconServiceOperator(
            task_id='update_text_value_127',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
            "objectUri": "{{ result('log_newlycreated_task_uri_116') }}",
            "customFieldUri": "{{ dag_run.conf.DealOpportunityIDTaskURI }}",
            "value": "{{ dag_run.conf.taskinfo2 }}"
            }
        )


        if_request_taskinfo3_present_128=rail.IfOperator(
            task_id='if_request_taskinfo3_present_128',
            test='''{{ dag_run.conf.taskinfo3 | is_truthy }}''',
            yes_task="if_request_taskinfo3uri_present_129",
            no_task="if_request_taskcode_present_133",
        )


        if_request_taskinfo3uri_present_129=rail.IfOperator(
            task_id='if_request_taskinfo3uri_present_129',
            test='''{{ dag_run.conf.taskinfo3URI | is_truthy }}''',
            yes_task="update_dropdown_value_130",
            no_task="log_exception_132",
        )


        update_dropdown_value_130=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_130',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
            "objectUri": "{{ result('log_newlycreated_task_uri_116') }}",
            "customFieldUri": "{{ dag_run.conf.TaskDepartmentURI }}",
            "customFieldDropDownOptionUri": "{{ dag_run.conf.taskinfo3URI }}"
            }
        )


        log_exception_132=rail.PythonOperator(
            task_id='log_exception_132',
            python_callable= lambda:  rail.render_template("Task Department not found - '{{ dag_run.conf.taskinfo3 }}';")
        )


        if_request_taskcode_present_133=rail.IfOperator(
            task_id='if_request_taskcode_present_133',
            test='''{{ dag_run.conf.taskcode | is_truthy }}''',
            yes_task="update_code_134",
            no_task="if_request_istimeentryallowed_present_135",
        )


        update_code_134=rail.RepliconServiceOperator(
            task_id='update_code_134',
            endpoint="/services/TaskService1.svc/UpdateCode",
            data={
            "taskUri": "{{ result('log_newlycreated_task_uri_116') }}",
            "code": "{{ dag_run.conf.taskcode }}"
            }
        )


        if_request_istimeentryallowed_present_135=rail.IfOperator(
            task_id='if_request_istimeentryallowed_present_135',
            test='''{{ dag_run.conf.istimeentryallowed | is_truthy }}''',
            yes_task="update_allow_time_entry_136",
            no_task="if_request_taskdescription_present_137",
        )


        update_allow_time_entry_136=rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_136',
            endpoint="/services/TaskService1.svc/UpdateAllowTimeEntry",
            data={
                "taskUri": "{{ result('log_newlycreated_task_uri_116') }}",
                "allowTimeEntry": "{{ result('log_timeentryallowed_4') }}"
            }
        )


        if_request_taskdescription_present_137=rail.IfOperator(
            task_id='if_request_taskdescription_present_137',
            test='''{{ dag_run.conf.taskdescription | is_truthy }}''',
            yes_task="update_description_138",
            no_task="if_request_timeentrystartdate_present_139",
        )


        update_description_138=rail.RepliconServiceOperator(
            task_id='update_description_138',
            endpoint="/services/TaskService1.svc/UpdateDescription",
            data={
                "taskUri": "{{ result('log_newlycreated_task_uri_116') }}",
                "description": "{{ result('log_taskdescription_3') }}"
            }
        )


        if_request_timeentrystartdate_present_139=rail.IfOperator(
            task_id='if_request_timeentrystartdate_present_139',
            test='''{{ dag_run.conf.timeentrystartdate | is_truthy  or dag_run.conf.timeentryenddate | is_truthy }}''',
            yes_task="update_time_entry_date_range_140",
            no_task="log_compilelogs_141",
        )


        update_time_entry_date_range_140=rail.RepliconServiceOperator(
            task_id='update_time_entry_date_range_140',
            endpoint="/services/TaskService1.svc/UpdateTimeEntryDateRange",
            data=lambda dag_run:{
            "taskUri": rail.result('log_newlycreated_task_uri_116'),
            "dateRange":{
                "startDate": rail.result('log_conditional_startdate_34') if dag_run.conf['timeentrystartdate']  else None,
                "endDate": rail.result('log_conditional_enddate_38') if dag_run.conf['timeentryenddate'] else None,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            }
            }
        )


        log_compilelogs_141=rail.PythonOperator(
            task_id='log_compilelogs_141',
            python_callable= lambda:  rail.render_template("{{ result('log_log_87') | default('', true) }}{{ result('log_log_93') | default('', true) }}{{ result('log_log_98') | default('', true) }}{{ result('log_log_103') | default('', true) }}{{ result('log_log_108') | default('', true) }}{{ result('log_log_111') | default('', true) }}")
        )


        log_compile_exceptions_142=rail.PythonOperator(
            task_id='log_compile_exceptions_142',
            python_callable= lambda:  rail.render_template("{{ result('log_exception_57') | default('', true) }}{{ result('log_exception_59') | default('', true) }}{{ result('log_exception_70') | default('', true) }}{{ result('log_exception_89') | default('', true) }}{{ result('log_exception_100') | default('', true) }}{{ result('log_exception_125') | default('', true) }}{{ result('log_exception_132') | default('', true) }}")
        )


        audaxgroup_project_task_import_logs_add_entry_143=rail.WriteLogOperator(
            task_id='audaxgroup_project_task_import_logs_add_entry_143',
            log="{{ dag_run.conf.audax_project_task_import_logs }}",
            message="na",
            severity='''{{"Exception" if result('log_compile_exceptions_142') | is_truthy else "Success"}}''',
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "{{ dag_run.conf.tasklevel1 | default('', true) }}|{{ dag_run.conf.tasklevel2 | default('', true) }}|{{ dag_run.conf.tasklevel3 | default('', true) }}",
                "taskcode": "{{ dag_run.conf.taskcode }}",
                "status":'''{{"Exception" if result('log_compile_exceptions_142') | is_truthy else "Success"}}''',
                "details": "{{ result('log_task_level_79') | default('', true) }}{{ result('log_task_level_118') | default('', true) }} {{ result('log_compile_exceptions_142') | default('', true) }}{{ result('log_compilelogs_141') | default('', true) }}{{ result('log_resourceassignmentlogs_43') | default('', true) }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )


        log_job_history_144=rail.PythonOperator(
            task_id='log_job_history_144',
            python_callable= lambda:  "Added" if rail.result('log_newlycreated_task_uri_116') else "Updated" if rail.result('log_existing_task_uri_77') else null
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
            "jobid": "{{dag_run.conf.parentjobid}}",
            "projectname": "{{ dag_run.conf.projectname }}",
            "taskname": "{{ dag_run.conf.tasklevel1 | default('', true) }}|{{ dag_run.conf.tasklevel2 | default('', true) }}|{{ dag_run.conf.tasklevel3 | default('', true) }}",
            "taskcode": "{{ dag_run.conf.taskcode }}",
            "status": "Error",
            "details": "Error processing Task - {{ get_error_message() | default('', true) }} {{ result('log_exception_57') | default('', true) }}{{ result('log_exception_59') | default('', true) }}{{ result('log_exception_70') | default('', true) }}{{ result('log_exception_89') | default('', true) }}{{ result('log_exception_100') | default('', true) }}{{ result('log_exception_125') | default('', true) }}{{ result('log_exception_132') | default('', true) }}{{ result('log_log_87') | default('', true) }}{{ result('log_log_93') | default('', true) }}{{ result('log_log_98') | default('', true) }}{{ result('log_log_103') | default('', true) }}{{ result('log_log_108') | default('', true) }}{{ result('log_log_111') | default('', true) }}{{ result('log_resourceassignmentlogs_43') | default('', true) }}",
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
            'No') >> log_taskcode_2 >> log_taskdescription_3 >> log_timeentryallowed_4 >> declare_list_5 >> declare_list_6 >> if_request_assignedusers_present_8
        if_request_assignedusers_present_8 >> rail.Label('Yes')  >> log_userslist_9 >> foreach_create_list_10_11 >> log_userlogin_12 >> log_lookupuseruri_13 >> if_log_lookupuseruri_13_present_14
        if_log_lookupuseruri_13_present_14 >> rail.Label('Yes') >> insert_to_list_15 >> foreach_create_list_10_11_end
        if_log_lookupuseruri_13_present_14 >> rail.Label('No')  >> audaxgroup_project_task_import_logs_add_entry_17 >> insert_to_list_18 >> foreach_create_list_10_11_end
        foreach_create_list_10_11 >> foreach_create_list_10_11_end >> if_request_assigneddepartments_present_19 >> rail.Label('Yes') >> log_departmentlist_20 >> create_list_21 \
        >> foreach_create_list_21_22 >> log_departmentname_23 >> log_lookup_departmenturi_24 >> if_log_lookup_departmenturi_24_present_25
        if_log_lookup_departmenturi_24_present_25 >> rail.Label('Yes') >> insert_to_list_26 >> foreach_create_list_21_22_end
        if_log_lookup_departmenturi_24_present_25 >> rail.Label('No') >> insert_to_list_28 >> foreach_create_list_21_22_end
        foreach_create_list_21_22 >> foreach_create_list_21_22_end >> log_fordebugging_29 >> log_resourcestobeassigned_30 >> if_request_timeentrystartdate_present_31
        if_request_timeentrystartdate_present_31 >> rail.Label('Yes') >> invoke_custom_ruby_code_32 >> log_formatstartdate_33 >> log_conditional_startdate_34 >> if_request_timeentryenddate_present_35
        if_request_timeentryenddate_present_35  >> rail.Label('Yes') >> invoke_custom_ruby_code_36 >> log_formatenddate_37 >> log_conditional_enddate_38 >> log_resourceassignmentlogs_43

        if_request_timeentryenddate_present_35   >> rail.Label('No') >> log_resourceassignmentlogs_43
        if_request_timeentrystartdate_present_31 >> rail.Label('No') >> if_request_timeentryenddate_present_35
        if_request_assigneddepartments_present_19 >> rail.Label('No')  >> log_fordebugging_29

        if_request_assignedusers_present_8 >> rail.Label('No')  >> if_request_assigneddepartments_present_19
        log_resourceassignmentlogs_43 >>if_request_tasklevel3_present_44
        if_request_tasklevel3_present_44 >> rail.Label('Yes') >> get_children_task_details_45 >> log_tasklevel1_uri_46 >> if_log_tasklevel1_uri_46_present_47
        if_log_tasklevel1_uri_46_present_47 >>  rail.Label('Yes') >> get_children_task_details_48 >> log_tasklevel2_uri_49 >> if_log_tasklevel2_uri_49_present_50
        if_log_tasklevel2_uri_49_present_50 >> rail.Label('Yes') >> get_children_task_details_51 >> log_tasklevel3_uri_52 >> if_log_tasklevel3_uri_52_blank_53
        if_log_tasklevel3_uri_52_blank_53 >> rail.Label('Yes') >> put_task_54 >> if_request_tasklevel2_present_60
        if_log_tasklevel3_uri_52_blank_53 >> rail.Label('No') >> log_exception_59 >> if_request_tasklevel2_present_60
        if_log_tasklevel2_uri_49_present_50 >> rail.Label('No') >> log_exception_57 >> if_request_tasklevel2_present_60
        if_log_tasklevel1_uri_46_present_47 >>  rail.Label('No') >> log_exception_59 >> if_request_tasklevel2_present_60
        if_request_tasklevel3_present_44 >> rail.Label('No') >> if_request_tasklevel2_present_60

        if_request_tasklevel2_present_60 >> rail.Label('Yes') >> get_children_task_details_61 >> log_tasklevel1_uri_62 >> if_log_tasklevel1_uri_62_present_63
        if_log_tasklevel1_uri_62_present_63 >> rail.Label('Yes') >> get_children_task_details_64 >> log_tasklevel2_uri_65 >> if_log_tasklevel2_uri_65_blank_66
        if_log_tasklevel2_uri_65_blank_66 >> rail.Label('Yes') >> put_task_67 >> if_request_tasklevel1_present_71
        if_log_tasklevel2_uri_65_blank_66 >> rail.Label('No') >> if_request_tasklevel1_present_71
        if_log_tasklevel1_uri_62_present_63 >> rail.Label('No') >> log_exception_70 >> if_request_tasklevel1_present_71
        if_request_tasklevel2_present_60 >> rail.Label('No') >> if_request_tasklevel1_present_71
        if_request_tasklevel1_present_71 >> rail.Label('Yes') >> get_children_task_details_72 >> log_tasklevel1_uri_73 >> if_log_tasklevel1_uri_73_blank_74
        if_log_tasklevel1_uri_73_blank_74 >> rail.Label('Yes') >> put_task_75 >> log_existing_task_uri_77
        if_log_tasklevel1_uri_73_blank_74 >> rail.Label('No') >> log_existing_task_uri_77
        if_request_tasklevel1_present_71 >> rail.Label('No') >> log_existing_task_uri_77
        log_existing_task_uri_77 >> if_log_existing_task_uri_77_present_78
        if_log_existing_task_uri_77_present_78 >> rail.Label('Yes') >> log_task_level_79 >> bulk_get_task_details_80 >> if_log_resourcestobeassigned_30_present_81
        if_log_resourcestobeassigned_30_present_81 >> rail.Label('Yes') >> bulk_update_resource_assignments_82 >> log_udf_existing_task_business_unit_dropdownvalue_83 >> if_request_taskinfo1_present_84
        if_request_taskinfo1_present_84 >> rail.Label('Yes') >> if_request_taskinfo1uri_present_85
        if_request_taskinfo1uri_present_85 >> rail.Label('Yes') >> update_dropdown_value_86 >> log_log_87 >> log_udf_existing_deal_opportunity_i_d_tasktextvalue_90
        if_request_taskinfo1uri_present_85 >> rail.Label('No') >> log_exception_89
        log_exception_89 >> log_udf_existing_deal_opportunity_i_d_tasktextvalue_90 >> if_request_taskinfo2_present_91
        if_request_taskinfo2_present_91 >> rail.Label('Yes') >> update_text_value_92 >> log_log_93 >> log_udf_existing_task_departmentdropdownvalue_94 >> if_request_taskinfo3_present_95
        if_request_taskinfo3_present_95 >> rail.Label('Yes') >> if_request_taskinfo3uri_present_96
        if_request_taskinfo3uri_present_96 >> rail.Label('Yes') >> update_dropdown_value_97 >> log_log_98 >> if_request_taskcode_present_101
        if_request_taskinfo3uri_present_96 >> rail.Label('No') >> log_exception_100 >> if_request_taskcode_present_101
        if_request_taskcode_present_101 >> rail.Label('Yes') >> update_code_102 >> log_log_103 >> log_is_time_entry_allowed_existing_value_104 >> if_request_istimeentryallowed_present_105
        if_request_istimeentryallowed_present_105 >> rail.Label('Yes') >> if_istimeentryallowed_check_106
        if_istimeentryallowed_check_106 >> rail.Label('Yes') >> update_allow_time_entry_107 >> log_log_108 >> if_request_taskdescription_present_109
        if_istimeentryallowed_check_106 >> rail.Label('No') >> if_request_taskdescription_present_109
        if_request_taskdescription_present_109 >> rail.Label('Yes') >> update_description_110 >> log_log_111 >> if_request_timeentrystartdate_present_112
        if_request_timeentrystartdate_present_112 >> rail.Label('Yes') >> log_existing_start_date_113 >> log_existing_end_date_114 >> update_time_entry_date_range_115 >> log_newlycreated_task_uri_116
        if_request_timeentrystartdate_present_112 >> rail.Label('No') >> log_newlycreated_task_uri_116
        if_request_taskdescription_present_109 >> rail.Label('No') >> if_request_timeentrystartdate_present_112
        if_request_istimeentryallowed_present_105 >> rail.Label('No') >> if_request_taskdescription_present_109
        if_request_taskcode_present_101 >> rail.Label('No') >> log_is_time_entry_allowed_existing_value_104
        if_request_taskinfo3_present_95 >> rail.Label('No') >> if_request_taskcode_present_101
        if_request_taskinfo2_present_91 >> rail.Label('No') >> log_udf_existing_task_departmentdropdownvalue_94
        if_request_taskinfo1_present_84 >> rail.Label('No') >> log_udf_existing_deal_opportunity_i_d_tasktextvalue_90
        if_log_resourcestobeassigned_30_present_81 >> rail.Label('No') >> log_udf_existing_task_business_unit_dropdownvalue_83
        if_log_existing_task_uri_77_present_78 >> rail.Label('No') >> log_newlycreated_task_uri_116
        log_newlycreated_task_uri_116 >> if_log_newlycreated_task_uri_116_present_117
        if_log_newlycreated_task_uri_116_present_117 >> rail.Label('Yes') >> log_task_level_118 >> if_log_resourcestobeassigned_30_present_119
        if_log_resourcestobeassigned_30_present_119 >> rail.Label('Yes') >> bulk_update_resource_assignments_120 >>  if_request_taskinfo1_present_121
        if_request_taskinfo1_present_121 >> rail.Label('Yes') >> if_request_taskinfo1uri_present_122
        if_request_taskinfo1uri_present_122 >> rail.Label('Yes') >> update_dropdown_value_123 >> if_request_taskinfo2_present_126
        if_request_taskinfo1uri_present_122 >> rail.Label('No') >> log_exception_125 >> if_request_taskinfo2_present_126
        if_request_taskinfo2_present_126 >> rail.Label('Yes') >> update_text_value_127 >> if_request_taskinfo3_present_128
        if_request_taskinfo3_present_128 >> rail.Label('Yes') >> if_request_taskinfo3uri_present_129
        if_request_taskinfo3uri_present_129 >> rail.Label('Yes') >> update_dropdown_value_130 >> if_request_taskcode_present_133
        if_request_taskcode_present_133  >> rail.Label('Yes') >> update_code_134 >> if_request_istimeentryallowed_present_135
        if_request_istimeentryallowed_present_135 >> rail.Label('Yes') >> update_allow_time_entry_136 >> if_request_taskdescription_present_137
        if_request_taskdescription_present_137 >> rail.Label('Yes') >> update_description_138 >> if_request_timeentrystartdate_present_139
        if_request_timeentrystartdate_present_139 >> rail.Label('Yes') >> update_time_entry_date_range_140 >> log_compilelogs_141
        if_request_timeentrystartdate_present_139 >> rail.Label('No') >> log_compilelogs_141
        if_request_taskdescription_present_137 >> rail.Label('No') >> if_request_timeentrystartdate_present_139
        if_request_istimeentryallowed_present_135 >> rail.Label('No') >> if_request_taskdescription_present_137
        if_request_taskcode_present_133  >> rail.Label('No') >> if_request_istimeentryallowed_present_135
        if_request_taskinfo3uri_present_129 >> rail.Label('No') >> log_exception_132 >> if_request_taskcode_present_133
        if_request_taskinfo3_present_128 >> rail.Label('No') >> if_request_taskcode_present_133
        if_request_taskinfo2_present_126 >> rail.Label('No') >> if_request_taskinfo3_present_128
        if_request_taskinfo1_present_121 >> rail.Label('No') >> if_request_taskinfo2_present_126
        if_log_resourcestobeassigned_30_present_119 >> rail.Label('No') >> if_request_taskinfo1_present_121
        if_log_newlycreated_task_uri_116_present_117 >> rail.Label('No') >> log_compilelogs_141
        log_compilelogs_141 >> log_compile_exceptions_142 >> audaxgroup_project_task_import_logs_add_entry_143 >> log_job_history_144 >>finish
        finish >> catch_and_log_errors >> log_to_sumo
    return dag

rail.for_each_instance(create_dag)
