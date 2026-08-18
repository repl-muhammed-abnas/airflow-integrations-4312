from datetime import timedelta
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'broadridge_project_import_task_child_{config.instance}',
        description=f'Broadridge_project_import_task_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_childtask_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_childtask_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_childtask_3 = rail.PythonOperator(
            task_id='log_childtask_3',
            python_callable=lambda dag_run: dag_run.conf['task_items']['taskname'].split(
                '|')[-1].replace("\\'", "\\")
        )

        log_childtaskreplacewith_4 = rail.PythonOperator(
            task_id='log_childtaskreplacewith_4',
            python_callable=lambda:  rail.result(
                'log_childtask_3').replace("\\", "\\\\").replace('"', '\\"').strip()
        )

        get_task_customfield_groupuri_7 = rail.RepliconServiceOperator(
            task_id='get_task_customfield_groupuri_7',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:task"
            }
        )

        get_all_custom_fields_8 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_8',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('get_task_customfield_groupuri_7').uri }}"
            }
        )

        log_task_outlineleveluri_9 = rail.PythonOperator(
            task_id='log_task_outlineleveluri_9',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_fields_8'), 'displayText', 'TaskOutlinelevel', 'uri', null) if rail.result('get_all_custom_fields_8') else null
        )

        log_task_outline_numberuri_10 = rail.PythonOperator(
            task_id='log_task_outline_numberuri_10',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_fields_8'), 'displayText', 'TaskOutlineNumber', 'uri', null) if rail.result('get_all_custom_fields_8') else null
        )

        log_metis_task_u_i_duri_11 = rail.PythonOperator(
            task_id='log_metis_task_u_i_duri_11',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_fields_8'), 'displayText', 'Metis_TaskUID', 'uri', null) if rail.result('get_all_custom_fields_8') else null
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        generate_report = rail.run_report2(
            group_id="run_report_data",
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'ProjectFilter', 'uri', null),
                                "value": dag_run.conf['projectid'],
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "persistedReportName": null
                    }
                ]
            }
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document="{{result('run_report_data.get_report_result').reportGenerationResults[0].payload }}",
            headers=[
                'Project Name', 'Project Code', 'Project Start Date', 'Project End Date', 'Project Manager', 'Client Code', 'Task Name (Full Path)', 'Task Code', 'Taskuri', 'Task Start Date', 'Task End Date', 'Project uri', 'Client uri']
        )

        if_request_taskmetisuid_present_14 = rail.IfOperator(
            task_id='if_request_taskmetisuid_present_14',
            test='''{{ dag_run.conf.task_items.metistaskuid | is_truthy }}''',
            yes_task="searchtaskbasedoncode_15",
            no_task="on_error1",
        )

        searchtaskbasedoncode_15 = rail.RepliconServiceOperator(
            task_id='searchtaskbasedoncode_15',
            endpoint="/services/TaskListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:task-list-column:task",
                    "urn:replicon:task-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:task-list-filter:text"
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
                            "text": "{{ dag_run.conf.task_items.metistaskuid }}",
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
            }

        )

        foreach_create_list_do = rail.ForEachOperator(
            task_id='foreach_create_list_do',
            items=lambda: rail.result('searchtaskbasedoncode_15')['rows'] if rail.result(
                'searchtaskbasedoncode_15') else [],
            start_task='accumulate_list_items',
            end_task='foreach_create_list_do_end'
        )

        accumulate_list_items = rail.SetVariableOperator(
            task_id='accumulate_list_items',
            name='task details',
            append=True,
            value=lambda: {
                "taskname": rail.find_first_by_attr_and_get_attr(rail.result('foreach_create_list_do')['cells'], 'objectType',
                                                                 'urn:replicon:object-type:task', 'textValue', null),
                "taskuri": rail.find_first_by_attr_and_get_attr(rail.result('foreach_create_list_do')['cells'], 'objectType',
                                                                'urn:replicon:object-type:task', 'uri', null),
                "code": rail.find_first_by_attr_and_get_attr(rail.result('foreach_create_list_do')['cells'], 'objectType',
                                                             'urn:replicon:list-type:string', 'textValue', null),
            }
        )

        foreach_create_list_do_end = rail.EmptyOperator(
            task_id='foreach_create_list_do_end',
        )

        load_csv_data = rail.PythonOperator(
            task_id='load_csv_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('load_report_data'))
        )

        log_ifthetaskexistsbasedoncode_18 = rail.PythonOperator(
            task_id='log_ifthetaskexistsbasedoncode_18',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                'accumulate_list_items')['value'], 'code', dag_run.conf['task_items']['metistaskuid'], 'taskuri', null) if rail.result('accumulate_list_items') and rail.result('accumulate_list_items')['value'] and rail.result('accumulate_list_items')['value'][0]['taskname'] else None,
        )

        if_request_action_equals_to_add_19 = rail.IfOperator(
            task_id='if_request_action_equals_to_add_19',
            test='''{{ dag_run.conf.action == 'add' }}''',
            yes_task="if_log_ifthetaskexistsbasedoncode_18_blank_20",
            no_task="if_request_action_equals_to_update_50",
        )

        if_log_ifthetaskexistsbasedoncode_18_blank_20 = rail.IfOperator(
            task_id='if_log_ifthetaskexistsbasedoncode_18_blank_20',
            test='''{{ result('log_ifthetaskexistsbasedoncode_18') | is_falsy }}''',
            yes_task="log_lengthoftasknamereceived_21",
            no_task="if_request_action_equals_to_update_50",
        )

        log_lengthoftasknamereceived_21 = rail.PythonOperator(
            task_id='log_lengthoftasknamereceived_21',
            python_callable=lambda dag_run: len(
                dag_run.conf['task_items']['taskname'].split("|"))
        )

        if_log_28_equals_to_1_22 = rail.IfOperator(
            task_id='if_log_28_equals_to_1_22',
            test='''{{ result('log_lengthoftasknamereceived_21') == 1 }}''',
            yes_task="log_parenturi_23",
            no_task="if_log_28_equals_to_2_24",
        )

        log_parenturi_23 = rail.PythonOperator(
            task_id='log_parenturi_23',
            python_callable=lambda dag_run: dag_run.conf['projecturi']
        )

        if_log_28_equals_to_2_24 = rail.IfOperator(
            task_id='if_log_28_equals_to_2_24',
            test='''{{ result('log_lengthoftasknamereceived_21') == 2 }}''',
            yes_task="log_parenttaskpath_25",
            no_task="if_log_28_greater_than_2_27",
        )

        log_parenttaskpath_25 = rail.PythonOperator(
            task_id='log_parenttaskpath_25',
            python_callable=lambda dag_run:  dag_run.conf['task_items']['taskname'].split("|")[
                0]
        )

        log_parenttaskuri_26 = rail.PythonOperator(
            task_id='log_parenttaskuri_26',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('load_csv_data'), 'Task Name (Full Path)', rail.result(
                'log_parenttaskpath_25'), 'Taskuri', null) if rail.result('load_csv_data')[0]['Taskuri'] else None
        )

        if_log_28_greater_than_2_27 = rail.IfOperator(
            task_id='if_log_28_greater_than_2_27',
            test='''{{ result('log_lengthoftasknamereceived_21') > 2 }}''',
            yes_task="log_parenttaskpath_28",
            no_task="log_finalparenturi_30",
        )

        log_parenttaskpath_28 = rail.PythonOperator(
            task_id='log_parenttaskpath_28',
            python_callable=lambda dag_run: rail.smartjoin_by_delim(dag_run.conf['task_items']['taskname'].split(
                "|")[:-1], "|").replace("|", " / ").replace("  ", " ").strip()
        )

        log_parenttaskuri_29 = rail.PythonOperator(
            task_id='log_parenttaskuri_29',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('load_csv_data'), 'Task Name (Full Path)', rail.result(
                'log_parenttaskpath_28'), 'Taskuri', null) if rail.result('load_csv_data')[0]['Taskuri'] else None
        )

        log_finalparenturi_30 = rail.PythonOperator(
            task_id='log_finalparenturi_30',
            python_callable=lambda: rail.result('log_parenturi_23') or rail.result(
                'log_parenttaskuri_26') or rail.result('log_parenttaskuri_29')
        )

        def get_name():
            if rail.result('log_childtask_3').endswith("No Bill"):
                result = "urn:replicon:time-and-expense-entry-type:non-billable"
            else:
                result = "urn:replicon:time-and-expense-entry-type:billable"
            return result

        log_billableoptionfortask_31 = rail.PythonOperator(
            task_id='log_billableoptionfortask_31',
            python_callable=get_name
        )

        if_log_29_present_38 = rail.IfOperator(
            task_id='if_log_29_present_38',
            test='''{{ result('log_parenturi_23') | is_truthy }}''',
            yes_task="log_finalvaluefor_parent_42",
            no_task="log_valueforparentiftheparentistask_41",
        )

        log_valueforparentiftheparentistask_41 = rail.PythonOperator(
            task_id='log_valueforparentiftheparentistask_41',
            python_callable=lambda: {
                "uri": rail.result('log_finalparenturi_30'),
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            }
        )

        log_finalvaluefor_parent_42 = rail.PythonOperator(
            task_id='log_finalvaluefor_parent_42',
            python_callable=lambda: rail.result('log_valueforparentiftheparentistask_41') if rail.result(
                'log_valueforparentiftheparentistask_41') else null
        )

        put_task_43 = rail.RepliconServiceOperator(
            task_id='put_task_43',
            endpoint="/services/ProjectService1.svc/PutTask",
            data=lambda dag_run: {
                "project": {
                    "uri": dag_run.conf['projecturi'],
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": rail.result('log_childtaskreplacewith_4'),
                        "parent": rail.result('log_finalvaluefor_parent_42'),
                        "parameterCorrelationId": null
                    },
                    "name": rail.result('log_childtaskreplacewith_4'),
                    "code": dag_run.conf['task_items']['metistaskuid'],
                    "description": null,
                    "timeEntryDateRange": {
                        "startDate": {
                            "year": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[2]),
                            "month": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[0]),
                            "day": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[1])
                        },
                        "endDate": {
                            "year": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[2]),
                            "month": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[0]),
                            "day": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[1])
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "true",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": rail.result('log_billableoptionfortask_31'),
                    "assignedResources": []
                }
            }
        )

        update_code_44 = rail.RepliconServiceOperator(
            task_id='update_code_44',
            endpoint="/services/TaskService1.svc/UpdateCode",
            data={
                "taskUri": "{{ result('put_task_43').uri }}",
                "code": "{{ dag_run.conf.task_items.metistaskuid }}"
            }
        )

        update_task_outline_level_45 = rail.RepliconServiceOperator(
            task_id='update_task_outline_level_45',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_task_43').uri }}",
                "customFieldUri": "{{ result('log_task_outlineleveluri_9') }}",
                "value": "{{ dag_run.conf.task_items.taskoutlinelevel }}"
            }
        )

        update_task_outline_number_46 = rail.RepliconServiceOperator(
            task_id='update_task_outline_number_46',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_task_43').uri }}",
                "customFieldUri": "{{ result('log_task_outline_numberuri_10') }}",
                "value": "{{ dag_run.conf.task_items.taskoutlinenumber }}"
            }
        )

        update_taskmetisuid_47 = rail.RepliconServiceOperator(
            task_id='update_taskmetisuid_47',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_task_43').uri }}",
                "customFieldUri": "{{ result('log_metis_task_u_i_duri_11') }}",
                "value": "{{ dag_run.conf.task_items.metistaskuid }}"
            }
        )

        log_task_uriforreference_48 = rail.PythonOperator(
            task_id='log_task_uriforreference_48',
            python_callable=lambda:  rail.result('put_task_43')['uri']
        )

        add_success_entries_for_task = rail.WriteLogOperator(
            task_id='add_success_entries_for_task',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['task_items']['projectname'],
                "status": "Success",
                "failure/reason": "Added",
                "taskname": dag_run.conf['task_items']['taskname'],
                "jobid": dag_run.conf['jobid']
            }
        )

        if_request_action_equals_to_update_50 = rail.IfOperator(
            task_id='if_request_action_equals_to_update_50',
            test='''{{ dag_run.conf.action == 'update' }}''',
            yes_task="if_log_ifthetaskexistsbasedoncode_18_present_51",
            no_task="if_request_taskteam_not_contains_null_92",
        )

        if_log_ifthetaskexistsbasedoncode_18_present_51 = rail.IfOperator(
            task_id='if_log_ifthetaskexistsbasedoncode_18_present_51',
            test='''{{ result('log_ifthetaskexistsbasedoncode_18') | is_truthy }}''',
            yes_task="update_name_58",
            no_task="if_log_ifthetaskexistsbasedoncode_18_blank_63",
        )

        update_name_58 = rail.RepliconServiceOperator(
            task_id='update_name_58',
            endpoint="/services/TaskService1.svc/UpdateName",
            data={
                "taskUri": "{{ result('log_ifthetaskexistsbasedoncode_18') }}",
                "name": "{{ result('log_childtaskreplacewith_4') }}"
            }
        )

        update_time_entry_date_range_59 = rail.RepliconServiceOperator(
            task_id='update_time_entry_date_range_59',
            endpoint="/services/TaskService1.svc/UpdateTimeEntryDateRange",
            data=lambda dag_run: {
                "taskUri": rail.result('log_ifthetaskexistsbasedoncode_18'),
                "dateRange": {
                    "startDate": {
                        "year": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[2]),
                        "month": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[0]),
                        "day": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[1])
                    },
                    "endDate": {
                        "year": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[2]),
                        "month": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[0]),
                        "day": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[1])
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }

        )

        update_taskmetisuid_60 = rail.RepliconServiceOperator(
            task_id='update_taskmetisuid_60',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('log_ifthetaskexistsbasedoncode_18') }}",
                "customFieldUri": "{{ result('log_metis_task_u_i_duri_11') }}",
                "value": "{{ dag_run.conf.task_items.metistaskuid }}"
            }
        )

        update_task_outline_level_61 = rail.RepliconServiceOperator(
            task_id='update_task_outline_level_61',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('log_ifthetaskexistsbasedoncode_18') }}",
                "customFieldUri": "{{ result('log_task_outlineleveluri_9') }}",
                "value": "{{ dag_run.conf.task_items.taskoutlinelevel }}"
            }
        )

        update_task_outline_level_62 = rail.RepliconServiceOperator(
            task_id='update_task_outline_level_62',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('log_ifthetaskexistsbasedoncode_18') }}",
                "customFieldUri": "{{ result('log_task_outline_numberuri_10') }}",
                "value": "{{ dag_run.conf.task_items.taskoutlinenumber }}"
            }
        )

        if_log_ifthetaskexistsbasedoncode_18_blank_63 = rail.IfOperator(
            task_id='if_log_ifthetaskexistsbasedoncode_18_blank_63',
            test='''{{ result('log_ifthetaskexistsbasedoncode_18') | is_falsy }}''',
            yes_task="log_tasknamelength_64",
            no_task="add_success_entries_for_task_updateaction",
        )

        log_tasknamelength_64 = rail.PythonOperator(
            task_id='log_tasknamelength_64',
            python_callable=lambda dag_run:  len(
                dag_run.conf['task_items']['taskname'].split("|"))
        )

        if_log_23_equals_to_1_65 = rail.IfOperator(
            task_id='if_log_23_equals_to_1_65',
            test='''{{ result('log_tasknamelength_64') == 1 }}''',
            yes_task="log_parenturi_66",
            no_task="if_log_23_equals_to_2_67",
        )

        log_parenturi_66 = rail.PythonOperator(
            task_id='log_parenturi_66',
            python_callable=lambda dag_run:  dag_run.conf['projecturi']
        )

        if_log_23_equals_to_2_67 = rail.IfOperator(
            task_id='if_log_23_equals_to_2_67',
            test='''{{ result('log_tasknamelength_64') == 2 }}''',
            yes_task="log_parenttaskpath_68",
            no_task="if_log_23_greater_than_2_70",
        )

        log_parenttaskpath_68 = rail.PythonOperator(
            task_id='log_parenttaskpath_68',
            python_callable=lambda dag_run: dag_run.conf['task_items']['taskname'].split("|")[
                0]
        )

        log_parenttaskuri_69 = rail.PythonOperator(
            task_id='log_parenttaskuri_69',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('load_csv_data'), 'Task Name (Full Path)', rail.result(
                'log_parenttaskpath_68'), 'Taskuri', '') if rail.result('load_csv_data')[0]['Taskuri'] else None
        )

        if_log_23_greater_than_2_70 = rail.IfOperator(
            task_id='if_log_23_greater_than_2_70',
            test='''{{ result('log_tasknamelength_64') > 2 }}''',
            yes_task="log_parenttaskpath_71",
            no_task="log_billingtypeurifinal_73",
        )

        log_parenttaskpath_71 = rail.PythonOperator(
            task_id='log_parenttaskpath_71',
            python_callable=lambda dag_run: rail.smartjoin_by_delim(dag_run.conf['task_items']['taskname'].split(
                "|")[:-1], "|").replace("|", " / ")
        )

        log_parenttaskuri_72 = rail.PythonOperator(
            task_id='log_parenttaskuri_72',
            python_callable=lambda: lambda: rail.find_first_by_attr_and_get_attr(rail.result('load_csv_data'), 'Task Name (Full Path)', rail.result(
                'log_parenttaskpath_71'), 'Taskuri', '') if rail.result('load_csv_data')[0]['Taskuri'] else None
        )

        def get_billinguri():
            str_data = rail.result('log_childtaskreplacewith_4')
            if str_data.endswith("No Bill"):
                result1 = "urn:replicon:time-and-expense-entry-type:non-billable"
            else:
                result1 = "urn:replicon:time-and-expense-entry-type:billable"
            return result1

        log_billingtypeurifinal_73 = rail.PythonOperator(
            task_id='log_billingtypeurifinal_73',
            python_callable=get_billinguri
        )

        log_parenttaskurifinal_74 = rail.PythonOperator(
            task_id='log_parenttaskurifinal_74',
            python_callable=lambda:  rail.result('log_parenturi_66') or rail.result(
                'log_parenttaskuri_69') or rail.result('log_parenttaskuri_72')
        )

        if_log_24_present_81 = rail.IfOperator(
            task_id='if_log_24_present_81',
            test='''{{ result('log_parenturi_66') | is_truthy }}''',
            yes_task="log_finalvaluefor_parent_85",
            no_task="log_valueforparentif_task_84",
        )

        log_valueforparentif_task_84 = rail.PythonOperator(
            task_id='log_valueforparentif_task_84',
            python_callable=lambda: {
                "uri": "{{ result('log_parenttaskurifinal_74') }}",
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            }
        )

        log_finalvaluefor_parent_85 = rail.PythonOperator(
            task_id='log_finalvaluefor_parent_85',
            python_callable=lambda: rail.result('log_valueforparentif_task_84') if rail.result(
                'log_valueforparentif_task_84') else null
        )

        put_task_86 = rail.RepliconServiceOperator(
            task_id='put_task_86',
            endpoint="/services/ProjectService1.svc/PutTask",
            data=lambda dag_run: {
                "project": {
                    "uri": dag_run.conf['projecturi'],
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": rail.result('log_childtaskreplacewith_4'),
                        "parent": rail.result('log_finalvaluefor_parent_85'),
                        "parameterCorrelationId": null
                    },
                    "name": rail.result('log_childtaskreplacewith_4'),
                    "code": dag_run.conf['task_items']['metistaskuid'],
                    "description": null,
                    "timeEntryDateRange": {
                        "startDate": {
                            "year": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[2]),
                            "month": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[0]),
                            "day": int(dag_run.conf['task_items']['startdate'].replace("-", "/").split("/")[1])
                        },
                        "endDate": {
                            "year": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[2]),
                            "month": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[0]),
                            "day": int(dag_run.conf['task_items']['enddate'].replace("-", "/").split("/")[1])
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "true",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": rail.result('log_billingtypeurifinal_73'),
                    "assignedResources": []
                }
            }

        )

        update_code_87 = rail.RepliconServiceOperator(
            task_id='update_code_87',
            endpoint="/services/TaskService1.svc/UpdateCode",
            data={
                "taskUri": "{{ result('put_task_86').uri }}",
                "code": "{{ dag_run.conf.task_items.metistaskuid }}"
            }
        )

        update_task_outline_level_88 = rail.RepliconServiceOperator(
            task_id='update_task_outline_level_88',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_task_86').uri }}",
                "customFieldUri": "{{ result('log_task_outlineleveluri_9') }}",
                "value": "{{ dag_run.conf.task_items.taskoutlinelevel }}"
            }
        )

        update_task_outline_level_89 = rail.RepliconServiceOperator(
            task_id='update_task_outline_level_89',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_task_86').uri }}",
                "customFieldUri": "{{ result('log_task_outline_numberuri_10') }}",
                "value": "{{ dag_run.conf.task_items.taskoutlinenumber }}"
            }
        )

        update_task_outline_level_90 = rail.RepliconServiceOperator(
            task_id='update_task_outline_level_90',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_task_86').uri }}",
                "customFieldUri": "{{ result('log_metis_task_u_i_duri_11') }}",
                "value": "{{ dag_run.conf.task_items.metistaskuid }}"
            }
        )

        add_success_entries_for_task_updateaction = rail.WriteLogOperator(
            task_id='add_success_entries_for_task_updateaction',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['task_items']['projectname'],
                "status": "Success" + "|" + dag_run.conf['jobid'],
                "failure/reason": "Added",
                "taskname": dag_run.conf['task_items']['taskname'],
                "jobid": dag_run.conf['jobid']
            }
        )

        if_request_taskteam_not_contains_null_92 = rail.IfOperator(
            task_id='if_request_taskteam_not_contains_null_92',
            test="{{ dag_run.conf.task_items.taskteam | matches('NULL') | is_falsy }}",
            yes_task="log_final_taskuriforteamassignment_93",
            no_task="on_error1",
        )

        log_final_taskuriforteamassignment_93 = rail.PythonOperator(
            task_id='log_final_taskuriforteamassignment_93',
            python_callable=lambda: rail.result('log_ifthetaskexistsbasedoncode_18') if rail.result('log_ifthetaskexistsbasedoncode_18') else (rail.result(
                'put_task_43')['uri'] if rail.result('put_task_43') and rail.result(
                'put_task_43')['uri'] else (rail.result('put_task_86')['uri'] if rail.result('put_task_86') and rail.result('put_task_86')['uri'] else null))
        )

        log_taskteammodified_94 = rail.PythonOperator(
            task_id='log_taskteammodified_94',
            python_callable=lambda dag_run: rail.smartjoin_by_delim(
                dag_run.conf['task_items']['taskteam'].split("|"), ";") if dag_run.conf['task_items']['taskteam'] else null
        )

        log_taskteamsplitby_95 = rail.PythonOperator(
            task_id='log_taskteamsplitby_95',
            python_callable=lambda: rail.result(
                'log_taskteammodified_94').split(";")
        )

        foreach_create_list = rail.ForEachOperator(
            task_id='foreach_create_list',
            items=lambda: rail.result('log_taskteamsplitby_95'),
            start_task='log_individualresourcename_98',
            end_task='foreach_create_list_end'
        )

        log_individualresourcename_98 = rail.PythonOperator(
            task_id='log_individualresourcename_98',
            python_callable=lambda:  rail.result('foreach_create_list')
        )

        get_teammembers_uri_99 = rail.RepliconServiceOperator(
            task_id='get_teammembers_uri_99',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id"
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
                            "text": "{{ result('log_individualresourcename_98') }}",
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
            }

        )

        accumulate_list_to_items = rail.SetVariableOperator(
            task_id='accumulate_list_to_items',
            name='teammember',
            append=True,
            value=lambda: {
                "teammemberid": rail.result('foreach_create_list'),
                "teammemberuri": rail.result('get_teammembers_uri_99')['rows'][0]['cells'][0]['uri'] if rail.result('get_teammembers_uri_99') and rail.result('get_teammembers_uri_99')['rows'] and rail.result('get_teammembers_uri_99')['rows'][0] and rail.result('get_teammembers_uri_99')['rows'][0]['cells'] and rail.result('get_teammembers_uri_99')['rows'][0]['cells'][0]['uri'] else null
            }
        )

        foreach_create_list_end = rail.EmptyOperator(
            task_id='foreach_create_list_end',
        )

        def get_uri():
            data = rail.result('accumulate_list_to_items')[
                'value'] if rail.result('accumulate_list_to_items') and rail.result('accumulate_list_to_items')[
                'value'] else null
            uris = []
            uris = [value['teammemberuri']
                    for value in data if 'teammemberuri' in value and value['teammemberuri'] is not None]
            return uris

        log_finalteammembersuri_101 = rail.PythonOperator(
            task_id='log_finalteammembersuri_101',
            python_callable=get_uri
        )

        if_log_45_contains_urn_102 = rail.IfOperator(
            task_id='if_log_45_contains_urn_102',
            test='''{{ result('log_finalteammembersuri_101') | is_truthy }}''',
            yes_task="bulk_update_resource_assignments_103",
            no_task="on_error1",
        )

        bulk_update_resource_assignments_103 = rail.RepliconServiceOperator(
            task_id='bulk_update_resource_assignments_103',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda: {
                "taskUri": rail.result('log_final_taskuriforteamassignment_93'),
                "resourceUris": rail.result('log_finalteammembersuri_101'),
                "isAssigned": "true"
            }
        )

        on_error1 = rail.EmptyOperator(
            task_id='on_error1',
            trigger_rule="one_failed"

        )

        catch_and_log = rail.WriteLogOperator(
            task_id='catch_and_log',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['task_items']['projectname'],
                "status": "Failed" + "|" + dag_run.conf['jobid'],
                "failure/reason": rail.render_template("{{get_error_message()}}"),
                "taskname": dag_run.conf['task_items']['taskname'],
                "jobid": dag_run.conf['jobid']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_childtask_3
        log_childtask_3 >> log_childtaskreplacewith_4 >> get_task_customfield_groupuri_7
        get_task_customfield_groupuri_7 >> get_all_custom_fields_8 >> log_task_outlineleveluri_9 >> log_task_outline_numberuri_10
        log_task_outline_numberuri_10 >> log_metis_task_u_i_duri_11 >> get_report_details >> generate_report
        generate_report >> load_report_data >> if_request_taskmetisuid_present_14
        if_request_taskmetisuid_present_14 >> rail.Label(
            'Yes') >> searchtaskbasedoncode_15 >> foreach_create_list_do
        if_request_taskmetisuid_present_14 >> rail.Label(
            'No') >> on_error1
        foreach_create_list_do >> accumulate_list_items >> foreach_create_list_do_end
        foreach_create_list_do >> foreach_create_list_do_end >> load_csv_data >> log_ifthetaskexistsbasedoncode_18
        log_ifthetaskexistsbasedoncode_18 >> if_request_action_equals_to_add_19
        foreach_create_list_do >> foreach_create_list_do_end
        if_request_action_equals_to_add_19 >> rail.Label(
            'Yes') >> if_log_ifthetaskexistsbasedoncode_18_blank_20
        if_log_ifthetaskexistsbasedoncode_18_blank_20 >> rail.Label(
            'Yes') >> log_lengthoftasknamereceived_21
        log_lengthoftasknamereceived_21 >> if_log_28_equals_to_1_22
        if_log_28_equals_to_1_22 >> rail.Label(
            'Yes') >> log_parenturi_23 >> if_log_28_equals_to_2_24
        if_log_28_equals_to_1_22 >> rail.Label(
            'No') >> if_log_28_equals_to_2_24
        if_log_28_equals_to_2_24 >> rail.Label(
            'Yes') >> log_parenttaskpath_25 >> log_parenttaskuri_26
        log_parenttaskuri_26 >> if_log_28_greater_than_2_27
        if_log_28_equals_to_2_24 >> rail.Label(
            'No') >> if_log_28_greater_than_2_27
        if_log_28_greater_than_2_27 >> rail.Label(
            'Yes') >> log_parenttaskpath_28 >> log_parenttaskuri_29 >> log_finalparenturi_30
        if_log_28_greater_than_2_27 >> rail.Label(
            'No') >> log_finalparenturi_30 >> log_billableoptionfortask_31
        log_billableoptionfortask_31 >> if_log_29_present_38
        if_log_29_present_38 >> rail.Label(
            'Yes') >> log_finalvaluefor_parent_42
        if_log_29_present_38 >> rail.Label(
            'No') >> log_valueforparentiftheparentistask_41 >> log_finalvaluefor_parent_42 >> put_task_43
        put_task_43 >> update_code_44 >> update_task_outline_level_45 >> update_task_outline_number_46
        update_task_outline_number_46 >> update_taskmetisuid_47
        update_taskmetisuid_47 >> log_task_uriforreference_48 >> add_success_entries_for_task
        add_success_entries_for_task >> if_request_action_equals_to_update_50
        if_log_ifthetaskexistsbasedoncode_18_blank_20 >> rail.Label(
            'No') >> if_request_action_equals_to_update_50
        if_request_action_equals_to_add_19 >> rail.Label(
            'No') >> if_request_action_equals_to_update_50
        if_request_action_equals_to_update_50 >> rail.Label(
            'Yes') >> if_log_ifthetaskexistsbasedoncode_18_present_51

        if_log_ifthetaskexistsbasedoncode_18_present_51 >> rail.Label(
            'Yes') >> update_name_58
        update_name_58 >> update_time_entry_date_range_59 >> update_taskmetisuid_60 >> update_task_outline_level_61
        update_task_outline_level_61 >> update_task_outline_level_62 >> if_log_ifthetaskexistsbasedoncode_18_blank_63

        if_log_ifthetaskexistsbasedoncode_18_present_51 >> rail.Label(
            'No') >> if_log_ifthetaskexistsbasedoncode_18_blank_63
        if_request_action_equals_to_update_50 >> rail.Label(
            'No') >> if_request_taskteam_not_contains_null_92

        if_log_ifthetaskexistsbasedoncode_18_blank_63 >> rail.Label(
            'Yes') >> log_tasknamelength_64 >> if_log_23_equals_to_1_65
        if_log_23_equals_to_1_65 >> rail.Label(
            'Yes') >> log_parenturi_66 >> if_log_23_equals_to_2_67
        if_log_23_equals_to_1_65 >> rail.Label(
            'No') >> if_log_23_equals_to_2_67
        if_log_23_equals_to_2_67 >> rail.Label(
            'Yes') >> log_parenttaskpath_68 >> log_parenttaskuri_69
        log_parenttaskuri_69 >> if_log_23_greater_than_2_70
        if_log_23_equals_to_2_67 >> rail.Label(
            'No') >> if_log_23_greater_than_2_70
        if_log_23_greater_than_2_70 >> rail.Label(
            'Yes') >> log_parenttaskpath_71
        log_parenttaskpath_71 >> log_parenttaskuri_72 >> log_billingtypeurifinal_73
        if_log_23_greater_than_2_70 >> rail.Label(
            'No') >> log_billingtypeurifinal_73
        log_billingtypeurifinal_73 >> log_parenttaskurifinal_74 >> if_log_24_present_81

        if_log_24_present_81 >> rail.Label(
            'Yes') >> log_finalvaluefor_parent_85
        if_log_24_present_81 >> rail.Label(
            'No') >> log_valueforparentif_task_84 >> log_finalvaluefor_parent_85
        log_finalvaluefor_parent_85 >> put_task_86 >> update_code_87 >> update_task_outline_level_88
        update_task_outline_level_88 >> update_task_outline_level_89 >> update_task_outline_level_90
        update_task_outline_level_90 >> add_success_entries_for_task_updateaction

        if_log_ifthetaskexistsbasedoncode_18_blank_63 >> rail.Label(
            'No') >> add_success_entries_for_task_updateaction >> if_request_taskteam_not_contains_null_92

        if_request_taskteam_not_contains_null_92 >> rail.Label(
            'Yes') >> log_final_taskuriforteamassignment_93
        log_final_taskuriforteamassignment_93 >> log_taskteammodified_94 >> log_taskteamsplitby_95
        log_taskteamsplitby_95 >> foreach_create_list >> log_individualresourcename_98 >> get_teammembers_uri_99
        get_teammembers_uri_99 >> accumulate_list_to_items
        accumulate_list_to_items >> foreach_create_list_end
        foreach_create_list >> foreach_create_list_end >> log_finalteammembersuri_101
        log_finalteammembersuri_101 >> if_log_45_contains_urn_102
        if_log_45_contains_urn_102 >> rail.Label(
            'Yes') >> bulk_update_resource_assignments_103
        bulk_update_resource_assignments_103 >> on_error1 >> catch_and_log
        if_log_45_contains_urn_102 >> rail.Label(
            'No') >> on_error1 >> catch_and_log >> log_to_sumo
        if_request_taskteam_not_contains_null_92 >> rail.Label(
            'No') >> on_error1 >> catch_and_log

    return dag


rail.for_each_instance(create_dag)
