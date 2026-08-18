
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail
from dxctechnology.india_earned_leave_export.daily_export_v1.dxc_payroll_extract_mapper_india_mapper import dxc_payroll_extract_mapper_india

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_daily_export_master_{config.instance}_v1',
        description=f'DXC_India_Payroll_Export_Master Daily - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_cut_off_date_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_cut_off_date_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_cut_off_date_3 = rail.PythonOperator(
            task_id='log_cut_off_date_3',
            python_callable=lambda: {"month": 1,
                                     "day": 4, "year": 2022}  # "01/04/2022"
        )

        get_all_scripts_5 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_5',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
        )

        dxc_payroll_extract_mapper_india_search_entries_6 = rail.PythonOperator(
            task_id='dxc_payroll_extract_mapper_india_search_entries_6',
            python_callable=lambda:  list(
                filter(lambda x: x["export"] == "Yes", dxc_payroll_extract_mapper_india))
        )

        if_pluckuri_smart_joinnil_present_7 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_present_7',
            test=lambda: bool(list(filter(lambda x: x, map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_5'), 'displayText', x['fileformat_name'], 'uri'), rail.result(
                'dxc_payroll_extract_mapper_india_search_entries_6'))))) and rail.result('dxc_payroll_extract_mapper_india_search_entries_6') and rail.result('dxc_payroll_extract_mapper_india_search_entries_6')[0]['type'] == 'Compass',
            yes_task="get_enabled_divisionscompanycodes_8",
            no_task="stop_25",
        )

        get_enabled_divisionscompanycodes_8 = rail.RepliconServiceOperator(
            task_id='get_enabled_divisionscompanycodes_8',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        invoke_custom_ruby_code_9 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_9',
            python_callable=lambda: list(map(lambda x: {
                "name": x['companycode'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisionscompanycodes_8'), 'displayText', x['companycode'], 'uri'),
                "script_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_5'), 'displayText', x['fileformat_name'], 'uri'),
                "type": x['type'],
            }, rail.result('dxc_payroll_extract_mapper_india_search_entries_6'))),
        )

        invoke_custom_ruby_code_10 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_10',
            python_callable=lambda: {
                "companycodejson": list(set(map(lambda x: x['uri'], rail.result('invoke_custom_ruby_code_9')))),
                "division": list(set(map(lambda x: x['name'], rail.result('invoke_custom_ruby_code_9')))),
                "divisionuri": list(set(map(lambda x: x['uri'], rail.result('invoke_custom_ruby_code_9')))),
                "startdate": rail.get_replicon_date(datetime.strptime(Variable.get(config.startdate_test_var_name, default_var=''), '%m/%d/%Y')) if Variable.get(config.startdate_test_var_name, default_var='') else rail.get_replicon_date(rail.result('log_cut_off_date_3') if (datetime.utcnow() - relativedelta(months=3)) - timedelta(days=(datetime.utcnow() - relativedelta(days=84)).weekday()) < datetime(**rail.result('log_cut_off_date_3')) else (datetime.utcnow() - relativedelta(days=84)) - timedelta(days=(datetime.utcnow() - relativedelta(days=84)).weekday())),
                "enddate": rail.get_replicon_date(datetime.strptime(Variable.get(config.enddate_test_var_name, default_var=''), '%m/%d/%Y')) if Variable.get(config.enddate_test_var_name, default_var='') else rail.get_replicon_date(datetime.utcnow()),
                "companycode": list(set(map(lambda x: x['name'], rail.result('invoke_custom_ruby_code_9')))),
            }
        )

        get_allterminateduserswithinthegivendaterange_11 = rail.RepliconServiceOperator(
            task_id='get_allterminateduserswithinthegivendaterange_11',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:division",
                    "urn:replicon:user-list-column:end-date",
                    "urn:replicon:user-list-column:employee-id"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:end-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
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
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": {
                                "startDate": {
                                    "year": "{{result('invoke_custom_ruby_code_10').startdate.year }}",
                                    "month": "{{result('invoke_custom_ruby_code_10').startdate.month}}",
                                    "day": "{{result('invoke_custom_ruby_code_10').startdate.day}}"
                                },
                                "endDate": {
                                    "year": "{{result('invoke_custom_ruby_code_10').enddate.year}}",
                                    "month": "{{result('invoke_custom_ruby_code_10').enddate.month}}",
                                    "day": "{{result('invoke_custom_ruby_code_10').enddate.day}}"
                                },
                                "relativeDateRangeUri": null,
                                "relativeDateRangeAsOfDate": null
                            },
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

        create_csv_lines_terminated_user_list_12 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_terminated_user_list_12',
            source="{{ result('get_allterminateduserswithinthegivendaterange_11').rows | to_json }}",
            header=['username',
                    'useruri',
                    'employeeid',
                    'companycode',
                    'enddate',
                    'userid',
                    'companycodeinlist'],
            row=lambda item: {
                "column_0": item['cells'][0].get('textValue'),
                "column_1": item['cells'][0].get('uri'),
                "column_2": item['cells'][3].get('textValue'),
                "column_3": item['cells'][1].get('textValue'),
                "column_4": item['cells'][2].get('textValue'),
                "column_5": item['cells'][0].get('uri', '').split(":")[-1],
                "column_6": "Yes" if rail.find_first_by_attr_and_get_attr(rail.result('dxc_payroll_extract_mapper_india_search_entries_6'), 'companycode', item['cells'][1].get('textValue')) else "No"
            }.values(),
        )

        load_csv_create_list_from_csv_13 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_13",
            document="{{ result('create_csv_lines_terminated_user_list_12') }}",
        )

        create_collection_create_list_from_csv_13 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_13',
            source="{{ result('load_csv_create_list_from_csv_13') }}",
            name="terminateduserslist",
            columns={
                'username': 'username',
                'useruri': 'useruri',
                'employeeid': 'employeeid',
                'companycode': 'companycode',
                'enddate': 'enddate',
                'userid': 'userid',
                'companycodeinlist': 'iscompanycodeallowed'
            }
        )

        query_list_checkalltheuserswithallowedcompanycode_14 = rail.QueryCollectionOperator(
            task_id='query_list_checkalltheuserswithallowedcompanycode_14',
            query="""SELECT * FROM  terminateduserslist WHERE  terminateduserslist.iscompanycodeallowed="Yes" """,
            name="validatedterminateduserslist",
        )

        if_query_list_checkalltheuserswithallowedcompanycode_14_row_count_greater_than_0_16 = rail.IfOperator(
            task_id='if_query_list_checkalltheuserswithallowedcompanycode_14_row_count_greater_than_0_16',
            test='''{{ result('query_list_checkalltheuserswithallowedcompanycode_14','length') > 0 }}''',
            yes_task="query_list_terminated_users_list_17",
            no_task="finish",
        )

        query_list_terminated_users_list_17 = rail.QueryCollectionOperator(
            task_id='query_list_terminated_users_list_17',
            query="""SELECT * FROM  validatedterminateduserslist""",
        )

        trigger_child_dag_run_18 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_dag_run_18',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_daily_export_child_{config.instance}_v1',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "fileformaturi": rail.result('invoke_custom_ruby_code_9')[0]['script_uri'],
                "startdate": rail.result('invoke_custom_ruby_code_10')['startdate'],
                "enddate": rail.result('invoke_custom_ruby_code_10')['enddate'],
                "division": rail.result('invoke_custom_ruby_code_10')['division'],
                "divisionuri": rail.result('invoke_custom_ruby_code_10')['divisionuri'],
                "timenow": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                "rundateinYYYYMMDDformat": datetime.utcnow().strftime("%Y%m%d"),
                "runtimeinHHMMSSformat": datetime.utcnow().strftime("%H%M%S"),
                "today": rail.get_replicon_date(datetime.utcnow()),
                "useruri": list(set(map(lambda x: x['useruri'], rail.load_all_records(rail.result('query_list_terminated_users_list_17'))))),
                "userids": list(set(map(lambda x: x['userid'], rail.load_all_records(rail.result('query_list_terminated_users_list_17'))))),
            }
        )

        wait_for_completion_trigger_child_dag_run_18 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_child_dag_run_18',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_child_dag_run_18") }}'
        )

        stop_25 = rail.FailOperator(
            task_id='stop_25',
            message='''Required file format "{{ result('dxc_payroll_extract_mapper_india_search_entries_6')['fileformat_name'] }}" not available in Replicon'''
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_cut_off_date_3
        log_cut_off_date_3 >> get_all_scripts_5 >> dxc_payroll_extract_mapper_india_search_entries_6 >> if_pluckuri_smart_joinnil_present_7
        if_pluckuri_smart_joinnil_present_7 >> rail.Label(
            'Yes') >> get_enabled_divisionscompanycodes_8 >> invoke_custom_ruby_code_9 >> invoke_custom_ruby_code_10 >> get_allterminateduserswithinthegivendaterange_11 >> create_csv_lines_terminated_user_list_12 >> load_csv_create_list_from_csv_13 >> create_collection_create_list_from_csv_13 >> query_list_checkalltheuserswithallowedcompanycode_14 >> if_query_list_checkalltheuserswithallowedcompanycode_14_row_count_greater_than_0_16
        if_query_list_checkalltheuserswithallowedcompanycode_14_row_count_greater_than_0_16 >> rail.Label(
            'Yes') >> query_list_terminated_users_list_17 >> trigger_child_dag_run_18 >> wait_for_completion_trigger_child_dag_run_18 >> finish
        if_query_list_checkalltheuserswithallowedcompanycode_14_row_count_greater_than_0_16 >> rail.Label(
            'No') >> finish >> log_to_sumo
        if_pluckuri_smart_joinnil_present_7 >> rail.Label(
            'Yes') >> stop_25 >> finish

    return dag


rail.for_each_instance(create_dag)
